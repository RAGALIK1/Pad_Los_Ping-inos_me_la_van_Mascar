#!/usr/bin/env python3
"""
Agent de mesagerie (message broker) — Partea 1, socket-uri TCP.

Arhitectură (vezi docs/ARHITECTURA.md):
  * un fir de execuție per conexiune (thread-per-client) citește cadrele;
    citirea nu este blocată niciodată de livrare;
  * N work-job-uri („dispatcher-e”) golesc concurent cozile abonaților;
  * un cron-job la 1s retrimite mesajele fără ACK și trezește cozile rămase;
  * un job de persistență salvează starea în XML sau JSON (opțional).

Rulare:  python3 broker.py --port 5000 --storage persistent --storage-format xml
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import queue
import re
import signal
import socket
import threading
import time
import uuid

from adapters import AdapterFactory, FormatError, MessageAdapter
from framing import FrameError, recv_frame, send_frame
from storage import Envelope, MessageStore, PersistenceJob, now_iso

log = logging.getLogger("broker")

NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,64}$")
TOPIC_RE = re.compile(r"^[A-Za-z0-9_\-]+(\.[A-Za-z0-9_\-]+)*$")
PATTERN_RE = re.compile(r"^([A-Za-z0-9_\-]+|\*|#)(\.([A-Za-z0-9_\-]+|\*|#))*$")
MSG_TYPES = {"connect", "subscribe", "unsubscribe", "publish", "ack", "stats", "ping"}


class ValidationError(Exception):
    pass


def require(msg: dict, field: str, regex: re.Pattern | None = None, max_len: int = 256) -> str:
    val = msg.get(field)
    if val is None or val == "":
        raise ValidationError(f"lipsește câmpul obligatoriu '{field}'")
    if len(val) > max_len:
        raise ValidationError(f"câmpul '{field}' depășește {max_len} caractere")
    if regex and not regex.match(val):
        raise ValidationError(f"valoare invalidă pentru '{field}': {val!r}")
    return val


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class ClientConnection:
    def __init__(self, sock: socket.socket, addr):
        self.sock = sock
        self.addr = f"{addr[0]}:{addr[1]}"
        self.name: str | None = None
        self.adapter: MessageAdapter = AdapterFactory.get("json")
        self._wlock = threading.Lock()   # un singur scriitor pe socket la un moment dat
        self.alive = True

    def send(self, msg: dict, adapter: MessageAdapter | None = None) -> None:
        data = (adapter or self.adapter).serialize(msg)
        with self._wlock:
            send_frame(self.sock, data)

    def close(self) -> None:
        self.alive = False
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()

    @property
    def label(self) -> str:
        return self.name or self.addr


class Broker:
    def __init__(self, args):
        self.args = args
        self.store = MessageStore()
        self.work: queue.Queue[str] = queue.Queue()   # coadă thread-safe pentru work-job-uri
        self.running = threading.Event()
        self.persistence = None
        if args.storage == "persistent":
            self.persistence = PersistenceJob(self.store, args.data_dir, args.storage_format)

    # ================================================================ pornire
    def serve(self) -> None:
        if self.persistence:
            n = self.persistence.load()
            if n:
                self.store.event("storage", f"stare restaurată din {self.persistence.path} ({n} înregistrări)")
            self.persistence.start()

        self.running.set()
        for i in range(self.args.workers):
            threading.Thread(target=self._worker, name=f"dispatcher-{i}", daemon=True).start()
        threading.Thread(target=self._cron, name="cron-redelivery", daemon=True).start()

        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.args.host, self.args.port))
        srv.listen(128)
        srv.settimeout(0.5)
        self.store.event("start", f"broker TCP pe {self.args.host}:{self.args.port}, "
                                  f"{self.args.workers} dispatcher-e, stocare {self.describe_storage()}")
        try:
            while self.running.is_set():
                try:
                    sock, addr = srv.accept()
                except socket.timeout:
                    continue
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                conn = ClientConnection(sock, addr)
                threading.Thread(target=self._handle_client, args=(conn,),
                                 name=f"conn-{conn.addr}", daemon=True).start()
        finally:
            srv.close()
            if self.persistence:
                self.persistence.stop()
            log.info("broker oprit")

    def describe_storage(self) -> str:
        if self.args.storage == "persistent":
            return f"persistentă ({self.args.storage_format.upper()})"
        return "tranzientă (memorie)"

    def stop(self, *_):
        self.running.clear()

    # ============================================= un fir per client (citire)
    def _handle_client(self, conn: ClientConnection) -> None:
        log.debug("conexiune nouă %s", conn.addr)
        try:
            while conn.alive and self.running.is_set():
                try:
                    raw = recv_frame(conn.sock)
                except FrameError as e:
                    # fluxul nu mai poate fi resincronizat: închidem DOAR această conexiune
                    self.store.record_invalid(conn.label, b"", f"cadru invalid: {e}")
                    self._safe_send(conn, {"type": "error", "error": f"cadru invalid: {e}"})
                    break
                except OSError:
                    break
                if raw is None:
                    break
                self._process(conn, raw)
        finally:
            if conn.name:
                self.store.detach(conn.name, conn)
                self.store.event("disconnect", f"{conn.name} s-a deconectat")
            conn.close()

    def _process(self, conn: ClientConnection, raw: bytes) -> None:
        adapter, msg = None, {}
        try:
            adapter = AdapterFactory.detect(raw)
            msg = adapter.deserialize(raw)
            self._dispatch(conn, msg, adapter)
        except (FormatError, ValidationError) as e:
            # Politica pentru mesaje invalide: răspuns „error”, mesaj în dead-letter,
            # conexiunea rămâne deschisă, brokerul continuă să funcționeze.
            self.store.record_invalid(conn.label, raw, str(e))
            self._safe_send(conn, {"type": "error", "error": str(e), "ref": msg.get("id")}, adapter)
        except Exception as e:  # plasă de siguranță: un bug nu doboară brokerul
            log.exception("eroare internă la procesarea mesajului de la %s", conn.label)
            self.store.record_invalid(conn.label, raw, f"eroare internă: {e}")
            self._safe_send(conn, {"type": "error", "error": "eroare internă a brokerului"}, adapter)

    def _safe_send(self, conn, msg, adapter=None):
        try:
            conn.send(msg, adapter)
        except (OSError, FrameError):
            conn.alive = False

    # ============================================================== comenzi
    def _dispatch(self, conn: ClientConnection, msg: dict, adapter: MessageAdapter) -> None:
        mtype = msg.get("type")
        if mtype not in MSG_TYPES:
            raise ValidationError(f"tip de mesaj necunoscut: {mtype!r}")
        if mtype == "ping":
            conn.send({"type": "pong", "ref": msg.get("id"), "timestamp": now_iso()}, adapter)
            return
        if mtype == "connect":
            return self._on_connect(conn, msg, adapter)
        if conn.name is None:
            raise ValidationError("trimite întâi un mesaj 'connect' cu câmpul 'sender'")
        getattr(self, f"_on_{mtype}")(conn, msg, adapter)

    def _on_connect(self, conn, msg, adapter):
        if conn.name is not None:
            raise ValidationError("conexiunea este deja identificată")
        name = require(msg, "sender", NAME_RE, 64)
        fmt = (msg.get("format") or adapter.name).lower()
        conn.adapter = AdapterFactory.get(fmt)   # formatul în care vrea să primească livrările
        role = msg.get("role") or "client"
        conn.name = name
        old, patterns, queued = self.store.attach(name, conn, conn.adapter.name, role, conn.addr)
        if old is not None and old is not conn:
            old.close()  # preluare de sesiune: clientul s-a reconectat
            self.store.event("takeover", f"{name}: sesiunea veche a fost înlocuită")
        conn.send({"type": "connack", "ref": msg.get("id"), "sender": "broker",
                   "payload": json.dumps({"subscriptions": patterns, "queued": queued,
                                          "delivery_format": conn.adapter.name})}, adapter)
        self.store.event("connect", f"{name} ({role}, {conn.addr}) livrare în {conn.adapter.name.upper()}"
                         + (f", {queued} mesaje în așteptare" if queued else ""))
        self.work.put(name)

    def _on_subscribe(self, conn, msg, adapter):
        pattern = require(msg, "topic", PATTERN_RE, 128)
        moved = self.store.subscribe(conn.name, pattern)
        conn.send({"type": "suback", "ref": msg.get("id"), "topic": pattern,
                   "payload": str(moved)}, adapter)
        self.store.event("subscribe", f"{conn.name} ← {pattern}" + (f" (+{moved} din backlog)" if moved else ""))
        self.work.put(conn.name)

    def _on_unsubscribe(self, conn, msg, adapter):
        pattern = require(msg, "topic", PATTERN_RE, 128)
        ok = self.store.unsubscribe(conn.name, pattern)
        conn.send({"type": "unsuback", "ref": msg.get("id"), "topic": pattern,
                   "payload": "ok" if ok else "not-subscribed"}, adapter)
        if ok:
            self.store.event("unsubscribe", f"{conn.name} ✕ {pattern}")

    def _on_publish(self, conn, msg, adapter):
        topic = require(msg, "topic", TOPIC_RE, 128)
        if "payload" not in msg:
            raise ValidationError("lipsește câmpul obligatoriu 'payload'")
        payload = msg["payload"]
        checksum = msg.get("checksum", "")
        if checksum and checksum.lower() != sha256(payload):
            raise ValidationError("checksum SHA-256 nu corespunde: mesajul a fost alterat")
        env = Envelope(id=msg.get("id") or str(uuid.uuid4()), topic=topic, sender=conn.name,
                       payload=payload, timestamp=msg.get("timestamp") or now_iso(),
                       checksum=checksum or sha256(payload), source_format=adapter.name)
        result, wake = self.store.publish(env)
        conn.send({"type": "puback", "ref": env.id, "topic": topic, "payload": result}, adapter)
        self.store.event("publish", f"{conn.name} → {topic} [{adapter.name.upper()}] {env.id[:8]}: {result}")
        for name in wake:
            self.work.put(name)

    def _on_ack(self, conn, msg, adapter):
        ref = require(msg, "ref")
        if self.store.ack(conn.name, ref):
            self.work.put(conn.name)

    def _on_stats(self, conn, msg, adapter):
        s = self.store.stats()
        s["broker"] = {"host": self.args.host, "port": self.args.port, "workers": self.args.workers,
                       "storage": self.describe_storage(), "ack_timeout": self.args.ack_timeout,
                       "max_attempts": self.args.max_attempts, "transport": "TCP",
                       "formats": AdapterFactory.names()}
        conn.send({"type": "stats", "ref": msg.get("id"), "payload": json.dumps(s, ensure_ascii=False)}, adapter)

    # ======================================================== work-job-uri
    def _worker(self) -> None:
        """Dispatcher: preia un abonat din coada de lucru și îi livrează mesajele."""
        while self.running.is_set():
            try:
                name = self.work.get(timeout=0.5)
            except queue.Empty:
                continue
            if not self.store.claim(name):      # alt worker îl deservește deja → ordinea se păstrează
                continue
            more = False
            try:
                while True:
                    item = self.store.take_next(name)
                    if item is None:
                        break
                    env, conn = item
                    try:
                        conn.send(env.to_deliver())
                    except (OSError, FrameError):
                        self.store.return_front(name, env)
                        conn.alive = False
                        break
            finally:
                more = self.store.release(name)
            if more:
                self.work.put(name)

    def _cron(self) -> None:
        """Cron-job la 1s: retransmitere la expirarea ACK + trezirea cozilor rămase."""
        while self.running.is_set():
            time.sleep(1.0)
            try:
                for name in self.store.requeue_expired(self.args.ack_timeout, self.args.max_attempts):
                    self.work.put(name)
            except Exception:
                log.exception("eroare în cron-job")


def main():
    p = argparse.ArgumentParser(description="Agent de mesagerie (Partea 1, TCP)")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=5000)
    p.add_argument("--workers", type=int, default=4, help="număr de work-job-uri de livrare")
    p.add_argument("--storage", choices=["transient", "persistent"], default="persistent")
    p.add_argument("--storage-format", choices=AdapterFactory.names(), default="xml")
    p.add_argument("--data-dir", default="data")
    p.add_argument("--ack-timeout", type=float, default=10.0, help="secunde până la retransmitere")
    p.add_argument("--max-attempts", type=int, default=5, help="încercări înainte de dead-letter")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(threadName)-18s %(message)s", datefmt="%H:%M:%S")
    broker = Broker(args)
    signal.signal(signal.SIGINT, broker.stop)
    signal.signal(signal.SIGTERM, broker.stop)
    broker.serve()


if __name__ == "__main__":
    main()
