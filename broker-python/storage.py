"""
Stocarea mesajelor și rutarea pe subiecte (topics).

Metoda tranzientă: totul stă în memorie, în structuri protejate de un
RLock (collections.deque + dict). Am ales un lacăt comun în locul unor
queue.Queue separate deoarece o publicare trebuie să modifice atomic mai
multe structuri deodată (cozile tuturor abonaților + backlog + contoare) —
cu cozi thread-safe independente, un observator ar putea vedea o stare
„pe jumătate rutată”. queue.Queue este folosit totuși pentru coada de
lucru a work-job-urilor (vezi broker.py), unde se potrivește perfect.

Metoda persistentă: PersistenceJob face periodic un snapshot al stării
(sub lacăt, rapid, doar copiere) și îl serializează ÎN AFARA lacătului,
pe un fir separat, cu adaptorul ales (JSON sau XML). Scrierea e atomică
(fișier temporar + os.replace), deci o cădere a brokerului în timpul
scrierii nu corupe fișierul.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

from adapters import AdapterFactory, FormatError

log = logging.getLogger("storage")

INFLIGHT_WINDOW = 32        # câte mesaje neconfirmate poate avea un abonat
BACKLOG_LIMIT = 1000        # mesaje reținute per topic fără abonați
DEAD_LETTER_LIMIT = 200


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def topic_matches(pattern: str, topic: str) -> bool:
    """'*' = exact un segment, '#' = zero sau mai multe segmente (doar la final)."""
    p, t = pattern.split("."), topic.split(".")
    for i, seg in enumerate(p):
        if seg == "#":
            return True
        if i >= len(t):
            return False
        if seg != "*" and seg != t[i]:
            return False
    return len(p) == len(t)


@dataclass
class Envelope:
    id: str
    topic: str
    sender: str
    payload: str
    timestamp: str
    checksum: str = ""
    source_format: str = "json"
    attempts: int = 0

    def to_deliver(self) -> dict:
        return {
            "type": "deliver", "id": self.id, "topic": self.topic, "sender": self.sender,
            "timestamp": self.timestamp, "payload": self.payload, "checksum": self.checksum,
            "format": self.source_format, "attempt": str(self.attempts),
        }

    def to_record(self, kind: str, **extra) -> dict:
        return {"kind": kind, "id": self.id, "topic": self.topic, "sender": self.sender,
                "payload": self.payload, "timestamp": self.timestamp, "checksum": self.checksum,
                "format": self.source_format, "attempt": str(self.attempts), **extra}

    @classmethod
    def from_record(cls, r: dict) -> "Envelope":
        return cls(id=r["id"], topic=r["topic"], sender=r.get("sender", ""), payload=r.get("payload", ""),
                   timestamp=r.get("timestamp", ""), checksum=r.get("checksum", ""),
                   source_format=r.get("format", "json"), attempts=int(r.get("attempt", "0") or 0))


@dataclass
class Subscriber:
    name: str
    format: str = "json"
    role: str = "receiver"
    patterns: set = field(default_factory=set)
    queue: deque = field(default_factory=deque)
    inflight: OrderedDict = field(default_factory=OrderedDict)   # id -> (Envelope, sent_at)
    conn: object = None
    addr: str = ""
    dispatching: bool = False
    last_seen: str = ""

    @property
    def online(self) -> bool:
        return self.conn is not None


class MessageStore:
    def __init__(self):
        self.lock = threading.RLock()
        self.subscribers: dict[str, Subscriber] = {}
        self.backlog: dict[str, deque] = {}
        self.topics: dict[str, int] = {}                 # canale create dinamic -> nr. mesaje
        self.dead_letters: deque = deque(maxlen=DEAD_LETTER_LIMIT)
        self.events: deque = deque(maxlen=120)
        self.recent_ids: deque = deque(maxlen=5000)
        self._recent_set: set = set()
        self.counters = dict(published=0, delivered=0, acked=0, redelivered=0,
                             invalid=0, dead=0, duplicates=0)
        self.dirty = False

    # ------------------------------------------------------------------ util
    def event(self, kind: str, text: str) -> None:
        with self.lock:
            self.events.append({"t": now_iso(), "kind": kind, "text": text})
        log.info("[%s] %s", kind, text)

    def _touch(self):
        self.dirty = True

    # ------------------------------------------------------- conexiuni/abonați
    def attach(self, name: str, conn, fmt: str, role: str, addr: str):
        """Leagă o conexiune de un abonat (durabil). Întoarce conexiunea veche, dacă exista."""
        with self.lock:
            sub = self.subscribers.get(name)
            if sub is None:
                sub = self.subscribers[name] = Subscriber(name=name)
            old = sub.conn
            sub.conn, sub.format, sub.role, sub.addr = conn, fmt, role, addr
            sub.last_seen = now_iso()
            # mesajele rămase neconfirmate de la sesiunea precedentă se retrimit
            for env, _ in reversed(list(sub.inflight.values())):
                sub.queue.appendleft(env)
            sub.inflight.clear()
            self._touch()
            return old, sorted(sub.patterns), len(sub.queue)

    def detach(self, name: str, conn) -> None:
        with self.lock:
            sub = self.subscribers.get(name)
            if sub is None or sub.conn is not conn:
                return
            sub.conn = None
            sub.last_seen = now_iso()
            for env, _ in reversed(list(sub.inflight.values())):
                sub.queue.appendleft(env)
            sub.inflight.clear()
            if not sub.patterns and not sub.queue:
                del self.subscribers[name]          # nu era abonat la nimic: nu îl păstrăm
            self._touch()

    def subscribe(self, name: str, pattern: str) -> int:
        """Adaugă abonarea; mută în coada abonatului mesajele din backlog. Întoarce câte a primit."""
        with self.lock:
            sub = self.subscribers[name]
            sub.patterns.add(pattern)
            moved = 0
            for topic in list(self.backlog):
                if topic_matches(pattern, topic):
                    q = self.backlog.pop(topic)
                    sub.queue.extend(q)
                    moved += len(q)
            self._touch()
            return moved

    def unsubscribe(self, name: str, pattern: str) -> bool:
        with self.lock:
            sub = self.subscribers.get(name)
            if not sub or pattern not in sub.patterns:
                return False
            sub.patterns.discard(pattern)
            self._touch()
            return True

    # -------------------------------------------------------------- publicare
    def publish(self, env: Envelope) -> tuple[str, list[str]]:
        """Rutează mesajul. Întoarce (rezultat, abonați online de notificat)."""
        with self.lock:
            if env.id in self._recent_set:
                self.counters["duplicates"] += 1
                return "duplicate", []
            if len(self.recent_ids) == self.recent_ids.maxlen:
                self._recent_set.discard(self.recent_ids[0])
            self.recent_ids.append(env.id)
            self._recent_set.add(env.id)

            self.counters["published"] += 1
            self.topics[env.topic] = self.topics.get(env.topic, 0) + 1
            targets = [s for s in self.subscribers.values()
                       if any(topic_matches(p, env.topic) for p in s.patterns)]
            self._touch()
            if not targets:
                q = self.backlog.setdefault(env.topic, deque(maxlen=BACKLOG_LIMIT))
                q.append(env)
                return "backlog", []
            for s in targets:
                # fiecare abonat primește propria copie (fan-out publish/subscribe)
                s.queue.append(Envelope(**{**env.__dict__}))
            return f"routed:{len(targets)}", [s.name for s in targets if s.online]

    # ------------------------------------------------------ livrare (workeri)
    def claim(self, name: str) -> bool:
        with self.lock:
            sub = self.subscribers.get(name)
            if not sub or sub.dispatching or not sub.online:
                return False
            sub.dispatching = True
            return True

    def release(self, name: str) -> bool:
        """Eliberează abonatul; întoarce True dacă mai are ce livra."""
        with self.lock:
            sub = self.subscribers.get(name)
            if not sub:
                return False
            sub.dispatching = False
            return sub.online and bool(sub.queue) and len(sub.inflight) < INFLIGHT_WINDOW

    def take_next(self, name: str):
        with self.lock:
            sub = self.subscribers.get(name)
            if not sub or not sub.online or not sub.queue or len(sub.inflight) >= INFLIGHT_WINDOW:
                return None
            env = sub.queue.popleft()
            env.attempts += 1
            if env.attempts > 1:
                self.counters["redelivered"] += 1
            sub.inflight[env.id] = (env, time.monotonic())
            self.counters["delivered"] += 1
            return env, sub.conn

    def return_front(self, name: str, env: Envelope) -> None:
        with self.lock:
            sub = self.subscribers.get(name)
            if sub:
                sub.inflight.pop(env.id, None)
                env.attempts -= 1
                sub.queue.appendleft(env)

    def ack(self, name: str, msg_id: str) -> bool:
        """Confirmă un mesaj. Întoarce True dacă abonatul trebuie retrezit pentru livrare."""
        with self.lock:
            sub = self.subscribers.get(name)
            if not sub or msg_id not in sub.inflight:
                return False
            del sub.inflight[msg_id]
            self.counters["acked"] += 1
            self._touch()
            # s-a eliberat loc în fereastră: dacă mai sunt mesaje, abonatul trebuie trezit
            return sub.online and bool(sub.queue) and not sub.dispatching

    def requeue_expired(self, timeout: float, max_attempts: int) -> list[str]:
        """Work-job: mesajele neconfirmate la timp se retrimit sau ajung în dead-letter."""
        now = time.monotonic()
        wake = []
        with self.lock:
            for sub in self.subscribers.values():
                expired = [(i, e) for i, (e, t) in sub.inflight.items() if now - t > timeout]
                for msg_id, env in expired:
                    del sub.inflight[msg_id]
                    if env.attempts >= max_attempts:
                        self._dead(sub.name, f"fără confirmare după {env.attempts} încercări", env=env)
                    else:
                        sub.queue.appendleft(env)
                        self.event("retry", f"{env.id[:8]} → {sub.name}: fără ACK, se retrimite")
                if expired:
                    self._touch()
                if sub.online and sub.queue and len(sub.inflight) < INFLIGHT_WINDOW:
                    wake.append(sub.name)
        return wake

    # -------------------------------------------------------- mesaje invalide
    def _dead(self, who: str, reason: str, env: Envelope | None = None, raw: bytes = b""):
        rec = {"t": now_iso(), "from": who, "reason": reason}
        if env:
            rec.update(id=env.id, topic=env.topic, payload=env.payload[:500])
        if raw:
            rec["raw"] = raw[:500].decode("utf-8", "replace")
        self.dead_letters.append(rec)
        self.counters["dead"] += 1
        self.event("dead", f"{who}: {reason}")

    def record_invalid(self, who: str, raw: bytes, reason: str) -> None:
        with self.lock:
            self.counters["invalid"] += 1
            self._dead(who, reason, raw=raw)

    # ------------------------------------------------------------ statistici
    def stats(self) -> dict:
        with self.lock:
            waiting: dict[str, int] = {}
            for s in self.subscribers.values():
                for e in list(s.queue) + [e for e, _ in s.inflight.values()]:
                    waiting[e.topic] = waiting.get(e.topic, 0) + 1
            return {
                "counters": dict(self.counters),
                "topics": [{"name": t, "published": n, "waiting": waiting.get(t, 0),
                            "backlog": len(self.backlog.get(t, ())),
                            "subscribers": sum(1 for s in self.subscribers.values()
                                               if any(topic_matches(p, t) for p in s.patterns))}
                           for t, n in sorted(self.topics.items())],
                "subscribers": [{"name": s.name, "online": s.online, "format": s.format, "role": s.role,
                                 "addr": s.addr, "patterns": sorted(s.patterns), "queued": len(s.queue),
                                 "inflight": len(s.inflight), "last_seen": s.last_seen}
                                for s in sorted(self.subscribers.values(), key=lambda x: x.name)],
                "dead_letters": list(self.dead_letters)[-25:][::-1],
                "events": list(self.events)[-60:][::-1],
            }

    # ----------------------------------------------------------- persistență
    def snapshot(self) -> list[dict]:
        with self.lock:
            self.dirty = False
            recs: list[dict] = [{"kind": "counters", **{k: str(v) for k, v in self.counters.items()}}]
            recs += [{"kind": "topic", "topic": t, "count": str(n)} for t, n in self.topics.items()]
            for s in self.subscribers.values():
                recs.append({"kind": "subscriber", "subscriber": s.name, "format": s.format, "role": s.role})
                recs += [{"kind": "subscription", "subscriber": s.name, "topic": p} for p in s.patterns]
                pending = [e for e, _ in s.inflight.values()] + list(s.queue)
                recs += [e.to_record("queued", subscriber=s.name) for e in pending]
            for q in self.backlog.values():
                recs += [e.to_record("backlog") for e in q]
            return recs

    def restore(self, recs: list[dict]) -> None:
        with self.lock:
            for r in recs:
                kind = r.get("kind")
                if kind == "counters":
                    for k in self.counters:
                        self.counters[k] = int(r.get(k, "0") or 0)
                elif kind == "topic":
                    self.topics[r["topic"]] = int(r.get("count", "0") or 0)
                elif kind == "subscriber":
                    self.subscribers[r["subscriber"]] = Subscriber(
                        name=r["subscriber"], format=r.get("format", "json"), role=r.get("role", "receiver"))
                elif kind == "subscription":
                    self.subscribers.setdefault(r["subscriber"], Subscriber(name=r["subscriber"])).patterns.add(r["topic"])
                elif kind == "queued":
                    self.subscribers.setdefault(r["subscriber"], Subscriber(name=r["subscriber"])).queue.append(
                        Envelope.from_record(r))
                elif kind == "backlog":
                    env = Envelope.from_record(r)
                    self.backlog.setdefault(env.topic, deque(maxlen=BACKLOG_LIMIT)).append(env)


class PersistenceJob(threading.Thread):
    """Fir separat care salvează starea la interval, fără să blocheze rutarea."""

    def __init__(self, store: MessageStore, data_dir: str, fmt: str, interval: float = 1.0):
        super().__init__(name="persistence-job", daemon=True)
        self.store, self.interval = store, interval
        self.adapter = AdapterFactory.get(fmt)
        os.makedirs(data_dir, exist_ok=True)
        self.path = os.path.join(data_dir, f"store.{self.adapter.name}")
        self._halt = threading.Event()

    def load(self) -> int:
        if not os.path.exists(self.path):
            return 0
        try:
            with open(self.path, "rb") as f:
                recs = self.adapter.deserialize_many(f.read())
            self.store.restore(recs)
            return len(recs)
        except (FormatError, KeyError, ValueError) as e:
            broken = self.path + ".corrupt"
            os.replace(self.path, broken)
            log.error("fișierul de stocare este corupt (%s); mutat în %s", e, broken)
            return 0

    def flush(self) -> None:
        if not self.store.dirty:
            return
        recs = self.store.snapshot()              # rapid, sub lacăt
        data = self.adapter.serialize_many(recs)  # lent, în afara lacătului
        tmp = self.path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def run(self):
        while not self._halt.wait(self.interval):
            try:
                self.flush()
            except Exception:
                log.exception("eroare la salvarea stării")

    def stop(self):
        self._halt.set()
        self.flush()
