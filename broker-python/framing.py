"""
Încadrarea mesajelor pe fluxul TCP.

TCP este un flux de octeți, nu de mesaje: un send() poate ajunge în două
recv()-uri, iar două send()-uri pot ajunge într-unul singur. De aceea fiecare
mesaj este precedat de lungimea lui pe 4 octeți (big-endian):

    +----------------+---------------------------+
    | lungime (u32)  |  corp UTF-8 (JSON / XML)  |
    +----------------+---------------------------+

Prefixul de lungime funcționează la fel de bine pentru JSON și pentru XML
(care poate conține linii noi), deci nu depinde de format.
"""
import socket
import struct

MAX_FRAME = 1024 * 1024  # 1 MiB
_HEADER = struct.Struct(">I")


class FrameError(Exception):
    """Cadru invalid; conexiunea nu mai poate fi resincronizată."""


def send_frame(sock: socket.socket, data: bytes) -> None:
    if len(data) > MAX_FRAME:
        raise FrameError(f"mesaj prea mare ({len(data)} > {MAX_FRAME} octeți)")
    sock.sendall(_HEADER.pack(len(data)) + data)


def _recv_exact(sock: socket.socket, n: int) -> bytes | None:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            if buf:
                raise FrameError("conexiune închisă în mijlocul unui cadru")
            return None
        buf.extend(chunk)
    return bytes(buf)


def recv_frame(sock: socket.socket) -> bytes | None:
    """Întoarce corpul mesajului sau None dacă peer-ul a închis conexiunea."""
    header = _recv_exact(sock, _HEADER.size)
    if header is None:
        return None
    (length,) = _HEADER.unpack(header)
    if length > MAX_FRAME:
        raise FrameError(f"cadru declarat prea mare: {length} octeți")
    if length == 0:
        return b""
    body = _recv_exact(sock, length)
    if body is None:
        raise FrameError("conexiune închisă în mijlocul unui cadru")
    return body
