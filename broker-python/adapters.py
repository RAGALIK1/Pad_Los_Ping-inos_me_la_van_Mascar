"""
Șablonul Adapter pentru formatul mesajelor.

Brokerul lucrează intern cu un singur model: un dicționar „plat” de câmpuri
string (type, id, topic, sender, payload, ...). Fiecare format de pe fir
(JSON sau XML) are un adaptor care traduce între bytes și acest model.
Restul brokerului nu știe și nu îi pasă în ce format a venit mesajul.

Adăugarea unui format nou (ex. YAML, MessagePack) = o clasă nouă + o linie
în AdapterFactory. Nimic altceva nu se schimbă.
"""
from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod


class FormatError(Exception):
    """Mesajul nu poate fi (de)serializat în formatul declarat."""


class MessageAdapter(ABC):
    name: str = "abstract"

    # --- un singur mesaj (folosit pe rețea) ---------------------------------
    @abstractmethod
    def serialize(self, msg: dict) -> bytes: ...

    @abstractmethod
    def deserialize(self, data: bytes) -> dict: ...

    # --- o listă de mesaje (folosit de storage-ul persistent) ---------------
    @abstractmethod
    def serialize_many(self, msgs: list[dict]) -> bytes: ...

    @abstractmethod
    def deserialize_many(self, data: bytes) -> list[dict]: ...

    @staticmethod
    def _clean(msg: dict) -> dict:
        return {k: str(v) for k, v in msg.items() if v is not None}


class JsonAdapter(MessageAdapter):
    name = "json"

    def serialize(self, msg: dict) -> bytes:
        return json.dumps(self._clean(msg), ensure_ascii=False).encode("utf-8")

    def deserialize(self, data: bytes) -> dict:
        try:
            obj = json.loads(data.decode("utf-8"))
        except UnicodeDecodeError as e:
            raise FormatError(f"mesajul nu este UTF-8 valid: {e}") from e
        except json.JSONDecodeError as e:
            raise FormatError(f"JSON invalid: {e.msg} (poziția {e.pos})") from e
        return self._check_flat(obj)

    def serialize_many(self, msgs: list[dict]) -> bytes:
        return json.dumps([self._clean(m) for m in msgs], ensure_ascii=False, indent=2).encode("utf-8")

    def deserialize_many(self, data: bytes) -> list[dict]:
        try:
            arr = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise FormatError(f"fișier JSON invalid: {e}") from e
        if not isinstance(arr, list):
            raise FormatError("fișierul JSON trebuie să conțină o listă")
        return [self._check_flat(o) for o in arr]

    @staticmethod
    def _check_flat(obj) -> dict:
        if not isinstance(obj, dict):
            raise FormatError("rădăcina JSON trebuie să fie un obiect")
        for k, v in obj.items():
            if not isinstance(v, str):
                raise FormatError(f"câmpul '{k}' trebuie să fie string")
        return obj


class XmlAdapter(MessageAdapter):
    name = "xml"

    def serialize(self, msg: dict) -> bytes:
        return ET.tostring(self._to_element(msg), encoding="unicode").encode("utf-8")

    def deserialize(self, data: bytes) -> dict:
        root = self._parse(data)
        return self._from_element(root)

    def serialize_many(self, msgs: list[dict]) -> bytes:
        root = ET.Element("store")
        for m in msgs:
            root.append(self._to_element(m))
        ET.indent(root, space="  ")
        return b'<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="unicode").encode("utf-8")

    def deserialize_many(self, data: bytes) -> list[dict]:
        root = self._parse(data, expected_root="store")
        return [self._from_element(el) for el in root]

    # ------------------------------------------------------------------------
    def _to_element(self, msg: dict) -> ET.Element:
        el = ET.Element("message")
        for k, v in self._clean(msg).items():
            ET.SubElement(el, k).text = v
        return el

    @staticmethod
    def _parse(data: bytes, expected_root: str = "message") -> ET.Element:
        # Protecție: fără DOCTYPE/entități externe (XXE, billion laughs)
        if b"<!DOCTYPE" in data.upper() or b"<!ENTITY" in data.upper():
            raise FormatError("XML cu DOCTYPE/ENTITY nu este permis")
        try:
            root = ET.fromstring(data)
        except ET.ParseError as e:
            raise FormatError(f"XML invalid: {e}") from e
        if root.tag != expected_root:
            raise FormatError(f"elementul rădăcină trebuie să fie <{expected_root}>, nu <{root.tag}>")
        return root

    @staticmethod
    def _from_element(el: ET.Element) -> dict:
        if el.tag != "message":
            raise FormatError(f"element neașteptat <{el.tag}>")
        out = {}
        for child in el:
            if len(child):
                raise FormatError(f"câmpul <{child.tag}> nu poate avea sub-elemente")
            out[child.tag] = child.text or ""
        return out


class AdapterFactory:
    """Alege adaptorul după nume sau îl detectează din conținutul mesajului."""

    _registry: dict[str, MessageAdapter] = {a.name: a for a in (JsonAdapter(), XmlAdapter())}

    @classmethod
    def get(cls, name: str) -> MessageAdapter:
        try:
            return cls._registry[name.lower()]
        except KeyError:
            raise FormatError(f"format necunoscut '{name}' (acceptate: {', '.join(cls._registry)})") from None

    @classmethod
    def detect(cls, data: bytes) -> MessageAdapter:
        head = data.lstrip(b"\xef\xbb\xbf \t\r\n")[:1]
        if head == b"{":
            return cls._registry["json"]
        if head == b"<":
            return cls._registry["xml"]
        raise FormatError("formatul mesajului nu poate fi detectat (se așteaptă JSON '{' sau XML '<')")

    @classmethod
    def names(cls) -> list[str]:
        return list(cls._registry)
