"""Binary wire codec: byte-exact port of ``blitz-protocol/src/codec.rs``.

All integers little-endian. Frames: ``[ver u8][len u32le][payload]``.
Payloads start with a kind byte (0x01 single, 0x02 batch, 0x03 atomic,
0x11 response, 0x12 batch response).
"""

from __future__ import annotations

import datetime as _dt
import struct as _struct

from . import values as _v

PROTOCOL_VERSION = 2
HEADER_LEN = 5
DEFAULT_MAX_FRAME = 8 * 1024 * 1024

KIND_REQUEST = 0x01
KIND_BATCH_REQUEST = 0x02
KIND_ATOMIC_BATCH_REQUEST = 0x03
KIND_RESPONSE = 0x11
KIND_BATCH_RESPONSE = 0x12

OP_TO_TAG = {
    "ping": 0, "insert": 1, "get": 2, "update": 3, "delete": 4,
    "scan": 5, "subscribe": 6, "find": 7, "search": 8, "call": 9,
    "job_submit": 10, "job_poll": 11,
    "proc_deploy": 12, "proc_list": 13, "proc_drop": 14,
    "version": 15,
}
TAG_TO_OP = {v: k for k, v in OP_TO_TAG.items()}


class DecodeError(ValueError):
    pass


class Writer:
    def __init__(self) -> None:
        self.parts: list[bytes] = []

    def u8(self, v: int) -> None:
        self.parts.append(_struct.pack("<B", v & 0xFF))

    def u16(self, v: int) -> None:
        self.parts.append(_struct.pack("<H", v & 0xFFFF))

    def u32(self, v: int) -> None:
        self.parts.append(_struct.pack("<I", v & 0xFFFFFFFF))

    def i16(self, v: int) -> None:
        self.parts.append(_struct.pack("<h", v))

    def i32(self, v: int) -> None:
        self.parts.append(_struct.pack("<i", v))

    def u64(self, v: int) -> None:
        self.parts.append(_struct.pack("<Q", v))

    def i64(self, v: int) -> None:
        self.parts.append(_struct.pack("<q", v))

    def f32(self, v: float) -> None:
        self.parts.append(_struct.pack("<f", v))

    def f64(self, v: float) -> None:
        self.parts.append(_struct.pack("<d", v))

    def blob(self, b: bytes) -> None:
        self.parts.append(bytes(b))

    def string(self, s: str) -> None:
        b = s.encode("utf-8")
        self.u32(len(b))
        self.blob(b)

    def bytes(self) -> bytes:
        return b"".join(self.parts)

    # -- values ------------------------------------------------------

    def value(self, v) -> None:
        if v is None:
            self.u8(0x00)
            return
        if isinstance(v, bool):
            self.u8(0x01)
            self.u8(1 if v else 0)
            return
        if isinstance(v, int):
            if _v.I64_MIN <= v <= _v.I64_MAX:
                self.u8(0x05)
                self.i64(v)
            else:
                raise DecodeError(f"int out of int64 range (use V.u64): {v}")
            return
        if isinstance(v, float):
            self.u8(0x0B)
            self.f64(v)
            return
        if isinstance(v, str):
            if _v.is_uuid_shaped(v):
                self.u8(0x0F)
                self.blob(_v.uuid_to_bytes(v))
            else:
                self.u8(0x0D)
                self.string(v)
            return
        if isinstance(v, (bytes, bytearray, memoryview)):
            b = bytes(v)
            self.u8(0x0E)
            self.u32(len(b))
            self.blob(b)
            return
        if isinstance(v, _dt.datetime):
            self.u8(0x10)
            micros = int(_v.ensure_aware_utc(v).timestamp() * 1_000_000)
            self.i64(micros)
            return
        if isinstance(v, _dt.date):
            self.u8(0x11)
            self.i32(_v.days_from_civil(v.year, v.month, v.day))
            self.u32(v.month)
            self.u32(v.day)
            return
        if isinstance(v, (list, tuple)):
            if len(v) > 65536:
                raise DecodeError(f"array too large: {len(v)}")
            self.u8(0x13)
            self.u32(len(v))
            for item in v:
                self.value(item)
            return
        if isinstance(v, dict):
            if _v._is_wrapper(v, "$i32"):
                self.u8(0x02)
                self.u8(v["$i32"] & 0xFF)
                return
            if _v._is_wrapper(v, "$u32"):
                self.u8(0x08)
                self.u32(v["$u32"])
                return
            if _v._is_wrapper(v, "$i64"):
                n = v["$i64"]
                if not _v.I64_MIN <= n <= _v.I64_MAX:
                    raise DecodeError(f"i64 out of range: {n}")
                self.u8(0x05)
                self.i64(n)
                return
            if _v._is_wrapper(v, "$u64"):
                n = v["$u64"]
                if not 0 <= n <= _v.U64_MAX:
                    raise DecodeError(f"u64 out of range: {n}")
                self.u8(0x09)
                self.u64(n)
                return
            if _v._is_wrapper(v, "$f32"):
                self.u8(0x0A)
                self.f32(float(v["$f32"]))
                return
            if _v._is_wrapper(v, "$decimal"):
                self.u8(0x0C)
                self.string(str(v["$decimal"]))
                return
            if _v._is_wrapper(v, "$date"):
                y, mo, d = _parse_ymd(str(v["$date"]))
                self.u8(0x11)
                self.i32(_v.days_from_civil(y, mo, d))
                self.u32(mo)
                self.u32(d)
                return
            if _v._is_wrapper(v, "$uuid"):
                self.u8(0x0F)
                self.blob(_v.uuid_to_bytes(str(v["$uuid"])))
                return
            if _v.is_plain_dict(v):
                self.u8(0x12)
                self.json(v)
                return
        raise DecodeError(f"unencodable value: {v!r}")

    def json(self, j) -> None:
        if j is None:
            self.u8(0x00)
            return
        if isinstance(j, bool):
            self.u8(0x01)
            self.u8(1 if j else 0)
            return
        if isinstance(j, int):
            if _v.I64_MIN <= j <= _v.I64_MAX:
                self.u8(0x05)
                self.i64(j)
            else:
                raise DecodeError(f"json int out of range: {j}")
            return
        if isinstance(j, float):
            self.u8(0x0B)
            self.f64(j)
            return
        if isinstance(j, str):
            self.u8(0x0D)
            self.string(j)
            return
        if isinstance(j, (list, tuple)):
            self.u8(0x13)
            self.u32(len(j))
            for item in j:
                self.json(item)
            return
        if isinstance(j, dict):
            self.u8(0x12)
            self.u32(len(j))
            for k, item in j.items():
                self.string(str(k))
                self.json(item)
            return
        raise DecodeError(f"unencodable json value: {j!r}")

    def value_map(self, m: dict) -> None:
        self.u32(len(m))
        for k, v in m.items():
            self.string(str(k))
            self.value(v)

    def request_body(self, req: dict) -> None:
        self.u64(_as_u64(req["id"]))
        tag = OP_TO_TAG.get(req["op"])
        if tag is None:
            raise DecodeError(f"unknown op: {req['op']}")
        self.u8(tag)
        self.string(req.get("table") or "")
        row_id = req.get("row_id")
        if row_id is None:
            self.u8(0)
        else:
            self.u8(1)
            self.u64(_as_u64(row_id))
        values = req.get("values")
        if values is None:
            self.u8(0)
        else:
            self.u8(1)
            self.value_map(values)

    def response_body(self, resp: dict) -> None:
        self.u64(_as_u64(resp["id"]))
        self.u8(1 if resp["ok"] else 0)
        self.u32(len(resp["rows"]))
        for row in resp["rows"]:
            self.u64(_as_u64(row["id"]))
            self.value_map(row["values"])
        err = resp.get("error")
        if err is None:
            self.u8(0)
        else:
            self.u8(1)
            self.string(err)


def _as_u64(n) -> int:
    n = int(n)
    if not 0 <= n <= _v.U64_MAX:
        raise DecodeError(f"id out of u64 range: {n}")
    return n


def _parse_ymd(s: str):
    import re

    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
    if not m:
        raise DecodeError(f"bad $date: {s}")
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


class Reader:
    def __init__(self, buf: bytes) -> None:
        self.buf = buf
        self.off = 0

    def _need(self, n: int, what: str = "frame") -> None:
        if len(self.buf) - self.off < n:
            raise DecodeError(f"truncated {what}")

    def u8(self) -> int:
        self._need(1)
        v = self.buf[self.off]
        self.off += 1
        return v

    def u16(self) -> int:
        self._need(2)
        (v,) = _struct.unpack_from("<H", self.buf, self.off)
        self.off += 2
        return v

    def i16(self) -> int:
        self._need(2)
        (v,) = _struct.unpack_from("<h", self.buf, self.off)
        self.off += 2
        return v

    def u32(self) -> int:
        self._need(4)
        (v,) = _struct.unpack_from("<I", self.buf, self.off)
        self.off += 4
        return v

    def i32(self) -> int:
        self._need(4)
        (v,) = _struct.unpack_from("<i", self.buf, self.off)
        self.off += 4
        return v

    def u64(self) -> int:
        self._need(8)
        (v,) = _struct.unpack_from("<Q", self.buf, self.off)
        self.off += 8
        return v

    def i64(self) -> int:
        self._need(8)
        (v,) = _struct.unpack_from("<q", self.buf, self.off)
        self.off += 8
        return v

    def f32(self) -> float:
        self._need(4)
        (v,) = _struct.unpack_from("<f", self.buf, self.off)
        self.off += 4
        return v

    def f64(self) -> float:
        self._need(8)
        (v,) = _struct.unpack_from("<d", self.buf, self.off)
        self.off += 8
        return v

    def string(self) -> str:
        ln = self.u32()
        self._need(ln, "string")
        s = self.buf[self.off:self.off + ln].decode("utf-8")
        self.off += ln
        return s

    def take(self, n: int, what: str = "bytes") -> bytes:
        self._need(n, what)
        b = self.buf[self.off:self.off + n]
        self.off += n
        return bytes(b)

    def value(self):
        tag = self.u8()
        if tag == 0x00:
            return None
        if tag == 0x01:
            return self.u8() != 0
        if tag == 0x02:
            b = self.u8()
            return b - 256 if b > 127 else b
        if tag == 0x03:
            return self.i16()
        if tag == 0x04:
            return self.i32()
        if tag == 0x05:
            return self.i64()
        if tag == 0x06:
            return self.u8()
        if tag == 0x07:
            return self.u16()
        if tag == 0x08:
            return self.u32()
        if tag == 0x09:
            return self.u64()
        if tag == 0x0A:
            return self.f32()
        if tag == 0x0B:
            return self.f64()
        if tag == 0x0C:
            return {"$decimal": self.string()}
        if tag == 0x0D:
            return self.string()
        if tag == 0x0E:
            return self.take(self.u32(), "bytes")
        if tag == 0x0F:
            return _v.bytes_to_uuid(self.take(16, "uuid"))
        if tag == 0x10:
            micros = self.i64()
            return _dt.datetime.fromtimestamp(micros / 1_000_000, tz=_dt.timezone.utc)
        if tag == 0x11:
            days = self.i32()
            month = self.u32()
            day = self.u32()
            y, mo, d = _v.civil_from_days(days)
            if mo != month or d != day:
                raise DecodeError("bad date")
            return {"$date": f"{y:04d}-{mo:02d}-{d:02d}"}
        if tag == 0x12:
            return self.json()
        if tag == 0x13:
            count = self.u32()
            if count > 65536:
                raise DecodeError(f"array too large: {count}")
            return [self.value() for _ in range(count)]
        raise DecodeError(f"unknown value tag: {tag:#x}")

    def json(self):
        tag = self.u8()
        if tag == 0x00:
            return None
        if tag == 0x01:
            return self.u8() != 0
        if tag == 0x05:
            return self.i64()
        if tag == 0x09:
            return self.u64()
        if tag == 0x0B:
            return self.f64()
        if tag == 0x0D:
            return self.string()
        if tag == 0x12:
            count = self.u32()
            if count > 4096:
                raise DecodeError(f"json object too large: {count}")
            return {self.string(): self.json() for _ in range(count)}
        if tag == 0x13:
            count = self.u32()
            if count > 65536:
                raise DecodeError(f"json array too large: {count}")
            return [self.json() for _ in range(count)]
        raise DecodeError(f"bad json tag: {tag:#x}")

    def value_map(self) -> dict:
        count = self.u32()
        return {self.string(): self.value() for _ in range(count)}

    def request_body(self) -> dict:
        rid = self.u64()
        op = TAG_TO_OP.get(self.u8())
        if op is None:
            raise DecodeError("unknown op tag")
        table = self.string()
        row_id = self.u64() if self.u8() != 0 else None
        values = self.value_map() if self.u8() != 0 else None
        return {"id": rid, "op": op, "table": table, "row_id": row_id, "values": values}

    def row_view(self) -> dict:
        return {"id": self.u64(), "values": self.value_map()}

    def response_body(self) -> dict:
        rid = self.u64()
        ok = self.u8() != 0
        rows = [self.row_view() for _ in range(self.u32())]
        error = self.string() if self.u8() != 0 else None
        return {"id": rid, "ok": ok, "rows": rows, "error": error}

    def end(self) -> None:
        if len(self.buf) - self.off != 0:
            raise DecodeError("trailing bytes")


class FrameCodec:
    """Streaming frame codec (mirrors the Rust ``FrameCodec``)."""

    def __init__(self, max_frame: int = DEFAULT_MAX_FRAME) -> None:
        self.max_frame = max_frame
        self._stash = bytearray()

    def frame(self, payload: bytes) -> bytes:
        head = _struct.pack("<BI", PROTOCOL_VERSION, len(payload))
        return head + bytes(payload)

    def feed(self, chunk: bytes) -> list:
        """Push bytes; returns all complete payloads."""
        self._stash += chunk
        out = []
        while len(self._stash) >= HEADER_LEN:
            ver = self._stash[0]
            if ver != PROTOCOL_VERSION:
                raise DecodeError(f"unsupported version: {ver}")
            (ln,) = _struct.unpack_from("<I", self._stash, 1)
            if ln > self.max_frame:
                raise DecodeError(f"frame too large: {ln} > {self.max_frame}")
            if len(self._stash) < HEADER_LEN + ln:
                break
            out.append(bytes(self._stash[HEADER_LEN:HEADER_LEN + ln]))
            del self._stash[:HEADER_LEN + ln]
        return out

    def encode_request(self, req: dict) -> bytes:
        w = Writer()
        w.u8(KIND_REQUEST)
        w.request_body(req)
        return self.frame(w.bytes())

    def encode_batch(self, batch: dict, atomic: bool) -> bytes:
        if len(batch["ops"]) > 4096:
            raise DecodeError(f"batch too large: {len(batch['ops'])}")
        w = Writer()
        w.u8(KIND_ATOMIC_BATCH_REQUEST if atomic else KIND_BATCH_REQUEST)
        w.u64(_as_u64(batch["id"]))
        w.u32(len(batch["ops"]))
        for op in batch["ops"]:
            w.request_body(op)
        return self.frame(w.bytes())

    def decode_incoming(self, payload: bytes) -> dict:
        r = Reader(payload)
        kind = r.u8()
        if kind == KIND_REQUEST:
            req = r.request_body()
            r.end()
            return {"kind": "single", "req": req}
        if kind in (KIND_BATCH_REQUEST, KIND_ATOMIC_BATCH_REQUEST):
            rid = r.u64()
            count = r.u32()
            if count > 4096:
                raise DecodeError(f"batch too large: {count}")
            ops = [r.request_body() for _ in range(count)]
            r.end()
            batch = {"id": rid, "ops": ops}
            return {"kind": "atomic" if kind == KIND_ATOMIC_BATCH_REQUEST else "batch",
                    "batch": batch}
        raise DecodeError(f"unknown incoming kind: {kind:#x}")

    def decode_response(self, payload: bytes) -> dict:
        r = Reader(payload)
        kind = r.u8()
        if kind != KIND_RESPONSE:
            raise DecodeError(f"wrong payload kind: {kind:#x}")
        resp = r.response_body()
        r.end()
        return resp

    def decode_batch_response(self, payload: bytes) -> dict:
        r = Reader(payload)
        kind = r.u8()
        if kind != KIND_BATCH_RESPONSE:
            raise DecodeError(f"wrong payload kind: {kind:#x}")
        rid = r.u64()
        count = r.u32()
        if count > 4096:
            raise DecodeError(f"batch response too large: {count}")
        results = [r.response_body() for _ in range(count)]
        r.end()
        return {"id": rid, "results": results}

    def encode_batch_response(self, resp: dict) -> bytes:
        w = Writer()
        w.u8(KIND_BATCH_RESPONSE)
        w.u64(_as_u64(resp["id"]))
        w.u32(len(resp["results"]))
        for item in resp["results"]:
            w.response_body(item)
        return self.frame(w.bytes())
