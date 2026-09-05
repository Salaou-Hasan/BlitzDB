"""BlitzDB value model for Python (stdlib only).

Wire tags mirror ``blitz-protocol/src/codec.rs`` exactly (0x00-0x13).
Python ints are unbounded; the mapping is explicit at the edges:
- ``bool`` -> Boolean (checked before int: bool subclasses int).
- ``int`` in int64 range -> Int64 (0x05); larger magnitude -> error
  (use :func:`V.u64` for big IDs, or another exact wrapper).
- ``float`` -> Float64. Need Float32/Int32/UInt32? Use ``V.i32`` etc.
- ``str`` -> String, except UUID-shaped strings -> Uuid (0x0F), which
  decode back to the same string (roundtrip-stable by construction).
- ``bytes``/``bytearray`` -> Bytes. ``datetime`` -> Timestamp (naive
  assumed UTC; ms precision — sub-ms truncated). ``date`` -> ``$date``.
- ``{"$decimal": s}`` -> Decimal, ``{"$date": "YYYY-MM-DD"}`` -> Date,
  ``{"$uuid": s}`` -> Uuid. Everywhere else a dict is Json.
- ``list``/``tuple`` -> Array (recursive values); dict -> Json object
  (nested subset: null/bool/int/uint/float/str/object/array only).
"""

from __future__ import annotations

import datetime as _dt
import re as _re

I64_MIN = -(2 ** 63)
I64_MAX = 2 ** 63 - 1
U64_MAX = 2 ** 64 - 1

_UUID_RE = _re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


class V:
    """Exact-width constructors for schema-strict columns."""

    @staticmethod
    def i32(n: int) -> dict:
        return {"$i32": int(n)}

    @staticmethod
    def u32(n: int) -> dict:
        return {"$u32": int(n)}

    @staticmethod
    def i64(n: int) -> dict:
        return {"$i64": int(n)}

    @staticmethod
    def u64(n: int) -> dict:
        return {"$u64": int(n)}

    @staticmethod
    def f32(n: float) -> dict:
        return {"$f32": float(n)}

    @staticmethod
    def dec(s: str) -> dict:
        return {"$decimal": str(s)}

    @staticmethod
    def date(ymd: str) -> dict:
        return {"$date": str(ymd)}

    @staticmethod
    def uuid(s: str) -> dict:
        return {"$uuid": str(s)}


def _is_wrapper(v, key: str) -> bool:
    return isinstance(v, dict) and set(v.keys()) == {key}


def is_plain_dict(v) -> bool:
    if not isinstance(v, dict):
        return False
    return not (
        _is_wrapper(v, "$i32") or _is_wrapper(v, "$u32")
        or _is_wrapper(v, "$i64") or _is_wrapper(v, "$u64")
        or _is_wrapper(v, "$f32") or _is_wrapper(v, "$decimal")
        or _is_wrapper(v, "$date") or _is_wrapper(v, "$uuid")
    )


def is_uuid_shaped(s: str) -> bool:
    return isinstance(s, str) and _UUID_RE.match(s) is not None


def uuid_to_bytes(s: str) -> bytes:
    try:
        h = s.replace("-", "")
        if len(h) != 32:
            raise ValueError(s)
        return bytes.fromhex(h)
    except ValueError:
        raise ValueError(f"bad uuid: {s}")


def bytes_to_uuid(b: bytes) -> str:
    h = bytes(b).hex()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def days_from_civil(y: int, m: int, d: int) -> int:
    """Proleptic Gregorian days matching chrono's num_days_from_ce."""
    y_adj = y - 1 if m <= 2 else y
    era = y_adj // 400
    yoe = y_adj - era * 400
    mp = (m + 9) % 12
    doy = (153 * mp + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468 + 719162


def civil_from_days(z: int):
    z2 = z - 719162 + 719468
    era = z2 // 146097
    doe = z2 - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + 3 if mp < 10 else mp - 9
    return (y + 1 if m <= 2 else y, m, d)


def ensure_aware_utc(v: _dt.datetime) -> _dt.datetime:
    if v.tzinfo is None:
        return v.replace(tzinfo=_dt.timezone.utc)
    return v.astimezone(_dt.timezone.utc)
