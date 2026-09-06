"""Codec roundtrips: byte-exactness against the Rust framing rules."""

import datetime
import unittest

from blitz_client.codec import (
    FrameCodec, PROTOCOL_VERSION, DecodeError,
    KIND_REQUEST, KIND_BATCH_REQUEST, KIND_ATOMIC_BATCH_REQUEST,
)
from blitz_client.values import V


def round_value(v):
    c = FrameCodec()
    req = {"id": 1, "op": "insert", "table": "t", "values": {"v": v}}
    (payload,) = c.feed(c.encode_request(req))
    back = c.decode_incoming(payload)
    assert back["kind"] == "single", back
    return back["req"]["values"]["v"]


class TestValues(unittest.TestCase):
    def test_scalars(self):
        self.assertIsNone(round_value(None))
        self.assertTrue(round_value(True))
        self.assertEqual(round_value(42), 42)
        self.assertEqual(round_value(1.5), 1.5)
        self.assertEqual(round_value(V.i32(-7)), -7)
        self.assertEqual(round_value(V.u32(4000000000)), 4000000000)
        self.assertEqual(round_value(V.u64(2 ** 63)), 2 ** 63)
        with self.assertRaises(DecodeError):
            round_value(2 ** 70)
        with self.assertRaises(DecodeError):
            round_value(V.u64(2 ** 70))

    def test_string_uuid_bytes_date(self):
        self.assertEqual(round_value("hello"), "hello")
        uid = "123e4567-e89b-12d3-a456-426614174000"
        self.assertEqual(round_value(uid), uid)
        self.assertEqual(round_value(V.uuid(uid)), uid)
        self.assertEqual(round_value(b"\x01\x02\xfa"), b"\x01\x02\xfa")
        self.assertEqual(round_value(V.dec("12.50")), {"$decimal": "12.50"})
        self.assertEqual(round_value(V.date("2026-09-05")), {"$date": "2026-09-05"})
        ts = datetime.datetime(2026, 9, 5, 12, 0, 0, tzinfo=datetime.timezone.utc)
        self.assertEqual(round_value(ts), ts)

    def test_containers(self):
        self.assertEqual(round_value([1, "a", None, True]), [1, "a", None, True])
        self.assertEqual(
            round_value({"a": 1, "b": "x", "c": [1, 2], "d": {"e": True}}),
            {"a": 1, "b": "x", "c": [1, 2], "d": {"e": True}},
        )


class TestFrames(unittest.TestCase):
    def test_kinds(self):
        c = FrameCodec()
        req = {"id": 7, "op": "get", "table": "users", "row_id": 42}
        (p1,) = c.feed(c.encode_request(req))
        self.assertEqual(p1[0], KIND_REQUEST)
        back = c.decode_incoming(p1)
        self.assertEqual(back["kind"], "single")
        self.assertEqual(back["req"]["row_id"], 42)
        batch = {"id": 9, "ops": [req, dict(req, id=8)]}
        for atomic, kind in ((False, KIND_BATCH_REQUEST), (True, KIND_ATOMIC_BATCH_REQUEST)):
            (p,) = c.feed(c.encode_batch(batch, atomic))
            self.assertEqual(p[0], kind)
            b = c.decode_incoming(p)
            self.assertEqual(b["kind"], "atomic" if atomic else "batch")

    def test_feed_split_and_version(self):
        c = FrameCodec()
        f = c.encode_request({"id": 1, "op": "ping", "table": ""})
        self.assertEqual(c.feed(f[:3]), [])
        self.assertEqual(len(c.feed(f[3:])), 1)
        bad = bytearray(f)
        bad[0] = 0x7F
        with self.assertRaises(DecodeError):
            FrameCodec().feed(bytes(bad))

    def test_batch_response(self):
        c = FrameCodec()
        resp = {"id": 99, "results": [
            {"id": 1, "ok": True, "rows": [{"id": 5, "values": {"name": "Ada"}}],
             "error": None},
            {"id": 2, "ok": False, "rows": [], "error": "gone"},
        ]}
        (p,) = c.feed(c.encode_batch_response(resp))
        self.assertEqual(p[0], 0x12)
        self.assertEqual(c.decode_batch_response(p), resp)

    def test_version(self):
        self.assertEqual(PROTOCOL_VERSION, 2)


if __name__ == "__main__":
    unittest.main()


class TestCompat(unittest.TestCase):
    def test_matrix(self):
        from blitz_client.client import check_compat, parse_version
        from blitz_client.errors import SdkError
        check_compat("0.1.0", 2)
        check_compat("1.4.2", 2)
        with self.assertRaises(SdkError) as ctx:
            check_compat("0.1.0", 3)
        self.assertIn("requires server protocol", str(ctx.exception))
        with self.assertRaises(SdkError) as ctx:
            check_compat("0.0.9", 2)
        self.assertIn("requires BlitzDB server >=", str(ctx.exception))
        with self.assertRaises(SdkError):
            check_compat("banana", 2)
        self.assertEqual(parse_version("1.2.3"), (1, 2, 3))
        self.assertIsNone(parse_version("nope"))
