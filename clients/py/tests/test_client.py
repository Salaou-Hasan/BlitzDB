"""Client integration tests against a real BlitzDB server binary.

Spawns ``target/release/Blitz serve`` on an ephemeral port. Skipped when
the binary is missing (``cargo build -p blitz-cli``).
"""

import shutil
import socket
import subprocess
import threading
import time
import unittest
from pathlib import Path

from blitz_client import Client, SdkError

BIN = Path(__file__).resolve().parents[3] / "target" / "release" / "Blitz"
HAVE_SERVER = BIN.exists()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@unittest.skipUnless(HAVE_SERVER, "Blitz binary missing (cargo build -p blitz-cli)")
class TestClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.port = free_port()
        cls.proc = subprocess.Popen(
            [str(BIN), "serve", "--port", str(cls.port)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 10
        while True:
            try:
                probe = Client.connect(cls.port, timeout=0.5)
                probe.close()
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise RuntimeError("server did not start")
                time.sleep(0.05)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.proc.kill()

    def test_create_table(self) -> None:
        c = Client.connect(self.port)
        try:
            name = c.create_table({"table": "py_t", "columns": [
                {"name": "id", "type": "int64"},
                {"name": "v", "type": "string", "nullable": True},
            ]})
            self.assertEqual(name, "py_t")
            row = c.insert("py_t", {"id": 1})
            self.assertIsNotNone(row)
        finally:
            c.close()

    def test_crud_roundtrip(self) -> None:
        c = Client.connect(self.port)
        try:
            c.ping()
            row = c.insert("users", {"id": 900001, "name": "Ada", "email": "pycrud@x.com"})
            got = c.get("users", row["id"])
            self.assertIsNotNone(got)
            self.assertEqual(got["values"]["name"], "Ada")
            upd = c.update("users", row["id"], {"name": "Ada L."})
            self.assertEqual(upd["values"]["name"], "Ada L.")
            self.assertIsNotNone(c.find("users", "email", "pycrud@x.com"))
            self.assertIsNone(c.find("users", "email", "nope@x.com"))
            rows = c.scan("users", 1000)
            self.assertTrue(any(r["id"] == row["id"] for r in rows))
            c.delete("users", row["id"])
            self.assertIsNone(c.get("users", row["id"]))
        finally:
            c.close()

    def test_concurrent_sharing_batches(self) -> None:
        c = Client.connect(self.port)
        try:
            ids: list = []
            lock = threading.Lock()

            def worker(t: int) -> None:
                local = []
                for i in range(10):
                    row = c.insert("users", {
                        "id": 910000 + t * 100 + i,
                        "name": f"pyu{t}-{i}",
                        "email": f"pyu{t}-{i}@x.com",
                    })
                    local.append(row["id"])
                with lock:
                    ids.extend(local)

            threads = [threading.Thread(target=worker, args=(t,)) for t in range(30)]
            for th in threads:
                th.start()
            for th in threads:
                th.join()
            self.assertEqual(len(ids), 300)
            self.assertEqual(len(set(ids)), 300)
            self.assertGreaterEqual(len(c.scan("users", 1000)), 300)
        finally:
            c.close()

    def test_error_mapping(self) -> None:
        c = Client.connect(self.port)
        try:
            self.assertIsNone(c.get("users", 424242))
            with self.assertRaises(SdkError) as ctx:
                c.update("users", 424242, {"name": "x"})
            self.assertEqual(ctx.exception.kind, SdkError.NOT_FOUND)
            with self.assertRaises(SdkError) as ctx:
                c.scan("nope", 10)
            self.assertEqual(ctx.exception.kind, SdkError.NOT_FOUND)
        finally:
            c.close()

    def test_call_unknown_procedure(self) -> None:
        c = Client.connect(self.port)
        try:
            with self.assertRaises(SdkError) as ctx:
                c.call("nope", {})
            self.assertIn(ctx.exception.kind,
                          (SdkError.NOT_FOUND, SdkError.SERVER))
        finally:
            c.close()


if __name__ == "__main__":
    unittest.main()
