"""Reference client: one TCP connection, invisible autobatching.

The programming model never changes with scale: every method looks like a
single op. Under the hood a dedicated flusher thread drains everything
already queued and flushes it as one frame — under load the drain IS the
batch; at low load a lone op flushes immediately (no timer tax). Single-op
drains go as SINGLE frames (the Get/Scan fast paths stay hot); multi-op
drains chunk at :data:`FLUSH_CHUNK`.

Retry contract (v1, honest): a flush that fails before any response byte
is retried ONCE after reconnect iff every op is a read or an ``_idem``
insert (auto-stamped on every :meth:`insert`). Updates/deletes/calls are
never auto-retried: retry them yourself (updates are effect-idempotent;
treat delete's "not found" as success; design procedures around an app
``_idem``). Threads share one client freely.
"""

from __future__ import annotations

import queue
import socket
import threading
import time
import uuid

from .codec import FrameCodec
from .errors import SdkError, map_server_error

DEFAULT_TIMEOUT = 5.0

CLIENT_VERSION = "0.2.4"
MIN_SERVER_VERSION = "0.1.0"
PROTOCOL_VERSION_PY = 2


def parse_version(v: str):
    """Parse major.minor.patch (ignores pre-release/build metadata)."""
    core = v.split("-")[0].split("+")[0]
    parts = core.split(".")
    if len(parts) < 3:
        return None
    try:
        nums = tuple(int(x) if x.isdigit() else None for x in parts[:3])
    except ValueError:
        return None
    return nums


def check_compat(server: str, protocol: int) -> None:
    """Compatibility verdict (§35): exact protocol match plus server floor."""
    if protocol != PROTOCOL_VERSION_PY:
        raise SdkError(SdkError.SERVER,
                       f"BlitzDB client v{CLIENT_VERSION} (protocol {PROTOCOL_VERSION_PY}) "
                       f"requires server protocol {PROTOCOL_VERSION_PY} "
                       f"(server v{server} speaks v{protocol})")
    floor = parse_version(MIN_SERVER_VERSION) or (0, 1, 0)
    got = parse_version(server)
    if got is None or not all(g is not None for g in got) or tuple(got) < floor:
        raise SdkError(SdkError.SERVER,
                       f"BlitzDB client v{CLIENT_VERSION} requires BlitzDB server "
                       f">= {MIN_SERVER_VERSION} (found {server})")

MAX_FLUSH_OPS = 4096
FLUSH_CHUNK = 16


class _Pending:
    __slots__ = ("req", "retry_safe", "deadline", "event", "result", "error")

    def __init__(self, req, retry_safe: bool, deadline: float) -> None:
        self.req = req
        self.retry_safe = retry_safe
        self.deadline = deadline
        self.event = threading.Event()
        self.result = None
        self.error = None


class Client:
    """Cloneable-by-sharing handle (thread-safe; share across threads)."""

    def __init__(self, sock: socket.socket, host: str, port: int,
                 timeout: float = DEFAULT_TIMEOUT) -> None:
        self._sock = sock
        self._host = host
        self._port = port
        self._timeout = timeout
        self._codec = FrameCodec()
        self._queue: queue.Queue = queue.Queue()
        self._direct: queue.Queue = queue.Queue()
        self._wake = threading.Event()
        self._closed = False
        self._id = 1
        self._id_lock = threading.Lock()
        self._idem_prefix = uuid.uuid4().hex
        self._idem_next = 1
        self._worker = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker.start()
        # One socket, one owner: all IO happens on the worker thread.
        self._sock_lock = threading.Lock()

    @classmethod
    def connect(cls, port: int, host: str = "127.0.0.1",
                timeout: float = DEFAULT_TIMEOUT) -> "Client":
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(timeout)
        client = cls(sock, host, port, timeout)
        # Compatibility handshake first (§35): one RTT that turns version
        # skew into a readable error instead of later garbage.
        client._check_server_version()
        return client

    def _check_server_version(self) -> None:
        from .codec import FrameCodec
        codec = FrameCodec()
        frame = codec.encode_request({"id": 0, "op": "version", "table": ""})
        try:
            self._sock.sendall(frame)
            # Feed each chunk once: the codec stashes internally until the
            # single response frame completes (usually the first read).
            payloads: list = []
            while not payloads:
                chunk = self._sock.recv(4096)
                if not chunk:
                    raise SdkError(SdkError.TRANSPORT, "closed during version check")
                payloads = codec.feed(chunk)
        except SdkError:
            raise
        except Exception as err:
            raise SdkError(SdkError.TRANSPORT, f"version check: {err}")
        resp = codec.decode_response(payloads[0])
        if not resp["ok"]:
            raise SdkError(SdkError.TRANSPORT, f"version refused: {resp.get('error')}")
        rows = resp["rows"]
        if not rows:
            raise SdkError(SdkError.TRANSPORT, "empty version response")
        values = rows[0]["values"]
        server = values.get("server")
        protocol = values.get("protocol")
        if not isinstance(server, str) or not isinstance(protocol, int):
            raise SdkError(SdkError.TRANSPORT, "malformed version response")
        check_compat(server, protocol)

    def close(self) -> None:
        self._closed = True
        self._wake.set()
        try:
            self._sock.close()
        except OSError:
            pass

    # -- internals --------------------------------------------------------

    def _alloc_id(self) -> int:
        with self._id_lock:
            nid = self._id
            self._id += 1
            return nid

    def _alloc_idem(self) -> str:
        with self._id_lock:
            n = self._idem_next
            self._idem_next += 1
            return f"{self._idem_prefix}-{n}"

    def _exec(self, req: dict, retry_safe: bool, direct: bool = False) -> dict:
        if self._closed:
            raise SdkError(SdkError.CLOSED, "client closed")
        deadline = time.monotonic() + self._timeout
        pending = _Pending(req, retry_safe, deadline)
        (self._direct if direct else self._queue).put(pending)
        self._wake.set()
        # No per-op timer: the worker enforces the deadline as a queue bound
        # plus per-flush socket timeouts, and always settles every pending.
        remaining = deadline - time.monotonic()
        while not pending.event.wait(timeout=max(0.0, remaining)):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SdkError(SdkError.TIMEOUT, f"timeout after {self._timeout}s")
        if pending.error is not None:
            raise pending.error
        return pending.result

    def _worker_loop(self) -> None:
        while True:
            self._wake.wait()
            self._wake.clear()
            if self._closed:
                self._fail_all("client closed")
                return
            # Direct frames first (handshakes/probes jump the queue), then
            # drain everything queued into chunked frames, in order.
            while True:
                try:
                    pending = self._direct.get_nowait()
                except queue.Empty:
                    break
                self._flush_run([pending])
                if self._closed:
                    return
            while True:
                run = []
                while len(run) < FLUSH_CHUNK:
                    try:
                        run.append(self._queue.get_nowait())
                    except queue.Empty:
                        break
                if not run:
                    break
                self._flush_run(run)
                if self._closed:
                    return

    def _fail_all(self, msg: str) -> None:
        for q in (self._direct, self._queue):
            while True:
                try:
                    p = q.get_nowait()
                except queue.Empty:
                    break
                p.error = SdkError(SdkError.TRANSPORT, msg)
                p.event.set()

    def _flush_run(self, run: list) -> None:
        if self._closed:
            for p in run:
                p.error = SdkError(SdkError.CLOSED, "client closed")
                p.event.set()
            return
        now = time.monotonic()
        fresh = []
        for p in run:
            if now >= p.deadline:
                p.error = SdkError(SdkError.TIMEOUT, f"timeout after {self._timeout}s")
                p.event.set()
            else:
                fresh.append(p)
        if not fresh:
            return
        retry_safe = all(p.retry_safe for p in fresh)
        reqs = [p.req for p in fresh]
        try:
            resps = self._roundtrip(reqs, retry_safe)
        except SdkError as err:
            for p in fresh:
                p.error = err
                p.event.set()
            return
        for p, r in zip(fresh, resps):
            p.result = r
            p.event.set()

    def _roundtrip(self, reqs: list, retry_safe: bool) -> list:
        if len(reqs) == 1:
            frame = self._codec.encode_request(reqs[0])
        else:
            frame = self._codec.encode_batch({"id": reqs[0]["id"], "ops": reqs}, False)
        try:
            payloads = self._write_read(frame)
        except SdkError:
            if not retry_safe:
                raise SdkError(SdkError.TRANSPORT,
                               "flush failed (not retry-safe; retry manually)")
            self._reconnect()
            try:
                payloads = self._write_read(frame)
            except SdkError:
                raise SdkError(SdkError.TRANSPORT, "flush failed after reconnect")
        if len(reqs) == 1:
            return [self._codec.decode_response(payloads[0])]
        batch = self._codec.decode_batch_response(payloads[0])
        if len(batch["results"]) != len(reqs):
            raise SdkError(SdkError.TRANSPORT, "batch/response length mismatch")
        return batch["results"]

    def _write_read(self, frame: bytes) -> list:
        with self._sock_lock:
            try:
                self._sock.sendall(frame)
            except OSError as err:
                raise SdkError(SdkError.TRANSPORT, f"write failed: {err}")
            return self._read_loop()

    def _read_loop(self) -> list:
        try:
            while True:
                try:
                    data = self._sock.recv(65536)
                except socket.timeout as err:
                    raise SdkError(SdkError.TRANSPORT, f"read timeout: {err}")
                except OSError as err:
                    raise SdkError(SdkError.TRANSPORT, f"read failed: {err}")
                if not data:
                    raise SdkError(SdkError.TRANSPORT, "connection closed mid-flush")
                payloads = self._codec.feed(data)
                if payloads:
                    return payloads
        except SdkError:
            raise

    def _reconnect(self) -> None:
        with self._sock_lock:
            try:
                self._sock.close()
            except OSError:
                pass
            try:
                sock = socket.create_connection((self._host, self._port),
                                                timeout=self._timeout)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.settimeout(self._timeout)
                self._sock = sock
            except OSError as err:
                raise SdkError(SdkError.TRANSPORT, f"reconnect failed: {err}")

    def _ok_rows(self, resp: dict) -> list:
        if resp["ok"]:
            return resp["rows"]
        raise map_server_error(resp.get("error") or "unknown error")

    # -- primitive ops (each looks single; the worker batches) ---------

    def authenticate(self, token: str) -> None:
        req = {"id": self._alloc_id(), "op": "ping", "table": "",
               "values": {"_auth": token}}
        self._ok_rows(self._exec(req, True, direct=True))

    def ping(self) -> None:
        req = {"id": self._alloc_id(), "op": "ping", "table": ""}
        self._ok_rows(self._exec(req, True, direct=True))

    def insert(self, table: str, values: dict) -> dict:
        """Insert with auto ``_idem`` (safe reconnect-replay in-window)."""
        vals = dict(values)
        vals["_idem"] = self._alloc_idem()
        req = {"id": self._alloc_id(), "op": "insert", "table": table,
               "values": vals}
        rows = self._ok_rows(self._exec(req, True))
        if not rows:
            raise SdkError(SdkError.SERVER, "insert returned no rows")
        return rows[-1]

    def insert_fast(self, table: str, values: dict) -> dict:
        """Insert without ``_idem`` (expert path, bench wire parity).

        At-most-once on transport failure: never auto-retried.
        """
        req = {"id": self._alloc_id(), "op": "insert", "table": table,
               "values": values}
        rows = self._ok_rows(self._exec(req, False))
        if not rows:
            raise SdkError(SdkError.SERVER, "insert returned no rows")
        return rows[-1]

    def get(self, table: str, rid: int):
        req = {"id": self._alloc_id(), "op": "get", "table": table,
               "row_id": rid}
        resp = self._exec(req, True)
        if resp["ok"]:
            return resp["rows"][0] if resp["rows"] else None
        msg = resp.get("error") or "unknown error"
        if msg.startswith("row not found"):
            return None
        raise map_server_error(msg)

    def update(self, table: str, rid: int, values: dict) -> dict:
        """Not auto-retried: retry manually on transport error."""
        req = {"id": self._alloc_id(), "op": "update", "table": table,
               "row_id": rid, "values": values}
        rows = self._ok_rows(self._exec(req, False))
        if not rows:
            raise SdkError(SdkError.SERVER, "update returned no rows")
        return rows[-1]

    def delete(self, table: str, rid: int) -> None:
        """Not auto-retried: on transport error, re-Get first; treat a
        retry's "not found" as success (already deleted)."""
        req = {"id": self._alloc_id(), "op": "delete", "table": table,
               "row_id": rid}
        self._ok_rows(self._exec(req, False))

    def scan(self, table: str, limit: int, cursor=None, desc: bool = False) -> list:
        values = {"_limit": limit}
        if desc:
            values["_order"] = "desc"
        if cursor is not None:
            values["_cursor"] = cursor
        req = {"id": self._alloc_id(), "op": "scan", "table": table,
               "values": values}
        return self._ok_rows(self._exec(req, True))

    def find(self, table: str, column: str, value):
        req = {"id": self._alloc_id(), "op": "find", "table": table,
               "values": {"_col": column, "_val": value}}
        resp = self._exec(req, True)
        if resp["ok"]:
            return resp["rows"][0] if resp["rows"] else None
        msg = resp.get("error") or "unknown error"
        if msg == "not found":
            return None
        raise map_server_error(msg)

    def search(self, table: str, query: str, limit: int) -> list:
        req = {"id": self._alloc_id(), "op": "search", "table": table,
               "values": {"_q": query, "_limit": limit}}
        return self._ok_rows(self._exec(req, True))

    def create_table(self, schema: dict) -> str:
        """Create a table from a JSON schema dict. Never auto-retried:
        a retried-after-success call honestly reports "already exists"."""
        req = {"id": self._alloc_id(), "op": "table_create",
               "values": {"schema": schema}}
        rows = self._ok_rows(self._exec(req, False))
        if not rows:
            raise SdkError(SdkError.SERVER, "table_create returned no rows")
        name = rows[-1]["values"].get("table")
        if not isinstance(name, str):
            raise SdkError(SdkError.SERVER, "table_create response missing table")
        return name

    def call(self, fn: str, args: dict) -> dict:
        """Execute a registered procedure transactionally. Not auto-retried:
        design procedures around an application ``_idem`` argument."""
        req = {"id": self._alloc_id(), "op": "call", "table": f"fn:{fn}",
               "values": args}
        rows = self._ok_rows(self._exec(req, False))
        if not rows:
            raise SdkError(SdkError.SERVER, "call returned no rows")
        values = dict(rows[-1]["values"])
        applied = []
        raw = values.pop("_applied", None)
        if isinstance(raw, list):
            for e in raw:
                if (isinstance(e, dict) and isinstance(e.get("table"), str)
                        and isinstance(e.get("id"), int)):
                    applied.append({"table": e["table"], "id": e["id"]})
        return {"values": values, "applied": applied}

    def poll_changes(self, table: str, since: int = 0, limit: int = 100) -> list:
        """Poll recent changes (single long-poll, not a stream)."""
        req = {"id": self._alloc_id(), "op": "subscribe", "table": table,
               "values": {"_since": since, "_limit": limit}}
        return self._ok_rows(self._exec(req, True))
