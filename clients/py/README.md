# blitz-client — Python SDK for BlitzDB (stdlib only)

```python
from blitz_client import Client
c = Client.connect(7420)
row = c.insert("users", {"id": 1, "name": "Ada"})
c.close()
```

Every method looks like a single op; a flusher thread autobatches across
threads sharing one client (drain → 16-op chunks; singles go as SINGLE
frames). Same retry contract as the Rust/TS SDKs: reads + `_idem`
inserts retry once after reconnect; updates/deletes/calls never
auto-retry. Errors are typed (`SdkError.kind`: `Auth`/`Retryable`/
`NotFound`/`Invalid`/`Server`/`Transport`/`Timeout`).

Value mapping: `bool` → Boolean (before int!), safe `int` → Int64 (use
`V.i32()` etc. for strict columns, `V.u64()` past int64), `float` →
Float64, UUID-shaped strings → Uuid, `bytes` → Bytes, `datetime` →
Timestamp (naive assumed UTC), `date` → `$date`, `{"$decimal": s}` →
Decimal, `list`/`tuple` → Array, other dicts → Json. The server is
type-strict: pass IDs in the column's own type.

Tests: `PYTHONPATH=. python3 -m unittest discover -s tests` (spawns
`target/release/Blitz`; skips if missing).
