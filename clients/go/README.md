# blitzdb — Go SDK for BlitzDB (stdlib only)

```go
import blitzdb "github.com/Salaou-Hasan/BlitzDB/clients/go"

c, _ := blitzdb.Connect("127.0.0.1", 7420)
defer c.Close()
row, _ := c.Insert("users", map[string]any{"id": int64(1), "name": "Ada"})
```

Share one `*Client` across goroutines: a worker drains queued ops into
chunked frames (singles go as SINGLE frames so the Get/Scan fast paths
stay hot). Same retry contract as the Rust/TS/Python SDKs: reads +
`_idem` inserts retry once after reconnect; updates/deletes/calls never
auto-retry. Errors are typed (`*SdkError.Kind`: Auth/Retryable/NotFound/
Invalid/Server/Transport/Timeout/Closed).

Value mapping: native Go ints map 1:1 to wire tags (`int` → Int64);
UUID-shaped strings encode as Uuid; `time.Time` → Timestamp (ms);
`[]byte` → Bytes; single-key `{"$i32":…}`/`{"$u32"}`/`{"$i64"}`/
`{"$u64"}`/`{"$f32"}`/`{"$decimal"}`/`{"$date"}`/`{"$uuid"}`/`{"$bytes"}`/
`{"$ts"}` wrappers for exact widths; other maps → Json. The server is
type-strict: pass IDs in the column's own type.

Tests: `go test ./...` (spawns `target/release/Blitz`; skips if missing).
