# BlitzDB Wire Protocol (v2 + ops 6-8)

Frame: `[ver u8][len u32 LE][payload]`, `ver = 2`, `HEADER_LEN = 5`.
Payload kind: `0x01` Request, `0x02` BatchRequest, `0x11` Response,
`0x12` BatchResponse. Limits: frame ≤ `max_message_size` (default 1MiB),
batch ≤4096 ops, map ≤4096 entries, array/JSON nesting bounded — oversize
fails the frame, never the server.

## Ops (`Op` tag)

| Tag | Op | Fields | Semantics |
|---|---|---|---|
| 0 | Ping | — | Always ok (even unauth). Carries `_auth` handshake. |
| 1 | Insert | `table`, `values` | Validates (unless `skip_validation`), enforces unique, WAL, returns assigned id. `_idem` dedups retry. `media*` blobs ≤256KiB. |
| 2 | Get | `table`, `row_id` | Zero-copy read. Miss → err (not empty-ok). |
| 3 | Update | `table`, `row_id`, `values` | Returns moved row. Unique conflicts never partially apply. |
| 4 | Delete | `table`, `row_id` | Missing → err. True deletes free unique keys. |
| 5 | Scan | `table`, `values` window | Paginated + cursor (below). Admin/backfill pages, not hot timelines. |
| 6 | Subscribe | `table`, `values` | Bounded long-poll (below) or `_stream:1` push upgrade. |
| 7 | Find | `values {_col,_val}` | O(1) unique/PK lookup. Non-unique → err (never scanned). |
| 8 | Search | `values {_q,_limit}` | Exact-term bounded postings (≤100 rows). |

## Special `values` keys (all stripped before storage engine use)

| Key | Ops | Meaning |
|---|---|---|
| `_auth` | any | Bearer token; binds identity to the connection. Stripped. |
| `_idem` | Insert | Idempotency key → same RowId on retry. Stripped. In-memory (group-window RPO). |
| `_limit` / `_offset` | Scan/Subscribe | Page size (Scan ≤10K, Subscribe ≤1K) / skip. |
| `_order` | Scan | `asc` (default) / `desc`. |
| `_cursor` | Scan | Exclusive last-seen RowId; binary-searched, stable under inserts. |
| `_col` / `_val` | Find | Unique column + value. |
| `_q` | Search | Query text (first two terms ANDed). |
| `_since` | Subscribe | Only changes with `ts_micros > since`. |
| `_stream` | Subscribe | `1` upgrades a dedicated connection to server-push (ack + seq-ID frames until EOF/idle). Must be last frame in flight. |

## Batching (mandatory past ~8K active)

`BatchRequest{id, ops[N]}` → `BatchResponse{id, results[N]}` in order, one
frame = one read + one write. Per-op ok/err independent (partial failure
normal, never atomic). Per-op latency ≈ `batch_time / N` (throughput exact).
Recommended N = 25.

## Sharding (server-side, stable names)

App code always uses BASE table names (`posts`, never `posts_03`). When the
server configures `table_shards: {base: (N, column)}`, inserts hash
`values[column] % N` (FNV-1a, deterministic) into `{base}_{NN}` physicals;
point reads/writes route by RowId shard bits; Scan/Find fan out server-side.
RowIds are global (`shard<<56 | local`): echo them back verbatim — Gets,
Updates, Deletes, and cursors all work on globals. `Subscribe`/change-log
stay on base names; search postings resolve internally.

- Configure before data lands (no online resharding in v1); legacy
  high-bits-0 ids route to shard 0.
- Unique indexes are per-shard (global uniqueness needs the shard key to be
  the unique column, or an unsharded table).
- Rows without the shard-key column hash deterministically to one shard
  (they don't scatter — by design, not accident).

## Atomic batches (all-or-nothing)

Same `BatchRequest` layout under kind byte `0x03` (`encode_atomic_batch_request`).
The server runs all ops in one OCC transaction (`RepeatableRead`) and commits
once: either every result is ok, or every result is err with the abort reason
and nothing was applied. `BatchResponse` shape is unchanged.

- Allowed ops: `Ping`, `Get`, `Insert`, `Update`, `Delete`.
- Rejected (whole frame aborts): `Scan`, `Find`, `Subscribe`, `Search` — they
  read outside tx versioning, so they fail loudly instead of faking atomicity.
- Reads see a stable snapshot + the batch's own buffered updates/deletes
  (insert-then-get-same-row is unaddressable: IDs are assigned at commit).
- Retries use per-op `_idem` (same as single/batch): a retried identical frame
  replays cached IDs and commits an empty tx.
- Residuals: concurrent duplicate races on unique-constrained tables can both
  commit (same as non-atomic today); post-commit WAL failure bumps
  `wal_dropped` (memory-committed, durable at next snapshot); `skip_validation`
  is not honored (tx apply always validates).

## Procedures (`Op::Call`, tag 9 — FUNCTIONS)

`table: "fn:<name>"` (policy resource namespace), `values` = call arguments.
The server runs the registered procedure's steps in one OCC transaction
(`RepeatableRead`): all-or-nothing, one response row.

- Steps: `Read` (strict — missing row aborts; columns land as `<into>.<col>`
  plus `<into>.#id`), `Insert`/`Update`/`Delete` (buffered; missing target
  aborts), `CallFunction` (pure builtins: `now`/`concat`/`upper`/`lower`/
  `len`/`abs`/`coalesce`), `SetVariable`, `If` (`Equals`/`NotEquals`/
  `IsNotNull`/`GreaterOrEqual`/`LessThan`/`All`/`Any`), `Return` (ends the
  call), `Fail` (aborts with message → whole call rolls back).
- `$var` references resolve against call args + step outputs (same convention
  as `CallFunction` args). No loops in v1; fuel capped at 10K steps/runaway.
- Auth is two-level: `Custom("call")` on `fn:<name>` to invoke, plus the
  caller's table permission re-checked per DB step (invoker rights — a
  procedure can't exceed what the caller could do op-by-op).
- Response: one row `{"result": v, "_applied": [{table, id}...]}` (`id: 0`;
  a `Json`-object return flattens scalar tops). Insert-assigned IDs are
  reported in `_applied`, not during execution (`into` holds `0` meanwhile:
  referencing a just-inserted row by id in later steps is unsupported).
- `Call` inside an atomic batch is rejected (no nested transactions).
- Registration is in-process at startup in v1 (`register_procedure`);
  over-TCP deploy/versioning is future work. Strict `Int64`/`UInt64` columns
  (no coercion): pass IDs in the column's own type.

## Errors (typed strings for SDK mapping)

- `unauthorized: authentication required` → handshake first, then retry.
- `forbidden: policy denies` → do not retry (fix grants).
- `WAL backpressure...` / `group full` / `rotation in progress` → retry with
  jitter ≤3× **with the same `_idem`**, then surface.
- `row not found` / `not found` → do not retry blindly.
- `blob too large`, `batch too large`, `table not found`, `DuplicateKey`,
  type/validation errors → caller bug, don't retry.
- Transport close/EOF/timeout → reconnect, resend with same `_idem`
  (at-most-once without it, effectively-once with it inside the RPO).

## TLS

`serve_tls` speaks the identical framing post-handshake (only handshake RTT
added). PEM cert+key via CLI. Plaintext stays for loopback/bench.
