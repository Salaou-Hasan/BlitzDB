# @blitzdb/client — TypeScript SDK for BlitzDB

Reference client. Zero dependencies (Node ≥22.7; runs on built-in type
stripping — no build step: `node --test 'test/*.test.ts'`).

```ts
import { Client } from './src/index.ts';
const client = await Client.connect(7420);
const row = await client.insert('users', { id: 1, name: 'Ada' });
await client.close();
```

Every method looks like a single op; the client autobatches under load
(drain → chunked frames; lone ops flush on the next tick, singles go as
SINGLE frames so the Get/Scan fast paths stay hot). Same retry contract
as the Rust SDK: reads + `_idem` inserts retry once after reconnect;
updates/deletes/calls never auto-retry. Errors are typed (`SdkError.kind`:
`Auth`/`Retryable`/`NotFound`/`Invalid`/`Server`/`Transport`/`Timeout`).

Value mapping: `number` safe-integers → Int64 (use `V.i32()` etc. for
strict columns), `bigint` → Int64/UInt64, `Date` → Timestamp,
`Uint8Array` → Bytes, `{ $decimal }`/`{ $date }`/`{ $uuid }` for the rest,
UUID-shaped strings encode as Uuid. The server is type-strict: pass IDs in
the column's own type.
