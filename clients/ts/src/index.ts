//! @blitzdb/client — reference TypeScript SDK for BlitzDB.
//!
//! ```ts
//! import { Client } from '@blitzdb/client';
//! const client = await Client.connect(7420);
//! const row = await client.insert('users', { id: 1, name: 'Ada' });
//! ```
//!
//! Every method looks like a single op; the client autobatches under load.
//! See `client.ts` for the exact retry contract.

export { Client, MAX_FLUSH_OPS, FLUSH_CHUNK } from './client.ts';
export type { CallResult, ClientOptions } from './client.ts';
export { HttpClient } from './http.ts';
export type { HttpCallResult, HttpOptions } from './http.ts';
export { SdkError, mapServerError } from './errors.ts';
export type { SdkErrorKind } from './errors.ts';
export { FrameCodec, PROTOCOL_VERSION, DEFAULT_MAX_FRAME } from './codec.ts';
export type { Op, Request, Response, RowView, BatchRequest, BatchResponse, Incoming } from './codec.ts';
export { V } from './values.ts';
export type { Value, JsonValue, JsonObject, I32, U32, I64, U64, F32, Decimal, CivilDate, UuidStr } from './values.ts';
