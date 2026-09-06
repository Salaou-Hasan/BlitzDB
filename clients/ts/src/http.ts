//! Fetch-based client over the HTTP bridge (`POST /v1/op`, `/v1/batch`).
//! Same method names and retry contract as the TCP `Client`, for runtimes
//! without sockets (browsers, workers, React Native): reads + `_idem`
//! inserts are safe to retry on network failure; updates/deletes/calls
//! never auto-retry. No batching (one fetch per call; HTTP keep-alive
//! reuses the socket underneath). Errors map through the same table.
//!
//! Value rules across the JSON boundary: safe-integer numbers → Int64,
//! other numbers → Float64 (server is type-strict — use the `V.*`
//! wrappers from `values.ts`, which serialize to the bridge's `$`
//! forms... except plain numbers: pass `V.i32()` etc. where exactness
//! matters); `bigint` → number when safe, else RangeError (pre-stringify
//! huge IDs yourself); UUID-shaped strings → Uuid; `Uint8Array` → base64
//! `$bytes`; `Date` → `$ts` micros; `{ $decimal }` / `{ $date }` pass
//! through. Outbound big u64 (sharded RowIds) arrive as STRINGS and are
//! normalized to `bigint` for `id` fields.

import { SdkError, mapServerError } from './errors.ts';
import type { Value } from './values.ts';
import { isI32, isU32, isI64, isU64, isF32, isDecimal, isCivilDate, isUuidStr, isPlainObject } from './values.ts';

export interface HttpCallResult {
  values: Record<string, Value>;
  applied: Array<{ table: string; id: number | bigint }>;
}

export interface HttpOptions {
  token?: string;
  timeoutMs?: number;
}

const DEFAULT_TIMEOUT_MS = 5000;

type Json = null | boolean | number | string | Json[] | { [k: string]: Json };

function toJson(v: Value): Json {
  if (v === null || typeof v === 'boolean') return v;
  if (typeof v === 'bigint') {
    if (v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(v);
    throw new RangeError(`bigint out of safe range (pre-stringify): ${v}`);
  }
  if (typeof v === 'number') return v;
  if (typeof v === 'string') return v;
  if (v instanceof Uint8Array) {
    let bin = '';
    for (const b of v) bin += String.fromCharCode(b);
    return { $bytes: btoa(bin) };
  }
  if (v instanceof Date) return { $ts: Math.trunc(v.getTime() * 1000) };
  if (Array.isArray(v)) return v.map(toJson);
  if (isI32(v)) return { $i32: v.$i32 };
  if (isU32(v)) return { $u32: v.$u32 };
  if (isI64(v)) return { $i64: typeof v.$i64 === 'bigint' ? Number(v.$i64) : v.$i64 };
  if (isU64(v)) return { $u64: typeof v.$u64 === 'bigint' ? Number(v.$u64) : v.$u64 };
  if (isF32(v)) return { $f32: v.$f32 };
  if (isDecimal(v)) return { $decimal: v.$decimal };
  if (isCivilDate(v)) return { $date: v.$date };
  if (isUuidStr(v)) return { $uuid: v.$uuid };
  if (isPlainObject(v)) {
    const out: Record<string, Json> = {};
    for (const [k, item] of Object.entries(v)) out[k] = toJson(item as Value);
    return out;
  }
  throw new SdkError('Invalid', `unencodable value: ${String(v)}`);
}

function fromJson(j: Json): Value {
  if (j === null || typeof j === 'boolean' || typeof j === 'string') return j;
  if (typeof j === 'number') return j;
  if (Array.isArray(j)) return j.map(fromJson);
  const o = j as Record<string, Json>;
  if (typeof o['$decimal'] === 'string') return { $decimal: o['$decimal'] as string };
  if (typeof o['$date'] === 'string') return { $date: o['$date'] as string };
  if (typeof o['$uuid'] === 'string') return { $uuid: o['$uuid'] as string };
  if (typeof o['$bytes'] === 'string') {
    const bin = atob(o['$bytes'] as string);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }
  if (typeof o['$ts'] === 'number') return new Date((o['$ts'] as number) / 1000);
  const out: Record<string, Value> = {};
  for (const [k, item] of Object.entries(o)) out[k] = fromJson(item);
  return out;
}

function normId(id: unknown): number | bigint {
  if (typeof id === 'bigint') return id;
  if (typeof id === 'number') return id;
  if (typeof id === 'string' && /^[0-9]+$/.test(id)) {
    try {
      const b = BigInt(id);
      return b <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(b) : b;
    } catch {
      return 0;
    }
  }
  return 0;
}

export class HttpClient {
  private baseUrl: string;
  private token?: string;
  private timeoutMs: number;
  private nextId = 1;

  constructor(baseUrl: string, opts: HttpOptions = {}) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.token = opts.token;
    this.timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  }

  setToken(token: string | undefined): void {
    this.token = token;
  }

  private async post<T>(path: '/v1/op' | '/v1/batch', body: unknown): Promise<{ status: number; json: T }> {
    const headers: Record<string, string> = { 'content-type': 'application/json' };
    if (this.token) headers['authorization'] = `Bearer ${this.token}`;
    let res: Response;
    try {
      res = await fetch(`${this.baseUrl}${path}`, {
        method: 'POST',
        headers,
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(this.timeoutMs),
      });
    } catch (e) {
      throw new SdkError('Transport', `fetch failed: ${String(e)}`);
    }
    let json: T;
    try {
      json = (await res.json()) as T;
    } catch {
      throw new SdkError('Transport', `non-JSON response (status ${res.status})`);
    }
    return { status: res.status, json };
  }

  private async callOp(body: Record<string, unknown>): Promise<{ id: number | bigint; ok: boolean; rows: Array<{ id: unknown; values: Record<string, Json> }>; error?: string }> {
    const { status, json } = await this.post<{ id: number | bigint; ok: boolean; rows: Array<{ id: unknown; values: Record<string, Json> }>; error?: string }>('/v1/op', body);
    if (status === 401 || status === 403) {
      throw mapServerError(status === 401 ? `unauthorized: ${json.error ?? ''}` : `forbidden: ${json.error ?? ''}`);
    }
    if (status === 400) throw new SdkError('Invalid', String(json.error ?? 'bad request'));
    if (status !== 200) throw new SdkError('Transport', `status ${status}`);
    return json;
  }

  private rowsOf(resp: { ok: boolean; rows: Array<{ id: unknown; values: Record<string, Json> }>; error?: string }): Array<{ id: number | bigint; values: Record<string, Value> }> {
    if (!resp.ok) throw mapServerError(resp.error ?? 'unknown error');
    return resp.rows.map((r) => {
      const values: Record<string, Value> = {};
      for (const [k, v] of Object.entries(r.values)) values[k] = fromJson(v);
      return { id: normId(r.id), values };
    });
  }

  async ping(): Promise<void> {
    await this.callOp({ id: this.nextId++, op: 'ping' });
  }

  async authenticate(token: string): Promise<void> {
    this.token = token;
    await this.ping();
  }

  async insert(table: string, values: Record<string, Value>): Promise<{ id: number | bigint; values: Record<string, Value> }> {
    const idem = `${Date.now().toString(36)}-${(this.nextId * 7919).toString(36)}`;
    const body: Record<string, unknown> = {
      id: this.nextId++, op: 'insert', table,
      values: { ...mapValues(values), _idem: idem },
    };
    const rows = this.rowsOf(await this.callOp(body));
    const row = rows.pop();
    if (!row) throw new SdkError('Server', 'insert returned no rows');
    return row;
  }

  async insertFast(table: string, values: Record<string, Value>): Promise<{ id: number | bigint; values: Record<string, Value> }> {
    const body: Record<string, unknown> = { id: this.nextId++, op: 'insert', table, values: mapValues(values) };
    const rows = this.rowsOf(await this.callOp(body));
    const row = rows.pop();
    if (!row) throw new SdkError('Server', 'insert returned no rows');
    return row;
  }

  async get(table: string, id: number | bigint): Promise<{ id: number | bigint; values: Record<string, Value> } | null> {
    const body: Record<string, unknown> = { id: this.nextId++, op: 'get', table, row_id: id.toString() };
    const resp = await this.callOp(body);
    if (resp.ok) return this.rowsOf(resp)[0] ?? null;
    const msg = resp.error ?? 'unknown error';
    if (msg.startsWith('row not found')) return null;
    throw mapServerError(msg);
  }

  async update(table: string, id: number | bigint, values: Record<string, Value>): Promise<{ id: number | bigint; values: Record<string, Value> }> {
    const body: Record<string, unknown> = { id: this.nextId++, op: 'update', table, row_id: id.toString(), values: mapValues(values) };
    const rows = this.rowsOf(await this.callOp(body));
    const row = rows.pop();
    if (!row) throw new SdkError('Server', 'update returned no rows');
    return row;
  }

  async delete(table: string, id: number | bigint): Promise<void> {
    const body: Record<string, unknown> = { id: this.nextId++, op: 'delete', table, row_id: id.toString() };
    this.rowsOf(await this.callOp(body));
  }

  async scan(table: string, limit: number, cursor?: number | bigint | null, desc = false): Promise<Array<{ id: number | bigint; values: Record<string, Value> }>> {
    const values: Record<string, unknown> = { _limit: limit };
    if (desc) values['_order'] = 'desc';
    if (cursor !== undefined && cursor !== null) values['_cursor'] = cursor.toString();
    const body: Record<string, unknown> = { id: this.nextId++, op: 'scan', table, values };
    return this.rowsOf(await this.callOp(body));
  }

  async find(table: string, column: string, value: Value): Promise<{ id: number | bigint; values: Record<string, Value> } | null> {
    const body: Record<string, unknown> = { id: this.nextId++, op: 'find', table, values: { _col: column, _val: toJson(value) } };
    const resp = await this.callOp(body);
    if (resp.ok) return this.rowsOf(resp)[0] ?? null;
    const msg = resp.error ?? 'unknown error';
    if (msg === 'not found') return null;
    throw mapServerError(msg);
  }

  async search(table: string, query: string, limit: number): Promise<Array<{ id: number | bigint; values: Record<string, Value> }>> {
    const body: Record<string, unknown> = { id: this.nextId++, op: 'search', table, values: { _q: query, _limit: limit } };
    return this.rowsOf(await this.callOp(body));
  }

  async call(fn: string, args: Record<string, Value>): Promise<HttpCallResult> {
    const body: Record<string, unknown> = { id: this.nextId++, op: 'call', table: `fn:${fn}`, values: mapValues(args) };
    const rows = this.rowsOf(await this.callOp(body));
    const row = rows.pop();
    if (!row) throw new SdkError('Server', 'call returned no rows');
    const values = { ...row.values };
    const applied: Array<{ table: string; id: number | bigint }> = [];
    const raw = values['_applied'];
    delete values['_applied'];
    if (Array.isArray(raw)) {
      for (const e of raw as Array<{ table?: unknown; id?: unknown }>) {
        if (typeof e?.table === 'string' && (typeof e?.id === 'number' || typeof e?.id === 'bigint')) {
          applied.push({ table: e.table, id: e.id });
        }
      }
    }
    return { values, applied };
  }

  async pollChanges(table: string, since = 0, limit = 100): Promise<Array<{ id: number | bigint; values: Record<string, Value> }>> {
    const body: Record<string, unknown> = { id: this.nextId++, op: 'subscribe', table, values: { _since: since, _limit: limit } };
    return this.rowsOf(await this.callOp(body));
  }

  /** Live change stream (SSE): calls `onRecord` per record until aborted. */
  async stream(table: string, onRecord: (rec: { table: string; op: string; row_id: number | bigint; ts: number }) => void, opts: { since?: number; signal?: AbortSignal } = {}): Promise<void> {
    const params = new URLSearchParams({ table, since: String(opts.since ?? 0) });
    if (this.token) params.set('token', this.token);
    let res: globalThis.Response;
    try {
      res = await fetch(`${this.baseUrl}/v1/stream?${params}`, {
        headers: this.token ? { authorization: `Bearer ${this.token}` } : {},
        signal: opts.signal,
      });
    } catch (e) {
      throw new SdkError('Transport', `stream failed: ${String(e)}`);
    }
    if (res.status === 401 || res.status === 403) throw new SdkError('Auth', `stream denied (status ${res.status})`);
    if (!res.ok || !res.body) throw new SdkError('Transport', `stream status ${res.status}`);
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      buf += decoder.decode(value, { stream: true });
      let idx: number;
      while ((idx = buf.indexOf('\n\n')) >= 0) {
        const frame = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        for (const line of frame.split('\n')) {
          const text = line.startsWith(':') ? '' : line.replace(/^data:\s?/, '');
          if (!line.startsWith('data:')) continue;
          try {
            const rec = JSON.parse(text) as { table?: unknown; op?: unknown; row_id?: unknown; ts?: unknown };
            if (typeof rec.table === 'string' && typeof rec.op === 'string') {
              onRecord({ table: rec.table, op: rec.op, row_id: normId(rec.row_id), ts: typeof rec.ts === 'number' ? rec.ts : 0 });
            }
          } catch {
            // Skip malformed frames (comments/keep-alives carry no data).
          }
        }
      }
    }
  }
}

function mapValues(values: Record<string, Value>): Record<string, Json> {
  const out: Record<string, Json> = {};
  for (const [k, v] of Object.entries(values)) out[k] = toJson(v);
  return out;
}
