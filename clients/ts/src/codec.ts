//! Binary wire codec: byte-exact port of `blitz-protocol/src/codec.rs`.
//! All integers little-endian. Frames: `[ver u8][len u32le][payload]`.
//! Payloads start with a kind byte (0x01 single, 0x02 batch, 0x03 atomic,
//! 0x11 response, 0x12 batch response).

import type { Value, JsonValue } from './values.ts';
import {
  isI32, isU32, isI64, isU64, isF32, isDecimal, isCivilDate, isUuidStr, isPlainObject,
  uuidToBytes, bytesToUuid, isUuidShaped, daysFromCivil, civilFromDays,
} from './values.ts';

export const PROTOCOL_VERSION = 2;
export const HEADER_LEN = 5;
export const DEFAULT_MAX_FRAME = 8 * 1024 * 1024;

export const KIND_REQUEST = 0x01;
export const KIND_BATCH_REQUEST = 0x02;
export const KIND_ATOMIC_BATCH_REQUEST = 0x03;
export const KIND_RESPONSE = 0x11;
export const KIND_BATCH_RESPONSE = 0x12;

export type Op =
  | 'ping' | 'insert' | 'get' | 'update' | 'delete'
  | 'scan' | 'subscribe' | 'find' | 'search' | 'call';

const OP_TO_TAG: Record<Op, number> = {
  ping: 0, insert: 1, get: 2, update: 3, delete: 4,
  scan: 5, subscribe: 6, find: 7, search: 8, call: 9,
};
const TAG_TO_OP: Record<number, Op> = {
  0: 'ping', 1: 'insert', 2: 'get', 3: 'update', 4: 'delete',
  5: 'scan', 6: 'subscribe', 7: 'find', 8: 'search', 9: 'call',
};

export interface Request {
  id: number | bigint;
  op: Op;
  table: string;
  rowId?: number | bigint | null;
  values?: Record<string, Value> | null;
}

export interface RowView {
  id: number | bigint;
  values: Record<string, Value>;
}

export interface BatchRequest {
  id: number | bigint;
  ops: Request[];
}

export interface Response {
  id: number | bigint;
  ok: boolean;
  rows: RowView[];
  error?: string | null;
}

export interface BatchResponse {
  id: number | bigint;
  results: Response[];
}

export type Incoming =
  | { kind: 'single'; req: Request }
  | { kind: 'batch'; batch: BatchRequest }
  | { kind: 'atomic'; batch: BatchRequest };

export class DecodeError extends Error {}

// -- Writer ---------------------------------------------------------------

class Writer {
  private parts: Buffer[] = [];
  private len = 0;

  u8(v: number): void { const b = Buffer.allocUnsafe(1); b.writeUInt8(v & 0xff, 0); this.push(b); }
  u16(v: number): void { const b = Buffer.allocUnsafe(2); b.writeUInt16LE(v, 0); this.push(b); }
  u32(v: number): void { const b = Buffer.allocUnsafe(4); b.writeUInt32LE(v >>> 0, 0); this.push(b); }
  i16(v: number): void { const b = Buffer.allocUnsafe(2); b.writeInt16LE(v, 0); this.push(b); }
  i32(v: number): void { const b = Buffer.allocUnsafe(4); b.writeInt32LE(v, 0); this.push(b); }
  u64(v: number | bigint): void {
    const b = Buffer.allocUnsafe(8);
    b.writeBigUInt64LE(typeof v === 'bigint' ? v : BigInt(v), 0);
    this.push(b);
  }
  i64(v: number | bigint): void {
    const b = Buffer.allocUnsafe(8);
    b.writeBigInt64LE(typeof v === 'bigint' ? v : BigInt(Math.trunc(v as number)), 0);
    this.push(b);
  }
  f32(v: number): void { const b = Buffer.allocUnsafe(4); b.writeFloatLE(v, 0); this.push(b); }
  f64(v: number): void { const b = Buffer.allocUnsafe(8); b.writeDoubleLE(v, 0); this.push(b); }
  str(s: string): void {
    const b = Buffer.from(s, 'utf8');
    this.u32(b.length);
    this.push(b);
  }
  raw(b: Uint8Array): void { this.push(Buffer.from(b)); }

  private push(b: Buffer): void { this.parts.push(b); this.len += b.length; }
  bytes(): Buffer { return Buffer.concat(this.parts, this.len); }

  value(v: Value): void {
    if (v === null) { this.u8(0x00); return; }
    if (typeof v === 'boolean') { this.u8(0x01); this.u8(v ? 1 : 0); return; }
    if (typeof v === 'bigint') {
      if (v >= 0n) {
        if (v > 0xffffffffffffffffn) throw new DecodeError(`u64 out of range: ${v}`);
        this.u8(0x09); this.u64(v);
      } else {
        if (v < -(2n ** 63n)) throw new DecodeError(`i64 out of range: ${v}`);
        this.u8(0x05); this.i64(v);
      }
      return;
    }
    if (typeof v === 'number') {
      if (Number.isSafeInteger(v)) { this.u8(0x05); this.i64(v); }
      else { this.u8(0x0b); this.f64(v); }
      return;
    }
    if (typeof v === 'string') {
      if (isUuidShaped(v)) { this.u8(0x0f); this.raw(uuidToBytes(v)); }
      else { this.u8(0x0d); this.str(v); }
      return;
    }
    if (v instanceof Uint8Array) {
      this.u8(0x0e);
      this.u32(v.length);
      this.raw(v);
      return;
    }
    if (v instanceof Date) {
      this.u8(0x10);
      this.i64(BigInt(v.getTime()) * 1000n);
      return;
    }
    if (Array.isArray(v)) {
      if (v.length > 65536) throw new DecodeError(`array too large: ${v.length}`);
      this.u8(0x13);
      this.u32(v.length);
      for (const item of v) this.value(item);
      return;
    }
    if (isI32(v)) { this.u8(0x02); this.u8(v.$i32 & 0xff); return; }
    if (isU32(v)) { this.u8(0x08); this.u32(v.$u32); return; }
    if (isI64(v)) { this.u8(0x05); this.i64(v.$i64); return; }
    if (isU64(v)) { this.u8(0x09); this.u64(v.$u64); return; }
    if (isF32(v)) { this.u8(0x0a); this.f32(v.$f32); return; }
    if (isDecimal(v)) { this.u8(0x0c); this.str(v.$decimal); return; }
    if (isCivilDate(v)) {
      const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(v.$date);
      if (!m) throw new DecodeError(`bad $date: ${v.$date}`);
      const y = +m[1], mo = +m[2], d = +m[3];
      this.u8(0x11);
      this.i32(daysFromCivil(y, mo, d));
      this.u32(mo);
      this.u32(d);
      return;
    }
    if (isUuidStr(v)) { this.u8(0x0f); this.raw(uuidToBytes(v.$uuid)); return; }
    if (isPlainObject(v)) {
      this.u8(0x12);
      this.json(v as Record<string, JsonValue>);
      return;
    }
    throw new DecodeError(`unencodable value: ${String(v)}`);
  }

  json(j: JsonValue): void {
    if (j === null) { this.u8(0x00); return; }
    if (typeof j === 'boolean') { this.u8(0x01); this.u8(j ? 1 : 0); return; }
    if (typeof j === 'number') {
      if (Number.isSafeInteger(j)) { this.u8(0x05); this.i64(j); }
      else { this.u8(0x0b); this.f64(j); }
      return;
    }
    if (typeof j === 'string') { this.u8(0x0d); this.str(j); return; }
    if (Array.isArray(j)) {
      this.u8(0x13);
      this.u32(j.length);
      for (const item of j) this.json(item);
      return;
    }
    const keys = Object.keys(j);
    this.u8(0x12);
    this.u32(keys.length);
    for (const k of keys) {
      this.str(k);
      this.json((j as Record<string, JsonValue>)[k]);
    }
  }

  map(m: Record<string, Value>): void {
    const keys = Object.keys(m);
    this.u32(keys.length);
    for (const k of keys) {
      this.str(k);
      this.value(m[k]);
    }
  }

  requestBody(r: Request): void {
    this.u64(r.id);
    const tag = OP_TO_TAG[r.op];
    if (tag === undefined) throw new DecodeError(`unknown op: ${r.op}`);
    this.u8(tag);
    this.str(r.table);
    if (r.rowId === undefined || r.rowId === null) this.u8(0);
    else { this.u8(1); this.u64(r.rowId); }
    if (r.values === undefined || r.values === null) this.u8(0);
    else { this.u8(1); this.map(r.values); }
  }

  responseBody(r: Response): void {
    this.u64(r.id);
    this.u8(r.ok ? 1 : 0);
    this.u32(r.rows.length);
    for (const row of r.rows) {
      this.u64(row.id);
      this.map(row.values);
    }
    if (r.error === undefined || r.error === null) this.u8(0);
    else { this.u8(1); this.str(r.error); }
  }
}

// -- Reader ---------------------------------------------------------------

class Reader {
  private off = 0;
  private buf: Buffer;
  constructor(buf: Buffer) { this.buf = buf; }

  get remaining(): number { return this.buf.length - this.off; }

  u8(): number { this.need(1); return this.buf.readUInt8(this.off++); }
  u16(): number { this.need(2); const v = this.buf.readUInt16LE(this.off); this.off += 2; return v; }
  i16(): number { this.need(2); const v = this.buf.readInt16LE(this.off); this.off += 2; return v; }
  u32(): number { this.need(4); const v = this.buf.readUInt32LE(this.off); this.off += 4; return v; }
  i32(): number { this.need(4); const v = this.buf.readInt32LE(this.off); this.off += 4; return v; }
  u64(): number | bigint {
    this.need(8);
    const v = this.buf.readBigUInt64LE(this.off);
    this.off += 8;
    return v <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(v) : v;
  }
  i64(): number | bigint {
    this.need(8);
    const v = this.buf.readBigInt64LE(this.off);
    this.off += 8;
    return (v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER)) ? Number(v) : v;
  }
  f32(): number { this.need(4); const v = this.buf.readFloatLE(this.off); this.off += 4; return v; }
  f64(): number { this.need(8); const v = this.buf.readDoubleLE(this.off); this.off += 8; return v; }
  str(): string {
    const len = this.u32();
    this.need(len);
    const s = this.buf.toString('utf8', this.off, this.off + len);
    this.off += len;
    return s;
  }
  take(n: number, what: string): Buffer {
    this.need(n, what);
    const b = this.buf.subarray(this.off, this.off + n);
    this.off += n;
    return b;
  }

  private need(n: number, what = 'frame'): void {
    if (this.remaining < n) throw new DecodeError(`truncated ${what}: need ${n}, have ${this.remaining}`);
  }

  value(): Value {
    const tag = this.u8();
    switch (tag) {
      case 0x00: return null;
      case 0x01: return this.u8() !== 0;
      case 0x02: { const b = this.u8(); return b > 127 ? b - 256 : b; }
      case 0x03: return this.i16();
      case 0x04: return this.i32();
      case 0x05: return this.i64();
      case 0x06: return this.u8();
      case 0x07: return this.u16();
      case 0x08: return this.u32();
      case 0x09: return this.u64();
      case 0x0a: return this.f32();
      case 0x0b: return this.f64();
      case 0x0c: return { $decimal: this.str() };
      case 0x0d: return this.str();
      case 0x0e: {
        const len = this.u32();
        return new Uint8Array(this.take(len, 'bytes'));
      }
      case 0x0f: return bytesToUuid(new Uint8Array(this.take(16, 'uuid')));
      case 0x10: {
        const micros = this.i64();
        const ms = typeof micros === 'bigint' ? micros / 1000n : Math.trunc(micros / 1000);
        return new Date(typeof ms === 'bigint' ? Number(ms) : ms);
      }
      case 0x11: {
        const days = this.i32();
        const month = this.u32();
        const day = this.u32();
        const [y, mo, d] = civilFromDays(days);
        if (mo !== month || d !== day) throw new DecodeError('bad date');
        const pad = (n: number, w: number) => String(n).padStart(w, '0');
        return { $date: `${pad(y, 4)}-${pad(mo, 2)}-${pad(d, 2)}` };
      }
      case 0x12: return this.json();
      case 0x13: {
        const count = this.u32();
        if (count > 65536) throw new DecodeError(`array too large: ${count}`);
        const items: Value[] = [];
        for (let i = 0; i < count; i++) items.push(this.value());
        return items;
      }
      default: throw new DecodeError(`unknown value tag: 0x${tag.toString(16)}`);
    }
  }

  json(): JsonValue {
    const tag = this.u8();
    switch (tag) {
      case 0x00: return null;
      case 0x01: return this.u8() !== 0;
      case 0x05: return this.i64() as number;
      case 0x09: return this.u64() as number;
      case 0x0b: return this.f64();
      case 0x0d: return this.str();
      case 0x12: {
        const count = this.u32();
        if (count > 4096) throw new DecodeError(`json object too large: ${count}`);
        const obj: Record<string, JsonValue> = {};
        for (let i = 0; i < count; i++) obj[this.str()] = this.json();
        return obj;
      }
      case 0x13: {
        const count = this.u32();
        if (count > 65536) throw new DecodeError(`json array too large: ${count}`);
        const items: JsonValue[] = [];
        for (let i = 0; i < count; i++) items.push(this.json());
        return items;
      }
      default: throw new DecodeError(`bad json tag: 0x${tag.toString(16)}`);
    }
  }

  map(): Record<string, Value> {
    const count = this.u32();
    const out: Record<string, Value> = {};
    for (let i = 0; i < count; i++) out[this.str()] = this.value();
    return out;
  }

  requestBody(): Request {
    const id = this.u64();
    const op = TAG_TO_OP[this.u8()];
    if (op === undefined) throw new DecodeError('unknown op tag');
    const table = this.str();
    const hasRow = this.u8();
    const rowId = hasRow !== 0 ? this.u64() : null;
    const hasVals = this.u8();
    const values = hasVals !== 0 ? this.map() : null;
    return { id, op, table, rowId, values };
  }

  rowView(): RowView {
    const id = this.u64();
    return { id, values: this.map() };
  }

  responseBody(): Response {
    const id = this.u64();
    const ok = this.u8() !== 0;
    const n = this.u32();
    const rows: RowView[] = [];
    for (let i = 0; i < n; i++) rows.push(this.rowView());
    const hasErr = this.u8();
    const error = hasErr !== 0 ? this.str() : null;
    return { id, ok, rows, error };
  }

  end(expected: number): void {
    if (this.remaining !== expected) throw new DecodeError(`trailing bytes: ${this.remaining}`);
  }
}

// -- FrameCodec ------------------------------------------------------------

/** Streaming frame codec: `feed()` buffers TCP chunks, yields payloads. */
export class FrameCodec {
  private stash: Buffer = Buffer.alloc(0);
  public maxFrame: number;
  constructor(maxFrame: number = DEFAULT_MAX_FRAME) { this.maxFrame = maxFrame; }

  /** Frame one payload: `[ver][len le][kind...]`. */
  frame(payload: Buffer): Buffer {
    const head = Buffer.allocUnsafe(HEADER_LEN);
    head.writeUInt8(PROTOCOL_VERSION, 0);
    head.writeUInt32LE(payload.length, 1);
    return Buffer.concat([head, payload]);
  }

  /** Push bytes; returns all complete payloads (version + size checked). */
  feed(chunk: Buffer): Buffer[] {
    this.stash = Buffer.concat([this.stash, chunk]);
    const out: Buffer[] = [];
    while (this.stash.length >= HEADER_LEN) {
      const ver = this.stash.readUInt8(0);
      if (ver !== PROTOCOL_VERSION) throw new DecodeError(`unsupported version: ${ver}`);
      const len = this.stash.readUInt32LE(1);
      if (len > this.maxFrame) throw new DecodeError(`frame too large: ${len} > ${this.maxFrame}`);
      if (this.stash.length < HEADER_LEN + len) break;
      out.push(this.stash.subarray(HEADER_LEN, HEADER_LEN + len));
      this.stash = this.stash.subarray(HEADER_LEN + len);
    }
    return out;
  }

  encodeRequest(req: Request): Buffer {
    const w = new Writer();
    w.u8(KIND_REQUEST);
    w.requestBody(req);
    return this.frame(w.bytes());
  }

  encodeBatch(batch: BatchRequest, atomic: boolean): Buffer {
    if (batch.ops.length > 4096) throw new DecodeError(`batch too large: ${batch.ops.length}`);
    const w = new Writer();
    w.u8(atomic ? KIND_ATOMIC_BATCH_REQUEST : KIND_BATCH_REQUEST);
    w.u64(batch.id);
    w.u32(batch.ops.length);
    for (const op of batch.ops) w.requestBody(op);
    return this.frame(w.bytes());
  }

  decodeIncoming(payload: Buffer): Incoming {
    const r = new Reader(payload);
    const kind = r.u8();
    if (kind === KIND_REQUEST) {
      const req = r.requestBody();
      r.end(0);
      return { kind: 'single', req };
    }
    if (kind === KIND_BATCH_REQUEST || kind === KIND_ATOMIC_BATCH_REQUEST) {
      const id = r.u64();
      const count = r.u32();
      if (count > 4096) throw new DecodeError(`batch too large: ${count}`);
      const ops: Request[] = [];
      for (let i = 0; i < count; i++) ops.push(r.requestBody());
      r.end(0);
      const batch = { id, ops };
      return kind === KIND_ATOMIC_BATCH_REQUEST ? { kind: 'atomic', batch } : { kind: 'batch', batch };
    }
    throw new DecodeError(`unknown incoming kind: 0x${kind.toString(16)}`);
  }

  decodeResponse(payload: Buffer): Response {
    const r = new Reader(payload);
    const kind = r.u8();
    if (kind !== KIND_RESPONSE) throw new DecodeError(`wrong payload kind: 0x${kind.toString(16)}`);
    const resp = r.responseBody();
    r.end(0);
    return resp;
  }

  decodeBatchResponse(payload: Buffer): BatchResponse {
    const r = new Reader(payload);
    const kind = r.u8();
    if (kind !== KIND_BATCH_RESPONSE) throw new DecodeError(`wrong payload kind: 0x${kind.toString(16)}`);
    const id = r.u64();
    const count = r.u32();
    if (count > 4096) throw new DecodeError(`batch response too large: ${count}`);
    const results: Response[] = [];
    for (let i = 0; i < count; i++) results.push(r.responseBody());
    r.end(0);
    return { id, results };
  }

  encodeBatchResponse(resp: BatchResponse): Buffer {
    const w = new Writer();
    w.u8(KIND_BATCH_RESPONSE);
    w.u64(resp.id);
    w.u32(resp.results.length);
    for (const r of resp.results) w.responseBody(r);
    return this.frame(w.bytes());
  }
}
