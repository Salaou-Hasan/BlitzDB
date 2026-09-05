//! BlitzDB value model for TypeScript.
//!
//! Wire tags mirror `blitz-protocol/src/codec.rs` exactly (0x00–0x13).
//! JS has one `number` type, so integer width is explicit at the edges:
//! - `bigint` ⇄ Int64/UInt64 (by sign/range; decode prefers `number` when
//!   the value is a safe integer — lossless either way).
//! - Plain `number`: safe integer → Int64 (0x05), else Float64 (0x0B).
//!   The server validates column types strictly, so use the explicit
//!   wrappers below when a column demands another width.
//! - `{ $i32: n }`, `{ $u32: n }`, `{ $i64: n|bigint }`, `{ $u64: n|bigint }`,
//!   `{ $f32: n }` force exact tags.
//! - `{ $decimal: "12.50" }` ⇄ Decimal (never a bare string).
//! - `{ $date: "YYYY-MM-DD" }` ⇄ Date (never a `Date`: those are Timestamps).
//! - UUID-shaped strings (`8-4-4-4-12` hex) encode as Uuid (0x0F) and decode
//!   back to the same string: roundtrip-stable by construction.
//! - `Date` ⇄ Timestamp (0x10, micros; ms precision — sub-ms is truncated).
//! - `Uint8Array` ⇄ Bytes. Plain objects/arrays ⇄ Json (0x12 subset).

export interface I32 { $i32: number }
export interface U32 { $u32: number }
export interface I64 { $i64: number | bigint }
export interface U64 { $u64: number | bigint }
export interface F32 { $f32: number }
export interface Decimal { $decimal: string }
export interface CivilDate { $date: string }
export interface UuidStr { $uuid: string }

export type JsonScalar = null | boolean | number | string;
export interface JsonObject { [k: string]: JsonValue }
export type JsonValue = JsonScalar | JsonValue[] | JsonObject;

export type Value =
  | null | boolean | number | bigint | string
  | Uint8Array | Date
  | I32 | U32 | I64 | U64 | F32 | Decimal | CivilDate | UuidStr
  | JsonObject | Value[];

export const V = {
  i32: (n: number): I32 => ({ $i32: n }),
  u32: (n: number): U32 => ({ $u32: n }),
  i64: (n: number | bigint): I64 => ({ $i64: n }),
  u64: (n: number | bigint): U64 => ({ $u64: n }),
  f32: (n: number): F32 => ({ $f32: n }),
  dec: (s: string): Decimal => ({ $decimal: s }),
  date: (ymd: string): CivilDate => ({ $date: ymd }),
  uuid: (s: string): UuidStr => ({ $uuid: s }),
};

export function isI32(v: Value): v is I32 {
  return typeof v === 'object' && v !== null && !(v instanceof Uint8Array) && !(v instanceof Date) && !Array.isArray(v) && '$i32' in v;
}
export function isU32(v: Value): v is U32 {
  return typeof v === 'object' && v !== null && !(v instanceof Uint8Array) && !(v instanceof Date) && !Array.isArray(v) && '$u32' in v;
}
export function isI64(v: Value): v is I64 {
  return typeof v === 'object' && v !== null && !(v instanceof Uint8Array) && !(v instanceof Date) && !Array.isArray(v) && '$i64' in v;
}
export function isU64(v: Value): v is U64 {
  return typeof v === 'object' && v !== null && !(v instanceof Uint8Array) && !(v instanceof Date) && !Array.isArray(v) && '$u64' in v;
}
export function isF32(v: Value): v is F32 {
  return typeof v === 'object' && v !== null && !(v instanceof Uint8Array) && !(v instanceof Date) && !Array.isArray(v) && '$f32' in v;
}
export function isDecimal(v: Value): v is Decimal {
  return typeof v === 'object' && v !== null && !(v instanceof Uint8Array) && !(v instanceof Date) && !Array.isArray(v) && '$decimal' in v;
}
export function isCivilDate(v: Value): v is CivilDate {
  return typeof v === 'object' && v !== null && !(v instanceof Uint8Array) && !(v instanceof Date) && !Array.isArray(v) && '$date' in v;
}
export function isUuidStr(v: Value): v is UuidStr {
  return typeof v === 'object' && v !== null && !(v instanceof Uint8Array) && !(v instanceof Date) && !Array.isArray(v) && '$uuid' in v;
}
export function isPlainObject(v: Value): v is JsonObject {
  if (typeof v !== 'object' || v === null || v instanceof Uint8Array || v instanceof Date || Array.isArray(v)) return false;
  const o = v as Record<string, unknown>;
  return !('$i32' in o || '$u32' in o || '$i64' in o || '$u64' in o || '$f32' in o || '$decimal' in o || '$date' in o || '$uuid' in o);
}

const UUID_RE = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;

export function uuidToBytes(s: string): Uint8Array {
  const hex = s.replace(/-/g, '');
  if (hex.length !== 32 || !/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error(`bad uuid: ${s}`);
  const out = new Uint8Array(16);
  for (let i = 0; i < 16; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

export function bytesToUuid(b: Uint8Array): string {
  const hex = [...b].map((x) => x.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function isUuidShaped(s: string): boolean {
  return UUID_RE.test(s);
}

// Days-from-civil-epoch (Howard Hinnant algorithms; proleptic Gregorian,
// matches chrono's `num_days_from_ce` baseline used on the Rust side).
export function daysFromCivil(y: number, m: number, d: number): number {
  const yAdj = m <= 2 ? y - 1 : y;
  const era = Math.floor(yAdj / 400);
  const yoe = yAdj - era * 400;
  const mp = (m + 9) % 12;
  const doy = Math.floor((153 * mp + 2) / 5) + d - 1;
  const doe = yoe * 365 + Math.floor(yoe / 4) - Math.floor(yoe / 100) + doy;
  return era * 146097 + doe - 719468 + 719162;
}

export function civilFromDays(z: number): [number, number, number] {
  const z2 = z - 719162 + 719468;
  const era = Math.floor(z2 / 146097);
  const doe = z2 - era * 146097;
  const yoe = Math.floor((doe - Math.floor(doe / 1460) + Math.floor(doe / 36524) - Math.floor(doe / 146096)) / 365);
  const y = yoe + era * 400;
  const doy = doe - (365 * yoe + Math.floor(yoe / 4) - Math.floor(yoe / 100));
  const mp = Math.floor((5 * doy + 2) / 153);
  const d = doy - Math.floor((153 * mp + 2) / 5) + 1;
  const m = mp < 10 ? mp + 3 : mp - 9;
  return [m <= 2 ? y + 1 : y, m, d];
}
