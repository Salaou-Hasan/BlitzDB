//! Codec roundtrips: byte-exactness against the Rust framing rules.
//! No server needed. Run: `node --test test/codec.test.ts`
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { FrameCodec, PROTOCOL_VERSION } from '../src/codec.ts';
import type { Value } from '../src/values.ts';
import { V } from '../src/values.ts';

function roundValue(v: Value): Value {
  const c = new FrameCodec();
  // Wrap in a single-value map to reuse the request path.
  const req = { id: 1, op: 'insert' as const, table: 't', values: { v } };
  const payload = c.feed(c.encodeRequest(req))[0];
  const back = c.decodeIncoming(payload);
  assert.equal(back.kind, 'single');
  if (back.kind !== 'single') throw new Error('unreachable');
  return (back.req.values as Record<string, Value>)['v'];
}

describe('values', () => {
  it('null/bool/ints/floats roundtrip', () => {
    assert.equal(roundValue(null), null);
    assert.equal(roundValue(true), true);
    assert.equal(roundValue(42), 42); // safe int -> Int64
    assert.equal(roundValue(1.5), 1.5); // -> Float64
    assert.equal(roundValue(V.i32(-7)), -7);
    assert.equal(roundValue(V.u32(4000000000)), 4000000000);
    assert.equal(roundValue(2n ** 60n), 2n ** 60n); // bigint preserved
    assert.throws(() => roundValue(2n ** 70n), /u64 out of range/);
    assert.throws(() => roundValue(-(2n ** 70n)), /i64 out of range/);
    assert.equal(roundValue(V.f32(0.5)), V.f32(0.5).$f32); // f32 loses precision vs f64 literal
  });

  it('strings/uuid/bytes/date/timestamp', () => {
    assert.equal(roundValue('hello'), 'hello');
    const uuid = '123e4567-e89b-12d3-a456-426614174000';
    assert.equal(roundValue(uuid), uuid); // uuid-shaped -> Uuid tag, same string back
    assert.deepEqual(roundValue(V.uuid(uuid)), uuid);
    assert.deepEqual(roundValue(new Uint8Array([1, 2, 250])), new Uint8Array([1, 2, 250]));
    assert.deepEqual(roundValue(V.dec('12.50')), { $decimal: '12.50' });
    assert.deepEqual(roundValue(V.date('2026-09-05')), { $date: '2026-09-05' });
    const ts = new Date('2026-09-05T12:00:00.000Z');
    assert.deepEqual(roundValue(ts), ts);
  });

  it('arrays and json objects', () => {
    assert.deepEqual(roundValue([1, 'a', null, true]), [1, 'a', null, true]);
    assert.deepEqual(roundValue({ a: 1, b: 'x', c: [1, 2], d: { e: true } }), {
      a: 1, b: 'x', c: [1, 2], d: { e: true },
    });
  });

  it('i64/u64 boundary prefers number when safe', () => {
    assert.equal(roundValue(V.i64(9007199254740991)), 9007199254740991);
    assert.equal(typeof roundValue(V.i64(2n ** 62n)), 'bigint');
  });
});

describe('frames', () => {
  it('request/response/batch/atomic roundtrip with kinds', () => {
    const c = new FrameCodec();
    const req = { id: 7, op: 'get' as const, table: 'users', rowId: 42, values: null };
    const [p1] = c.feed(c.encodeRequest(req));
    assert.equal(p1[0], 0x01);
    const back = c.decodeIncoming(p1);
    assert.equal(back.kind, 'single');
    if (back.kind === 'single') assert.deepEqual({ ...back.req, rowId: Number(back.req.rowId) }, { ...req, rowId: 42 });

    const batch = { id: 9, ops: [req, { ...req, id: 8 }] };
    for (const atomic of [false, true]) {
      const [p] = c.feed(c.encodeBatch(batch, atomic));
      assert.equal(p[0], atomic ? 0x03 : 0x02);
      const b = c.decodeIncoming(p);
      assert.equal(b.kind, atomic ? 'atomic' : 'batch');
    }
  });

  it('feed splits streams and rejects versions', () => {
    const c = new FrameCodec();
    const req = { id: 1, op: 'ping' as const, table: '' };
    const f = c.encodeRequest(req);
    // Split mid-frame: no payload until complete.
    assert.deepEqual(c.feed(f.subarray(0, 3)), []);
    const rest = c.feed(f.subarray(3));
    assert.equal(rest.length, 1);
    const bad = Buffer.from(f);
    bad[0] = 0x7f;
    assert.throws(() => c.feed(bad), /unsupported version/);
  });

  it('batch response roundtrip', () => {
    const c = new FrameCodec();
    const resp = {
      id: 99,
      results: [
        { id: 1, ok: true, rows: [{ id: 5, values: { name: 'Ada' } }], error: null },
        { id: 2, ok: false, rows: [], error: 'gone' },
      ],
    };
    const [p] = c.feed(c.encodeBatchResponse(resp));
    assert.equal(p[0], 0x12);
    assert.deepEqual(c.decodeBatchResponse(p), resp);
  });

  it('protocol version', () => {
    assert.equal(PROTOCOL_VERSION, 2);
  });
});
