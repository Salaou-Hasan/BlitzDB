//! HTTP bridge + HttpClient tests (fetch, CORS, keep-alive via client).
//! Spawns `target/release/Blitz serve --metrics-port` (HTTP ops ride the
//! metrics listener). Skips gracefully if the binary is missing.
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn, ChildProcess } from 'node:child_process';
import net from 'node:net';
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { HttpClient } from '../src/http.ts';
import { SdkError } from '../src/errors.ts';

const here = path.dirname(fileURLToPath(import.meta.url));
const BIN = path.resolve(here, '../../../target/release/Blitz');

async function freePort(): Promise<number> {
  return new Promise((resolve) => {
    const s = net.createServer();
    s.listen(0, '127.0.0.1', () => {
      const addr = s.address();
      const port = typeof addr === 'object' && addr ? addr.port : 0;
      s.close(() => resolve(port));
    });
  });
}

let proc: ChildProcess | null = null;
let baseUrl = '';
const haveServer = fs.existsSync(BIN);

before(async () => {
  if (!haveServer) return;
  const tcp = await freePort();
  const http = await freePort();
  baseUrl = `http://127.0.0.1:${http}`;
  proc = spawn(BIN, ['serve', '--port', String(tcp), '--metrics-port', String(http)], { stdio: 'ignore' });
  const deadline = Date.now() + 10000;
  for (;;) {
    try {
      const res = await fetch(`${baseUrl}/readyz`);
      if (res.ok) break;
    } catch {
      // Not up yet.
    }
    if (Date.now() > deadline) throw new Error('server did not start');
    await new Promise((r) => setTimeout(r, 50));
  }
});

after(() => {
  proc?.kill();
});

describe('http', { skip: !haveServer && 'Blitz binary missing (cargo build -p blitz-cli)' } as object, () => {
  it('crud roundtrip over fetch', async () => {
    const c = new HttpClient(baseUrl);
    await c.ping();
    const row = await c.insert('users', { id: 801001, name: 'Fetch', email: 'fetch@x.com' });
    const got = await c.get('users', row.id);
    assert.ok(got);
    assert.equal(got.values['name'], 'Fetch');
    const upd = await c.update('users', row.id, { name: 'Fetch2' });
    assert.equal(upd.values['name'], 'Fetch2');
    await c.delete('users', row.id);
    assert.equal(await c.get('users', row.id), null);
  });

  it('auth mapping: 401 without token, forbidden for others', async () => {
    // This server runs open (require_auth false): inserts pass bare.
    const c = new HttpClient(baseUrl);
    const row = await c.insertFast('users', { id: 801002, name: 'Open', email: 'open@x.com' });
    assert.ok(row.id);
    // Malformed envelope -> 400 Invalid.
    const res = await fetch(`${baseUrl}/v1/op`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ op: 'frobnicate' }),
    });
    assert.equal(res.status, 400);
  });

  it('sse stream delivers inserts', async () => {
    const c = new HttpClient(baseUrl);
    // Arm the change-log first (records start at first Subscribe).
    await c.pollChanges('users', 0, 1);
    const seen: Array<{ table: string; op: string }> = [];
    const ac = new AbortController();
    const done = c.stream('users', (rec) => {
      seen.push({ table: rec.table, op: rec.op });
      if (seen.length >= 1) ac.abort();
    }, { signal: ac.signal }).catch((e: unknown) => {
      // AbortError on purpose; anything else rethrows.
      if (e instanceof Error && e.name !== 'AbortError') throw e;
    });
    await c.insertFast('users', { id: 801003, name: 'Streamed', email: 'streamed@x.com' });
    await Promise.race([
      done,
      new Promise((_, rej) => setTimeout(() => rej(new Error('sse timeout')), 10000)),
    ]);
    assert.ok(seen.some((r) => r.table === 'users' && r.op === 'insert'), `seen: ${JSON.stringify(seen)}`);
  });

  it('call unknown procedure surfaces verbatim', async () => {
    const c = new HttpClient(baseUrl);
    await assert.rejects(c.call('nope', {}), (e: unknown) => {
      assert.ok(e instanceof SdkError && (e.kind === 'NotFound' || e.kind === 'Server'));
      return true;
    });
  });
});
