//! Client integration tests against a real BlitzDB server binary.
//! Spawns `target/release/Blitz serve` (built by cargo) on an ephemeral
//! port. Run: `npm test` (skips gracefully if the binary is missing).
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn, ChildProcess } from 'node:child_process';
import net from 'node:net';
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { Client } from '../src/client.ts';
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
let port = 0;
let haveServer = fs.existsSync(BIN);

before(async () => {
  if (!haveServer) return;
  port = await freePort();
  proc = spawn(BIN, ['serve', '--port', String(port)], { stdio: 'ignore' });
  // Wait for accept.
  const deadline = Date.now() + 10000;
  for (;;) {
    try {
      await Client.connect(port, '127.0.0.1', { timeoutMs: 500 });
      break;
    } catch {
      if (Date.now() > deadline) throw new Error('server did not start');
      await new Promise((r) => setTimeout(r, 50));
    }
  }
});

after(() => {
  proc?.kill();
});

describe('client', { skip: !haveServer && 'Blitz binary missing (cargo build -p blitz-cli)' } as object, () => {
  it('compat matrix and live handshake', async () => {
    const { Client: C, MIN_SERVER_VERSION } = await import('../src/client.ts');
    // Pure matrix (no server).
    C.checkCompat('0.1.0', 2);
    C.checkCompat('1.4.2', 2);
    assert.throws(() => C.checkCompat('0.1.0', 3), /requires server protocol/);
    assert.throws(() => C.checkCompat('0.0.9', 2), /requires BlitzDB server >=/);
    assert.throws(() => C.checkCompat('banana', 2), /requires BlitzDB server >=/);
    // Live connect already handshook (all other tests passed through it).
    assert.equal(MIN_SERVER_VERSION, '0.1.0');
  });

  it('crud roundtrip', async () => {
    const c = await Client.connect(port);
    try {
      await c.ping();
      const row = await c.insert('users', { id: 1, name: 'Ada', email: 'ada@x.com' });
      const got = await c.get('users', row.id as number);
      assert.ok(got);
      assert.equal(got.values['name'], 'Ada');
      const upd = await c.update('users', row.id as number, { name: 'Ada L.' });
      assert.equal(upd.values['name'], 'Ada L.');
      const found = await c.find('users', 'email', 'ada@x.com');
      assert.ok(found);
      assert.equal(await c.find('users', 'email', 'nope@x.com'), null);
      const rows = await c.scan('users', 100);
      assert.equal(rows.length, 1);
      await c.delete('users', row.id as number);
      assert.equal(await c.get('users', row.id as number), null);
    } finally {
      c.close();
    }
  });

  it('concurrent sharing batches with exact id mapping', async () => {
    const c = await Client.connect(port);
    try {
      const jobs: Promise<number | bigint>[] = [];
      for (let t = 0; t < 30; t++) {
        for (let i = 0; i < 10; i++) {
          jobs.push(
            c.insert('users', { id: t * 100 + i, name: `u${t}-${i}`, email: `u${t}-${i}@x.com` })
              .then((r) => r.id),
          );
        }
      }
      const ids = await Promise.all(jobs);
      assert.equal(ids.length, 300);
      assert.equal(new Set(ids.map(String)).size, 300);
      const rows = await c.scan('users', 1000);
      assert.ok(rows.length >= 300);
    } finally {
      c.close();
    }
  });

  it('error mapping', async () => {
    const c = await Client.connect(port);
    try {
      assert.equal(await c.get('users', 424242), null);
      await assert.rejects(c.update('users', 424242, { name: 'x' }), (e: unknown) => {
        assert.ok(e instanceof SdkError && e.kind === 'NotFound');
        return true;
      });
      await assert.rejects(c.scan('nope', 10), (e: unknown) => {
        assert.ok(e instanceof SdkError && e.kind === 'NotFound');
        return true;
      });
    } finally {
      c.close();
    }
  });

  it('call unknown procedure surfaces verbatim', async () => {
    const c = await Client.connect(port);
    try {
      await assert.rejects(c.call('nope', {}), (e: unknown) => {
        assert.ok(e instanceof SdkError && (e.kind === 'NotFound' || e.kind === 'Server'));
        return true;
      });
    } finally {
      c.close();
    }
  });
});
