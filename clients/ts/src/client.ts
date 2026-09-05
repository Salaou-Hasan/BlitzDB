//! Reference client: one TCP connection, invisible autobatching.
//!
//! The programming model never changes with scale: every method looks like a
//! single op. Under the hood the client drains everything already queued and
//! flushes it as one frame — under load the drain IS the batch; at low load
//! a lone op flushes on the next tick (no timer tax: `setImmediate`, not a
//! millisecond timer). Single-op drains go as SINGLE frames (the Get/Scan
//! fast paths stay hot); multi-op drains go as one batch.
//!
//! Retry contract (v1, honest): a flush that fails before any response byte
//! is retried ONCE after reconnect iff every op is a read or an `_idem`
//! insert (auto-stamped on every `insert`). Updates/deletes/calls are never
//! auto-retried: retry them yourself (updates are effect-idempotent; treat
//! delete's "not found" as success; design procedures around an app `_idem`).

import net from 'node:net';
import { randomUUID } from 'node:crypto';
import { FrameCodec } from './codec.ts';
import type { Request, Response, RowView, Op } from './codec.ts';
import type { Value } from './values.ts';
import { SdkError, mapServerError } from './errors.ts';

export interface CallResult {
  values: Record<string, Value>;
  applied: Array<{ table: string; id: number | bigint }>;
}

interface Pending {
  req: Request;
  retrySafe: boolean;
  resolve: (r: Response) => void;
  reject: (e: Error) => void;
  timer?: ReturnType<typeof setTimeout>;
}

export interface ClientOptions {
  timeoutMs?: number;
}

const DEFAULT_TIMEOUT_MS = 5000;
/** Hard cap per flush frame (protocol bound; larger drains chunk). */
export const MAX_FLUSH_OPS = 4096;
/** Target ops per batch frame (bounds head-of-line wait inside a frame). */
export const FLUSH_CHUNK = 16;

export class Client {
  private socket: net.Socket;
  private codec = new FrameCodec();
  private queue: Pending[] = [];
  private flushScheduled = false;
  private nextId = 1;
  private idemPrefix = randomUUID();
  private idemNext = 1;
  private timeoutMs: number;
  private closed = false;
  private host: string;
  private port: number;

  private constructor(socket: net.Socket, host: string, port: number, timeoutMs: number) {
    this.socket = socket;
    this.host = host;
    this.port = port;
    this.timeoutMs = timeoutMs;
    socket.on('error', () => {});
  }

  static connect(port: number, host = '127.0.0.1', opts: ClientOptions = {}): Promise<Client> {
    const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    return new Promise((resolve, reject) => {
      const socket = net.connect(port, host, () => {
        socket.setNoDelay(true);
        resolve(new Client(socket, host, port, timeoutMs));
      });
      socket.once('error', reject);
    });
  }

  close(): void {
    this.closed = true;
    for (const p of this.queue.splice(0)) {
      if (p.timer) clearTimeout(p.timer);
      p.reject(new SdkError('Closed', 'client closed'));
    }
    this.socket.destroy();
  }

  private allocId(): number {
    return this.nextId++;
  }

  private allocIdem(): string {
    return `${this.idemPrefix}-${this.idemNext++}`;
  }

  private enqueue(req: Request, retrySafe: boolean, direct: boolean): Promise<Response> {
    if (this.closed) return Promise.reject(new SdkError('Closed', 'client closed'));
    return new Promise<Response>((resolve, reject) => {
      const pending: Pending = { req, retrySafe, resolve, reject };
      pending.timer = setTimeout(() => {
        const i = this.queue.indexOf(pending);
        if (i >= 0) this.queue.splice(i, 1);
        reject(new SdkError('Timeout', `timeout after ${this.timeoutMs}ms`));
      }, this.timeoutMs);
      if (direct) {
        // Latency probes + handshakes jump the queue (still one socket:
        // flush what's buffered first, then the direct frame alone).
        void this.flushQueue().then(() => {
          this.flushRun([pending]);
        });
      } else {
        this.queue.push(pending);
        if (!this.flushScheduled) {
          this.flushScheduled = true;
          setImmediate(() => {
            this.flushScheduled = false;
            void this.flushQueue();
          });
        }
      }
    });
  }

  /** Drain everything queued into chunked frames, in order. */
  private async flushQueue(): Promise<void> {
    while (this.queue.length > 0 && !this.closed) {
      const run = this.queue.splice(0, FLUSH_CHUNK);
      for (const p of run) {
        if (p.timer) { clearTimeout(p.timer); p.timer = undefined; }
      }
      await this.flushRun(run);
    }
  }

  private flushRun(run: Pending[]): Promise<void> {
    // Disarm per-op timers: the run now owns settlement (reply or flush
    // error); a stray timer firing later finds nothing to time out.
    for (const p of run) {
      if (p.timer) { clearTimeout(p.timer); p.timer = undefined; }
    }
    if (run.length === 0 || this.closed) {
      for (const p of run) p.reject(new SdkError('Closed', 'client closed'));
      return Promise.resolve();
    }
    const retrySafe = run.every((p) => p.retrySafe);
    const reqs = run.map((p) => p.req);
    return this.roundtrip(reqs, retrySafe).then(
      (resps) => {
        run.forEach((p, i) => p.resolve(resps[i]));
      },
      (err: Error) => {
        run.forEach((p) => p.reject(err));
      },
    );
  }

  private roundtrip(reqs: Request[], retrySafe: boolean): Promise<Response[]> {
    const frame = reqs.length === 1
      ? this.codec.encodeRequest(reqs[0])
      : this.codec.encodeBatch({ id: reqs[0].id, ops: reqs }, false);
    return this.writeRead(frame).catch(() => {
      if (!retrySafe) throw new SdkError('Transport', 'flush failed (not retry-safe; retry manually)');
      // One reconnect + resend with identical payloads (same `_idem`).
      return this.reconnect().then(() => this.writeRead(frame)).catch(() => {
        throw new SdkError('Transport', 'flush failed after reconnect');
      });
    }).then((payloads) => {
      if (reqs.length === 1) return [this.codec.decodeResponse(payloads[0])];
      const b = this.codec.decodeBatchResponse(payloads[0]);
      if (b.results.length !== reqs.length) throw new SdkError('Transport', 'batch/response length mismatch');
      return b.results;
    });
  }

  private writeRead(frame: Buffer): Promise<Buffer[]> {
    return new Promise((resolve, reject) => {
      const onData = (chunk: Buffer) => {
        try {
          const payloads = this.codec.feed(chunk);
          if (payloads.length > 0) {
            cleanup();
            resolve(payloads);
          }
        } catch (e) {
          cleanup();
          reject(e);
        }
      };
      const onError = (e: Error) => { cleanup(); reject(e); };
      const onClose = () => { cleanup(); reject(new SdkError('Transport', 'connection closed mid-flush')); };
      const cleanup = () => {
        this.socket.off('data', onData);
        this.socket.off('error', onError);
        this.socket.off('close', onClose);
      };
      this.socket.on('data', onData);
      this.socket.once('error', onError);
      this.socket.once('close', onClose);
      this.socket.write(frame, (e) => {
        if (e) { cleanup(); reject(e); }
      });
    });
  }

  private reconnect(): Promise<void> {
    const host = this.host;
    const port = this.port;
    return new Promise((resolve, reject) => {
      const s = net.connect(port, host, () => {
        s.setNoDelay(true);
        this.socket.destroy();
        this.socket = s;
        s.on('error', () => {});
        resolve();
      });
      s.once('error', reject);
    });
  }

  private okRows(resp: Response): RowView[] {
    if (resp.ok) return resp.rows;
    throw mapServerError(resp.error ?? 'unknown error');
  }

  // -- Primitive ops (each looks single; the client batches) -------

  async authenticate(token: string): Promise<void> {
    const req: Request = {
      id: this.allocId(), op: 'ping', table: '',
      values: { _auth: token },
    };
    this.okRows(await this.enqueue(req, true, true));
  }

  async ping(): Promise<void> {
    const req: Request = { id: this.allocId(), op: 'ping', table: '' };
    this.okRows(await this.enqueue(req, true, true));
  }

  /** Insert with auto `_idem` (safe reconnect-replay inside the window). */
  async insert(table: string, values: Record<string, Value>): Promise<RowView> {
    const req: Request = {
      id: this.allocId(), op: 'insert', table,
      values: { ...values, _idem: this.allocIdem() },
    };
    const rows = this.okRows(await this.enqueue(req, true, false));
    const row = rows.pop();
    if (!row) throw new SdkError('Server', 'insert returned no rows');
    return row;
  }

  /** Insert without `_idem` (expert path, bench wire parity). At-most-once:
   * never auto-retried — retry manually with the same values. */
  async insertFast(table: string, values: Record<string, Value>): Promise<RowView> {
    const req: Request = { id: this.allocId(), op: 'insert', table, values };
    const rows = this.okRows(await this.enqueue(req, false, false));
    const row = rows.pop();
    if (!row) throw new SdkError('Server', 'insert returned no rows');
    return row;
  }

  async get(table: string, id: number | bigint): Promise<RowView | null> {
    const req: Request = { id: this.allocId(), op: 'get', table, rowId: id };
    const resp = await this.enqueue(req, true, false);
    if (resp.ok) return resp.rows[0] ?? null;
    const msg = resp.error ?? 'unknown error';
    if (msg.startsWith('row not found')) return null;
    throw mapServerError(msg);
  }

  /** Not auto-retried: retry manually on transport error. */
  async update(table: string, id: number | bigint, values: Record<string, Value>): Promise<RowView> {
    const req: Request = { id: this.allocId(), op: 'update', table, rowId: id, values };
    const rows = this.okRows(await this.enqueue(req, false, false));
    const row = rows.pop();
    if (!row) throw new SdkError('Server', 'update returned no rows');
    return row;
  }

  /** Not auto-retried: on transport error, re-Get first; treat a retry's
   * "not found" as success (already deleted). */
  async delete(table: string, id: number | bigint): Promise<void> {
    const req: Request = { id: this.allocId(), op: 'delete', table, rowId: id };
    this.okRows(await this.enqueue(req, false, false));
  }

  async scan(table: string, limit: number, cursor?: number | bigint | null, desc = false): Promise<RowView[]> {
    const values: Record<string, Value> = { _limit: limit };
    if (desc) values['_order'] = 'desc';
    if (cursor !== undefined && cursor !== null) values['_cursor'] = cursor;
    const req: Request = { id: this.allocId(), op: 'scan', table, values };
    return this.okRows(await this.enqueue(req, true, false));
  }

  async find(table: string, column: string, value: Value): Promise<RowView | null> {
    const req: Request = {
      id: this.allocId(), op: 'find', table,
      values: { _col: column, _val: value },
    };
    const resp = await this.enqueue(req, true, false);
    if (resp.ok) return resp.rows[0] ?? null;
    const msg = resp.error ?? 'unknown error';
    if (msg === 'not found') return null;
    throw mapServerError(msg);
  }

  async search(table: string, query: string, limit: number): Promise<RowView[]> {
    const req: Request = {
      id: this.allocId(), op: 'search', table,
      values: { _q: query, _limit: limit },
    };
    return this.okRows(await this.enqueue(req, true, false));
  }

  /** Execute a registered procedure transactionally. Not auto-retried:
   * design procedures around an application `_idem` argument instead. */
  async call(fn: string, args: Record<string, Value>): Promise<CallResult> {
    const req: Request = { id: this.allocId(), op: 'call', table: `fn:${fn}`, values: args };
    const rows = this.okRows(await this.enqueue(req, false, false));
    const row = rows.pop();
    if (!row) throw new SdkError('Server', 'call returned no rows');
    const values = { ...row.values };
    const applied: Array<{ table: string; id: number | bigint }> = [];
    const raw: unknown = values['_applied'];
    delete values['_applied'];
    // _applied arrives as a Json array of {table, id} (plain JS values).
    if (Array.isArray(raw)) {
      for (const e of raw as Array<{ table?: unknown; id?: unknown }>) {
        if (typeof e?.table === 'string' && (typeof e?.id === 'number' || typeof e?.id === 'bigint')) {
          applied.push({ table: e.table, id: e.id });
        }
      }
    }
    return { values, applied };
  }

  /** Poll recent changes (single long-poll, not a stream). */
  async pollChanges(table: string, since = 0, limit = 100): Promise<RowView[]> {
    const req: Request = {
      id: this.allocId(), op: 'subscribe', table,
      values: { _since: since, _limit: limit },
    };
    return this.okRows(await this.enqueue(req, true, false));
  }
}
