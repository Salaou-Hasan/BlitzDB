// Server-side BlitzDB access (Node runtime: Route Handlers, Server
// Actions, React Server Components). NEVER import this file from a
// Client Component — it pulls `node:net` via the TCP client. Browser
// code uses lib/http.ts (fetch) instead.
//
// Auth model: the demo login stores the owner's NAME in a cookie (dev
// server runs open; see README "Production checklist" for require_auth
// + row_owner + bearer tokens). Per-request clients: connection identity
// is sticky, so each user gets a fresh client per request (pool in prod).
import { Client } from '@blitzdb/client';

function endpoint(): { host: string; port: number } {
  const raw = process.env.BLITZ_URL ?? '127.0.0.1:7420';
  const [host, port] = raw.split(':');
  return { host, port: Number(port) };
}

/** One-shot client (connect → fn → close). Pool connections in prod. */
export async function withClient<T>(fn: (c: Client) => Promise<T>): Promise<T> {
  const { host, port } = endpoint();
  const client = await Client.connect(port, host);
  try {
    return await fn(client);
  } finally {
    client.close();
  }
}

/** Currently logged-in owner (dev: plain cookie; prod: session lookup). */
export async function currentOwner(cookies: { get(name: string): { value?: string } | undefined }): Promise<string | null> {
  return cookies.get('blitz_owner')?.value ?? null;
}
