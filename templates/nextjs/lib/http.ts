// Browser-safe BlitzDB access ('use client' components).
// Uses the fetch-based HttpClient over the HTTP bridge — no sockets.
// Import ONLY from '@blitzdb/client/http' here (the package root pulls
// node:net, which does not exist in browsers).
'use client';

import { HttpClient } from '@blitzdb/client/http';

let singleton: HttpClient | null = null;

/** Shared browser client (base URL + optional token). */
export function browserDb(token?: string): HttpClient {
  if (!singleton) {
    const baseUrl = process.env.NEXT_PUBLIC_BLITZ_HTTP_URL ?? 'http://127.0.0.1:7421';
    singleton = new HttpClient(baseUrl);
  }
  if (token !== undefined) singleton.setToken(token);
  return singleton;
}
