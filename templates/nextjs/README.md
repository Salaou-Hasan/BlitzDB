# Pulse — Next.js + BlitzDB template

Microblog proving the ecosystem: scaffolded by `blitz init`, every
screen maps to one BlitzDB primitive (see table). No ORM, no backend
framework between Next.js and BlitzDB.

## Run it

```bash
npm install
blitz serve --port 7420 --metrics-port 7421   # binary TCP + HTTP bridge
node scripts/seed.mjs                          # tables + procedure + hello
npm run dev                                    # Next.js on :3000
```

Copy `.env.example` to `.env.local` first (endpoints + optional token).

## Screens → primitives

| Screen | Primitive | Path |
|---|---|---|
| Post list (RSC) | `Scan` desc pages | `app/page.tsx` → TCP SDK server-side |
| New post (Server Action) | `Call createPost` (atomic procedure) | `app/actions.ts` → TCP SDK |
| Live feed (client) | SSE `/v1/stream` | `components/LiveFeed.tsx` → HTTP bridge |
| Login/logout | cookie owner (dev) | `app/actions.ts` |

Browser code imports `@blitzdb/client/http` ONLY (package root pulls
`node:net`, which browsers lack). Server code uses the TCP `Client`.

## Production checklist

- [ ] Server: `require_auth` + bearer sessions (`register_session`/TTL),
  `row_owner: {posts: owner}`, grants for `call` on `fn:createPost`.
- [ ] Replace dev cookie login with session lookup (cookie holds a
  session token, server resolves the owner — never trust a raw name).
- [ ] Pool TCP clients (`withClient` connects per request here).
- [ ] CORS origins locked down (`http_cors_origins`), TLS termination.
- [ ] `insert_fast` only where natural idempotency holds.
