// Minimal BlitzDB TypeScript app (no build step: runs on Node type stripping).
//
// 1. Start a server:  blitz serve --port 7420   (or `cargo run -p blitz-cli -- serve`)
// 2. Run:             node src/index.ts [port]
//
// Uses the reference SDK in ../../clients/ts (in a real project this would
// be the published @blitzdb/client package at the pinned version recorded
// in blitz.project.json).
import { Client } from '../../../clients/ts/src/index.ts';

const port = Number(process.argv[2] ?? 7420);
const client = await Client.connect(port);
await client.ping();
const row = await client.insert('users', { id: 1, name: 'Ada', email: 'ada@x.com' });
console.log('inserted row', row.id);
const got = await client.get('users', row.id as number);
console.log('read back:', got?.values);
client.close();
