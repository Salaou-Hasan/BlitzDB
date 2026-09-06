# Schema-first: `blitz/generate`

Project source of truth (conventional layout, minimal):

```text
<project>/
  blitz/
    schema/*.json      table schemas (the `table_create` JSON shape)
    functions/*.json   procedure deploy envelopes ({"v":1,"procedure":{...}})
  blitz.generated/     output — do not hand-edit
    manifest.json      {generator, inputs fingerprint, files[]}
    ts/tables.ts       row interfaces + TABLE_* constants
    ts/functions.ts    FN_* constants + typed call wrappers
    rs/tables.rs       row structs + TABLE_* + values() writers
```

```bash
blitz generate [dir]          # write outputs
blitz generate [dir] --check  # fail when output differs (CI mode)
```

Honesty rules (deliberate limits, not oversights):

- Column names pass through EXACTLY (no camelCase magic); reserved
  words get a trailing underscore (`type_`).
- `int64`/`uint64` are `number | bigint` (TS) — narrow with `V.i64()` /
  `V.u64()` when writing strict columns. Rust uses `i64`/`u64`
  (`Option<…>` for nullable).
- Procedure ARGUMENTS are not typed: steps reference `$vars` without
  declared signatures, so wrappers take the args record the procedure
  documents. Declared arg schemas are future work — not faked.
- Reruns are byte-identical (tested); the manifest fingerprints inputs
  for the audit trail (FNV-1a hex: determinism only — `--check`
  compares full bytes anyway).
- `blitz dev` watches a different tree (`<dir>/procedures/*.json`
  envelopes deployed live). A future pass may unify `blitz/functions`
  as the single source feeding both `generate` and `dev --watch`;
  until then the two stay explicit and independent.
