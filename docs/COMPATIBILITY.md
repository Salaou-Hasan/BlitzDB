# BlitzDB Compatibility: manifests, versions, projects

No silent incompatibility: every layer declares what it needs, the CLI
checks before scaffolding, and SDKs fail fast at connect with
human-readable errors (§35).

## Version surfaces

| Layer | Version | Protocol | Advertised via |
|---|---|---|---|
| Server | `SERVER_VERSION` (workspace Cargo) | `PROTOCOL_VERSION` (wire `2`) | `Op::Version` (tag 15, no auth), `/v1/op {"op":"version"}` |
| CLI | `blitz version` (cargo pkg) | same constant | — |
| Rust SDK | `blitz-client` cargo version | `blitz-protocol` dep | handshake in `connect` |
| TS SDK | `CLIENT_VERSION` (`package.json`) | `PROTOCOL_VERSION_TS` | handshake in `connect` |
| Python SDK | `CLIENT_VERSION` | `PROTOCOL_VERSION_PY` | handshake in `connect` |
| Go SDK | `ClientVersion` | `ProtocolVersionGo` | handshake in `Connect` |

SDK rule (identical wording everywhere): exact protocol match, plus
server floor (`MIN_SERVER_VERSION`, currently `0.1.0`):

```text
BlitzDB client vX (protocol P) requires server protocol P (server vS speaks vQ)
BlitzDB client vX requires BlitzDB server >= 0.1.0 (found S)
```

## Template manifests (`blitz.template.json`)

Templates are discovered, never hard-coded — and never from the source
checkout. Sources, in priority order:

1. Explicit `--templates` dirs (flag order; first source wins on name
   collisions, with a shadowing warning).
2. User directory `~/.blitzdb/templates` (convention, never required).
3. **Bundled officials**, embedded in the binary at build time (sorted,
   deterministic; a missing bundle fails the BUILD, never the user).

So plain `blitz init` works offline with zero repository access, while
`blitz init --templates ./mine` behaves exactly as before (backward
compatible, including multi-dir combining). A remote registry is
deliberately absent: no silent internet downloads, ever.

```json
{
  "manifest": 1,
  "name": "ts-minimal",
  "version": "0.1.0",
  "language": "typescript",
  "framework": "none",
  "description": "...",
  "sdk": { "name": "@blitzdb/client", "range": ">=0.1.0, <1.0.0" },
  "protocol": { "min": 2, "max": 2 },
  "server": { "min": "0.1.0" }
}
```

Ranges are semver (comma-separated, e.g. `>=0.1.0, <1.0.0` — npm
space-separated ranges are rejected with a clear error).

## Resolution (`blitz init`)

```text
blitz init [dir] [--templates DIR]... [--template NAME]
           [--sdk-version name=ver]... [--server host:port]
           [--server-version X.Y.Z] [--protocol N] [--yes]
```

1. Discover templates; 2. known versions from flags, with a live
`Op::Version` probe filling server/protocol gaps (`--server`);
3. pick (named, `--yes` first-compatible, or interactive with compatible
first and incompatible greyed with reasons); 4. confirm; 5. scaffold
(copy tree) + write `blitz.project.json` pins.

`blitz.project.json` records `{template, template_version, sdk,
sdk_version, protocol, server_min}` — the reproducible audit trail.

## CLI layout note

`blitz init` scaffolds projects. The old database-dir initializer moved
to `blitz db init` (with `blitz db rotate`); server `blitz.json` config
is untouched. Project records use `blitz.project.json` (no collision).
