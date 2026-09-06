# BlitzDB

BlitzDB is the backend: a single-node, high-concurrency application
database **as a server**. Tables are state, functions are behavior —
storage, transactions, WAL, realtime, auth, and jobs come built in.

```text
Install blitz binary for your OS
    ↓
blitz version
    ↓
blitz init          (pick a template; versions checked for you)
    ↓
blitz dev           (server + hot-reload + logs)
    ↓
define tables, write functions, build your app
```

## Install (prebuilt binaries, no source build)

One-liners (checksums verified, PATH wired automatically):

```bash
# macOS / Linux
curl -sSf https://raw.githubusercontent.com/Salaou-Hasan/BlitzDB/v0.2.1/scripts/install.sh | sh
```

```powershell
# Windows (PowerShell)
iwr https://raw.githubusercontent.com/Salaou-Hasan/BlitzDB/v0.2.1/scripts/install.ps1 -useb | iex
```

Or pick your device from the release (checksums included):
https://github.com/Salaou-Hasan/BlitzDB/releases

| Device | Download | Or via CLI |
|---|---|---|
| Linux x64 | `blitz-linux-x64` | `blitz install` |
| Windows x64 | `blitz-windows-x64.exe` | `blitz install` |
| macOS ARM64 | `blitz-macos-arm64` | `blitz install` |

```bash
# Easiest: an existing blitz binary installs/updates itself.
# (Bootstrapping your very first copy: download it from the table above.)
blitz install            # latest for THIS device (OS/arch detected)
blitz install v0.2.1     # a specific release (checksums verified)
blitz upgrade            # latest, replacing the current install
blitz version            # CLI + protocol compatibility floor
```

Install wires itself onto PATH automatically (shell profiles on
Linux/macOS incl. fish, User environment on Windows — restart the
terminal, or use the `export` line it prints). Upgrading the binary
you're currently running stages beside it on Windows (locked image)
with exact swap instructions instead of failing opaquely.

Unsupported devices (Intel Macs, Linux ARM, …) fail with the supported
list instead of a 404 puzzle — or build from source (`cargo build -p
blitz-cli`, Rust 1.70+).

## Five minutes, end to end

```bash
blitz init myapp --templates templates --template ts-minimal \
  --sdk-version @blitzdb/client=0.1.0 \
  --server-version 0.1.0 --protocol 2 --yes
cd myapp
blitz dev              # server :7420, HTTP bridge :7421, hot-reload on
blitz generate --check # CI mode: generated bindings must be current
```

`blitz init` resolves template ↔ SDK ↔ protocol ↔ server versions
*before* scaffolding and writes the pins to `blitz.project.json` —
incompatible combos fail with a readable error, never a broken project.
Details: `docs/COMPATIBILITY.md`.

## SDKs (one protocol, one contract)

| SDK | Path | Notes |
|---|---|---|
| Rust | `crates/blitz-client` | Reference impl; invisible autobatching |
| TypeScript | `clients/ts` (`@blitzdb/client`) | TCP `Client` + fetch `HttpClient`; browser imports `/http` only |
| Python | `clients/py` | stdlib only, threaded batcher |
| Go | `clients/go` | stdlib only, goroutine batcher |

Every method looks like a single op; reads + `_idem` inserts retry once
after reconnect; updates/deletes/calls never auto-retry. Errors are
typed (`Auth`/`Retryable`/`NotFound`/`Invalid`/…). All SDKs handshake
`Op::Version` at connect and fail fast on skew.

## Status

Production-grade single node through 50K CCU inside its latency SLOs
(p50<50 p90/p95<70 p99<100 p99.9<200ms, 0 errors); durable profiles
included. Multi-node (replication/partitioning) is explicitly out of
scope — see `docs/architecture/ARCHITECTURE.md`.

| Suite | Result |
|---|---|
| Rust workspace + TS + Python + Go suites | green (CI gated) |
| Benchmark C app mix, 10K→50K CCU, batch-25/shards-16 | all `PASS`, 0 errors |
| Rust SDK 50K (shared pool) | `PASS`: ~1.95M/s, p99 ~30ms |
| Idle connections | 100K held, ~5.3KB/conn |

Latency contract (successful end-to-end only, 0 errors/timeouts): see
`docs/BENCHMARKS.md`. Ping-pong past ~8K active clients cannot meet it
(Little's law) — batch ≥25 is mandatory there, enforced by nothing but
physics and documented in `docs/PROTOCOL.md`.

## Layout

```text
BlitzDB/
├── crates/
│   ├── blitz-types / blitz-core / blitz-table   # values, rows, engine
│   ├── blitz-protocol                            # framing + codec (v2)
│   ├── blitz-tx / blitz-wal / blitz-snapshot    # tx, durability
│   ├── blitz-auth / blitz-policy                 # identities, rules
│   ├── blitz-runtime / blitz-jobs                # procedures, WASM jobs
│   ├── blitz-server                              # TCP + HTTP bridge, benches
│   ├── blitz-client                              # Rust SDK
│   └── blitz-cli                                 # `blitz` (serve/init/dev/...)
├── clients/ts|py|go                             # TS / Python / Go SDKs
├── templates/                                    # nextjs, ts-minimal (+manifests)
└── docs/
    ├── BENCHMARKS.md                             # SLO contract + numbers
    ├── PROTOCOL.md                               # ops, batching, errors, HTTP
    ├── COMPATIBILITY.md                          # versions, manifests, init
    ├── GENERATE.md                               # schema-first codegen
    ├── RELEASING.md                              # release pipeline
    ├── OPERATIONS.md                             # profiles, metrics, backup
    └── architecture/                             # ARCHITECTURE.md + ADRs
```

## From source (contributors only)

```bash
cargo build
cargo test --workspace          # must all pass (CI gates on this)
cargo run -p blitz-cli -- serve --port 7420 --metrics-port 7421
```

Normal users never need Cargo, Rust toolchains, or this repository —
install the prebuilt binary and start at the top of this file.
