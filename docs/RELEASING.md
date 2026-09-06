# BlitzDB Releases

One pipeline produces every artifact (§12–16). Never publish binaries
that were not produced by it.

## What CI enforces (every push/PR to `main`)

`.github/workflows/ci.yml` → reusable `ci-gate.yml`:

- Rust workspace tests + clippy (signal)
- Release `Blitz` binary build + version/status smoke
- TS / Python / Go SDK suites (each spawns that binary by path)
- CLI end-to-end: `init` from `templates/` (pass + refuse cases),
  `status` up/down contract
- Portability matrix (windows/macos, signal-only until green twice)

## Cutting a release

1. Bump `version` in the root `Cargo.toml` (`[workspace.package]`)
   and every SDK manifest to the same version (`clients/ts/package.json`,
   `CLIENT_VERSION` constants, `clients/py`, `clients/go`). They must
   agree — the release job fails otherwise.
2. Update `docs/BENCHMARKS.md` + `docs/COMPATIBILITY.md` if behavior changed.
3. `git tag vX.Y.Z && git push origin vX.Y.Z`.

`.github/workflows/release.yml` then: version-check (tag == workspace
version) → full gate → native builds (linux-x64, windows-x64,
macos-arm64) each re-validating templates → checksums → GitHub Release
with generated notes.

Manual runs (`workflow_dispatch`) do everything except the upload.

## Deliberately manual (not CI secrets)

- `cargo publish` / `npm publish` / `PyPI` (registry tokens + judgment)
- Template registry publication (no registry exists yet)
- Soak + kill-drill sign-off (needs a human home)

## Artifact contract

Each release provides the three `blitz-*` binaries, `SHA256SUMS`, and
notes. SDKs check `Op::Version` at connect and fail fast on skew
(see `docs/COMPATIBILITY.md`) — a stale client against a new server is
a readable error, never silent corruption.

## Installing a release

```bash
blitz install            # latest for THIS device (detected)
blitz install v0.2.1     # pinned (checksums verified, +x set)
blitz upgrade            # latest, replacing the current install
```

Devices map 1:1 to assets (`linux/x86_64`, `windows/x86_64`,
`macos/aarch64`); anything else errors with the supported list.
Manual equivalent per device: download the asset +
`SHA256SUMS` from the release page, verify
`sha256sum -c` (Linux) / `shasum -a 256` (macOS) /
`Get-FileHash` (Windows), then run it.

`install` also wires PATH (idempotent marker block in shell profiles,
fish included; Windows User environment via PowerShell — never `setx`,
which truncates). PATH failures warn, never fail the install.
