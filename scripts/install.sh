#!/bin/sh
# BlitzDB one-line installer (macOS / Linux).
#
#   curl -sSf https://raw.githubusercontent.com/Salaou-Hasan/BlitzDB/v0.2.2/scripts/install.sh | sh
#
# Env overrides: BLITZ_VERSION (tag like v0.2.1, or "latest"),
# BLITZ_DIR (install dir, default ~/.blitzdb/bin), BLITZ_NO_PATH=1
# (skip shell-rc PATH wiring), BLITZ_NO_VERIFY=1 (DANGEROUS: skip the
# checksum gate — only for air-gapped mirrors you already trust).
#
# Fail-closed: unknown OS/arch, missing curl, missing checksum tools,
# checksum mismatch, and download errors all abort with a clear message
# and install nothing. Never `sudo`: user-local install only.
set -eu

REPO="Salaou-Hasan/BlitzDB"
VERSION="${BLITZ_VERSION:-latest}"
INSTALL_DIR="${BLITZ_DIR:-$HOME/.blitzdb/bin}"
MARKER="# blitzdb (+blitz)"

die() { printf 'blitz install: %s\n' "$*" >&2; exit 1; }
info() { printf 'blitz install: %s\n' "$*"; }

command -v curl >/dev/null 2>&1 || die "curl is required (https://curl.se) — or download manually from https://github.com/$REPO/releases"

os="$(uname -s)"
arch="$(uname -m)"
case "$os/$arch" in
  Linux/x86_64)  ASSET="blitz-linux-x64" ; EXE="blitz" ;;
  Darwin/arm64)  ASSET="blitz-macos-arm64" ; EXE="blitz" ;;
  *) die "no prebuilt BlitzDB server for $os/$arch; supported: Linux/x86_64, macOS/arm64 (Apple Silicon).
  Intel Macs and Linux ARM: build from source (cargo build -p blitz-cli) or request a target." ;;
esac

if [ "$VERSION" = "latest" ]; then
  info "resolving latest release ..."
  # stderr silenced: grep -m1 closes the pipe early (SIGPIPE noise),
  # failure still surfaces via the empty-TAG check below.
  TAG="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" 2>/dev/null | grep -m1 '"tag_name"' | sed 's/.*: *"\([^"]*\)".*/\1/')"
  [ -n "$TAG" ] || die "could not resolve latest release (network or API rate limit?)"
  VERSION="$TAG"
fi
case "$VERSION" in
  v*) TAG="$VERSION" ;;
  *)  TAG="v$VERSION" ;;
esac

BASE="https://github.com/$REPO/releases/download/$TAG"
mkdir -p "$INSTALL_DIR" || die "cannot create $INSTALL_DIR"

tmp_bin="$INSTALL_DIR/.$EXE.pending"
tmp_sums="$INSTALL_DIR/.SHA256SUMS.pending"
cleanup() { rm -f "$tmp_bin" "$tmp_sums"; }
trap cleanup EXIT INT TERM

curl -fsSL --retry 2 -o "$tmp_sums" "$BASE/SHA256SUMS" || die "checksum manifest failed ($BASE/SHA256SUMS)"
want="$(grep -E " $ASSET\$" "$tmp_sums" | awk '{print $1}')"
[ -n "$want" ] || die "SHA256SUMS has no entry for $ASSET"

# PATH wiring (idempotent marker; fish gets fish_add_path).
wire_path() {
  [ -z "${BLITZ_NO_PATH:-}" ] || return 0
  case "$INSTALL_DIR" in
    "$HOME"/*) shown="\$HOME/${INSTALL_DIR#$HOME/}" ;;
    *) shown="$INSTALL_DIR" ;;
  esac
  wired=0
  touched_any=0
  for rc in "$HOME/.bashrc" "$HOME/.zshrc" "$HOME/.profile"; do
    if [ -f "$rc" ]; then
      touched_any=1
      if ! grep -qF "$MARKER" "$rc" 2>/dev/null; then
        printf '\n%s\nexport PATH="%s:$PATH"\n' "$MARKER" "$shown" >>"$rc" && wired=1
      fi
    fi
  done
  if [ -f "$HOME/.config/fish/config.fish" ]; then
    touched_any=1
    if ! grep -qF "$MARKER" "$HOME/.config/fish/config.fish" 2>/dev/null; then
      printf '\n%s\nfish_add_path %s\n' "$MARKER" "$shown" >>"$HOME/.config/fish/config.fish" && wired=1
    fi
  fi
  if [ "$touched_any" = "0" ]; then
    # Bare container / minimal home: create ~/.profile (POSIX shells read it).
    printf '\n%s\nexport PATH="%s:$PATH"\n' "$MARKER" "$shown" >>"$HOME/.profile" && wired=1
  fi
  if [ "$wired" = "1" ]; then
    info "PATH updated — restart your shell, or run now:"
    info "  export PATH=\"$shown:\$PATH\""
  fi
}

# Verified-idempotent: matching checksum means done already.
if [ -f "$INSTALL_DIR/$EXE" ]; then
  if command -v sha256sum >/dev/null 2>&1; then
    have="$(sha256sum "$INSTALL_DIR/$EXE" | awk '{print $1}')"
  else
    have="$(shasum -a 256 "$INSTALL_DIR/$EXE" | awk '{print $1}')"
  fi
  if [ "$have" = "$want" ]; then
    info "already installed: $INSTALL_DIR/$EXE (${want%????????????????????????????????}…)"
    rm -f "$tmp_sums"
    trap - EXIT INT TERM
    wire_path
    info "try it: blitz version"
    exit 0
  fi
fi

info "downloading $ASSET $TAG ..."
curl -fsSL --retry 2 -o "$tmp_bin" "$BASE/$ASSET" || die "download failed ($BASE/$ASSET)"
curl -fsSL --retry 2 -o "$tmp_sums" "$BASE/SHA256SUMS" || die "checksum manifest failed ($BASE/SHA256SUMS)"

if [ -z "${BLITZ_NO_VERIFY:-}" ]; then
  want="$(grep -E " $ASSET\$" "$tmp_sums" | awk '{print $1}')"
  [ -n "$want" ] || die "SHA256SUMS has no entry for $ASSET"
  if command -v sha256sum >/dev/null 2>&1; then
    have="$(sha256sum "$tmp_bin" | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1; then
    have="$(shasum -a 256 "$tmp_bin" | awk '{print $1}')"
  else
    die "no sha256sum/shasum found — refusing to install unverified. Provide one, or mirror + BLITZ_NO_VERIFY=1 at your own risk."
  fi
  [ "$have" = "$want" ] || die "checksum mismatch for $ASSET (want $want, got $have) — deleted, nothing installed"
  info "checksum ok (${want%????????????????????????????????}…)"
fi

mv -f "$tmp_bin" "$INSTALL_DIR/$EXE"
chmod +x "$INSTALL_DIR/$EXE"
rm -f "$tmp_sums"
trap - EXIT INT TERM
info "installed $INSTALL_DIR/$EXE"
wire_path

info "try it: blitz version"
