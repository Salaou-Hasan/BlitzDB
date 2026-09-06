# BlitzDB one-line installer (Windows PowerShell 5.1+).
#
#   iwr https://raw.githubusercontent.com/Salaou-Hasan/BlitzDB/v0.2.1/scripts/install.ps1 -useb | iex
#
# Env overrides: $env:BLITZ_VERSION (tag like v0.2.1, or "latest"),
# $env:BLITZ_DIR (install dir, default $HOME\.blitzdb\bin),
# $env:BLITZ_NO_PATH ("1" skips User-PATH wiring).
#
# Fail-closed: unsupported arch, download errors, missing checksum entry,
# checksum mismatch, and TLS failures all abort with a clear message and
# install nothing. Never needs admin (User scope only).
$ErrorActionPreference = 'Stop'

$Repo = 'Salaou-Hasan/BlitzDB'
$Version = if ($env:BLITZ_VERSION) { $env:BLITZ_VERSION } else { 'latest' }
$InstallDir = if ($env:BLITZ_DIR) { $env:BLITZ_DIR } else { Join-Path $HOME '.blitzdb\bin' }

function Fail($msg) { throw "blitz install: $msg" }
function Info($msg) { Write-Host "blitz install: $msg" }

# TLS 1.2 (old defaults break api.github.com on 5.1).
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$arch = $env:PROCESSOR_ARCHITECTURE
if ($arch -eq 'AMD64') {
  $Asset = 'blitz-windows-x64.exe'
} else {
  Fail "no prebuilt BlitzDB server for Windows/$arch; supported: Windows/x86_64 (AMD64). Build from source (cargo build -p blitz-cli) or request a target."
}

if ($Version -eq 'latest') {
  Info 'resolving latest release ...'
  try {
    $rel = Invoke-RestMethod "https://api.github.com/repos/$Repo/releases/latest"
    $Tag = $rel.tag_name
  } catch {
    Fail "could not resolve latest release (network or API rate limit?): $($_.Exception.Message)"
  }
  if (-not $Tag) { Fail 'could not resolve latest release (empty tag_name)' }
  $Version = $Tag
}
if ($Version -notlike 'v*') { $Tag = "v$Version" } else { $Tag = $Version }

$Base = "https://github.com/$Repo/releases/download/$Tag"
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$tmpBin = Join-Path $InstallDir ".$Asset.pending"
$SumsFile = Join-Path $InstallDir '.SHA256SUMS.pending'
try {
  Info "fetching SHA256SUMS for $Tag ..."
  Invoke-WebRequest -UseBasicParsing "$Base/SHA256SUMS" -OutFile $SumsFile
  $want = (Get-Content $SumsFile | Where-Object { $_ -match "\s$Asset\s*$" } | ForEach-Object { ($_ -split '\s+')[0] } | Select-Object -First 1)
  if (-not $want) { Fail "SHA256SUMS has no entry for $Asset" }

  # Verified-idempotent: matching checksum means done already.
  $dest = Join-Path $InstallDir $Asset
  if (Test-Path $dest) {
    $have = (Get-FileHash $dest -Algorithm SHA256).Hash.ToLower()
    if ($have -eq $want.ToLower()) {
      Info "already installed: $dest"
      Remove-Item $SumsFile -ErrorAction SilentlyContinue
      Wire-Path
      Info 'try it: blitz version'
      return
    }
  }

  Info "downloading $Asset $Tag ..."
  Invoke-WebRequest -UseBasicParsing "$Base/$Asset" -OutFile $tmpBin
  $have = (Get-FileHash $tmpBin -Algorithm SHA256).Hash.ToLower()
  if ($have -ne $want.ToLower()) {
    Remove-Item $tmpBin -ErrorAction SilentlyContinue
    Fail "checksum mismatch for $Asset (want $want, got $have) — deleted, nothing installed"
  }
  Info "checksum ok ($($want.Substring(0, 12))…)"
  try {
    Move-Item -Force $tmpBin $dest
  } catch {
    # Locked running image: stage beside it with exact swap instructions.
    $staged = "$dest.new"
    Move-Item -Force $tmpBin $staged
    Info "staged $staged (the running binary is locked)."
    Info 'Close BlitzDB processes, then run:'
    Info "  move /Y `"$staged`" `"$dest`""
    $dest = $staged
  }
  Remove-Item $SumsFile -ErrorAction SilentlyContinue
  Info "installed $dest"
  Wire-Path
  Info 'try it: blitz version (open a NEW terminal first if PATH was just wired)'
} finally {
  Remove-Item $tmpBin -ErrorAction SilentlyContinue
  Remove-Item $SumsFile -ErrorAction SilentlyContinue
}

function Wire-Path {
  # PATH wiring (idempotent; User scope — never setx, which truncates).
  if ($env:BLITZ_NO_PATH) { return }
  $current = [Environment]::GetEnvironmentVariable('Path', 'User')
  if ($null -eq $current) { $current = '' }
  $parts = $current -split ';' | Where-Object { $_ -ne '' }
  $hit = @($parts | Where-Object { $_.Equals($InstallDir, [StringComparison]::OrdinalIgnoreCase) }).Count -gt 0
  if ($hit) { return }
  $new = (($parts + @($InstallDir)) -join ';')
  [Environment]::SetEnvironmentVariable('Path', $new, 'User')
  Info 'PATH updated — open a NEW terminal (this shell still uses the old PATH).'
}
