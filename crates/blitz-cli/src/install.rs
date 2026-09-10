//! `blitz install` / `blitz upgrade`: device-detected server downloads.
//!
//! Resolves this machine to a release asset, fetches it from GitHub
//! Releases with `curl` (zero new network deps — TLS stays in the OS
//! tool), and verifies SHA256SUMS before installing (a mismatched binary
//! is deleted, never left in place).
//!
//! Supported matrix (exactly what the release pipeline builds):
//!
//! ```text
//! linux/x86_64    -> blitz-linux-x64
//! windows/x86_64  -> blitz-windows-x64.exe
//! macos/aarch64   -> blitz-macos-arm64
//! ```
//!
//! Anything else (Intel Macs, Linux ARM, …) fails with the supported
//! list instead of a 404 puzzle. `upgrade` is `install latest --force`
//! semantics with a friendlier name.

use anyhow::Result;
use std::path::PathBuf;

const OWNER_REPO: &str = "Salaou-Hasan/BlitzDB";

pub struct InstallArgs {
    /// Version tag (`v0.2.1`), bare version (`0.2.1`), or `"latest"`.
    pub version: String,
    /// Install directory (default: `~/.blitzdb/bin`).
    pub dir: Option<PathBuf>,
    /// Replace an existing verified install.
    pub force: bool,
}

/// Device mapping: `(download asset, installed executable name)`.
/// Installs expose the canonical `blitz` command on every platform;
/// platform tags live only in release artifact filenames.
pub struct DeviceAsset {
    pub download: &'static str,
    pub executable: &'static str,
}

/// This machine's release asset, or a clear unsupported-device error.
pub fn asset_for_this_device() -> Result<DeviceAsset, String> {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    match (os, arch) {
        ("linux", "x86_64") => Ok(DeviceAsset { download: "blitz-linux-x64", executable: "blitz" }),
        ("windows", "x86_64") => Ok(DeviceAsset { download: "blitz-windows-x64.exe", executable: "blitz.exe" }),
        ("macos", "aarch64") => Ok(DeviceAsset { download: "blitz-macos-arm64", executable: "blitz" }),
        _ => Err(format!(
            "no prebuilt BlitzDB server for {} {}; supported: linux/x86_64, windows/x86_64, macos/aarch64.\n\
             Build from source (cargo build -p blitz-cli) or request a target.",
            os, arch
        )),
    }
}

pub fn normalize_tag(version: &str) -> String {
    let v = version.trim();
    if v.eq_ignore_ascii_case("latest") {
        return "latest".to_string();
    }
    if v.starts_with('v') {
        v.to_string()
    } else {
        format!("v{}", v)
    }
}

fn default_install_dir() -> Result<PathBuf, String> {
    if let Ok(home) = std::env::var("HOME") {
        if !home.is_empty() {
            return Ok(PathBuf::from(home).join(".blitzdb").join("bin"));
        }
    }
    if let Ok(profile) = std::env::var("USERPROFILE") {
        if !profile.is_empty() {
            return Ok(PathBuf::from(profile).join(".blitzdb").join("bin"));
        }
    }
    Err("cannot locate a home directory; pass --dir explicitly".to_string())
}

fn require_curl() -> Result<(), String> {
    match std::process::Command::new("curl").arg("--version").output() {
        Ok(out) if out.status.success() => Ok(()),
        _ => Err("`curl` is required but not on PATH.\n\
                  Install curl, or download manually from \
                  https://github.com/Salaou-Hasan/BlitzDB/releases".to_string()),
    }
}

fn curl_download(url: &str, dest: &PathBuf) -> Result<(), String> {
    let status = std::process::Command::new("curl")
        .args(["-fsSL", "--retry", "2", "-o"])
        .arg(dest)
        .arg(url)
        .status()
        .map_err(|e| format!("failed to run curl: {}", e))?;
    if !status.success() {
        return Err(format!("download failed ({} -> {})", url, dest.display()));
    }
    Ok(())
}

fn resolve_latest() -> Result<String, String> {
    let out = std::process::Command::new("curl")
        .args([
            "-fsSL",
            "https://api.github.com/repos/Salaou-Hasan/BlitzDB/releases/latest",
        ])
        .output()
        .map_err(|e| format!("failed to run curl: {}", e))?;
    if !out.status.success() {
        return Err("could not query latest release (network or rate limit?)".to_string());
    }
    let v: serde_json::Value =
        serde_json::from_slice(&out.stdout).map_err(|e| format!("bad release API JSON: {}", e))?;
    v.get("tag_name")
        .and_then(|t| t.as_str())
        .map(|t| t.to_string())
        .ok_or_else(|| "release API response has no tag_name".to_string())
}

fn sha256_file(path: &PathBuf) -> Result<String, String> {
    use sha2::Digest;
    let bytes =
        std::fs::read(path).map_err(|e| format!("read {}: {}", path.display(), e))?;
    Ok(format!("{:x}", sha2::Sha256::digest(&bytes)))
}

/// Install (or upgrade to) a server release. Returns the installed path.
pub fn run_install(args: InstallArgs) -> Result<PathBuf> {
    let device = asset_for_this_device().map_err(anyhow::Error::msg)?;
    let (asset, exe) = (device.download, device.executable);
    let tag = if args.version.eq_ignore_ascii_case("latest") {
        resolve_latest().map_err(anyhow::Error::msg)?
    } else {
        normalize_tag(&args.version)
    };
    require_curl().map_err(anyhow::Error::msg)?;
    let dir = match args.dir {
        Some(d) => d,
        None => default_install_dir().map_err(anyhow::Error::msg)?,
    };
    std::fs::create_dir_all(&dir)
        .map_err(|e| anyhow::anyhow!("create {}: {}", dir.display(), e))?;
    let dest = dir.join(exe);
    let base = format!("https://github.com/{}/releases/download/{}/{}", OWNER_REPO, tag, asset);

    // Verify checksums FIRST (fail before touching any existing install).
    println!("fetching SHA256SUMS for {} ...", tag);
    let sums_path = dir.join("SHA256SUMS.pending");
    curl_download(
        &format!("https://github.com/{}/releases/download/{}/SHA256SUMS", OWNER_REPO, tag),
        &sums_path,
    )
    .map_err(anyhow::Error::msg)?;
    let sums = std::fs::read_to_string(&sums_path)
        .map_err(|e| anyhow::anyhow!("read checksums: {}", e))?;
    let want = sums
        .lines()
        .filter_map(|l| {
            let (hash, name) = l.split_once(char::is_whitespace)?;
            (name.trim() == asset).then(|| hash.trim().to_string())
        })
        .next()
        .ok_or_else(|| anyhow::anyhow!("SHA256SUMS has no entry for {}", asset))?;

    if dest.exists() && !args.force {
        // Verified-idempotent: matching checksum means done already —
        // but still ensure PATH (a cleaned rc file shouldn't strand us).
        if let Ok(have) = sha256_file(&dest) {
            if have == want {
                println!("already installed: {} ({})", dest.display(), &want[..12]);
                let _ = std::fs::remove_file(&sums_path);
                match ensure_on_path(&dir) {
                    Ok(true) => print_path_hint(&dir),
                    Ok(false) => {}
                    Err(e) => eprintln!("warning: PATH wiring skipped ({})", e),
                }
                return Ok(dest);
            }
        }
    }
    println!("downloading {} {} ...", asset, tag);
    let tmp = dir.join(format!("{}.pending", exe));
    if let Err(e) = curl_download(&base, &tmp) {
        let _ = std::fs::remove_file(&tmp);
        anyhow::bail!("{}", e);
    }
    let have = sha256_file(&tmp).map_err(anyhow::Error::msg)?;
    if have != want {
        let _ = std::fs::remove_file(&tmp);
        anyhow::bail!(
            "checksum mismatch for {} (want {}, got {}) — deleted, nothing installed",
            asset,
            want,
            have
        );
    }
    let installed: PathBuf = match std::fs::rename(&tmp, &dest) {
        Ok(()) => dest.clone(),
        Err(_) => {
            // Windows locks the running image: stage beside it with exact
            // swap instructions instead of failing opaquely. (Unix rename
            // replaces running files atomically, so this only triggers on
            // Windows self-upgrade.)
            let staged = dir.join(format!("{}.new", exe));
            std::fs::rename(&tmp, &staged).map_err(|e| {
                anyhow::anyhow!("install {}: {} (rename blocked — is blitz running from it?)", dest.display(), e)
            })?;
            println!(
                "staged {} (the running binary is locked).\n\
                 Close BlitzDB processes, then run:\n  \
                 {} {} \"{}\"",
                staged.display(),
                swap_command(),
                staged.display(),
                dest.display()
            );
            staged
        }
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&installed)
            .map_err(|e| anyhow::anyhow!("stat {}: {}", installed.display(), e))?
            .permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&installed, perms)
            .map_err(|e| anyhow::anyhow!("chmod {}: {}", installed.display(), e))?;
    }
    let _ = std::fs::remove_file(&sums_path);
    println!("installed {} ({})", installed.display(), &want[..12]);
    // PATH wiring is best-effort: the binary works by absolute path
    // regardless; a wiring failure must never fail the install.
    match ensure_on_path(&dir) {
        Ok(true) => print_path_hint(&dir),
        Ok(false) => {}
        Err(e) => eprintln!("warning: PATH wiring skipped ({})\n  run with the full path or export PATH manually.", e),
    }
    Ok(installed)
}

#[cfg(windows)]
fn swap_command() -> &'static str {
    "move /Y"
}

#[cfg(not(windows))]
fn swap_command() -> &'static str {
    "mv -f"
}

// -- PATH wiring (per OS) -------------------------------------------------

/// Marker so repeated installs never duplicate entries.
const PATH_MARKER: &str = "# blitzdb (+blitz)";

/// Render `dir` for shell files (`$HOME`-relative when under home, so
/// dotfiles stay portable).
fn display_dir(dir: &PathBuf, home: &str) -> String {
    let s = dir.to_string_lossy().into_owned();
    if !home.is_empty() {
        if let Some(rest) = s.strip_prefix(home) {
            if rest.starts_with('/') || rest.starts_with('\\') {
                return format!("$HOME{}", rest);
            }
        }
    }
    s
}

fn sh_block(rendered: &str) -> String {
    format!("{}\nexport PATH=\"{}:$PATH\"\n", PATH_MARKER, rendered)
}

/// Append `block` to `rcfile` unless the marker is already present.
/// Creates parent dirs; creates the file when missing.
fn ensure_blocked_entry(rcfile: &PathBuf, block: &str) -> Result<bool, String> {
    if rcfile.is_file() {
        let text =
            std::fs::read_to_string(rcfile).map_err(|e| format!("read {}: {}", rcfile.display(), e))?;
        if text.contains(PATH_MARKER) {
            return Ok(false);
        }
    } else if let Some(parent) = rcfile.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("create {}: {}", parent.display(), e))?;
    }
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(rcfile)
        .map_err(|e| format!("write {}: {}", rcfile.display(), e))?;
    writeln!(f, "\n{}", block).map_err(|e| format!("write {}: {}", rcfile.display(), e))?;
    Ok(true)
}

/// True when `dir` is already on the (given) PATH value. Separator and
/// case rules follow the OS (Windows: `;` + case-insensitive).
fn path_contains(path_var: &str, dir: &str) -> bool {
    #[cfg(windows)]
    {
        path_var.split(';').any(|p| p.eq_ignore_ascii_case(dir))
    }
    #[cfg(not(windows))]
    {
        // Tilde forms count: shells expand $HOME at use time.
        path_var.split(':').any(|p| p == dir || p == "$HOME/.blitzdb/bin")
    }
}

/// Wire `dir` onto PATH for future shells. Returns true when anything
/// changed (caller prints the refresh hint). Never fails the install:
/// errors become warnings at the call site.
pub fn ensure_on_path(dir: &PathBuf) -> Result<bool, String> {
    #[cfg(windows)]
    {
        ensure_on_path_windows(dir)
    }
    #[cfg(not(windows))]
    {
        ensure_on_path_unix(dir)
    }
}

#[cfg(not(windows))]
fn ensure_on_path_unix(dir: &PathBuf) -> Result<bool, String> {
    let home = std::env::var("HOME").unwrap_or_default();
    let rendered = display_dir(dir, &home);
    let home_path = PathBuf::from(&home);
    let candidates = [
        home_path.join(".bashrc"),
        home_path.join(".zshrc"),
        home_path.join(".profile"),
    ];
    let fish = home_path.join(".config").join("fish").join("config.fish");
    let mut changed = false;
    let mut touched_any = false;
    for rc in &candidates {
        if rc.is_file() {
            touched_any = true;
            changed |= ensure_blocked_entry(rc, &sh_block(&rendered))?;
        }
    }
    if fish.is_file() {
        touched_any = true;
        // fish_add_path dedupes by design — safe to ensure unconditionally,
        // but keep our marker for idempotency + auditability.
        let block = format!("{}\nfish_add_path {}\n", PATH_MARKER, rendered);
        changed |= ensure_blocked_entry(&fish, &block)?;
    }
    if !touched_any {
        // Bare container / minimal home: create ~/.profile (POSIX shells
        // read it; richest default available).
        changed |= ensure_blocked_entry(&home_path.join(".profile"), &sh_block(&rendered))?;
    }
    Ok(changed)
}

#[cfg(windows)]
fn ensure_on_path_windows(dir: &PathBuf) -> Result<bool, String> {
    let dir_s = dir.to_string_lossy().into_owned();
    let current = user_path_var()?;
    if path_contains(&current, &dir_s) {
        return Ok(false);
    }
    // powershell.exe ships every supported Windows (setx truncates at
    // ~1024 chars — never use it for PATH).
    let script = format!(
        "[Environment]::SetEnvironmentVariable('Path', [Environment]::GetEnvironmentVariable('Path','User') + ';{}', 'User')",
        dir_s.replace('\'', "''")
    );
    let status = std::process::Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .status()
        .map_err(|e| format!("powershell unavailable: {}", e))?;
    if !status.success() {
        return Err("powershell failed to persist PATH".to_string());
    }
    Ok(true)
}

#[cfg(windows)]
fn user_path_var() -> Result<String, String> {
    let out = std::process::Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command",
               "[Environment]::GetEnvironmentVariable('Path','User')"])
        .output()
        .map_err(|e| format!("powershell unavailable: {}", e))?;
    if !out.status.success() {
        return Err("powershell failed to read PATH".to_string());
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// What to print so the CURRENT shell picks it up (rc/registry edits only
/// affect new shells).
fn print_path_hint(dir: &PathBuf) {
    #[cfg(windows)]
    {
        println!("PATH updated — open a NEW terminal (this shell still uses the old PATH).");
        let _ = dir;
    }
    #[cfg(not(windows))]
    {
        let home = std::env::var("HOME").unwrap_or_default();
        println!(
            "PATH updated — restart your shell, or run now:\n  export PATH=\"{}:$PATH\"",
            display_dir(dir, &home)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_matrix() {
        // This machine must resolve (CI runs linux/mac/windows matrix).
        assert!(asset_for_this_device().is_ok());
    }

    #[test]
    fn tag_normalization() {
        assert_eq!(normalize_tag("latest"), "latest");
        assert_eq!(normalize_tag("LATEST"), "latest");
        assert_eq!(normalize_tag("v0.2.1"), "v0.2.1");
        assert_eq!(normalize_tag("0.2.1"), "v0.2.1");
    }

    #[test]
    fn sha256_known_vector() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("blitz-sha-{}", std::process::id()));
        std::fs::write(&path, b"abc").unwrap();
        assert_eq!(
            sha256_file(&path).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let _ = std::fs::remove_file(&path);
    }
}

#[cfg(test)]
mod path_tests {
    use super::*;

    fn tmp_home(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("blitz-path-{}-{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn profile_entry_is_idempotent() {
        let home = tmp_home("idempotent");
        let dir = home.join(".blitzdb").join("bin");
        std::fs::create_dir_all(&dir).unwrap();
        let rc = home.join(".bashrc");
        std::fs::write(&rc, "# mine\n").unwrap();
        assert!(ensure_blocked_entry(&rc, &sh_block("$HOME/.blitzdb/bin")).unwrap());
        assert!(!ensure_blocked_entry(&rc, &sh_block("$HOME/.blitzdb/bin")).unwrap());
        let text = std::fs::read_to_string(&rc).unwrap();
        assert!(text.contains("# mine"));
        assert_eq!(text.matches(PATH_MARKER).count(), 1);
        // display_dir relativizes under home.
        assert_eq!(display_dir(&dir, &home.to_string_lossy()), "$HOME/.blitzdb/bin");
        assert_eq!(display_dir(&PathBuf::from("/opt/x"), &home.to_string_lossy()), "/opt/x");
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn ensure_unix_wires_existing_rc_only() {
        // HOME override is process-global: run serially-safe via unique home
        // and restore afterwards (single-threaded test binary assumed here
        // the same as the rest of this module's tests).
        let home = tmp_home("unix");
        std::fs::write(home.join(".bashrc"), "").unwrap();
        // Point HOME at the sandbox for this call.
        let old = std::env::var("HOME").unwrap_or_default();
        unsafe { std::env::set_var("HOME", &home) };
        let changed = ensure_on_path(&home.join(".blitzdb").join("bin")).unwrap();
        unsafe { std::env::set_var("HOME", old) };
        assert!(changed);
        let text = std::fs::read_to_string(home.join(".bashrc")).unwrap();
        assert!(text.contains("$HOME/.blitzdb/bin"));
        assert!(!home.join(".profile").exists(), "must not create files unasked when rcs exist");
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn path_contains_rules() {
        #[cfg(windows)]
        {
            assert!(path_contains("C:\\a;C:\\b", "c:\\B"));
            assert!(!path_contains("C:\\a;C:\\b", "C:\\c"));
            assert!(!path_contains("", "C:\\c"));
        }
        #[cfg(not(windows))]
        {
            assert!(path_contains("/a:/b", "/b"));
            assert!(!path_contains("/a:/b", "/c"));
            assert!(!path_contains("", "/c"));
        }
    }
}
