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

/// This machine's release asset name, or a clear unsupported-device error.
pub fn asset_for_this_device() -> Result<&'static str, String> {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    match (os, arch) {
        ("linux", "x86_64") => Ok("blitz-linux-x64"),
        ("windows", "x86_64") => Ok("blitz-windows-x64.exe"),
        ("macos", "aarch64") => Ok("blitz-macos-arm64"),
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
    let asset = asset_for_this_device().map_err(anyhow::Error::msg)?;
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
    let dest = dir.join(asset);
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
        // Verified-idempotent: matching checksum means done already.
        if let Ok(have) = sha256_file(&dest) {
            if have == want {
                println!("already installed: {} ({})", dest.display(), &want[..12]);
                let _ = std::fs::remove_file(&sums_path);
                return Ok(dest);
            }
        }
    }
    println!("downloading {} {} ...", asset, tag);
    let tmp = dir.join(format!("{}.pending", asset));
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
    std::fs::rename(&tmp, &dest)
        .map_err(|e| anyhow::anyhow!("install {}: {}", dest.display(), e))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&dest)
            .map_err(|e| anyhow::anyhow!("stat {}: {}", dest.display(), e))?
            .permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&dest, perms)
            .map_err(|e| anyhow::anyhow!("chmod {}: {}", dest.display(), e))?;
    }
    let _ = std::fs::remove_file(&sums_path);
    println!("installed {} ({})", dest.display(), &want[..12]);
    Ok(dest)
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
