//! Compatibility manifests + resolver: the load-bearing piece of the
//! ecosystem vision (§10/§11/§35).
//!
//! Templates are DISCOVERED, never hard-coded: `blitz init` scans template
//! directories for `blitz.template.json`. Each manifest declares its SDK,
//! protocol, and server requirements as ranges; the resolver checks them
//! against known versions and fails with human-readable errors instead of
//! letting developers build silently incompatible projects.
//!
//! ```text
//! CLI
//!  ↓ reads blitz.template.json {template, sdk, protocol, server}
//! template + SDK + protocol + server checked pairwise
//!  ↓ writes blitz.project.json {resolved pins}
//! project records exact versions (reproducible, auditable)
//! ```
//!
//! Chain: template→SDK (range), SDK→protocol (exact), protocol→server
//! (server advertises both via `Op::Version`).

use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Template manifest filename (in the template root).
pub const TEMPLATE_MANIFEST: &str = "blitz.template.json";
/// Project record filename (written into scaffolded projects).
pub const PROJECT_FILE: &str = "blitz.project.json";
/// Deploy envelope / manifest schema version we understand.
pub const MANIFEST_VERSION: u32 = 1;

/// A discovered template and where it came from.
#[derive(Debug, Clone)]
pub struct DiscoveredTemplate {
    pub manifest: TemplateManifest,
    pub dir: PathBuf,
}

/// `blitz.template.json` schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateManifest {
    /// Manifest schema version (we accept exactly 1).
    pub manifest: u32,
    pub name: String,
    pub version: String,
    pub language: String,
    #[serde(default)]
    pub framework: String,
    #[serde(default)]
    pub description: String,
    pub sdk: SdkReq,
    pub protocol: ProtocolReq,
    pub server: ServerReq,
}

/// SDK requirement: package name + accepted range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SdkReq {
    pub name: String,
    pub range: String,
}

/// Protocol requirement: exact wire version window (inclusive).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolReq {
    pub min: u8,
    pub max: u8,
}

/// Server requirement: minimum server release.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerReq {
    pub min: String,
}

/// What a project pins after successful resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectRecord {
    pub blitz_project: u32,
    pub template: String,
    pub template_version: String,
    pub sdk: String,
    pub sdk_version: String,
    pub protocol: u8,
    pub server_min: String,
}

/// Versions known at resolve time (CLI flags / local SDK installs / probe).
#[derive(Debug, Clone, Default)]
pub struct KnownVersions {
    /// Installed SDK versions by package name.
    pub sdks: HashMap<String, String>,
    /// Server version (from `Op::Version` probe or flag).
    pub server: Option<String>,
    /// Server protocol (from `Op::Version` probe or flag).
    pub protocol: Option<u8>,
}

impl TemplateManifest {
    /// Parse + structurally validate (ranges parsed eagerly so bad
    /// manifests fail at discovery, not mid-scaffold).
    pub fn parse(text: &str) -> Result<Self, String> {
        let m: TemplateManifest =
            serde_json::from_str(text).map_err(|e| format!("malformed manifest: {}", e))?;
        if m.manifest != MANIFEST_VERSION {
            return Err(format!(
                "unsupported manifest version {} (want {})",
                m.manifest, MANIFEST_VERSION
            ));
        }
        if m.name.is_empty() || m.name.len() > 128 {
            return Err("template name must be 1..=128 chars".into());
        }
        Version::parse(&m.version)
            .map_err(|_| format!("template version is not semver: {}", m.version))?;
        if m.language.is_empty() {
            return Err("template language is required".into());
        }
        if m.sdk.name.is_empty() {
            return Err("template sdk.name is required".into());
        }
        VersionReq::parse(&m.sdk.range)
            .map_err(|_| format!("sdk range is not a valid semver req: {}", m.sdk.range))?;
        if m.protocol.min == 0 || m.protocol.max < m.protocol.min {
            return Err("protocol window is empty (need 1 <= min <= max)".into());
        }
        Version::parse(&m.server.min)
            .map_err(|_| format!("server.min is not semver: {}", m.server.min))?;
        Ok(m)
    }
}

/// Scan directories for templates (each immediate subdir with a manifest).
/// Bad manifests are reported and skipped (one broken template must not
/// hide the healthy ones); the caller decides empty-is-error.
pub fn discover(dirs: &[PathBuf]) -> (Vec<DiscoveredTemplate>, Vec<String>) {
    let mut found = Vec::new();
    let mut warnings = Vec::new();
    for dir in dirs {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let manifest_path = path.join(TEMPLATE_MANIFEST);
            let text = match std::fs::read_to_string(&manifest_path) {
                Ok(t) => t,
                Err(_) => continue,
            };
            match TemplateManifest::parse(&text) {
                Ok(manifest) => found.push(DiscoveredTemplate { manifest, dir: path }),
                Err(e) => warnings.push(format!("{}: {}", manifest_path.display(), e)),
            }
        }
    }
    found.sort_by(|a, b| a.manifest.name.cmp(&b.manifest.name));
    (found, warnings)
}

/// Check one template against known versions. On success returns the
/// project record to write. Errors read like §35 demands:
/// `BlitzDB SDK x requires ...`, never a mysterious failure later.
pub fn resolve(
    template: &DiscoveredTemplate,
    known: &KnownVersions,
    cli_version: &str,
) -> Result<ProjectRecord, String> {
    let m = &template.manifest;
    // SDK range.
    let sdk_version = known.sdks.get(&m.sdk.name).ok_or_else(|| {
        format!(
            "template '{}' needs SDK '{}' ({}), which is not installed/known.\n\
             Install it or pass --sdk-version name=version.",
            m.name, m.sdk.name, m.sdk.range
        )
    })?;
    let sdk_v = Version::parse(sdk_version)
        .map_err(|_| format!("known SDK version is not semver: {}", sdk_version))?;
    let req = VersionReq::parse(&m.sdk.range).map_err(|_| "unreachable: range pre-validated".to_string())?;
    if !req.matches(&sdk_v) {
        return Err(format!(
            "template '{}' v{} needs {} {}, but {} is installed.\n\
             Install a compatible SDK or pick another template.",
            m.name, m.version, m.sdk.name, m.sdk.range, sdk_version
        ));
    }
    // Protocol window.
    let protocol = known.protocol.ok_or_else(|| {
        format!(
            "template '{}' needs protocol v{}–v{}, but no server was probed.\n\
             Run against a live server or pass --protocol N.",
            m.name, m.protocol.min, m.protocol.max
        )
    })?;
    if protocol < m.protocol.min || protocol > m.protocol.max {
        return Err(format!(
            "template '{}' needs protocol v{}–v{}, but the server speaks v{}.\n\
             Upgrade the server or pick another template.",
            m.name, m.protocol.min, m.protocol.max, protocol
        ));
    }
    // Server floor.
    let server = known.server.as_deref().ok_or_else(|| {
        format!(
            "template '{}' needs server >= {}, but no server was probed.\n\
             Run against a live server or pass --server-version X.Y.Z.",
            m.name, m.server.min
        )
    })?;
    let server_v =
        Version::parse(server).map_err(|_| format!("server version is not semver: {}", server))?;
    let floor =
        Version::parse(&m.server.min).map_err(|_| "unreachable: floor pre-validated".to_string())?;
    if server_v < floor {
        return Err(format!(
            "template '{}' needs BlitzDB server >= {}, but {} is running.\n\
             Upgrade the server or pick another template.",
            m.name, m.server.min, server
        ));
    }
    let _ = cli_version; // Reserved: CLI↔template feature gates (future).
    Ok(ProjectRecord {
        blitz_project: 1,
        template: m.name.clone(),
        template_version: m.version.clone(),
        sdk: m.sdk.name.clone(),
        sdk_version: sdk_version.clone(),
        protocol,
        server_min: m.server.min.clone(),
    })
}

pub fn default_template_dirs() -> Vec<PathBuf> {
    vec![Path::new("templates").to_path_buf()]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(range: &str) -> TemplateManifest {
        TemplateManifest::parse(&format!(
            r#"{{"manifest":1,"name":"demo","version":"0.2.0","language":"typescript",
                "sdk":{{"name":"@blitzdb/client","range":"{}"}},
                "protocol":{{"min":2,"max":2}},"server":{{"min":"0.1.0"}}}}"#,
            range
        ))
        .unwrap()
    }

    fn known() -> KnownVersions {
        KnownVersions {
            sdks: [("@blitzdb/client".to_string(), "0.1.0".to_string())].into(),
            server: Some("0.1.0".to_string()),
            protocol: Some(2),
        }
    }

    fn discovered(m: TemplateManifest) -> DiscoveredTemplate {
        DiscoveredTemplate { manifest: m, dir: PathBuf::from("/tmp/x") }
    }

    #[test]
    fn resolve_happy_path_pins() {
        let rec = resolve(&discovered(manifest(">=0.1.0, <1.0.0")), &known(), "0.1.0").unwrap();
        assert_eq!(rec.sdk_version, "0.1.0");
        assert_eq!(rec.protocol, 2);
    }

    #[test]
    fn sdk_mismatch_names_versions() {
        let err = resolve(&discovered(manifest(">=0.2.0")), &known(), "0.1.0").unwrap_err();
        assert!(err.contains("@blitzdb/client") && err.contains("0.1.0"), "got {}", err);
    }

    #[test]
    fn protocol_mismatch_is_clear() {
        let mut k = known();
        k.protocol = Some(3);
        let err = resolve(&discovered(manifest(">=0.1.0")), &k, "0.1.0").unwrap_err();
        assert!(err.contains("protocol v2–v2") && err.contains("v3"), "got {}", err);
    }

    #[test]
    fn server_floor_is_clear() {
        let mut k = known();
        k.server = Some("0.0.9".to_string());
        let err = resolve(&discovered(manifest(">=0.1.0")), &k, "0.1.0").unwrap_err();
        assert!(err.contains("server >= 0.1.0") && err.contains("0.0.9"), "got {}", err);
    }

    #[test]
    fn bad_manifests_fail_at_parse() {
        assert!(TemplateManifest::parse(r#"{"manifest":99}"#).is_err());
        assert!(TemplateManifest::parse(r#"{"manifest":1,"name":"","version":"x"}"#).is_err());
    }

    #[test]
    fn discover_skips_bad_manifests() {
        let dir = std::env::temp_dir().join(format!("blitz-disc-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("good")).unwrap();
        std::fs::create_dir_all(dir.join("bad")).unwrap();
        std::fs::write(
            dir.join("good").join(TEMPLATE_MANIFEST),
            r#"{"manifest":1,"name":"g","version":"0.1.0","language":"ts",
                "sdk":{"name":"s","range":"*"},"protocol":{"min":2,"max":2},"server":{"min":"0.1.0"}}"#,
        )
        .unwrap();
        std::fs::write(dir.join("bad").join(TEMPLATE_MANIFEST), r#"{"manifest":1}"#).unwrap();
        let (found, warnings) = discover(&[dir.clone()]);
        assert_eq!(found.len(), 1);
        assert_eq!(warnings.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

// ---------------------------------------------------------------------------
// `blitz init` + `blitz status` runtime support
// ---------------------------------------------------------------------------

use std::io::Write;

/// A live server's advertised versions (`Op::Version` handshake).
#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub server: String,
    pub protocol: u8,
}

/// Probe `host:port` with a version handshake (3s budget). Clear errors,
// never hangs: unreachable/timeout/garbage all surface as strings.
pub async fn probe_server(host: &str, port: u16) -> Result<ServerInfo, String> {
    use bytes::BytesMut;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let addr = format!("{}:{}", host, port);
    let mut sock = tokio::time::timeout(std::time::Duration::from_secs(3), TcpStream::connect(&addr))
        .await
        .map_err(|_| format!("connect timed out after 3s: {}", addr))?
        .map_err(|e| format!("connect failed: {}", e))?;
    let codec = blitz_protocol::FrameCodec::with_default_limit();
    let frame = codec
        .encode_request(&blitz_protocol::Request {
            id: 1,
            op: blitz_protocol::Op::Version,
            table: String::new(),
            row_id: None,
            values: None,
        })
        .map_err(|e| format!("encode failed: {}", e))?;
    tokio::time::timeout(std::time::Duration::from_secs(3), sock.write_all(&frame))
        .await
        .map_err(|_| "write timed out".to_string())?
        .map_err(|e| format!("write failed: {}", e))?;
    let mut stash = BytesMut::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let mut tmp = [0u8; 4096];
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            return Err("read timed out".to_string());
        }
        let n = tokio::time::timeout(left, sock.read(&mut tmp))
            .await
            .map_err(|_| "read timed out".to_string())?
            .map_err(|e| format!("read failed: {}", e))?;
        if n == 0 {
            return Err("server closed the connection".to_string());
        }
        stash.extend_from_slice(&tmp[..n]);
        match codec.feed(&mut stash) {
            Ok(None) => continue,
            Ok(Some(payload)) => {
                let resp = codec
                    .decode_response(payload)
                    .map_err(|e| format!("decode failed (protocol mismatch?): {}", e))?;
                if !resp.ok {
                    return Err(format!("server refused version check: {:?}", resp.error));
                }
                let row = resp.rows.into_iter().next().ok_or("empty version response")?;
                let server = row
                    .values
                    .get("server")
                    .and_then(|v| match v {
                        blitz_types::value::Value::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .ok_or("version response missing server string")?;
                let protocol = row
                    .values
                    .get("protocol")
                    .and_then(|v| match v {
                        blitz_types::value::Value::Int64(n) => Some(*n as u8),
                        blitz_types::value::Value::UInt64(n) => Some(*n as u8),
                        _ => None,
                    })
                    .ok_or("version response missing protocol int")?;
                return Ok(ServerInfo { server, protocol });
            }
            Err(e) => return Err(format!("framing failed (protocol mismatch?): {}", e)),
        }
    }
}

/// Arguments for project scaffolding (mirrors the CLI flags).
pub struct InitArgs {
    pub dir: PathBuf,
    pub template_dirs: Vec<PathBuf>,
    pub template: Option<String>,
    pub sdk_version: Vec<String>,
    pub server: Option<String>,
    pub server_version: Option<String>,
    pub protocol: Option<u8>,
    pub yes: bool,
}

/// Scaffold a project: discover → resolve → copy → record.
pub async fn run_init(args: InitArgs) -> anyhow::Result<()> {
    // Target dir must not exist or must be empty (never clobber work).
    if args.dir.exists() {
        let empty = std::fs::read_dir(&args.dir)
            .map(|mut d| d.next().is_none())
            .unwrap_or(false);
        if !empty {
            anyhow::bail!("directory {} exists and is not empty", args.dir.display());
        }
    }
    let dirs = if args.template_dirs.is_empty() {
        default_template_dirs()
    } else {
        args.template_dirs.clone()
    };
    let (found, warnings) = discover(&dirs);
    for w in &warnings {
        eprintln!("warning: {}", w);
    }
    if found.is_empty() {
        anyhow::bail!(
            "no templates found in {}.\n\
             Templates are discovered (never hard-coded): each immediate \
             subdirectory with a blitz.template.json qualifies.\n\
             Pass --templates <dir> (repeatable) to point elsewhere.",
            dirs.iter()
                .map(|d| d.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    // Known versions: flags first, live probe fills gaps.
    let mut known = KnownVersions::default();
    for kv in &args.sdk_version {
        let (name, ver) = kv.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("--sdk-version must be name=version, got {:?}", kv)
        })?;
        known.sdks.insert(name.to_string(), ver.to_string());
    }
    if let Some(addr) = &args.server {
        let (host, port) = addr.split_once(':').ok_or_else(|| {
            anyhow::anyhow!("--server must be host:port, got {:?}", addr)
        })?;
        let port: u16 = port
            .parse()
            .map_err(|_| anyhow::anyhow!("bad port in --server {:?}", addr))?;
        let info = probe_server(host, port)
            .await
            .map_err(|e| anyhow::anyhow!("server probe failed: {}", e))?;
        println!("probed {}: server v{} / protocol v{}", addr, info.server, info.protocol);
        known.server = Some(info.server);
        known.protocol = Some(info.protocol);
    }
    if let Some(v) = args.server_version {
        known.server = Some(v);
    }
    if let Some(p) = args.protocol {
        known.protocol = Some(p);
    }
    // Candidate: named template, --yes fast path, or interactive pick.
    let chosen = if let Some(name) = &args.template {
        found
            .iter()
            .find(|t| &t.manifest.name == name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "template {:?} not found among: {}",
                    name,
                    found.iter().map(|t| t.manifest.name.clone()).collect::<Vec<_>>().join(", ")
                )
            })?
            .clone()
    } else if args.yes {
        found
            .iter()
            .find(|t| resolve(t, &known, env!("CARGO_PKG_VERSION")).is_ok())
            .ok_or_else(|| {
                anyhow::anyhow!("--yes given but no template is fully compatible (see --help for overrides)")
            })?
            .clone()
    } else {
        pick_template(&found, &known)?
    };
    // With a named template or --yes fast path, --yes only skips confirmation.
    if !args.yes {
        print!(
            "scaffold '{}' v{} into {}? [Y/n] ",
            chosen.manifest.name,
            chosen.manifest.version,
            args.dir.display()
        );
        std::io::stdout().flush()?;
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        if matches!(line.trim().to_ascii_lowercase().as_str(), "n" | "no") {
            println!("aborted.");
            return Ok(());
        }
    }
    let record = resolve(&chosen, &known, env!("CARGO_PKG_VERSION"))
        .map_err(|e| anyhow::anyhow!("incompatible template:\n{}", e))?;
    std::fs::create_dir_all(&args.dir)?;
    copy_dir(&chosen.dir, &args.dir)?;
    std::fs::write(
        args.dir.join(PROJECT_FILE),
        serde_json::to_string_pretty(&record)?,
    )?;
    println!(
        "created {} from template '{}' v{}",
        args.dir.display(),
        record.template,
        record.template_version
    );
    println!("  sdk: {} {}", record.sdk, record.sdk_version);
    println!("  protocol v{}, server >= {}", record.protocol, record.server_min);
    println!("  versions pinned in {}", PROJECT_FILE);
    println!("next: run your app against a BlitzDB server (blitz serve).");
    Ok(())
}

/// Interactive picker: compatible templates first (with pins previewed),
/// then incompatible ones greyed with their reason. `--yes` takes the
/// first compatible without asking.
fn pick_template(
    found: &[DiscoveredTemplate],
    known: &KnownVersions,
) -> anyhow::Result<DiscoveredTemplate> {
    let annotated: Vec<(&DiscoveredTemplate, Result<ProjectRecord, String>)> = found
        .iter()
        .map(|t| {
            let r = resolve(t, known, env!("CARGO_PKG_VERSION"));
            (t, r)
        })
        .collect();
    println!("\nChoose a template:\n");
    for (i, (t, r)) in annotated.iter().enumerate() {
        match r {
            Ok(rec) => println!(
                "  {}) {} v{} — {} [{} {}, protocol v{}, server >= {}] (compatible)",
                i + 1,
                t.manifest.name,
                t.manifest.version,
                t.manifest.description,
                rec.sdk,
                rec.sdk_version,
                rec.protocol,
                rec.server_min
            ),
            Err(e) => {
                let first = e.lines().next().unwrap_or("");
                println!(
                    "  {}) {} v{} — {} (INCOMPATIBLE: {})",
                    i + 1,
                    t.manifest.name,
                    t.manifest.version,
                    t.manifest.description,
                    first
                );
            }
        }
    }
    // Non-interactive shells can't pick: demand --template/--yes.
    let n: usize = {
        print!("\nnumber (first compatible in non-interactive use): ");
        std::io::stdout().flush()?;
        let mut line = String::new();
        let bytes = std::io::stdin().read_line(&mut line)?;
        if bytes == 0 {
            // EOF (piped): first compatible or a clear error.
            annotated
                .iter()
                .position(|(_, r)| r.is_ok())
                .ok_or_else(|| {
                    anyhow::anyhow!("no compatible template (all failed resolution; see list above)")
                })?
                + 1
        } else {
            line.trim()
                .parse()
                .map_err(|_| anyhow::anyhow!("expected a number, got {:?}", line.trim()))?
        }
    };
    if n < 1 || n > annotated.len() {
        anyhow::bail!("choice out of range 1..={}", annotated.len());
    }
    let (t, r) = &annotated[n - 1];
    if let Err(e) = r {
        anyhow::bail!("that template is incompatible:\n{}", e);
    }
    Ok((*t).clone())
}

/// Recursive directory copy (template → project). The manifest travels
/// along (it documents the project's origin).
fn copy_dir(src: &Path, dst: &Path) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            std::fs::create_dir_all(&to)?;
            copy_dir(&from, &to)?;
        } else {
            std::fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod init_tests {
    use super::*;

    fn template_dir(tag: &str, manifest: &str, files: &[(&str, &str)]) -> PathBuf {
        let root = std::env::temp_dir().join(format!("blitz-init-{}-{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&root);
        let tpl = root.join("templates").join("demo");
        std::fs::create_dir_all(&tpl).unwrap();
        std::fs::write(tpl.join(TEMPLATE_MANIFEST), manifest).unwrap();
        for (name, content) in files {
            let dest = tpl.join(name);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(dest, content).unwrap();
        }
        root
    }

    const MANIFEST: &str = r#"{"manifest":1,"name":"demo","version":"0.3.0","language":"typescript",
        "sdk":{"name":"@blitzdb/client","range":">=0.1.0, <1.0.0"},
        "protocol":{"min":2,"max":2},"server":{"min":"0.1.0"}}"#;

    #[tokio::test]
    async fn init_scaffolds_and_pins() {
        let root = template_dir("ok", MANIFEST, &[("hello.txt", "hi"), ("sub/nested.txt", "deep")]);
        let target = root.join("proj");
        run_init(InitArgs {
            dir: target.clone(),
            template_dirs: vec![root.join("templates")],
            template: Some("demo".into()),
            sdk_version: vec!["@blitzdb/client=0.1.0".into()],
            server: None,
            server_version: Some("0.1.0".into()),
            protocol: Some(2),
            yes: true,
        })
        .await
        .unwrap();
        assert_eq!(std::fs::read_to_string(target.join("hello.txt")).unwrap(), "hi");
        assert_eq!(std::fs::read_to_string(target.join("sub/nested.txt")).unwrap(), "deep");
        let record: ProjectRecord =
            serde_json::from_str(&std::fs::read_to_string(target.join(PROJECT_FILE)).unwrap()).unwrap();
        assert_eq!(record.template, "demo");
        assert_eq!(record.template_version, "0.3.0");
        assert_eq!(record.sdk_version, "0.1.0");
        assert_eq!(record.protocol, 2);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn init_refuses_incompatible() {
        let root = template_dir("bad", MANIFEST, &[]);
        let err = run_init(InitArgs {
            dir: root.join("proj"),
            template_dirs: vec![root.join("templates")],
            template: Some("demo".into()),
            sdk_version: vec!["@blitzdb/client=0.0.1".into()],
            server: None,
            server_version: Some("0.1.0".into()),
            protocol: Some(2),
            yes: true,
        })
        .await
        .unwrap_err();
        assert!(err.to_string().contains("needs @blitzdb/client"), "got {}", err);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn init_refuses_nonempty_dir() {
        let root = template_dir("ne", MANIFEST, &[]);
        let target = root.join("proj");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(target.join("mine.txt"), "x").unwrap();
        let err = run_init(InitArgs {
            dir: target,
            template_dirs: vec![root.join("templates")],
            template: Some("demo".into()),
            sdk_version: vec![],
            server: None,
            server_version: None,
            protocol: None,
            yes: true,
        })
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not empty"), "got {}", err);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn init_no_templates_is_clear() {
        let root = std::env::temp_dir().join(format!("blitz-init-empty-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let err = run_init(InitArgs {
            dir: root.join("proj"),
            template_dirs: vec![root.join("nothing-here")],
            template: None,
            sdk_version: vec![],
            server: None,
            server_version: None,
            protocol: None,
            yes: true,
        })
        .await
        .unwrap_err();
        assert!(err.to_string().contains("no templates found"), "got {}", err);
        let _ = std::fs::remove_dir_all(&root);
    }
}
