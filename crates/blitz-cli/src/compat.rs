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

/// A discovered template: manifest plus its file set (path → bytes).
/// Sources are uniform — disk directories and the embedded bundle both
/// produce this shape, so scaffolding never branches on template origin
/// (§11: data-driven, no per-template business logic).
#[derive(Debug, Clone)]
pub struct DiscoveredTemplate {
    pub manifest: TemplateManifest,
    /// `(path inside template with `/` separators, content)`, sorted.
    pub files: Vec<(String, Vec<u8>)>,
    /// Where it came from (for messages): `bundled`, `user`, or the dir.
    pub source: String,
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

/// Official templates embedded in the binary at build time (sorted,
/// deterministic). Missing bundle = build error, never a runtime search.
mod bundled {
    include!(concat!(env!("OUT_DIR"), "/bundled_templates.rs"));
}

/// Read one on-disk template directory into file bytes (sorted).
fn read_template_dir(path: &Path) -> Result<(TemplateManifest, Vec<(String, Vec<u8>)>), String> {
    let manifest_path = path.join(TEMPLATE_MANIFEST);
    let text = std::fs::read_to_string(&manifest_path)
        .map_err(|_| format!("no {}", TEMPLATE_MANIFEST))?;
    let manifest = TemplateManifest::parse(&text)
        .map_err(|e| format!("{}: {}", manifest_path.display(), e))?;
    let mut files = Vec::new();
    collect_files(path, path, &mut files)?;
    files.sort();
    Ok((manifest, files))
}

fn collect_files(base: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) -> Result<(), String> {
    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("read {}: {}", dir.display(), e))?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files(base, &path, out)?;
        } else if path.is_file() {
            let rel = path
                .strip_prefix(base)
                .map_err(|e| format!("path: {}", e))?
                .to_string_lossy()
                .replace('\\', "/");
            let bytes = std::fs::read(&path).map_err(|e| format!("read {}: {}", path.display(), e))?;
            out.push((rel, bytes));
        }
    }
    Ok(())
}

/// Scan directories for templates (each immediate subdir with a manifest).
/// Bad manifests are reported and skipped (one broken template must not
/// hide the healthy ones); the caller decides empty-is-error.
pub fn discover(dirs: &[PathBuf]) -> (Vec<DiscoveredTemplate>, Vec<String>) {
    let mut found = Vec::new();
    let mut warnings = Vec::new();
    for dir in dirs {
        let source = dir.display().to_string();
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            match read_template_dir(&path) {
                Ok((manifest, files)) => found.push(DiscoveredTemplate {
                    manifest,
                    files,
                    source: source.clone(),
                }),
                Err(e) => {
                    // Missing manifest = not a template dir, skip quietly;
                    // present-but-broken = warn loudly.
                    if path.join(TEMPLATE_MANIFEST).is_file() {
                        warnings.push(format!("{}: {}", path.display(), e));
                    }
                }
            }
        }
    }
    found.sort_by(|a, b| a.manifest.name.cmp(&b.manifest.name));
    (found, warnings)
}

/// Discover from ALL sources in priority order (§5 of the template spec):
/// explicit `--templates` dirs (flag order), then the user directory
/// (`~/.blitzdb/templates`), then bundled official templates. Same name in
/// an earlier source shadows later ones (warned, deterministic).
pub fn discover_all(explicit: &[PathBuf]) -> (Vec<DiscoveredTemplate>, Vec<String>) {
    let mut found = Vec::new();
    let mut warnings = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    let mut consider = |templates: Vec<DiscoveredTemplate>, mut warns: Vec<String>| {
        for t in templates {
            if seen.contains(&t.manifest.name) {
                warns.push(format!(
                    "template '{}' from {} shadows same-named template (using the first)",
                    t.manifest.name, t.source
                ));
                continue;
            }
            seen.insert(t.manifest.name.clone());
            found.push(t);
        }
        warnings.append(&mut warns);
    };

    let (explicit_found, explicit_warn) = discover(explicit);
    consider(explicit_found, explicit_warn);

    if let Some(home) = home_dir() {
        let user_dir = home.join(".blitzdb").join("templates");
        if user_dir.is_dir() {
            let (user_found, user_warn) = discover(&[user_dir]);
            consider(user_found, user_warn);
        }
    }

    let (bundled_found, bundled_warn) = discover_bundled();
    consider(bundled_found, bundled_warn);

    found.sort_by(|a, b| a.manifest.name.cmp(&b.manifest.name));
    (found, warnings)
}

/// Bundled official templates (embedded at build; always available,
/// offline included).
pub fn discover_bundled() -> (Vec<DiscoveredTemplate>, Vec<String>) {
    let mut found = Vec::new();
    let mut by_template: std::collections::BTreeMap<String, Vec<(String, Vec<u8>)>> =
        std::collections::BTreeMap::new();
    for (template, path, bytes) in bundled::bundled_templates() {
        by_template
            .entry(template.to_string())
            .or_default()
            .push((path.to_string(), bytes.to_vec()));
    }
    let mut warnings = Vec::new();
    for (name, mut files) in by_template {
        files.sort();
        let manifest_idx = files.iter().position(|(p, _)| p == TEMPLATE_MANIFEST);
        let manifest_text = match manifest_idx {
            Some(i) => String::from_utf8_lossy(&files[i].1).into_owned(),
            None => {
                warnings.push(format!("bundled template '{}' lacks a manifest (build bug)", name));
                continue;
            }
        };
        match TemplateManifest::parse(&manifest_text) {
            Ok(manifest) => {
                if manifest.name != name {
                    warnings.push(format!(
                        "bundled template dir '{}' declares name '{}' (using declared name)",
                        name, manifest.name
                    ));
                }
                found.push(DiscoveredTemplate {
                    manifest,
                    files,
                    source: "bundled".to_string(),
                })
            }
            Err(e) => warnings.push(format!("bundled template '{}': {}", name, e)),
        }
    }
    found.sort_by(|a, b| a.manifest.name.cmp(&b.manifest.name));
    (found, warnings)
}

fn home_dir() -> Option<PathBuf> {
    if let Ok(home) = std::env::var("HOME") {
        if !home.is_empty() {
            return Some(PathBuf::from(home));
        }
    }
    std::env::var("USERPROFILE")
        .ok()
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
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

/// User-level template directory (`~/.blitzdb/templates`), scanned
/// between explicit dirs and bundled officials. Documented; created by
/// convention, never required.
pub fn user_template_dir() -> Option<PathBuf> {
    home_dir().map(|h| h.join(".blitzdb").join("templates"))
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
        DiscoveredTemplate { manifest: m, files: Vec::new(), source: "test".to_string() }
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
    // Sources in priority order: explicit --templates dirs, the user
    // directory, then bundled officials. No ./templates default anymore:
    // the release binary must not assume a source checkout is nearby.
    // (Pass --templates explicitly to use repo trees while contributing.)
    let (found, warnings) = discover_all(&args.template_dirs);
    for w in &warnings {
        eprintln!("warning: {}", w);
    }
    if found.is_empty() {
        anyhow::bail!(
            "no templates found (explicit --templates dirs, ~/.blitzdb/templates, bundled).\n\
             Point --templates <dir> (repeatable) at directories whose \
             immediate subdirectories each carry a blitz.template.json."
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
    write_files(&chosen.files, &args.dir)?;
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

/// Materialize a template's file set (works identically for disk and
/// bundled sources — scaffolding never branches on template origin).
fn write_files(files: &[(String, Vec<u8>)], dst: &Path) -> anyhow::Result<()> {
    for (rel, bytes) in files {
        let dest = dst.join(rel);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&dest, bytes)?;
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
    async fn init_bundled_needs_no_source_tree() {
        // The acceptance core: no --templates flag at all (and cwd is the
        // repo here, which must NOT matter — bundled discovery ignores cwd).
        let root = std::env::temp_dir().join(format!("blitz-init-bundled-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let err = run_init(InitArgs {
            dir: root.join("proj"),
            template_dirs: vec![],
            template: Some("definitely-not-a-template".into()),
            sdk_version: vec![],
            server: None,
            server_version: None,
            protocol: None,
            yes: true,
        })
        .await
        .unwrap_err();
        // Unknown name proves discovery RAN (bundled set is non-empty);
        // the error lists real templates instead of "no templates found".
        assert!(!err.to_string().contains("no templates found"), "got {}", err);
        assert!(err.to_string().contains("not found among"), "got {}", err);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn init_explicit_dir_still_works() {
        // Backward compat: --templates with a plain dir (repo-tree style).
        let root = std::env::temp_dir().join(format!("blitz-init-expl-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let custom = root.join("custom");
        let tpl = custom.join("mine");
        std::fs::create_dir_all(&tpl).unwrap();
        std::fs::write(
            tpl.join(TEMPLATE_MANIFEST),
            r#"{"manifest":1,"name":"mine","version":"0.1.0","language":"rust",
                "sdk":{"name":"blitz-client-rs","range":"*"},"protocol":{"min":2,"max":2},"server":{"min":"0.1.0"}}"#,
        )
        .unwrap();
        std::fs::write(tpl.join("note.txt"), "custom").unwrap();
        run_init(InitArgs {
            dir: root.join("proj"),
            template_dirs: vec![custom],
            template: Some("mine".into()),
            sdk_version: vec!["blitz-client-rs=0.2.2".into()],
            server: None,
            server_version: Some("0.2.2".into()),
            protocol: Some(2),
            yes: true,
        })
        .await
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("proj").join("note.txt")).unwrap(),
            "custom"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}

/// One release-check result: name, pass/fail, detail.
pub struct ReleaseCheck {
    pub name: &'static str,
    pub pass: bool,
    pub detail: String,
}

/// Validate this binary as a release artifact (§7): version metadata,
/// bundled templates (present, parse, self-consistent with THIS binary's
/// protocol/server), and a scaffold dry-run per template into a temp dir.
/// No network, no source tree, no server needed. SDK-range resolution is
/// covered separately by `init` e2e (it needs chosen SDK versions); here
/// each template only must be structurally valid and protocol-compatible
/// with this exact binary.
pub fn release_check() -> Vec<ReleaseCheck> {
    let mut out = Vec::new();
    // 1. Version metadata parses as semver.
    let cli_version = env!("CARGO_PKG_VERSION");
    match semver::Version::parse(cli_version) {
        Ok(_) => out.push(ReleaseCheck {
            name: "version-metadata",
            pass: true,
            detail: format!("blitz-cli v{}", cli_version),
        }),
        Err(e) => out.push(ReleaseCheck {
            name: "version-metadata",
            pass: false,
            detail: format!("CARGO_PKG_VERSION is not semver: {}", e),
        }),
    }
    // 2. Bundled templates present + valid + self-consistent.
    let (bundled, warnings) = discover_bundled();
    for w in warnings {
        out.push(ReleaseCheck { name: "bundled-manifest", pass: false, detail: w });
    }
    if bundled.is_empty() {
        out.push(ReleaseCheck {
            name: "bundled-templates",
            pass: false,
            detail: "no bundled templates (release without templates is a build bug)".into(),
        });
        return out;
    }
    let own_protocol = blitz_protocol::PROTOCOL_VERSION;
    let own_server = match semver::Version::parse(blitz_server::SERVER_VERSION) {
        Ok(v) => v,
        Err(e) => {
            out.push(ReleaseCheck {
                name: "server-version",
                pass: false,
                detail: format!("SERVER_VERSION is not semver: {}", e),
            });
            return out;
        }
    };
    for t in &bundled {
        let m = &t.manifest;
        let mut problems = Vec::new();
        if own_protocol < m.protocol.min || own_protocol > m.protocol.max {
            problems.push(format!("protocol window v{}–v{} excludes this binary (v{})", m.protocol.min, m.protocol.max, own_protocol));
        }
        match semver::Version::parse(&m.server.min) {
            Ok(floor) if own_server < floor => problems.push(format!(
                "server floor {} is newer than this binary ({})",
                m.server.min,
                blitz_server::SERVER_VERSION
            )),
            Err(_) => problems.push(format!("server.min is not semver: {}", m.server.min)),
            _ => {}
        }
        if t.files.is_empty() {
            problems.push("template has no files".into());
        }
        if problems.is_empty() {
            out.push(ReleaseCheck {
                name: "template",
                pass: true,
                detail: format!("{} v{} ({} files)", m.name, m.version, t.files.len()),
            });
        } else {
            out.push(ReleaseCheck {
                name: "template",
                pass: false,
                detail: format!("{}: {}", m.name, problems.join("; ")),
            });
        }
    }
    // 3. Scaffold dry-run per template (files materialize, no resolve).
    let scratch = std::env::temp_dir().join(format!("blitz-relcheck-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    for t in &bundled {
        let dest = scratch.join(&t.manifest.name);
        let ok = (|| -> Result<usize, String> {
            std::fs::create_dir_all(&dest).map_err(|e| e.to_string())?;
            write_files(&t.files, &dest).map_err(|e| e.to_string())?;
            Ok(t.files.len())
        })();
        match ok {
            Ok(n) => out.push(ReleaseCheck {
                name: "scaffold",
                pass: true,
                detail: format!("{} materialized ({} files)", t.manifest.name, n),
            }),
            Err(e) => out.push(ReleaseCheck {
                name: "scaffold",
                pass: false,
                detail: format!("{}: {}", t.manifest.name, e),
            }),
        }
    }
    let _ = std::fs::remove_dir_all(&scratch);
    out
}
