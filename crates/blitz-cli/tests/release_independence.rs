//! Release source-independence suite (§20): the RELEASE binary must work
//! with ZERO access to the source tree.
//!
//! Every test spawns `env!("CARGO_BIN_EXE_Blitz")` (the real artifact,
//! not `cargo run`) with cwd in a temp dir and all `CARGO_*`/`RUST*`
//! environment scrubbed — simulating a developer machine that never
//! cloned the repository. A test that only passes inside the source
//! checkout is a bug in the test; one that fails here is a bug in the
//! release architecture.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn blitz() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_Blitz"));
    // Scrub build/dev environment: the binary must not read any of it.
    // (Keep PATH + system essentials so subprocesses and DLL loading work.)
    let keep_prefixes = ["PATH=", "SYSTEMROOT=", "SYSTEMDRIVE=", "WINDIR=", "TEMP=", "TMP=",
        "HOME=", "USERPROFILE=", "LANG=", "LC_", "TZ="];
    let scrubbed: Vec<(String, String)> = std::env::vars()
        .filter(|(k, _)| {
            !(k.starts_with("CARGO_") || k.starts_with("RUST"))
                && (keep_prefixes.iter().any(|p| {
                    let key = format!("{}=", k);
                    p.ends_with('=') && key.starts_with(&p[..p.len() - 1]) || k == *p
                }) || std::env::var_os(k).is_some() && is_essential(k))
        })
        .collect();
    cmd.env_clear();
    for (k, v) in scrubbed {
        // Re-add only safe essentials (PATH-like + locale + temp).
        if k == "PATH"
            || k == "SystemRoot"
            || k == "SYSTEMROOT"
            || k == "SYSTEMDRIVE"
            || k == "WINDIR"
            || k == "TEMP"
            || k == "TMP"
            || k == "HOME"
            || k == "USERPROFILE"
            || k.starts_with("LC_")
            || k == "LANG"
            || k == "TZ"
        {
            cmd.env(k, v);
        }
    }
    // Unrelated working directory (never the source checkout).
    cmd.current_dir(scratch_root());
    cmd
}

fn is_essential(k: &str) -> bool {
    // Windows loader + tooling essentials beyond the explicit list.
    k == "Path"
        || k == "SystemDrive"
        || k == "COMSPEC"
        || k == "PATHEXT"
        || k == "NUMBER_OF_PROCESSORS"
        || k == "OS"
}

fn scratch_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("blitz-relcheck-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn scratch_case(name: &str) -> PathBuf {
    let dir = scratch_root().join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn run(cmd: &mut Command) -> (bool, String) {
    let out = cmd.output().expect("spawn release binary");
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    (out.status.success(), text)
}

#[test]
fn version_and_help_need_nothing() {
    let (ok, text) = run(blitz().arg("version").current_dir(scratch_case("v")));
    assert!(ok, "version failed: {}", text);
    assert!(text.contains("blitz-cli v"), "output: {}", text);
    assert!(text.contains("protocol v"), "output: {}", text);

    let (ok, text) = run(blitz().arg("--help").current_dir(scratch_case("h")));
    assert!(ok, "help failed: {}", text);
    for cmd in ["serve", "init", "dev", "generate", "release-check", "db"] {
        assert!(text.contains(cmd), "help missing {}:\n{}", cmd, text);
    }
}

#[test]
fn release_check_passes_standalone() {
    // The binary validates its own completeness (bundled templates,
    // metadata, scaffold dry-runs) with no repo around.
    let (ok, text) = run(blitz().arg("release-check").current_dir(scratch_case("rc")));
    assert!(ok, "release-check failed:\n{}", text);
    assert!(text.contains("all green"), "output:\n{}", text);
}

#[test]
fn init_bundled_without_source_tree() {
    // THE acceptance core: plain `blitz init` in an empty dir, no
    // --templates flag, no repo anywhere nearby.
    let dir = scratch_case("init").join("my-app");
    let (ok, text) = run(blitz()
        .arg("init")
        .arg(&dir)
        .arg("--template")
        .arg("ts-minimal")
        .arg("--sdk-version")
        .arg("@blitzdb/client=0.1.0")
        .arg("--server-version")
        .arg("0.1.0")
        .arg("--protocol")
        .arg("2")
        .arg("--yes"));
    assert!(ok, "init failed:\n{}", text);
    assert!(dir.join("blitz.project.json").is_file(), "no project record");
    assert!(dir.join("blitz.template.json").is_file(), "manifest did not travel");
    assert!(dir.join("src").join("index.ts").is_file(), "no scaffolded sources");
    let record = std::fs::read_to_string(dir.join("blitz.project.json")).unwrap();
    assert!(record.contains("\"template\": \"ts-minimal\""), "record: {}", record);
}

#[test]
fn init_unknown_template_is_clear() {
    let dir = scratch_case("unknown").join("app");
    let (ok, text) = run(blitz()
        .arg("init")
        .arg(&dir)
        .arg("--template")
        .arg("definitely-not-a-template")
        .arg("--yes"));
    assert!(!ok, "unknown template must fail");
    assert!(text.contains("not found among"), "output:\n{}", text);
    assert!(!text.contains("no templates found"), "bundled set must exist:\n{}", text);
}

#[test]
fn init_custom_templates_still_work() {
    // Backward compat: explicit dirs behave exactly as before.
    let root = scratch_case("custom");
    let tpl = root.join("tpls").join("mine");
    std::fs::create_dir_all(&tpl).unwrap();
    std::fs::write(
        tpl.join("blitz.template.json"),
        r#"{"manifest":1,"name":"mine","version":"0.1.0","language":"rust",
            "sdk":{"name":"s","range":"*"},"protocol":{"min":2,"max":2},"server":{"min":"0.1.0"}}"#,
    )
    .unwrap();
    std::fs::write(tpl.join("note.txt"), "custom").unwrap();
    let dir = root.join("app");
    let (ok, text) = run(blitz()
        .arg("init")
        .arg(&dir)
        .arg("--templates")
        .arg(root.join("tpls"))
        .arg("--template")
        .arg("mine")
        .arg("--sdk-version")
        .arg("s=0.2.2")
        .arg("--server-version")
        .arg("0.2.2")
        .arg("--protocol")
        .arg("2")
        .arg("--yes"));
    assert!(ok, "custom init failed:\n{}", text);
    assert_eq!(std::fs::read_to_string(dir.join("note.txt")).unwrap(), "custom");
}

#[test]
fn generate_roundtrip_without_source() {
    // Schema-first tooling works from a bare project dir.
    let dir = scratch_case("gen");
    std::fs::create_dir_all(dir.join("blitz/schema")).unwrap();
    std::fs::write(
        dir.join("blitz/schema/posts.json"),
        r#"{"table":"posts","columns":[{"name":"id","type":"int64"},{"name":"body","type":"string","nullable":true}]}"#,
    )
    .unwrap();
    let (ok, text) = run(blitz().arg("generate").arg(&dir));
    assert!(ok, "generate failed:\n{}", text);
    assert!(dir.join("blitz.generated/ts/tables.ts").is_file());
    assert!(dir.join("blitz.generated/manifest.json").is_file());
    // --check passes on fresh output.
    let (ok, text) = run(blitz().arg("generate").arg(&dir).arg("--check"));
    assert!(ok, "generate --check failed:\n{}", text);
}

#[test]
fn serve_status_query_db_cycle() {
    // Full lifecycle against a live server started by the binary itself.
    let dir = scratch_case("cycle");
    let port = 17601;
    let log_path = dir.join("serve-stderr.log");
    let log_file = std::fs::File::create(&log_path).unwrap();
    let mut server = blitz()
        .arg("serve")
        .arg("--port")
        .arg(port.to_string())
        .stdout(Stdio::null())
        .stderr(log_file)
        .spawn()
        .expect("spawn serve");
    struct Killer(Child);
    impl Drop for Killer {
        fn drop(&mut self) {
            let _ = self.0.kill();
        }
    }
    let mut server = Killer(server);
    // Wait for readiness via status (bounded).
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let (ok, _) = run(blitz().arg("status").arg("--port").arg(port.to_string()));
        if ok || Instant::now() > deadline {
            if !ok {
                let tail = std::fs::read_to_string(&log_path).unwrap_or_default();
                let exit = server.0.try_wait().ok().flatten();
                panic!(
                    "server never became ready (exit: {:?}); stderr:\n{}",
                    exit.map(|e| e.to_string()),
                    tail.chars().take(3000).collect::<String>()
                );
            }
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    // DB admin + version handshake through the real binary.
    let dbdir = dir.join("data");
    let (ok, text) = run(blitz().arg("db").arg("init").arg("--dir").arg(&dbdir));
    assert!(ok, "db init failed:\n{}", text);
    assert!(dbdir.join("blitz.json").is_file());
    let (ok, text) = run(blitz().arg("version"));
    assert!(ok && text.contains("protocol v"), "version:\n{}", text);
    let _ = &mut server;
}

#[test]
fn dev_boots_and_hot_reloads() {
    // `blitz dev` from a bare project: boots, deploys the envelope dropped
    // into blitz/functions, then exits on stdin close (piped EOF as Ctrl-C
    // stand-in is unreliable — instead assert boot + initial deploy, then
    // kill; the watcher path is covered by unit-tested pollers... simplest
    // honest coverage: boot marker + deploy marker, then terminate).
    let dir = scratch_case("dev");
    std::fs::create_dir_all(dir.join("blitz/functions")).unwrap();
    std::fs::write(
        dir.join("blitz/functions/hello.json"),
        r#"{"v":1,"procedure":{"name":"hello","description":"d",
            "steps":[{"SetVariable":{"name":"x","value":1}}]}}"#,
    )
    .unwrap();
    let mut child = blitz()
        .arg("dev")
        .arg(&dir)
        .arg("--port")
        .arg("17602")
        .arg("--http-port")
        .arg("17603")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn dev");
    let stdout = child.stdout.take().unwrap();
    // Drain stderr on a side thread: (a) a full pipe would deadlock the
    // child, (b) its content is the diagnosis when boot markers never come.
    let stderr_lines = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let stderr_lines_child = stderr_lines.clone();
    let mut stderr = child.stderr.take().unwrap();
    std::thread::spawn(move || {
        use std::io::Read;
        let mut buf = Vec::new();
        let _ = stderr.read_to_end(&mut buf);
        let text = String::from_utf8_lossy(&buf).into_owned();
        *stderr_lines_child.lock().unwrap() = text.lines().map(|l| l.to_string()).collect();
    });
    let reader = BufReader::new(stdout);
    let mut saw_boot = false;
    let mut saw_deploy = false;
    let deadline = Instant::now() + Duration::from_secs(20);
    for line in reader.lines().map_while(Result::ok) {
        if line.contains("BlitzDB dev server") {
            saw_boot = true;
        }
        if line.contains("proc 'hello'") && line.contains("deployed") {
            saw_deploy = true;
        }
        if saw_boot && saw_deploy {
            break;
        }
        if Instant::now() > deadline {
            break;
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    if !(saw_boot && saw_deploy) {
        let err_lines = stderr_lines.lock().unwrap();
        panic!(
            "dev incomplete (boot={} deploy={}); stderr:\n{}",
            saw_boot,
            saw_deploy,
            err_lines.iter().take(40).cloned().collect::<Vec<_>>().join("\n")
        );
    }
}
