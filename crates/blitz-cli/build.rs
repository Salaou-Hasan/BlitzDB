//! Bundle official templates into the CLI binary (§4–5 of the release
//! architecture: missing resources are a BUILD error, never a runtime
//! source-tree search).
//!
//! Walks `<workspace>/templates/<name>/**`, emits sorted
//! `bundled_templates()` returning `(template, path, bytes)` triples.
//! Deterministic (sorted by template then path); fails the build when
//! `templates/` is absent or contains no manifests.

use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=templates");
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    // crates/blitz-cli -> workspace root.
    let root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("blitz-cli lives at crates/blitz-cli")
        .to_path_buf();
    let templates = root.join("templates");
    assert!(
        templates.is_dir(),
        "templates/ missing at {} — official templates must ship with the release; refusing to build a binary without them",
        templates.display()
    );

    // (template, relpath, abspath), sorted for deterministic embedding.
    let mut files: Vec<(String, String, PathBuf)> = Vec::new();
    let mut names: Vec<String> = dir_names(&templates);
    names.sort();
    for name in &names {
        let tdir = templates.join(name);
        if !tdir.join("blitz.template.json").is_file() {
            panic!(
                "templates/{} has no blitz.template.json — every bundled template needs a manifest",
                name
            );
        }
        collect(&tdir, &tdir, name, &mut files);
    }
    assert!(
        !files.is_empty(),
        "no template files found under templates/ — refusing to build an empty bundle"
    );
    files.sort();

    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("bundled_templates.rs");
    let mut code = String::from(
        "/// Official templates embedded at build time (sorted, deterministic).\n\
         /// `(template name, path inside template, file bytes)`.\n\
         pub fn bundled_templates() -> Vec<(&'static str, &'static str, &'static [u8])> {\n    vec![\n",
    );
    for (template, rel, abs) in &files {
        code.push_str(&format!(
            "        ({:?}, {:?}, include_bytes!({:?})),\n",
            template,
            rel,
            abs.to_string_lossy()
        ));
    }
    code.push_str("    ]\n}\n");
    fs::write(&out, code).expect("write bundled_templates.rs");
    println!(
        "cargo:warning=bundled {} template files from {} template(s)",
        files.len(),
        names.len()
    );
}

fn dir_names(dir: &Path) -> Vec<String> {
    fs::read_dir(dir)
        .expect("read templates/")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .filter_map(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.to_string())
        })
        .collect()
}

fn collect(base: &Path, dir: &Path, template: &str, out: &mut Vec<(String, String, PathBuf)>) {
    let mut entries: Vec<PathBuf> = fs::read_dir(dir)
        .expect("read template dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect(base, &path, template, out);
        } else if path.is_file() {
            let rel = path
                .strip_prefix(base)
                .expect("template file under template dir")
                .to_string_lossy()
                .replace('\\', "/");
            out.push((template.to_string(), rel, path));
        }
    }
}
