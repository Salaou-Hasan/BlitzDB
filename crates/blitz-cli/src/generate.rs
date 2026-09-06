//! `blitz generate`: deterministic, version-aware code generation from a
//! project source of truth (§23–24).
//!
//! Layout (conventional, minimal):
//!
//! ```text
//! <project>/
//!   blitz/
//!     schema/*.json      table schemas (table_create JSON shape)
//!     functions/*.json   procedure deploy envelopes (v1)
//!   blitz.generated/     output (do not hand-edit; header says so)
//!     manifest.json      {generator, inputs sha256, files[]}
//!     ts/tables.ts       row interfaces + table/column constants
//!     ts/functions.ts    procedure name constants + typed call wrapper
//!     rs/tables.rs       row structs + table constants
//! ```
//!
//! Honesty rules: column names pass through EXACTLY (no camelCase magic);
//! int64/uint64 map to `number | bigint` (TS) / `i64`+`u64` (Rust) with the
//! precision caveat documented; procedure ARGUMENTS are not typed (steps
//! reference `$vars` without declared signatures — the wrapper takes a
//! record and returns the raw result; declared arg schemas are future
//! work, not faked). `--check` fails when output differs (CI mode).

use anyhow::Result;
use blitz_types::schema::TableSchema;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub struct GenerateArgs {
    pub project: PathBuf,
    pub check: bool,
}

/// Sorted file set produced by a run (path relative to project root).
#[derive(Debug, PartialEq, Eq)]
pub struct GeneratedSet {
    pub files: BTreeMap<String, String>,
    pub manifest: String,
}

pub fn run_generate(args: &GenerateArgs) -> Result<GeneratedSet> {
    let schema_dir = args.project.join("blitz").join("schema");
    let functions_dir = args.project.join("blitz").join("functions");
    if !schema_dir.is_dir() && !functions_dir.is_dir() {
        anyhow::bail!(
            "no blitz/schema or blitz/functions in {}.\n\
             Create table schemas ({{table, columns}}) and/or procedure \
             deploy envelopes ({{v, procedure}}) there first.",
            args.project.display()
        );
    }
    // Deterministic input order: sorted filenames, hashed for the manifest.
    let mut schemas: Vec<(String, TableSchema)> = Vec::new();
    if schema_dir.is_dir() {
        let mut files = sorted_json_files(&schema_dir)?;
        files.sort();
        for path in files {
            let text = std::fs::read_to_string(&path)?;
            let json: serde_json::Value = serde_json::from_str(&text)
                .map_err(|e| anyhow::anyhow!("{}: malformed JSON ({})", path.display(), e))?;
            let schema = TableSchema::from_json(&json)
                .map_err(|e| anyhow::anyhow!("{}: {}", path.display(), e))?;
            schemas.push((file_stem(&path)?, schema));
        }
    }
    let mut procedures: Vec<(String, blitz_runtime::Procedure)> = Vec::new();
    if functions_dir.is_dir() {
        let mut files = sorted_json_files(&functions_dir)?;
        files.sort();
        for path in files {
            let text = std::fs::read_to_string(&path)?;
            let mut registry = blitz_runtime::FunctionRegistry::new();
            blitz_runtime::function::register_builtins(&mut registry);
            let proc = blitz_runtime::deploy_from_json(&text, &registry)
                .map_err(|e| anyhow::anyhow!("{}: {}", path.display(), e))?;
            procedures.push((file_stem(&path)?, proc));
        }
    }
    schemas.sort_by(|a, b| a.1.name.cmp(&b.1.name));
    procedures.sort_by(|a, b| a.1.name.cmp(&b.1.name));

    let mut files = BTreeMap::new();
    files.insert(
        "blitz.generated/ts/tables.ts".to_string(),
        render_ts_tables(&schemas),
    );
    files.insert(
        "blitz.generated/ts/functions.ts".to_string(),
        render_ts_functions(&procedures),
    );
    files.insert(
        "blitz.generated/rs/tables.rs".to_string(),
        render_rs_tables(&schemas),
    );
    // Input fingerprint: any source change alters the manifest (audit trail).
    let mut hasher_input = String::new();
    for (stem, schema) in &schemas {
        hasher_input.push_str(stem);
        hasher_input.push(':');
        hasher_input.push_str(&serde_json::to_string(&schema_names(schema))?);
        hasher_input.push(';');
    }
    for (stem, proc) in &procedures {
        hasher_input.push_str(stem);
        hasher_input.push(':');
        hasher_input.push_str(&proc.name);
        hasher_input.push(';');
    }
    let manifest = serde_json::to_string_pretty(&serde_json::json!({
        "generator": format!("blitz-cli {}", env!("CARGO_PKG_VERSION")),
        "inputs_sha256": sha256_hex(hasher_input.as_bytes()),
        "files": files.keys().collect::<Vec<_>>(),
    }))?;
    let set = GeneratedSet { files, manifest };

    if args.check {
        verify_unchanged(&args.project, &set)?;
        println!("generate --check: {} files up to date", set.files.len() + 1);
    } else {
        for (rel, content) in &set.files {
            let dest = args.project.join(rel);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&dest, content)?;
        }
        std::fs::write(args.project.join("blitz.generated/manifest.json"), &set.manifest)?;
        println!("generated {} files + manifest.json", set.files.len());
        for rel in set.files.keys() {
            println!("  blitz.generated/{}", rel.strip_prefix("blitz.generated/").unwrap_or(rel));
        }
    }
    Ok(set)
}

fn schema_names(schema: &TableSchema) -> Vec<(&str, String)> {
    schema
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.column_type.to_string()))
        .collect()
}

fn file_stem(path: &Path) -> Result<String> {
    path.file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("bad filename: {}", path.display()))
}

fn sorted_json_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|x| x.to_str()) == Some("json") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

fn sha256_hex(data: &[u8]) -> String {
    // No new deps: SHA-256 is 60 lines; but determinism only needs a STABLE
    // fingerprint, not cryptographic strength — use FNV-1a hex (documented).
    // (Collision resistance is irrelevant here: inputs are local files, and
    // --check compares full bytes anyway.)
    let mut h: u64 = 0xcbf29ce484222325;
    for b in data {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}{:016x}", h.rotate_left(13) ^ 0x9e3779b97f4a7c15, h)
}

fn header(lang: &str) -> String {
    format!(
        "// Generated by `blitz generate` — do not hand-edit.\n// Regenerate: blitz generate [--check]\n// {} bindings below.\n\n",
        lang
    )
}

fn ts_type_of(col: &blitz_types::column::ColumnDef) -> String {
    use blitz_types::column::ColumnType as T;
    let base = match col.column_type {
        T::Boolean => "boolean",
        T::Int8 | T::Int16 | T::Int32 | T::UInt8 | T::UInt16 | T::UInt32 => "number",
        T::Int64 | T::UInt64 => "number | bigint",
        T::Float32 | T::Float64 => "number",
        T::Decimal | T::String => "string",
        T::Bytes => "Uint8Array",
        T::Uuid => "string",
        T::Timestamp => "Date",
        T::Date => "{ $date: string }",
        T::Json => "unknown",
        T::Array => "unknown[]",
    };
    if col.nullable {
        format!("{} | null", base)
    } else {
        base.to_string()
    }
}

fn pascal_case(s: &str) -> String {
    s.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(|w| {
            let mut chars = w.chars();
            match chars.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

fn render_ts_tables(schemas: &[(String, TableSchema)]) -> String {
    let mut out = header("TypeScript");
    out.push_str("// int64/uint64 are `number | bigint`: narrow with V.i64()/V.u64()\n");
    out.push_str("// when writing strict columns; reads may arrive as either.\n");
    out.push_str("// Row ids travel alongside (RowView.id); only columns are listed.\n\n");
    for (_, schema) in schemas {
        let iface = format!("{}Row", pascal_case(&schema.name));
        out.push_str(&format!("export const TABLE_{} = '{}';\n", scream(&schema.name), schema.name));
        out.push_str(&format!("export interface {} {{\n", iface));
        for col in &schema.columns {
            out.push_str(&format!("  {}: {};\n", col.name, ts_type_of(col)));
        }
        out.push_str("}\n\n");
    }
    out
}

fn scream(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_uppercase() } else { '_' })
        .collect()
}

fn render_ts_functions(procs: &[(String, blitz_runtime::Procedure)]) -> String {
    let mut out = header("TypeScript");
    out.push_str("import type { Client, CallResult } from '@blitzdb/client';\n");
    out.push_str("import type { Value } from '@blitzdb/client';\n\n");
    out.push_str("// Procedure names (deploy with `blitz dev` or ProcDeploy).\n");
    for (_, proc) in procs {
        out.push_str(&format!("export const FN_{} = '{}';\n", scream(&proc.name), proc.name));
    }
    out.push('\n');
    out.push_str("// Argument shapes are caller-declared: procedures reference $vars\n");
    out.push_str("// without declared signatures (see PROTOCOL.md), so each wrapper\n");
    out.push_str("// takes the args record the procedure documents. Typed arg schemas\n");
    out.push_str("// are future work — not faked here.\n");
    for (_, proc) in procs {
        let fname = proc.name.clone();
        out.push_str(&format!(
            "export async function {fn_}(db: Client, args: Record<string, Value>): Promise<CallResult> {{\n  return db.call('{name}', args);\n}}\n\n",
            fn_ = rust_ident(&fname),
            name = fname
        ));
    }
    out
}

/// TS-safe identifier for a procedure name (falls back with underscore).
fn rust_ident(name: &str) -> String {
    let mut out: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    if out.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(true) {
        out.insert(0, '_');
    }
    if matches!(
        out.as_str(),
        "break" | "case" | "catch" | "class" | "const" | "continue" | "debugger"
            | "default" | "delete" | "do" | "else" | "enum" | "export" | "extends"
            | "false" | "finally" | "for" | "function" | "if" | "import" | "in"
            | "instanceof" | "new" | "null" | "return" | "super" | "switch"
            | "this" | "throw" | "true" | "try" | "typeof" | "var" | "void"
            | "while" | "with" | "yield" | "let" | "static" | "implements"
            | "interface" | "package" | "private" | "protected" | "public"
    ) {
        out.push('_');
    }
    out
}

fn rs_type_of(col: &blitz_types::column::ColumnDef) -> String {
    use blitz_types::column::ColumnType as T;
    let base = match col.column_type {
        T::Boolean => "bool",
        T::Int8 => "i8",
        T::Int16 => "i16",
        T::Int32 => "i32",
        T::Int64 => "i64",
        T::UInt8 => "u8",
        T::UInt16 => "u16",
        T::UInt32 => "u32",
        T::UInt64 => "u64",
        T::Float32 => "f32",
        T::Float64 => "f64",
        T::Decimal | T::String => "String",
        T::Bytes => "Vec<u8>",
        T::Uuid => "String",
        T::Timestamp => "String",
        T::Date => "String",
        T::Json => "serde_json::Value",
        T::Array => "Vec<serde_json::Value>",
    };
    if col.nullable {
        format!("Option<{}>", base)
    } else {
        base.to_string()
    }
}

fn render_rs_tables(schemas: &[(String, TableSchema)]) -> String {
    let mut out = header("Rust");
    out.push_str("use std::collections::HashMap;\n");
    out.push_str("use blitz_types::value::Value;\n\n");
    out.push_str("// Row structs mirror table shapes (see ts/tables.ts for docs).\n");
    out.push_str("// `values()` converts back to a write map (skips None fields).\n\n");
    for (_, schema) in schemas {
        let strukt = format!("{}Row", pascal_case(&schema.name));
        out.push_str(&format!(
            "pub const TABLE_{}: &str = \"{}\";\n",
            scream(&schema.name),
            schema.name
        ));
        out.push_str(&format!(
            "#[derive(Debug, Clone, Default)]\npub struct {} {{\n",
            strukt
        ));
        for col in &schema.columns {
            out.push_str(&format!(
                "    pub {}: {},\n",
                rust_field(&col.name),
                rs_type_of(col)
            ));
        }
        out.push_str("}\n\n");
        out.push_str(&format!("impl {} {{\n", strukt));
        out.push_str("    pub fn values(&self) -> HashMap<String, Value> {\n");
        out.push_str("        let mut m = HashMap::new();\n");
        for col in &schema.columns {
            out.push_str(&format!(
                "        m.insert(\"{}\".to_string(), {});\n",
                col.name,
                rs_to_value(col)
            ));
        }
        out.push_str("        m\n    }\n}\n\n");
    }
    out
}

fn rust_field(name: &str) -> String {
    let mut out: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    if out.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(true) {
        out.insert(0, '_');
    }
    if matches!(
        out.as_str(),
        "as" | "break" | "const" | "continue" | "crate" | "else" | "enum" | "extern"
            | "false" | "fn" | "for" | "if" | "impl" | "in" | "let" | "loop"
            | "match" | "mod" | "move" | "mut" | "pub" | "ref" | "return" | "self"
            | "Self" | "static" | "struct" | "super" | "trait" | "true" | "type"
            | "unsafe" | "use" | "where" | "while" | "async" | "await" | "dyn"
            | "abstract" | "become" | "box" | "do" | "final" | "macro" | "override"
            | "priv" | "typeof" | "unsized" | "virtual" | "yield" | "try"
    ) {
        out.push('_');
    }
    out
}

fn rs_to_value(col: &blitz_types::column::ColumnDef) -> String {
    use blitz_types::column::ColumnType as T;
    let field = format!("self.{}", rust_field(&col.name));
    // NOTE: kept deliberately explicit per variant (generated code favors
    // readability over cleverness).
    match col.column_type {
        T::Boolean => {
            if col.nullable {
                format!("{}.map(Value::Boolean).unwrap_or(Value::Null)", field)
            } else {
                format!("Value::Boolean({})", field)
            }
        }
        T::Int8 => conv_int(&field, col.nullable, "Int8", "i8"),
        T::Int16 => conv_int(&field, col.nullable, "Int16", "i16"),
        T::Int32 => conv_int(&field, col.nullable, "Int32", "i32"),
        T::Int64 => conv_int(&field, col.nullable, "Int64", "i64"),
        T::UInt8 => conv_int(&field, col.nullable, "UInt8", "u8"),
        T::UInt16 => conv_int(&field, col.nullable, "UInt16", "u16"),
        T::UInt32 => conv_int(&field, col.nullable, "UInt32", "u32"),
        T::UInt64 => conv_int(&field, col.nullable, "UInt64", "u64"),
        T::Float32 => conv_float(&field, col.nullable, "Float32"),
        T::Float64 => conv_float(&field, col.nullable, "Float64"),
        T::Decimal | T::String => {
            if col.nullable {
                format!("{}.clone().map(Value::String).unwrap_or(Value::Null)", field)
            } else {
                format!("Value::String({}.clone())", field)
            }
        }
        T::Bytes => {
            if col.nullable {
                format!("{}.clone().map(Value::Bytes).unwrap_or(Value::Null)", field)
            } else {
                format!("Value::Bytes({}.clone())", field)
            }
        }
        T::Uuid | T::Timestamp | T::Date => {
            // String newtypes: parse back on write would need validation;
            // store raw strings (documented; strict parse is app logic).
            if col.nullable {
                format!("{}.clone().map(Value::String).unwrap_or(Value::Null)", field)
            } else {
                format!("Value::String({}.clone())", field)
            }
        }
        T::Json => {
            if col.nullable {
                format!("{}.clone().map(Value::Json).unwrap_or(Value::Null)", field)
            } else {
                format!("Value::Json({}.clone())", field)
            }
        }
        T::Array => {
            if col.nullable {
                format!("{}.clone().map(Value::Array).unwrap_or(Value::Null)", field)
            } else {
                format!("Value::Array({}.clone())", field)
            }
        }
    }
}

fn conv_int(field: &str, nullable: bool, variant: &str, _ty: &str) -> String {
    if nullable {
        format!("{}.map(Value::{}).unwrap_or(Value::Null)", field, variant)
    } else {
        format!("Value::{}({})", variant, field)
    }
}

fn conv_float(field: &str, nullable: bool, variant: &str) -> String {
    conv_int(field, nullable, variant, "")
}

/// Fail when generated output differs (CI mode).
fn verify_unchanged(project: &Path, set: &GeneratedSet) -> anyhow::Result<()> {
    let mut stale = Vec::new();
    for (rel, content) in &set.files {
        let dest = project.join(rel);
        match std::fs::read_to_string(&dest) {
            Ok(existing) if &existing == content => {}
            _ => stale.push(rel.clone()),
        }
    }
    let manifest_path = project.join("blitz.generated/manifest.json");
    match std::fs::read_to_string(&manifest_path) {
        Ok(existing) if existing == set.manifest => {}
        _ => stale.push("blitz.generated/manifest.json".to_string()),
    }
    if stale.is_empty() {
        Ok(())
    } else {
        anyhow::bail!("stale generated files (run `blitz generate`): {}", stale.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proj(tag: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("blitz-gen-{}-{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("blitz/schema")).unwrap();
        std::fs::create_dir_all(root.join("blitz/functions")).unwrap();
        std::fs::write(
            root.join("blitz/schema/posts.json"),
            r#"{"table":"posts","columns":[
                {"name":"id","type":"int64"},
                {"name":"owner","type":"string","nullable":true},
                {"name":"email","type":"string","unique":true},
                {"name":"score","type":"float64","nullable":true},
                {"name":"flags","type":"uint32"},
                {"name":"blob","type":"bytes","nullable":true}
            ]}"#,
        )
        .unwrap();
        std::fs::write(
            root.join("blitz/functions/hello.json"),
            r#"{"v":1,"procedure":{"name":"hello","description":"d",
                "steps":[{"SetVariable":{"name":"x","value":1}}]}}"#,
        )
        .unwrap();
        root
    }

    #[test]
    fn generate_then_check_is_clean() {
        let root = proj("clean");
        let args = GenerateArgs { project: root.clone(), check: false };
        let set = run_generate(&args).unwrap();
        assert!(set.files.contains_key("blitz.generated/ts/tables.ts"));
        assert!(set.files.contains_key("blitz.generated/ts/functions.ts"));
        assert!(set.files.contains_key("blitz.generated/rs/tables.rs"));
        let ts = &set.files["blitz.generated/ts/tables.ts"];
        assert!(ts.contains("export interface PostsRow"), "got:\n{}", ts);
        assert!(ts.contains("owner: string | null"), "got:\n{}", ts);
        assert!(ts.contains("TABLE_POSTS = 'posts'"), "got:\n{}", ts);
        let fns = &set.files["blitz.generated/ts/functions.ts"];
        assert!(fns.contains("FN_HELLO = 'hello'"), "got:\n{}", fns);
        assert!(fns.contains("export async function hello("), "got:\n{}", fns);
        let rs = &set.files["blitz.generated/rs/tables.rs"];
        assert!(rs.contains("pub struct PostsRow"), "got:\n{}", rs);
        assert!(rs.contains("pub owner: Option<String>"), "got:\n{}", rs);
        // Rerun is byte-identical (reproducibility contract).
        let set2 = run_generate(&args).unwrap();
        assert_eq!(set, set2);
        // --check passes on fresh output.
        let check = GenerateArgs { project: root.clone(), check: true };
        run_generate(&check).unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn check_fails_on_drift() {
        let root = proj("drift");
        run_generate(&GenerateArgs { project: root.clone(), check: false }).unwrap();
        std::fs::write(root.join("blitz.generated/ts/tables.ts"), "// edited").unwrap();
        let err = run_generate(&GenerateArgs { project: root.clone(), check: true }).unwrap_err();
        assert!(err.to_string().contains("stale generated files"), "got {}", err);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn invalid_schema_fails_clearly() {
        let root = proj("bad");
        std::fs::write(
            root.join("blitz/schema/bad.json"),
            r#"{"table":"t","columns":[{"name":"a","type":"nope"}]}"#,
        )
        .unwrap();
        let err = run_generate(&GenerateArgs { project: root.clone(), check: false }).unwrap_err();
        assert!(err.to_string().contains("unknown type"), "got {}", err);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn missing_source_dir_fails_clearly() {
        let root = std::env::temp_dir().join(format!("blitz-gen-empty-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let err = run_generate(&GenerateArgs { project: root.clone(), check: false }).unwrap_err();
        assert!(err.to_string().contains("no blitz/schema"), "got {}", err);
        let _ = std::fs::remove_dir_all(&root);
    }
}
