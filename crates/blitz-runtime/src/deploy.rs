//! Deploy-time validation for procedures arriving over TCP.
//!
//! Envelope (JSON): `{"v":1,"procedure":{...}}` where `procedure` is the
//! derived-serde shape of [`Procedure`] (enum variants externally tagged,
//! e.g. step `{"Read":{"table":"t","id":{"String":"$x"},"into":"u"}}` and
//! value `{"String":"$x"}`). SDKs generate it; the reference shape is
//! documented in PROTOCOL.md with an example.
//!
//! Validation is structural + registry-aware, NOT a sandbox proof: runtime
//! fuel caps and invoker-rights still apply at call time. Tables are NOT
//! required to exist at deploy (deploy-then-migrate is legitimate).

use crate::function::{Condition, FunctionRegistry, Procedure, ProcedureStep};

/// Deploy envelope version. Bumped only for breaking envelope changes;
/// servers reject unknown versions loudly (never silently reinterpret).
pub const DEPLOY_ENVELOPE_VERSION: u32 = 1;
/// Max steps per procedure (bounds compile + walk time at deploy).
pub const MAX_DEPLOY_STEPS: usize = 256;
/// Max `If` nesting depth (bounds fuel-burn shape + reader recursion).
pub const MAX_DEPLOY_DEPTH: usize = 16;
/// Max procedure / variable / table name lengths (anti-DoS + index health).
pub const MAX_NAME_LEN: usize = 128;
pub const MAX_TEXT_LEN: usize = 1024;

/// Parse + validate a deploy envelope. Returns the procedure on success;
/// the caller assigns the version (monotonic per name, server-side).
///
/// Value literals use deploy-JSON (SDK-consistent, NOT derived-serde):
/// `null`, booleans, numbers (i64-range → Int64, u64-only → UInt64, float
/// → Float64), strings (UUID-shaped → Uuid), arrays, `{"$i32":n}`,
/// `{"$u32":n}`, `{"$i64":n}`, `{"$u64":n}`, `{"$f32":n}`,
/// `{"$decimal":s}`, `{"$date":"YYYY-MM-DD"}`, `{"$uuid":s}`,
/// `{"$bytes":"<base64>"}`, `{"$ts":<micros>}`; any other object → Json.
pub fn deploy_from_json(body: &str, functions: &FunctionRegistry) -> Result<Procedure, String> {
    let envelope: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("malformed envelope: {}", e))?;
    let obj = envelope.as_object().ok_or("envelope must be a JSON object")?;
    let v = obj.get("v").and_then(|v| v.as_u64());
    let proc_json = obj.get("procedure").ok_or("envelope missing 'procedure'")?;
    deploy_from_parsed(v, proc_json, functions)
}

/// Envelope-value entry: version + already-parsed procedure JSON (used by
/// the binary protocol path, where values arrive decoded).
pub fn deploy_from_parsed(
    version: Option<u64>,
    proc_json: &serde_json::Value,
    functions: &FunctionRegistry,
) -> Result<Procedure, String> {
    match version {
        Some(1) => {}
        other => return Err(format!("unsupported envelope version (want 1): {:?}", other)),
    }
    let mut proc_json = proc_json.clone();
    transform_procedure(&mut proc_json)?;
    let proc: Procedure =
        serde_json::from_value(proc_json).map_err(|e| format!("bad procedure: {}", e))?;
    validate_procedure(&proc, functions)?;
    Ok(proc)
}

/// Rewrite deploy-JSON value positions into derived-serde shape in place.
fn transform_procedure(v: &mut serde_json::Value) -> Result<(), String> {
    let steps = v
        .get_mut("steps")
        .and_then(|s| s.as_array_mut())
        .ok_or("procedure.steps must be an array")?;
    for step in steps.iter_mut() {
        transform_step(step)?;
    }
    Ok(())
}

fn transform_step(step: &mut serde_json::Value) -> Result<(), String> {
    let obj = step.as_object_mut().ok_or("step must be an object")?;
    if obj.len() != 1 {
        return Err("step must have exactly one variant".into());
    }
    let variant = obj.iter().next().map(|(k, _)| k.clone()).unwrap();
    if variant == "If" {
        let body_obj = obj.get_mut(&variant).and_then(|b| b.as_object_mut()).unwrap();
        if let Some(c) = body_obj.get_mut("condition") {
            transform_condition(c)?;
        }
        for branch in ["then_steps", "else_steps"] {
            if let Some(steps) = body_obj.get_mut(branch) {
                let arr = steps.as_array_mut().ok_or(format!("If.{} must be an array", branch))?;
                for s in arr.iter_mut() {
                    transform_step(s)?;
                }
            }
        }
        return Ok(());
    }
    let body_obj = obj.get_mut(&variant).and_then(|b| b.as_object_mut()).unwrap();
    let mut field = |body_obj: &mut serde_json::Map<String, serde_json::Value>, name: &str| -> Result<(), String> {
        if let Some(f) = body_obj.get_mut(name) {
            *f = transform_value(std::mem::take(f))?;
        }
        Ok(())
    };
    let mut map_field = |body_obj: &mut serde_json::Map<String, serde_json::Value>, name: &str| -> Result<(), String> {
        if let Some(m) = body_obj.get_mut(name) {
            let map = m.as_object_mut().ok_or(format!("{}.{} must be an object", variant, name))?;
            for (_, v) in map.iter_mut() {
                *v = transform_value(std::mem::take(v))?;
            }
        }
        Ok(())
    };
    match variant.as_str() {
        "CallFunction" => map_field(body_obj, "args")?,
        "SetVariable" => field(body_obj, "value")?,
        "Read" => field(body_obj, "id")?,
        "Insert" => map_field(body_obj, "values")?,
        "Update" => {
            field(body_obj, "id")?;
            map_field(body_obj, "values")?;
        }
        "Delete" => field(body_obj, "id")?,
        "Return" => field(body_obj, "value")?,
        "Fail" => {}
        _ => return Err(format!("unknown step: {}", variant)),
    }
    Ok(())
}

fn transform_condition(cond: &mut serde_json::Value) -> Result<(), String> {
    let obj = cond.as_object_mut().ok_or("condition must be an object")?;
    if obj.len() != 1 {
        return Err("condition must have exactly one variant".into());
    }
    let (variant, body) = obj.iter_mut().next().map(|(k, v)| (k.clone(), v)).unwrap();
    match variant.as_str() {
        "Equals" | "NotEquals" | "GreaterOrEqual" | "LessThan" => {
            let arr = body.as_array_mut().ok_or("comparison needs [var, value]")?;
            if arr.len() != 2 {
                return Err("comparison needs [var, value]".into());
            }
            arr[1] = transform_value(std::mem::take(&mut arr[1]))?;
            Ok(())
        }
        "IsNotNull" => Ok(()),
        "All" | "Any" => {
            let arr = body.as_array_mut().ok_or("All/Any need an array")?;
            for c in arr.iter_mut() {
                transform_condition(c)?;
            }
            Ok(())
        }
        _ => Err(format!("unknown condition: {}", variant)),
    }
}

/// Deploy-JSON literal -> derived-serde JSON (see [`deploy_from_json`]).
fn transform_value(v: serde_json::Value) -> Result<serde_json::Value, String> {
    use serde_json::{json, Value as J};
    match v {
        J::Null => Ok(J::String("Null".into())),
        J::Bool(b) => Ok(json!({"Boolean": b})),
        J::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(json!({"Int64": i}))
            } else if let Some(u) = n.as_u64() {
                Ok(json!({"UInt64": u}))
            } else if let Some(f) = n.as_f64() {
                Ok(json!({"Float64": f}))
            } else {
                Err(format!("bad number: {}", n))
            }
        }
        J::String(s) => {
            if is_uuid_shaped(&s) {
                parse_uuid(&s)
                    .map(|u| json!({"Uuid": u}))
                    .ok_or_else(|| format!("bad uuid: {}", s))
            } else {
                Ok(json!({"String": s}))
            }
        }
        J::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(transform_value(item)?);
            }
            Ok(json!({"Array": out}))
        }
        J::Object(m) => {
            if m.len() == 1 {
                // Exact widths, no narrowing ($i32:5 stays Int32 — the
                // server validates column types strictly).
                if let Some(n) = m.get("$i32").and_then(|x| x.as_i64()) {
                    if (-2147483648..2147483648).contains(&n) {
                        return Ok(json!({"Int32": n as i32}));
                    }
                    return Err(format!("$i32 out of range: {}", n));
                }
                if let Some(n) = m.get("$u32").and_then(|x| x.as_u64()) {
                    if n < 4294967296 {
                        return Ok(json!({"UInt32": n as u32}));
                    }
                    return Err(format!("$u32 out of range: {}", n));
                }
                if let Some(n) = m.get("$i64") {
                    return Ok(json!({"Int64": as_i64_lenient(n)?}));
                }
                if let Some(n) = m.get("$u64") {
                    return Ok(json!({"UInt64": as_u64_lenient(n)?}));
                }
                if let Some(n) = m.get("$f32").and_then(|x| x.as_f64()) {
                    return Ok(json!({"Float32": n as f32}));
                }
                if let Some(s) = m.get("$decimal").and_then(|x| x.as_str()) {
                    return Ok(json!({"Decimal": s}));
                }
                if let Some(s) = m.get("$date").and_then(|x| x.as_str()) {
                    let (y, mo, d) = parse_ymd(s)?;
                    chrono::NaiveDate::from_ymd_opt(y, mo, d)
                        .ok_or_else(|| format!("bad $date: {}", s))?;
                    return Ok(json!({"Date": s}));
                }
                if let Some(s) = m.get("$uuid").and_then(|x| x.as_str()) {
                    parse_uuid(s).ok_or_else(|| format!("bad $uuid: {}", s))?;
                    return Ok(json!({"Uuid": s}));
                }
                if let Some(s) = m.get("$bytes").and_then(|x| x.as_str()) {
                    let bytes = base64_decode(s)?;
                    if bytes.len() > 262_144 {
                        return Err("bytes literal too large (max 256KiB)".into());
                    }
                    return Ok(json!({"Bytes": bytes}));
                }
                if let Some(n) = m.get("$ts") {
                    let micros = as_i64_lenient(n)?;
                    let ts = chrono::DateTime::from_timestamp_micros(micros)
                        .ok_or_else(|| format!("bad $ts: {}", n))?;
                    return Ok(json!({"Timestamp": ts.to_rfc3339()}));
                }
            }
            Ok(json!({"Json": J::Object(m)}))
        }
    }
}

/// Structural + registry-aware validation (see module docs).
pub fn validate_procedure(proc: &Procedure, functions: &FunctionRegistry) -> Result<(), String> {
    check_name("procedure", &proc.name)?;
    if proc.description.len() > MAX_TEXT_LEN {
        return Err("description too long (max 1KiB)".into());
    }
    if proc.steps.is_empty() {
        return Err("procedure needs at least one step".into());
    }
    if proc.steps.len() > MAX_DEPLOY_STEPS {
        return Err(format!("too many steps (max {})", MAX_DEPLOY_STEPS));
    }
    for step in &proc.steps {
        check_step(step, functions, 1)?;
    }
    Ok(())
}

fn check_name(kind: &str, name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > MAX_NAME_LEN {
        return Err(format!("{} name must be 1..={} chars", kind, MAX_NAME_LEN));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ':' | '.' | '-' | '/'))
    {
        return Err(format!("{} name has illegal characters: {}", kind, name));
    }
    Ok(())
}

fn check_table(table: &str) -> Result<(), String> {
    if table.is_empty() || table.len() > MAX_NAME_LEN {
        return Err("table name must be 1..=128 chars".into());
    }
    Ok(())
}

fn check_step(step: &ProcedureStep, functions: &FunctionRegistry, depth: usize) -> Result<(), String> {
    if depth > MAX_DEPLOY_DEPTH + 1 {
        return Err(format!("nesting too deep (max {})", MAX_DEPLOY_DEPTH));
    }
    match step {
        ProcedureStep::CallFunction { function, args } => {
            if !functions.has(function) {
                return Err(format!("unknown function: {}", function));
            }
            if args.len() > 256 {
                return Err("call args too large (max 256 entries)".into());
            }
            Ok(())
        }
        ProcedureStep::SetVariable { name, value } => {
            check_name("variable", name)?;
            check_value_size(value)?;
            Ok(())
        }
        ProcedureStep::If { condition, then_steps, else_steps } => {
            check_condition(condition)?;
            if then_steps.len() + else_steps.len() > MAX_DEPLOY_STEPS {
                return Err("branch too large".into());
            }
            for s in then_steps.iter().chain(else_steps.iter()) {
                check_step(s, functions, depth + 1)?;
            }
            Ok(())
        }
        ProcedureStep::Read { table, into, .. } => {
            check_table(table)?;
            check_name("variable", into)?;
            Ok(())
        }
        ProcedureStep::Insert { table, values, into } => {
            check_table(table)?;
            check_name("variable", into)?;
            check_raw_values(values)?;
            Ok(())
        }
        ProcedureStep::Update { table, values, .. } => {
            check_table(table)?;
            check_raw_values(values)?;
            Ok(())
        }
        ProcedureStep::Delete { table, .. } => {
            check_table(table)?;
            Ok(())
        }
        ProcedureStep::Return { value } => {
            check_value_size(value)?;
            Ok(())
        }
        ProcedureStep::Fail { message } => {
            if message.len() > MAX_TEXT_LEN {
                return Err("fail message too long (max 1KiB)".into());
            }
            Ok(())
        }
    }
}

fn check_condition(cond: &Condition) -> Result<(), String> {
    match cond {
        Condition::Equals(v, e) | Condition::NotEquals(v, e) => {
            check_name("variable", v)?;
            check_value_size(e)?;
            Ok(())
        }
        Condition::IsNotNull(v) => check_name("variable", v),
        Condition::GreaterOrEqual(v, e) | Condition::LessThan(v, e) => {
            check_name("variable", v)?;
            check_value_size(e)?;
            Ok(())
        }
        Condition::All(cs) | Condition::Any(cs) => {
            if cs.len() > 64 {
                return Err("condition list too large".into());
            }
            for c in cs {
                check_condition(c)?;
            }
            Ok(())
        }
    }
}

fn check_raw_values(values: &std::collections::HashMap<String, blitz_types::value::Value>) -> Result<(), String> {
    if values.len() > 256 {
        return Err("values map too large (max 256 entries)".into());
    }
    for v in values.values() {
        check_value_size(v)?;
    }
    Ok(())
}

fn check_value_size(v: &blitz_types::value::Value) -> Result<(), String> {
    use blitz_types::value::Value;
    match v {
        Value::String(s) if s.len() > 65536 => Err("string literal too large (max 64KiB)".into()),
        Value::Bytes(b) if b.len() > 262_144 => Err("bytes literal too large (max 256KiB)".into()),
        Value::Array(items) if items.len() > 1024 => Err("array literal too large".into()),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::{register_builtins, FunctionRegistry};

    fn registry() -> FunctionRegistry {
        let mut r = FunctionRegistry::new();
        register_builtins(&mut r);
        r
    }

    fn envelope(proc: serde_json::Value) -> String {
        serde_json::json!({"v": 1, "procedure": proc}).to_string()
    }

    #[test]
    fn valid_minimal_procedure() {
        let body = envelope(serde_json::json!({
            "name": "ping_fn", "description": "d",
            "steps": [{"SetVariable": {"name": "x", "value": {"Int64": 1}}}]
        }));
        let proc = deploy_from_json(&body, &registry()).unwrap();
        assert_eq!(proc.name, "ping_fn");
    }

    #[test]
    fn bad_version_rejected() {
        let body = serde_json::json!({"v": 99, "procedure": {}}).to_string();
        let err = deploy_from_json(&body, &registry()).unwrap_err();
        assert!(err.contains("version"), "got {}", err);
    }

    #[test]
    fn unknown_function_rejected() {
        let body = envelope(serde_json::json!({
            "name": "bad", "description": "",
            "steps": [{"CallFunction": {"function": "nope", "args": {}}}]
        }));
        let err = deploy_from_json(&body, &registry()).unwrap_err();
        assert!(err.contains("unknown function"), "got {}", err);
    }

    #[test]
    fn bad_name_rejected() {
        for name in ["", "has space", "semi;colon", &"x".repeat(200)] {
            let body = envelope(serde_json::json!({
                "name": name, "description": "",
                "steps": [{"SetVariable": {"name": "x", "value": null}}]
            }));
            assert!(deploy_from_json(&body, &registry()).is_err(), "name {:?}", name);
        }
    }

    #[test]
    fn deep_nesting_rejected() {
        fn nest(depth: usize) -> serde_json::Value {
            if depth == 0 {
                return serde_json::json!({"SetVariable": {"name": "x", "value": null}});
            }
            serde_json::json!({"If": {
                "condition": {"IsNotNull": "y"},
                "then_steps": [nest(depth - 1)],
                "else_steps": [],
            }})
        }
        let body = envelope(serde_json::json!({
            "name": "deep", "description": "",
            "steps": [nest(40)],
        }));
        let err = deploy_from_json(&body, &registry()).unwrap_err();
        assert!(err.contains("deep"), "got {}", err);
    }

    #[test]
    fn empty_steps_rejected() {
        let body = envelope(serde_json::json!({"name": "e", "description": "", "steps": []}));
        assert!(deploy_from_json(&body, &registry()).is_err());
    }
}

fn as_i64_lenient(v: &serde_json::Value) -> Result<i64, String> {
    match v {
        serde_json::Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| format!("not an i64: {}", n)),
        serde_json::Value::String(s) => s
            .parse::<i64>()
            .map_err(|_| format!("not an i64: {}", s)),
        _ => Err("not an i64".into()),
    }
}

fn as_u64_lenient(v: &serde_json::Value) -> Result<u64, String> {
    match v {
        serde_json::Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| format!("not a u64: {}", n)),
        serde_json::Value::String(s) => s
            .parse::<u64>()
            .map_err(|_| format!("not a u64: {}", s)),
        _ => Err("not a u64".into()),
    }
}

fn is_uuid_shaped(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 36
        && b[8] == b'-'
        && b[13] == b'-'
        && b[18] == b'-'
        && b[23] == b'-'
        && s.chars().all(|c| c.is_ascii_hexdigit() || c == '-')
}

/// Shape-check only; the derived `Uuid` deserializer validates fully.
fn parse_uuid(s: &str) -> Option<String> {
    is_uuid_shaped(s).then(|| s.to_string())
}

fn parse_ymd(s: &str) -> Result<(i32, u32, u32), String> {
    let mut it = s.split('-');
    match (it.next(), it.next(), it.next(), it.next()) {
        (Some(y), Some(mo), Some(d), None) => {
            let y: i32 = y.parse().map_err(|_| format!("bad $date: {}", s))?;
            let mo: u32 = mo.parse().map_err(|_| format!("bad $date: {}", s))?;
            let d: u32 = d.parse().map_err(|_| format!("bad $date: {}", s))?;
            Ok((y, mo, d))
        }
        _ => Err(format!("bad $date (want YYYY-MM-DD): {}", s)),
    }
}

fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    const ALPH: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut vals = [0u8; 128];
    for (i, c) in ALPH.iter().enumerate() {
        vals[*c as usize] = i as u8;
    }
    let bytes: Vec<u8> = s.bytes().filter(|b| *b != b'\r' && *b != b'\n').collect();
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if bytes.len() % 4 != 0 {
        return Err("bad $bytes base64 length".into());
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let mut n = 0u32;
        let mut pad = 0;
        for b in chunk {
            if *b == b'=' {
                pad += 1;
                n <<= 6;
            } else {
                let c = *b as usize;
                if c >= 128 || (vals[c] == 0 && *b != b'A') {
                    return Err("bad $bytes base64 character".into());
                }
                n = (n << 6) | vals[c] as u32;
            }
        }
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Ok(out)
}
