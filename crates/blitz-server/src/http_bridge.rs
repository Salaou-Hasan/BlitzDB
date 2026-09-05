//! JSON-over-HTTP data bridge (mobile / curl / webhook story).
//!
//! The binary TCP protocol stays the hot path; this bridge trades ~µs of
//! JSON parsing for universality. One uniform envelope mirrors the ops:
//!
//! ```text
//! POST /v1/op    {"op":"insert","table":"users","values":{...}} -> {"ok":true,"rows":[...]}
//! POST /v1/batch {"ops":[{...}],"atomic":false}                 -> {"ok":true,"results":[...]}
//! ```
//!
//! Auth: `Authorization: Bearer <token>` per request (stateless — resolved
//! fresh every call; no connection stickiness to manage). Server error
//! payloads stay in-band (`{"ok":false,"error":...}`, HTTP 200) except
//! auth failures (401/403 by prefix) and malformed envelopes (400).
//! Collection rules, routing, atomicity and procedures behave exactly as on
//! TCP (same `dispatch`/`execute_atomic` underneath) — including
//! `fn:<name>` tables for `call` and base-name stability under sharding.
//!
//! JSON value rules (documented, SDK-consistent): safe-integer numbers ->
//! Int64, other numbers -> Float64 (the server is type-strict: use
//! `{"$i32":n}`/`{"$u32":n}`/`{"$i64":n}`/`{"$u64":n}`/`{"$f32":n}` for
//! exact widths); UUID-shaped strings -> Uuid; `{"$decimal":s}`,
//! `{"$date":"YYYY-MM-DD"}`, `{"$uuid":s}`, `{"$bytes":"<base64>"}`,
//! `{"$ts":<micros>}` or RFC3339 strings -> Timestamp. Outbound: big u64
//! (incl. sharded RowIds) that don't fit f64-safely come back as STRINGS;
//! bytes as `{"$bytes":...}`, dates/decimals/uuids tagged as inbound.

use std::collections::HashMap;

use blitz_protocol::{BatchRequest, Op, Request};
use blitz_types::value::Value;
use serde_json::json;

use crate::server::BlitzServer;

/// JSON envelope -> [`Request`]. `table` defaults to `""` (ping).
/// `row_id` accepts numbers or numeric strings (sharded ids exceed f64).
pub fn request_from_json(v: &serde_json::Value) -> Result<Request, String> {
    let obj = v.as_object().ok_or("body must be a JSON object")?;
    let opname = obj
        .get("op")
        .and_then(|o| o.as_str())
        .ok_or("missing string field 'op'")?;
    let op = match opname {
        "ping" => Op::Ping,
        "insert" => Op::Insert,
        "get" => Op::Get,
        "update" => Op::Update,
        "delete" => Op::Delete,
        "scan" => Op::Scan,
        "subscribe" => Op::Subscribe,
        "find" => Op::Find,
        "search" => Op::Search,
        "call" => Op::Call,
        "job_submit" => Op::JobSubmit,
        "job_poll" => Op::JobPoll,
        "proc_deploy" => Op::ProcDeploy,
        "proc_list" => Op::ProcList,
        "proc_drop" => Op::ProcDrop,
        _ => return Err(format!("unknown op: {}", opname)),
    };
    let id = obj
        .get("id")
        .map(json_to_u64)
        .transpose()?
        .unwrap_or(0);
    let table = obj
        .get("table")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();
    let row_id = obj.get("row_id").map(json_to_u64).transpose()?;
    let values = match obj.get("values") {
        None => None,
        Some(serde_json::Value::Object(m)) => {
            let mut out = HashMap::with_capacity(m.len());
            for (k, jv) in m {
                out.insert(k.clone(), json_to_value(jv)?);
            }
            Some(out)
        }
        Some(_) => return Err("'values' must be a JSON object".into()),
    };
    Ok(Request { id, op, table, row_id, values })
}

fn json_to_u64(v: &serde_json::Value) -> Result<u64, String> {
    match v {
        serde_json::Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| format!("id out of u64 range: {}", n)),
        serde_json::Value::String(s) => s
            .parse::<u64>()
            .map_err(|_| format!("id not a u64: {}", s)),
        _ => Err("id must be a number or numeric string".into()),
    }
}

/// JSON -> Value (see module docs for the rules).
pub fn json_to_value(v: &serde_json::Value) -> Result<Value, String> {
    match v {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int64(i))
            } else if let Some(u) = n.as_u64() {
                Ok(Value::UInt64(u))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float64(f))
            } else {
                Err(format!("bad number: {}", n))
            }
        }
        serde_json::Value::String(s) => {
            if is_uuid_shaped(s) {
                parse_uuid(s).map(Value::Uuid).ok_or_else(|| format!("bad uuid: {}", s))
            } else {
                Ok(Value::String(s.clone()))
            }
        }
        serde_json::Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(json_to_value(item)?);
            }
            Ok(Value::Array(out))
        }
        serde_json::Value::Object(m) => {
            if m.len() == 1 {
                if let Some(n) = m.get("$i32").and_then(|x| x.as_i64()) {
                    return Ok(Value::Int32(n as i32));
                }
                if let Some(n) = m.get("$u32").and_then(|x| x.as_u64()) {
                    return Ok(Value::UInt32(n as u32));
                }
                if let Some(n) = m.get("$i64") {
                    return Ok(Value::Int64(as_i64_lenient(n)?));
                }
                if let Some(n) = m.get("$u64") {
                    return Ok(Value::UInt64(as_u64_lenient(n)?));
                }
                if let Some(n) = m.get("$f32").and_then(|x| x.as_f64()) {
                    return Ok(Value::Float32(n as f32));
                }
                if let Some(s) = m.get("$decimal").and_then(|x| x.as_str()) {
                    return Ok(Value::Decimal(s.to_string()));
                }
                if let Some(s) = m.get("$date").and_then(|x| x.as_str()) {
                    let (y, mo, d) = parse_ymd(s)?;
                    return chrono::NaiveDate::from_ymd_opt(y, mo, d)
                        .map(Value::Date)
                        .ok_or_else(|| format!("bad $date: {}", s));
                }
                if let Some(s) = m.get("$uuid").and_then(|x| x.as_str()) {
                    return parse_uuid(s)
                        .map(Value::Uuid)
                        .ok_or_else(|| format!("bad uuid: {}", s));
                }
                if let Some(s) = m.get("$bytes").and_then(|x| x.as_str()) {
                    return base64_decode(s)
                        .map(Value::Bytes)
                        .ok_or_else(|| "bad $bytes base64".to_string());
                }
                if let Some(n) = m.get("$ts") {
                    let micros = as_i64_lenient(n)?;
                    return chrono::DateTime::from_timestamp_micros(micros)
                        .map(|t| Value::Timestamp(t.to_utc()))
                        .ok_or_else(|| format!("bad $ts: {}", n));
                }
            }
            // Plain object -> Json passthrough.
            Ok(Value::Json(v.clone()))
        }
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

fn parse_uuid(s: &str) -> Option<uuid::Uuid> {
    if !is_uuid_shaped(s) {
        return None;
    }
    uuid::Uuid::parse_str(s).ok()
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

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(s).ok()
}

fn base64_encode(b: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(b)
}

/// Value -> JSON (see module docs; big u64 become strings).
pub fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Int8(n) => json!(*n),
        Value::Int16(n) => json!(*n),
        Value::Int32(n) => json!(*n),
        Value::Int64(n) => json!(*n),
        Value::UInt8(n) => json!(*n),
        Value::UInt16(n) => json!(*n),
        Value::UInt32(n) => json!(*n),
        Value::UInt64(n) => {
            if *n <= (1u64 << 53) {
                json!(*n)
            } else {
                serde_json::Value::String(n.to_string())
            }
        }
        Value::Float32(n) => json!(*n),
        Value::Float64(n) => json!(*n),
        Value::Decimal(s) => json!({ "$decimal": s }),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => json!({ "$bytes": base64_encode(b) }),
        Value::Uuid(u) => serde_json::Value::String(u.to_string()),
        Value::Timestamp(t) => json!({ "$ts": t.timestamp_micros() }),
        Value::Date(d) => json!({ "$date": d.format("%Y-%m-%d").to_string() }),
        Value::Json(j) => j.clone(),
        Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(value_to_json).collect())
        }
    }
}

fn u64_to_json(n: u64) -> serde_json::Value {
    if n <= (1u64 << 53) {
        json!(n)
    } else {
        serde_json::Value::String(n.to_string())
    }
}

fn response_to_json(id: u64, resp: &blitz_protocol::Response) -> serde_json::Value {
    let rows: Vec<serde_json::Value> = resp
        .rows
        .iter()
        .map(|r| {
            let mut values = serde_json::Map::with_capacity(r.values.len());
            for (k, v) in &r.values {
                values.insert(k.clone(), value_to_json(v));
            }
            json!({ "id": u64_to_json(r.id), "values": values })
        })
        .collect();
    match &resp.error {
        Some(e) => json!({ "id": id, "ok": false, "rows": rows, "error": e }),
        None => json!({ "id": id, "ok": resp.ok, "rows": rows }),
    }
}

/// Auth prefix mapping for HTTP status: table-level denials stay in-band
/// (per-op payloads), but a wholly-unauthenticated request is 401 and a
/// policy denial on the envelope is 403.
fn auth_status(err: &str) -> Option<u16> {
    if err.starts_with("unauthorized") {
        Some(401)
    } else if err.starts_with("forbidden") {
        Some(403)
    } else {
        None
    }
}

/// Handle one `POST /v1/op` body. Returns (status, json body).
pub fn handle_op(server: &std::sync::Arc<BlitzServer>, ident: &Option<blitz_auth::Identity>, body: &[u8]) -> (u16, String) {
    let parsed: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return (400, json!({"ok": false, "error": format!("malformed JSON: {}", e)}).to_string()),
    };
    let req = match request_from_json(&parsed) {
        Ok(r) => r,
        Err(e) => return (400, json!({"ok": false, "error": e}).to_string()),
    };
    if let Err(e) = server.authorize(ident, req.op, &req.table) {
        let code = auth_status(e).unwrap_or(200);
        return (code, json!({"ok": false, "error": e}).to_string());
    }
    let id = req.id;
    let resp = crate::transport::dispatch(server, ident, req);
    // Surface envelope-level auth failures with status too (row-owner
    // denials on single ops); per-row successes stay 200.
    if !resp.ok {
        if let Some(e) = &resp.error {
            if let Some(code) = auth_status(e) {
                return (code, response_to_json(id, &resp).to_string());
            }
        }
    }
    (200, response_to_json(id, &resp).to_string())
}

/// Handle one `POST /v1/batch` body: `{"ops":[...],"atomic":bool}`.
/// Non-atomic batches return per-op results (partial failure normal);
/// atomic frames are all-or-nothing via the same OCC path as TCP.
pub fn handle_batch(server: &std::sync::Arc<BlitzServer>, ident: &Option<blitz_auth::Identity>, body: &[u8]) -> (u16, String) {
    let parsed: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return (400, json!({"ok": false, "error": format!("malformed JSON: {}", e)}).to_string()),
    };
    let obj = match parsed.as_object() {
        Some(o) => o,
        None => return (400, json!({"ok": false, "error": "body must be a JSON object"}).to_string()),
    };
    let ops_json = match obj.get("ops").and_then(|o| o.as_array()) {
        Some(a) => a,
        None => return (400, json!({"ok": false, "error": "missing array field 'ops'"}).to_string()),
    };
    if ops_json.len() > 4096 {
        return (400, json!({"ok": false, "error": "batch too large (max 4096 ops)"}).to_string());
    }
    let atomic = obj.get("atomic").and_then(|a| a.as_bool()).unwrap_or(false);
    let id = obj.get("id").map(json_to_u64).transpose();
    let id = match id {
        Ok(v) => v.unwrap_or(0),
        Err(e) => return (400, json!({"ok": false, "error": e}).to_string()),
    };
    let mut ops = Vec::with_capacity(ops_json.len());
    for (i, oj) in ops_json.iter().enumerate() {
        match request_from_json(oj) {
            Ok(r) => ops.push(r),
            Err(e) => {
                return (400, json!({"ok": false, "error": format!("ops[{}]: {}", i, e)}).to_string())
            }
        }
    }
    // Envelope auth: every op authorized upfront (denials become err
    // payloads per op for plain batches; atomic aborts whole-frame).
    // Handshake tokens inside ops are honored like TCP (`_auth` value).
    let mut authed = ident.clone();
    let mut denied: Vec<Option<&'static str>> = Vec::with_capacity(ops.len());
    for op in ops.iter_mut() {
        if let Some(values) = op.values.as_mut() {
            if let Some(tok) = values.remove("_auth").and_then(|v| match v {
                Value::String(s) => Some(s),
                _ => None,
            }) {
                if let Some(resolved) = server.resolve_token(&tok) {
                    authed = Some(resolved);
                }
            }
        }
        denied.push(server.authorize(&authed, op.op, &op.table).err());
    }
    if atomic {
        // Atomic path re-authorizes internally per op set; pre-check here so
        // the abort reason names the failing op (same message shape).
        if let Some((i, e)) = denied.iter().enumerate().find_map(|(i, d)| d.map(|e| (i, e))) {
            let msg = format!("atomic batch aborted: unauthorized op {}: {}", ops[i].id, e);
            let results: Vec<serde_json::Value> = ops
                .iter()
                .map(|op| json!({"id": op.id, "ok": false, "rows": [], "error": msg}))
                .collect();
            return (200, json!({"id": id, "ok": true, "results": results}).to_string());
        }
        let batch = BatchRequest { id, ops };
        let bresp = crate::transport::execute_atomic(server, &mut authed, batch);
        let results: Vec<serde_json::Value> = bresp
            .results
            .iter()
            .map(|r| response_to_json(r.id, r))
            .collect();
        return (200, json!({"id": bresp.id, "ok": true, "results": results}).to_string());
    }
    let mut results = Vec::with_capacity(ops.len());
    for (op, deny) in ops.into_iter().zip(denied.into_iter()) {
        let rid = op.id;
        match deny {
            Some(e) => results.push(json!({"id": rid, "ok": false, "rows": [], "error": e})),
            None => {
                let resp = crate::transport::dispatch(server, &authed, op);
                results.push(response_to_json(rid, &resp));
            }
        }
    }
    (200, json!({"id": id, "ok": true, "results": results}).to_string())
}
