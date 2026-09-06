//! Wire messages for the BlitzDB binary protocol.
//!
//! Frames on the wire are `[ver: u8][len: u32 LE][binary payload]` (see
//! [`codec`]). Values are [`blitz_types::Value`] encoded with the compact
//! tag-length scheme in [`codec`], never JSON. Rows travel as [`RowView`]
//! so row ids never collide with user columns.

use std::collections::HashMap;

use blitz_types::value::Value;

/// Operations a client can request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Ping,
    Insert,
    Get,
    Update,
    Delete,
    Scan,
    /// Bounded change long-poll: recent writes to `table` after `_since`.
    /// Polling (not push) keeps framing strictly request/response so
    /// pipelining/batching math and tail budgets stay intact.
    Subscribe,
    /// O(1) point lookup via a unique/PK column:
    /// `values {_col: String, _val: Value}`. Non-unique columns are rejected
    /// instead of degrading into full scans on the hot path.
    Find,
    /// Bounded exact-term search over indexed post bodies:
    /// `values {_q: String, _limit: Int}`. Single-node inverted index
    /// (8 terms/post, 128/posting cap). Multi-term ANDs first two terms.
    Search,
    /// Execute a registered server-side procedure transactionally:
    /// `table` carries the resource `fn:<name>` (policy namespace),
    /// `values` are the call arguments. Runs all steps in one OCC
    /// transaction (all-or-nothing); responds one row with the result.
    Call,
    /// Submit a WASM job for background execution: `values {wasm: Bytes,
    /// input: String}`. Returns immediately (`{job_id, status}`); poll with
    /// `JobPoll`. Never inline — guests run on the blocking pool.
    JobSubmit,
    /// Poll a submitted job: `values {_job: String id}` → `{job_id,
    /// status, result?, error?}`. Missing jobs err (bounded retention).
    JobPoll,
    /// Deploy a procedure: `table` is `fn:<name>` (must match the envelope's
    /// procedure name), `values` is the deploy envelope
    /// `{"v":1,"procedure":{...}}`. Validated; same-name redeploys bump the
    /// server-assigned monotonic version. Needs `proc.deploy` rights.
    ProcDeploy,
    /// List procedures: `values` ignored → one row per procedure
    /// `{name, description, version, steps}`. Needs `proc.list` rights.
    ProcList,
    /// Drop a procedure: `table` is `fn:<name>`. Needs `proc.drop` rights.
    ProcDrop,
    /// Server version handshake: no auth, table/values ignored. Responds
    /// one row `{server, protocol}`. SDKs call this at connect and fail
    /// fast on mismatch (clear errors, never mysterious decode failures).
    Version,
}

impl Op {
    pub(crate) fn tag(self) -> u8 {
        match self {
            Op::Ping => 0,
            Op::Insert => 1,
            Op::Get => 2,
            Op::Update => 3,
            Op::Delete => 4,
            Op::Scan => 5,
            Op::Subscribe => 6,
            Op::Find => 7,
            Op::Search => 8,
            Op::Call => 9,
            Op::JobSubmit => 10,
            Op::JobPoll => 11,
            Op::ProcDeploy => 12,
            Op::ProcList => 13,
            Op::ProcDrop => 14,
            Op::Version => 15,
        }
    }

    pub(crate) fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Op::Ping),
            1 => Some(Op::Insert),
            2 => Some(Op::Get),
            3 => Some(Op::Update),
            4 => Some(Op::Delete),
            5 => Some(Op::Scan),
            6 => Some(Op::Subscribe),
            7 => Some(Op::Find),
            8 => Some(Op::Search),
            9 => Some(Op::Call),
            10 => Some(Op::JobSubmit),
            11 => Some(Op::JobPoll),
            12 => Some(Op::ProcDeploy),
            13 => Some(Op::ProcList),
            14 => Some(Op::ProcDrop),
            15 => Some(Op::Version),
            _ => None,
        }
    }
}

/// A client request.
#[derive(Debug, Clone, PartialEq)]
pub struct Request {
    /// Client-chosen correlation id, echoed back in the response.
    pub id: u64,
    pub op: Op,
    /// Target table (unused for `Ping`).
    pub table: String,
    /// Target row id for `Get` / `Update` / `Delete`.
    pub row_id: Option<u64>,
    /// Column values for `Insert` / `Update`.
    pub values: Option<HashMap<String, Value>>,
}

impl Request {
    pub fn ping(id: u64) -> Self {
        Self {
            id,
            op: Op::Ping,
            table: String::new(),
            row_id: None,
            values: None,
        }
    }
}

/// A single row in a response.
#[derive(Debug, Clone, PartialEq)]
pub struct RowView {
    pub id: u64,
    pub values: HashMap<String, Value>,
}

/// A server response. `ok == false` implies `error` is set.
#[derive(Debug, Clone, PartialEq)]
pub struct Response {
    /// Echo of [`Request::id`].
    pub id: u64,
    pub ok: bool,
    pub rows: Vec<RowView>,
    pub error: Option<String>,
}

impl Response {
    pub fn ok(id: u64, rows: Vec<RowView>) -> Self {
        Self {
            id,
            ok: true,
            rows,
            error: None,
        }
    }

    pub fn err(id: u64, msg: impl Into<String>) -> Self {
        Self {
            id,
            ok: false,
            rows: Vec::new(),
            error: Some(msg.into()),
        }
    }
}

/// A batch of requests in one frame: N ops share one round trip, so
/// syscalls, task wakes and framing amortize ~N×. Inner ops execute in
/// order; each gets its own [`Response`] in `results`.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchRequest {
    /// Correlation id for the whole batch (inner ops keep their own).
    pub id: u64,
    pub ops: Vec<Request>,
}

/// One response per op of a [`BatchRequest`], in order. Partial failure
/// is normal: each inner response carries its own ok/err status.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchResponse {
    /// Echo of [`BatchRequest::id`].
    pub id: u64,
    pub results: Vec<Response>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_tags_roundtrip() {
        for (op, tag) in [
            (Op::Ping, 0),
            (Op::Insert, 1),
            (Op::Get, 2),
            (Op::Update, 3),
            (Op::Delete, 4),
            (Op::Scan, 5),
            (Op::Subscribe, 6),
            (Op::Find, 7),
            (Op::Search, 8),
            (Op::Call, 9),
            (Op::JobSubmit, 10),
            (Op::JobPoll, 11),
            (Op::ProcDeploy, 12),
            (Op::ProcList, 13),
            (Op::ProcDrop, 14),
            (Op::Version, 15),
        ] {
            assert_eq!(op.tag(), tag);
            assert_eq!(Op::from_tag(tag), Some(op));
        }
        assert_eq!(Op::from_tag(16), None);
        assert_eq!(Op::from_tag(255), None);
    }

    #[test]
    fn batch_carries_mixed_ops() {
        let batch = BatchRequest {
            id: 99,
            ops: vec![
                Request::ping(1),
                Request {
                    id: 2,
                    op: Op::Get,
                    table: "users".into(),
                    row_id: Some(7),
                    values: None,
                },
            ],
        };
        assert_eq!(batch.ops.len(), 2);
        assert_eq!(batch.ops[0].op, Op::Ping);

        let resp = BatchResponse {
            id: 99,
            results: vec![Response::ok(1, Vec::new()), Response::err(2, "gone")],
        };
        assert!(resp.results[0].ok);
        assert!(!resp.results[1].ok);
    }

    #[test]
    fn response_constructors() {
        let resp = Response::ok(
            7,
            vec![RowView {
                id: 1,
                values: [("name".to_string(), Value::String("Alice".into()))]
                    .into_iter()
                    .collect(),
            }],
        );
        assert!(resp.ok);
        assert!(resp.error.is_none());
        assert_eq!(resp.rows.len(), 1);

        let err = Response::err(3, "no such table");
        assert!(!err.ok);
        assert_eq!(err.error.as_deref(), Some("no such table"));
        assert!(err.rows.is_empty());
    }
}
