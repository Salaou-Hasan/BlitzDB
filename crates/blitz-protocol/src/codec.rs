//! Binary framing and value encoding for the BlitzDB wire protocol.
//!
//! Frame layout: `[ver: u8][len: u32 LE][payload]` where `len` is the
//! payload length. The version byte is checked first, so future protocol
//! revisions fail loudly via [`ProtocolError::UnsupportedVersion`].
//!
//! Every payload starts with a kind byte (`0x01` request, `0x02` batch
//! request, `0x11` response, `0x12` batch response), so batch and single
//! frames route correctly and misdirected frames fail loudly instead of
//! misparsing.
//!
//! Payloads use a compact tag-length scheme (all integers little-endian):
//!
//! ```text
//! str   = u32 len + utf8 bytes
//! map   = u32 count + (key str + value)*
//! value = tag u8 + body
//!   0x00 null
//!   0x01 bool + u8            0x08 u32 + 4B        0x0F uuid + 16B
//!   0x02 i8  + 1B             0x09 u64 + 8B        0x10 timestamp + i64 micros
//!   0x03 i16 + 2B             0x0A f32 + 4B        0x11 date + y/i32 m/u32 d/u32
//!   0x04 i32 + 4B             0x0B f64 + 8B        0x12 json (recursive subset)
//!   0x05 i64 + 8B             0x0C decimal str     0x13 array + u32 count + items
//!   0x06 u8  + 1B             0x0D string str
//!   0x07 u16 + 2B             0x0E bytes + u32 len + raw
//! ```
//!
//! `FrameCodec` is synchronous over [`BytesMut`] so any transport can use
//! it: feed incoming bytes with [`FrameCodec::feed`], pop complete frames,
//! then decode them with [`FrameCodec::decode_request`] /
//! [`FrameCodec::decode_response`].

use std::collections::HashMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::{DateTime, Datelike, NaiveDate};
use uuid::Uuid;

use crate::error::{ProtocolError, ProtocolResult};
use crate::message::{BatchRequest, BatchResponse, Op, Request, Response, RowView};
use blitz_types::value::Value;

/// Wire protocol version. Bumped whenever the payload layout changes.
pub const PROTOCOL_VERSION: u8 = 2;

/// Payload kind tags (first byte of every payload).
pub const KIND_REQUEST: u8 = 0x01;
pub const KIND_BATCH_REQUEST: u8 = 0x02;
pub const KIND_RESPONSE: u8 = 0x11;
pub const KIND_BATCH_RESPONSE: u8 = 0x12;

/// One decoded client frame: single or batch request.
#[derive(Debug, Clone, PartialEq)]
pub enum Incoming {
    Single(Request),
    Batch(BatchRequest),
}

fn expect_kind(r: &mut Reader<'_>, want: u8) -> ProtocolResult<()> {
    let got = r.u8()?;
    if got != want {
        return Err(ProtocolError::DecodeError(format!(
            "wrong payload kind: {:#x}, want {:#x}",
            got, want
        )));
    }
    Ok(())
}

/// Frame header size: 1 version byte + 4 length bytes.
pub const HEADER_LEN: usize = 5;

/// Maximum frame payload accepted by default (8 MiB).
pub const DEFAULT_MAX_FRAME: usize = 8 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    fn bytes(self) -> Bytes {
        Bytes::from(self.buf)
    }

    fn u8(&mut self, v: u8) {
        self.buf.put_u8(v);
    }

    fn u32(&mut self, v: u32) {
        self.buf.put_u32_le(v);
    }

    fn u64(&mut self, v: u64) {
        self.buf.put_u64_le(v);
    }

    fn i32(&mut self, v: i32) {
        self.buf.put_i32_le(v);
    }

    fn i64(&mut self, v: i64) {
        self.buf.put_i64_le(v);
    }

    fn f32(&mut self, v: f32) {
        self.buf.put_f32_le(v);
    }

    fn f64(&mut self, v: f64) {
        self.buf.put_f64_le(v);
    }

    fn raw(&mut self, bytes: &[u8]) {
        self.buf.put_slice(bytes);
    }

    fn str_(&mut self, s: &str) {
        self.u32(s.len() as u32);
        self.raw(s.as_bytes());
    }

    fn value(&mut self, v: &Value) {
        match v {
            Value::Null => self.u8(0x00),
            Value::Boolean(b) => {
                self.u8(0x01);
                self.u8(*b as u8);
            }
            Value::Int8(n) => {
                self.u8(0x02);
                self.u8(*n as u8);
            }
            Value::Int16(n) => {
                self.u8(0x03);
                self.buf.put_i16_le(*n);
            }
            Value::Int32(n) => {
                self.u8(0x04);
                self.i32(*n);
            }
            Value::Int64(n) => {
                self.u8(0x05);
                self.i64(*n);
            }
            Value::UInt8(n) => {
                self.u8(0x06);
                self.u8(*n);
            }
            Value::UInt16(n) => {
                self.u8(0x07);
                self.buf.put_u16_le(*n);
            }
            Value::UInt32(n) => {
                self.u8(0x08);
                self.u32(*n);
            }
            Value::UInt64(n) => {
                self.u8(0x09);
                self.u64(*n);
            }
            Value::Float32(n) => {
                self.u8(0x0A);
                self.f32(*n);
            }
            Value::Float64(n) => {
                self.u8(0x0B);
                self.f64(*n);
            }
            Value::Decimal(s) => {
                self.u8(0x0C);
                self.str_(s);
            }
            Value::String(s) => {
                self.u8(0x0D);
                self.str_(s);
            }
            Value::Bytes(b) => {
                self.u8(0x0E);
                self.u32(b.len() as u32);
                self.raw(b);
            }
            Value::Uuid(u) => {
                self.u8(0x0F);
                self.raw(u.as_bytes());
            }
            Value::Timestamp(t) => {
                self.u8(0x10);
                self.i64(t.timestamp_micros());
            }
            Value::Date(d) => {
                self.u8(0x11);
                self.i32(d.num_days_from_ce());
                self.u32(d.month());
                self.u32(d.day());
            }
            Value::Json(j) => {
                self.u8(0x12);
                self.json(j);
            }
            Value::Array(items) => {
                self.u8(0x13);
                self.u32(items.len() as u32);
                for item in items {
                    self.value(item);
                }
            }
        }
    }

    /// JSON subset: only null/bool/int/uint/float/string/array/object tags.
    fn json(&mut self, j: &serde_json::Value) {
        match j {
            serde_json::Value::Null => self.u8(0x00),
            serde_json::Value::Bool(b) => {
                self.u8(0x01);
                self.u8(*b as u8);
            }
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    self.u8(0x05);
                    self.i64(i);
                } else if let Some(u) = n.as_u64() {
                    self.u8(0x09);
                    self.u64(u);
                } else if let Some(f) = n.as_f64() {
                    self.u8(0x0B);
                    self.f64(f);
                }
            }
            serde_json::Value::String(s) => {
                self.u8(0x0D);
                self.str_(s);
            }
            serde_json::Value::Array(items) => {
                self.u8(0x13);
                self.u32(items.len() as u32);
                for item in items {
                    self.json(item);
                }
            }
            serde_json::Value::Object(map) => {
                self.u8(0x12);
                self.u32(map.len() as u32);
                for (k, v) in map {
                    self.str_(k);
                    self.json(v);
                }
            }
        }
    }

    fn map(&mut self, map: &HashMap<String, Value>) {
        self.u32(map.len() as u32);
        for (k, v) in map {
            self.str_(k);
            self.value(v);
        }
    }

    fn request_body(&mut self, req: &Request) {
        self.u64(req.id);
        self.u8(req.op.tag());
        self.str_(&req.table);
        match req.row_id {
            Some(id) => {
                self.u8(1);
                self.u64(id);
            }
            None => self.u8(0),
        }
        match &req.values {
            Some(values) => {
                self.u8(1);
                self.map(values);
            }
            None => self.u8(0),
        }
    }

    fn response_body(&mut self, resp: &Response) {
        self.u64(resp.id);
        self.u8(resp.ok as u8);
        self.u32(resp.rows.len() as u32);
        for row in &resp.rows {
            self.row_view(row);
        }
        match &resp.error {
            Some(msg) => {
                self.u8(1);
                self.str_(msg);
            }
            None => self.u8(0),
        }
    }

    fn row_view(&mut self, row: &RowView) {
        self.u64(row.id);
        self.map(&row.values);
    }
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize, what: &str) -> ProtocolResult<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(ProtocolError::DecodeError(format!(
                "truncated {}: need {} bytes at offset {}",
                what, n, self.pos
            )));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn u8(&mut self) -> ProtocolResult<u8> {
        Ok(self.take(1, "u8")?[0])
    }

    fn u16(&mut self) -> ProtocolResult<u16> {
        let b = self.take(2, "u16")?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> ProtocolResult<u32> {
        let b = self.take(4, "u32")?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64(&mut self) -> ProtocolResult<u64> {
        let b = self.take(8, "u64")?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_le_bytes(a))
    }

    fn i16(&mut self) -> ProtocolResult<i16> {
        let b = self.take(2, "i16")?;
        Ok(i16::from_le_bytes([b[0], b[1]]))
    }

    fn i32(&mut self) -> ProtocolResult<i32> {
        let b = self.take(4, "i32")?;
        Ok(i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn i64(&mut self) -> ProtocolResult<i64> {
        let b = self.take(8, "i64")?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(i64::from_le_bytes(a))
    }

    fn f32(&mut self) -> ProtocolResult<f32> {
        let b = self.take(4, "f32")?;
        Ok(f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn f64(&mut self) -> ProtocolResult<f64> {
        let b = self.take(8, "f64")?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(f64::from_le_bytes(a))
    }

    fn str_(&mut self) -> ProtocolResult<String> {
        let len = self.u32()? as usize;
        let bytes = self.take(len, "str")?;
        std::str::from_utf8(bytes)
            .map(|s| s.to_string())
            .map_err(|e| ProtocolError::DecodeError(format!("invalid utf8: {}", e)))
    }

    fn value(&mut self) -> ProtocolResult<Value> {
        let tag = self.u8()?;
        match tag {
            0x00 => Ok(Value::Null),
            0x01 => Ok(Value::Boolean(self.u8()? != 0)),
            0x02 => Ok(Value::Int8(self.u8()? as i8)),
            0x03 => Ok(Value::Int16(self.i16()?)),
            0x04 => Ok(Value::Int32(self.i32()?)),
            0x05 => Ok(Value::Int64(self.i64()?)),
            0x06 => Ok(Value::UInt8(self.u8()?)),
            0x07 => Ok(Value::UInt16(self.u16()?)),
            0x08 => Ok(Value::UInt32(self.u32()?)),
            0x09 => Ok(Value::UInt64(self.u64()?)),
            0x0A => Ok(Value::Float32(self.f32()?)),
            0x0B => Ok(Value::Float64(self.f64()?)),
            0x0C => Ok(Value::Decimal(self.str_()?)),
            0x0D => Ok(Value::String(self.str_()?)),
            0x0E => {
                let len = self.u32()? as usize;
                Ok(Value::Bytes(self.take(len, "bytes")?.to_vec()))
            }
            0x0F => {
                let raw = self.take(16, "uuid")?;
                Uuid::from_slice(raw)
                    .map(Value::Uuid)
                    .map_err(|e| ProtocolError::DecodeError(format!("bad uuid: {}", e)))
            }
            0x10 => {
                let micros = self.i64()?;
                DateTime::from_timestamp_micros(micros)
                    .map(|t| Value::Timestamp(t.to_utc()))
                    .ok_or_else(|| {
                        ProtocolError::DecodeError(format!("bad timestamp: {}", micros))
                    })
            }
            0x11 => {
                let days = self.i32()?;
                let month = self.u32()?;
                let day = self.u32()?;
                let date = NaiveDate::from_num_days_from_ce_opt(days)
                    .filter(|d| d.month() == month && d.day() == day)
                    .ok_or_else(|| ProtocolError::DecodeError("bad date".into()))?;
                Ok(Value::Date(date))
            }
            0x12 => Ok(Value::Json(self.json()?)),
            0x13 => {
                let count = self.u32()? as usize;
                let mut items = Vec::with_capacity(count.min(1024));
                for _ in 0..count {
                    items.push(self.value()?);
                }
                Ok(Value::Array(items))
            }
            other => Err(ProtocolError::DecodeError(format!(
                "unknown value tag: {:#x}",
                other
            ))),
        }
    }

    fn json(&mut self) -> ProtocolResult<serde_json::Value> {
        let tag = self.u8()?;
        match tag {
            0x00 => Ok(serde_json::Value::Null),
            0x01 => Ok(serde_json::Value::Bool(self.u8()? != 0)),
            0x05 => Ok(serde_json::json!(self.i64()?)),
            0x09 => Ok(serde_json::json!(self.u64()?)),
            0x0B => Ok(serde_json::json!(self.f64()?)),
            0x0D => Ok(serde_json::Value::String(self.str_()?)),
            0x12 => {
                let count = self.u32()? as usize;
                let mut map = serde_json::Map::with_capacity(count.min(64));
                for _ in 0..count {
                    let key = self.str_()?;
                    map.insert(key, self.json()?);
                }
                Ok(serde_json::Value::Object(map))
            }
            0x13 => {
                let count = self.u32()? as usize;
                let mut items = Vec::with_capacity(count.min(1024));
                for _ in 0..count {
                    items.push(self.json()?);
                }
                Ok(serde_json::Value::Array(items))
            }
            other => Err(ProtocolError::DecodeError(format!(
                "bad json tag: {:#x}",
                other
            ))),
        }
    }

    fn map(&mut self) -> ProtocolResult<HashMap<String, Value>> {
        let count = self.u32()? as usize;
        let mut out = HashMap::with_capacity(count.min(64));
        for _ in 0..count {
            let key = self.str_()?;
            out.insert(key, self.value()?);
        }
        Ok(out)
    }

    fn request_body(&mut self) -> ProtocolResult<Request> {
        Ok(Request {
            id: self.u64()?,
            op: Op::from_tag(self.u8()?).ok_or_else(|| {
                ProtocolError::DecodeError("unknown op tag".into())
            })?,
            table: self.str_()?,
            row_id: match self.u8()? {
                0 => None,
                1 => Some(self.u64()?),
                other => {
                    return Err(ProtocolError::DecodeError(format!(
                        "bad option tag: {}",
                        other
                    )))
                }
            },
            values: match self.u8()? {
                0 => None,
                1 => Some(self.map()?),
                other => {
                    return Err(ProtocolError::DecodeError(format!(
                        "bad option tag: {}",
                        other
                    )))
                }
            },
        })
    }

    fn response_body(&mut self) -> ProtocolResult<Response> {
        let id = self.u64()?;
        let ok = self.u8()? != 0;
        let row_count = self.u32()? as usize;
        let mut rows = Vec::with_capacity(row_count.min(1024));
        for _ in 0..row_count {
            rows.push(self.row_view()?);
        }
        let error = match self.u8()? {
            0 => None,
            1 => Some(self.str_()?),
            other => {
                return Err(ProtocolError::DecodeError(format!(
                    "bad option tag: {}",
                    other
                )))
            }
        };
        Ok(Response {
            id,
            ok,
            rows,
            error,
        })
    }

    fn row_view(&mut self) -> ProtocolResult<RowView> {
        Ok(RowView {
            id: self.u64()?,
            values: self.map()?,
        })
    }

    fn end(&self) -> ProtocolResult<()> {
        if self.pos != self.buf.len() {
            return Err(ProtocolError::DecodeError(format!(
                "trailing {} bytes after message",
                self.buf.len() - self.pos
            )));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

/// Encoder / incremental decoder for BlitzDB frames.
#[derive(Debug, Clone)]
pub struct FrameCodec {
    max_frame: usize,
}

impl FrameCodec {
    pub fn new(max_frame: usize) -> Self {
        Self { max_frame }
    }

    pub fn with_default_limit() -> Self {
        Self::new(DEFAULT_MAX_FRAME)
    }

    fn frame(&self, payload: Bytes) -> ProtocolResult<Bytes> {
        if payload.len() > self.max_frame {
            return Err(ProtocolError::EncodeError(format!(
                "frame too large: {} > {}",
                payload.len(),
                self.max_frame
            )));
        }
        let mut buf = BytesMut::with_capacity(HEADER_LEN + payload.len());
        buf.put_u8(PROTOCOL_VERSION);
        buf.put_u32_le(payload.len() as u32);
        buf.put_slice(&payload);
        Ok(buf.freeze())
    }

    /// Serialize a request into a single framed buffer.
    pub fn encode_request(&self, req: &Request) -> ProtocolResult<Bytes> {
        let mut w = Writer::new();
        w.u8(KIND_REQUEST);
        w.request_body(req);
        self.frame(w.bytes())
    }

    /// Serialize a batch of requests into a single framed buffer.
    /// One frame carries N ops: syscalls, wakes and framing amortize ~N×.
    pub fn encode_batch_request(&self, batch: &BatchRequest) -> ProtocolResult<Bytes> {
        let mut w = Writer::new();
        w.u8(KIND_BATCH_REQUEST);
        w.u64(batch.id);
        w.u32(batch.ops.len() as u32);
        for op in &batch.ops {
            w.request_body(op);
        }
        self.frame(w.bytes())
    }

    /// Serialize a response into a single framed buffer.
    pub fn encode_response(&self, resp: &Response) -> ProtocolResult<Bytes> {
        let mut w = Writer::new();
        w.u8(KIND_RESPONSE);
        w.response_body(resp);
        self.frame(w.bytes())
    }

    /// Serialize a batch of responses into a single framed buffer.
    pub fn encode_batch_response(&self, batch: &BatchResponse) -> ProtocolResult<Bytes> {
        let mut w = Writer::new();
        w.u8(KIND_BATCH_RESPONSE);
        w.u64(batch.id);
        w.u32(batch.results.len() as u32);
        for res in &batch.results {
            w.response_body(res);
        }
        self.frame(w.bytes())
    }

    /// Try to pop one complete frame payload from `buf`.
    ///
    /// Returns `Ok(None)` when more bytes are needed. Consumes exactly
    /// one frame when one is available.
    pub fn feed(&self, buf: &mut BytesMut) -> ProtocolResult<Option<Bytes>> {
        if buf.len() < HEADER_LEN {
            return Ok(None);
        }
        let ver = buf[0];
        if ver != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion(ver as u32));
        }
        let len =
            u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        if len > self.max_frame {
            return Err(ProtocolError::DecodeError(format!(
                "frame too large: {} > {}",
                len, self.max_frame
            )));
        }
        if buf.len() < HEADER_LEN + len {
            return Ok(None);
        }
        buf.advance(HEADER_LEN);
        Ok(Some(buf.split_to(len).freeze()))
    }

    /// Deserialize a request payload produced by [`FrameCodec::feed`].
    /// Rejects batch frames: use [`FrameCodec::decode_incoming`] for those.
    pub fn decode_request(&self, frame: Bytes) -> ProtocolResult<Request> {
        let mut r = Reader::new(&frame);
        expect_kind(&mut r, KIND_REQUEST)?;
        let req = r.request_body()?;
        r.end()?;
        Ok(req)
    }

    /// Deserialize one incoming client frame: single or batch request.
    pub fn decode_incoming(&self, frame: Bytes) -> ProtocolResult<Incoming> {
        let mut r = Reader::new(&frame);
        let out = match r.u8()? {
            KIND_REQUEST => Incoming::Single(r.request_body()?),
            KIND_BATCH_REQUEST => {
                let id = r.u64()?;
                let count = r.u32()? as usize;
                let mut ops = Vec::with_capacity(count.min(4096));
                for _ in 0..count {
                    ops.push(r.request_body()?);
                }
                Incoming::Batch(BatchRequest { id, ops })
            }
            other => {
                return Err(ProtocolError::DecodeError(format!(
                    "unknown incoming kind: {:#x}",
                    other
                )))
            }
        };
        r.end()?;
        Ok(out)
    }

    /// Deserialize a response payload produced by [`FrameCodec::feed`].
    pub fn decode_response(&self, frame: Bytes) -> ProtocolResult<Response> {
        let mut r = Reader::new(&frame);
        expect_kind(&mut r, KIND_RESPONSE)?;
        let resp = r.response_body()?;
        r.end()?;
        Ok(resp)
    }

    /// Deserialize a batch response payload.
    pub fn decode_batch_response(&self, frame: Bytes) -> ProtocolResult<BatchResponse> {
        let mut r = Reader::new(&frame);
        expect_kind(&mut r, KIND_BATCH_RESPONSE)?;
        let id = r.u64()?;
        let count = r.u32()? as usize;
        let mut results = Vec::with_capacity(count.min(4096));
        for _ in 0..count {
            results.push(r.response_body()?);
        }
        r.end()?;
        Ok(BatchResponse { id, results })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blitz_types::value::Value;
    use chrono::{TimeZone, Utc};

    fn all_values() -> Vec<(&'static str, Value)> {
        vec![
            ("null", Value::Null),
            ("bool", Value::Boolean(true)),
            ("i8", Value::Int8(-8)),
            ("i16", Value::Int16(-1600)),
            ("i32", Value::Int32(-32000)),
            ("i64", Value::Int64(-6400000000)),
            ("u8", Value::UInt8(8)),
            ("u16", Value::UInt16(1600)),
            ("u32", Value::UInt32(32000)),
            ("u64", Value::UInt64(6400000000)),
            ("f32", Value::Float32(1.5)),
            ("f64", Value::Float64(2.5)),
            ("dec", Value::Decimal("3.14".into())),
            ("str", Value::String("hello".into())),
            ("bytes", Value::Bytes(vec![1, 2, 3, 255])),
            ("uuid", Value::Uuid(Uuid::nil())),
            (
                "ts",
                Value::Timestamp(Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap()),
            ),
            (
                "date",
                Value::Date(NaiveDate::from_ymd_opt(2026, 9, 5).unwrap()),
            ),
            (
                "json",
                Value::Json(serde_json::json!({"a": 1, "b": [true, "x"], "c": null})),
            ),
            (
                "arr",
                Value::Array(vec![Value::Int64(1), Value::String("s".into())]),
            ),
        ]
    }

    fn sample_request() -> Request {
        Request {
            id: 42,
            op: Op::Insert,
            table: "users".into(),
            row_id: None,
            values: Some(all_values().into_iter().map(|(k, v)| (k.into(), v)).collect()),
        }
    }

    #[test]
    fn binary_request_roundtrip_covers_every_value_type() {
        let codec = FrameCodec::with_default_limit();
        let req = sample_request();
        let frame = codec.encode_request(&req).unwrap();

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&frame);
        let payload = codec.feed(&mut buf).unwrap().expect("complete frame");
        assert!(buf.is_empty());

        let back = codec.decode_request(payload).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn batch_roundtrip_and_incoming_routing() {
        let codec = FrameCodec::with_default_limit();
        let batch = BatchRequest {
            id: 99,
            ops: vec![Request::ping(1), sample_request()],
        };
        let frame = codec.encode_batch_request(&batch).unwrap();

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&frame);
        let payload = codec.feed(&mut buf).unwrap().expect("complete frame");

        match codec.decode_incoming(payload).unwrap() {
            Incoming::Batch(back) => assert_eq!(back, batch),
            Incoming::Single(_) => panic!("expected batch"),
        }

        // Single frames still route to Single.
        let frame = codec.encode_request(&Request::ping(3)).unwrap();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&frame);
        let payload = codec.feed(&mut buf).unwrap().expect("complete frame");
        match codec.decode_incoming(payload).unwrap() {
            Incoming::Single(req) => assert_eq!(req.id, 3),
            Incoming::Batch(_) => panic!("expected single"),
        }

        // Batch responses round-trip too.
        let bresp = BatchResponse {
            id: 99,
            results: vec![Response::ok(1, Vec::new()), Response::err(2, "gone")],
        };
        let frame = codec.encode_batch_response(&bresp).unwrap();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&frame);
        let payload = codec.feed(&mut buf).unwrap().expect("complete frame");
        assert_eq!(codec.decode_batch_response(payload).unwrap(), bresp);
    }

    #[test]
    fn strict_decoders_reject_wrong_kind() {
        let codec = FrameCodec::with_default_limit();
        let batch = BatchRequest {
            id: 1,
            ops: vec![Request::ping(1)],
        };
        let frame = codec.encode_batch_request(&batch).unwrap();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&frame);
        let payload = codec.feed(&mut buf).unwrap().expect("complete frame");
        assert!(codec.decode_request(payload).is_err());
    }

    #[test]
    fn binary_response_roundtrip() {
        let codec = FrameCodec::with_default_limit();
        let resp = Response::ok(
            7,
            vec![RowView {
                id: 99,
                values: all_values().into_iter().map(|(k, v)| (k.into(), v)).collect(),
            }],
        );
        let frame = codec.encode_response(&resp).unwrap();

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&frame);
        let payload = codec.feed(&mut buf).unwrap().expect("complete frame");
        let back = codec.decode_response(payload).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn binary_is_smaller_than_json_for_typical_rows() {
        let codec = FrameCodec::with_default_limit();
        let values: HashMap<String, Value> = [
            ("id".to_string(), Value::Int64(12345)),
            ("name".to_string(), Value::String("Alice Johnson".into())),
            ("email".to_string(), Value::String("alice@example.com".into())),
            ("active".to_string(), Value::Boolean(true)),
            ("score".to_string(), Value::Float64(98.6)),
        ]
        .into_iter()
        .collect();

        let req = Request {
            id: 1,
            op: Op::Insert,
            table: "users".into(),
            row_id: None,
            values: Some(values.clone()),
        };
        let binary_len = codec.encode_request(&req).unwrap().len();

        // Equivalent JSON encoding of the same logical message.
        let json_values: HashMap<String, serde_json::Value> = values
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::from(v)))
            .collect();
        let json_len = serde_json::to_vec(&serde_json::json!({
            "id": 1u64,
            "op": "Insert",
            "table": "users",
            "values": json_values,
        }))
        .unwrap()
        .len()
            + HEADER_LEN; // frame overhead parity

        println!("binary: {} bytes, json: {} bytes", binary_len, json_len);
        assert!(
            binary_len < json_len,
            "binary {} should beat json {}",
            binary_len,
            json_len
        );
    }

    #[test]
    fn feed_handles_partial_reads() {
        let codec = FrameCodec::with_default_limit();
        let frame = codec.encode_request(&Request::ping(9)).unwrap();

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&frame[..2]);
        assert!(codec.feed(&mut buf).unwrap().is_none());
        let mid = HEADER_LEN + 2;
        buf.extend_from_slice(&frame[2..mid]);
        assert!(codec.feed(&mut buf).unwrap().is_none());
        buf.extend_from_slice(&frame[mid..]);
        let payload = codec.feed(&mut buf).unwrap().expect("complete frame");
        let back = codec.decode_request(payload).unwrap();
        assert_eq!(back.id, 9);
        assert_eq!(back.op, Op::Ping);
    }

    #[test]
    fn feed_rejects_oversize_frames() {
        let codec = FrameCodec::new(8);
        let mut buf = BytesMut::new();
        buf.put_u8(PROTOCOL_VERSION);
        buf.put_u32_le(1024);
        let err = codec.feed(&mut buf).unwrap_err();
        assert!(matches!(err, ProtocolError::DecodeError(_)));
    }

    #[test]
    fn feed_rejects_unknown_version() {
        let codec = FrameCodec::with_default_limit();
        let mut buf = BytesMut::new();
        buf.put_u8(0xFF);
        buf.put_u32_le(0);
        let err = codec.feed(&mut buf).unwrap_err();
        assert!(matches!(err, ProtocolError::UnsupportedVersion(0xFF)));
    }

    #[test]
    fn decode_rejects_trailing_garbage() {
        let codec = FrameCodec::with_default_limit();
        let mut frame = codec.encode_request(&Request::ping(1)).unwrap().to_vec();
        frame.extend_from_slice(&[9, 9, 9]);
        // Re-frame with correct length covering the garbage.
        let mut buf = BytesMut::new();
        buf.put_u8(PROTOCOL_VERSION);
        buf.put_u32_le(frame.len() as u32);
        buf.extend_from_slice(&frame);
        let payload = codec.feed(&mut buf).unwrap().expect("frame");
        let err = codec.decode_request(payload).unwrap_err();
        assert!(matches!(err, ProtocolError::DecodeError(_)));
    }
}
