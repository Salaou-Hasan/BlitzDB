use crate::error::{ProtocolError, ProtocolResult};
use crate::message::{Message, Notification};
use bytes::{Buf, BufMut, BytesMut};

/// Protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

/// Maximum message size (16 MB).
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Wire format: [version:4][length:4][json_payload]
pub struct Codec;

impl Codec {
    pub fn new() -> Self {
        Self
    }

    /// Encode a message to bytes.
    pub fn encode(&self, msg: &Message) -> ProtocolResult<BytesMut> {
        let json = serde_json::to_vec(msg)
            .map_err(|e| ProtocolError::EncodeError(e.to_string()))?;

        if json.len() > MAX_MESSAGE_SIZE {
            return Err(ProtocolError::EncodeError(format!(
                "message too large: {} bytes",
                json.len()
            )));
        }

        let mut buf = BytesMut::with_capacity(8 + json.len());
        buf.put_u32(PROTOCOL_VERSION);
        buf.put_u32(json.len() as u32);
        buf.put_slice(&json);
        Ok(buf)
    }

    /// Decode a message from bytes.
    pub fn decode(&self, buf: &mut BytesMut) -> ProtocolResult<Option<Message>> {
        if buf.len() < 8 {
            return Ok(None);
        }

        let mut peek = buf.clone();
        let version = peek.get_u32();
        if version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion(version));
        }

        let length = peek.get_u32() as usize;

        if length > MAX_MESSAGE_SIZE {
            return Err(ProtocolError::DecodeError(format!(
                "message too large: {} bytes",
                length
            )));
        }

        if buf.len() < 8 + length {
            return Ok(None);
        }

        buf.advance(8);
        let json_bytes = buf.split_to(length);
        let msg: Message = serde_json::from_slice(&json_bytes)
            .map_err(|e| ProtocolError::DecodeError(e.to_string()))?;

        Ok(Some(msg))
    }

    /// Encode just the payload (no header) for simple use cases.
    pub fn encode_payload(msg: &Message) -> ProtocolResult<Vec<u8>> {
        serde_json::to_vec(msg)
            .map_err(|e| ProtocolError::EncodeError(e.to_string()))
    }

    /// Decode just the payload.
    pub fn decode_payload(data: &[u8]) -> ProtocolResult<Message> {
        serde_json::from_slice(data)
            .map_err(|e| ProtocolError::DecodeError(e.to_string()))
    }
}

impl Default for Codec {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Request, Response};
    use std::collections::HashMap;
    use blitz_types::value::Value;

    #[test]
    fn test_request_builder() {
        let req = Request::new(1, "table.insert")
            .with_param("table", Value::String("users".into()))
            .with_param("data", Value::Int64(42));
        assert_eq!(req.id, 1);
        assert_eq!(req.method, "table.insert");
        assert_eq!(req.params.len(), 2);
    }

    #[test]
    fn test_response_ok() {
        let resp = Response::ok(1, Value::Boolean(true));
        assert_eq!(resp.id, 1);
        assert!(resp.result.is_some());
        assert!(resp.error.is_none());
    }

    #[test]
    fn test_response_err() {
        let resp = Response::err(1, "something went wrong");
        assert_eq!(resp.id, 1);
        assert!(resp.result.is_none());
        assert_eq!(resp.error.as_deref(), Some("something went wrong"));
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let codec = Codec::new();
        let msg = Message::Request(Request::new(42, "ping"));
        let mut encoded = codec.encode(&msg).unwrap();
        let decoded = codec.decode(&mut encoded).unwrap().unwrap();
        if let Message::Request(req) = decoded {
            assert_eq!(req.id, 42);
            assert_eq!(req.method, "ping");
        } else {
            panic!("expected Request");
        }
    }

    #[test]
    fn test_encode_decode_response() {
        let codec = Codec::new();
        let msg = Message::Response(Response {
            id: 1,
            result: Some(Value::String("pong".into())),
            error: None,
        });
        let mut encoded = codec.encode(&msg).unwrap();
        let decoded = codec.decode(&mut encoded).unwrap().unwrap();
        if let Message::Response(resp) = decoded {
            assert_eq!(resp.id, 1);
            assert_eq!(resp.result, Some(Value::String("pong".into())));
        } else {
            panic!("expected Response");
        }
    }

    #[test]
    fn test_incomplete_message() {
        let codec = Codec::new();
        let msg = Message::Request(Request::new(1, "test"));
        let mut encoded = codec.encode(&msg).unwrap();
        let partial = encoded.split_to(4);
        let mut buf = partial;
        let result = codec.decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_payload_roundtrip() {
        let msg = Message::Notification(Notification {
            event: "row.inserted".into(),
            data: HashMap::new(),
        });
        let encoded = Codec::encode_payload(&msg).unwrap();
        let decoded = Codec::decode_payload(&encoded).unwrap();
        if let Message::Notification(notif) = decoded {
            assert_eq!(notif.event, "row.inserted");
        } else {
            panic!("expected Notification");
        }
    }
}
