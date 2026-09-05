use std::fmt;
use std::hash::{Hash, Hasher};

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A dynamically-typed value representing data in BlitzDB.
///
/// This is the fundamental data unit. All column values are represented as `Value`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// Boolean value.
    Boolean(bool),
    /// Signed 8-bit integer.
    Int8(i8),
    /// Signed 16-bit integer.
    Int16(i16),
    /// Signed 32-bit integer.
    Int32(i32),
    /// Signed 64-bit integer.
    Int64(i64),
    /// Unsigned 8-bit integer.
    UInt8(u8),
    /// Unsigned 16-bit integer.
    UInt16(u16),
    /// Unsigned 32-bit integer.
    UInt32(u32),
    /// Unsigned 64-bit integer.
    UInt64(u64),
    /// 32-bit floating point.
    Float32(f32),
    /// 64-bit floating point.
    Float64(f64),
    /// Decimal value stored as string for precision.
    Decimal(String),
    /// UTF-8 string.
    String(String),
    /// Byte array.
    Bytes(Vec<u8>),
    /// UUID.
    Uuid(Uuid),
    /// Timestamp with timezone.
    Timestamp(DateTime<Utc>),
    /// Date without time.
    Date(NaiveDate),
    /// JSON value.
    Json(serde_json::Value),
    /// Array of values.
    Array(Vec<Value>),
    /// Null value.
    Null,
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Int8(a), Value::Int8(b)) => a.cmp(b),
            (Value::Int16(a), Value::Int16(b)) => a.cmp(b),
            (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
            (Value::UInt8(a), Value::UInt8(b)) => a.cmp(b),
            (Value::UInt16(a), Value::UInt16(b)) => a.cmp(b),
            (Value::UInt32(a), Value::UInt32(b)) => a.cmp(b),
            (Value::UInt64(a), Value::UInt64(b)) => a.cmp(b),
            (Value::Float32(a), Value::Float32(b)) => a.to_bits().cmp(&b.to_bits()),
            (Value::Float64(a), Value::Float64(b)) => a.to_bits().cmp(&b.to_bits()),
            (Value::Decimal(a), Value::Decimal(b)) => a.cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Json(a), Value::Json(b)) => a.to_string().cmp(&b.to_string()),
            (Value::Array(a), Value::Array(b)) => {
                for (ai, bi) in a.iter().zip(b.iter()) {
                    match ai.cmp(bi) {
                        std::cmp::Ordering::Equal => continue,
                        other => return other,
                    }
                }
                a.len().cmp(&b.len())
            }
            _ => {
                // Different types - order by variant index
                let self_idx = match self {
                    Value::Boolean(_) => 0,
                    Value::Int8(_) => 1,
                    Value::Int16(_) => 2,
                    Value::Int32(_) => 3,
                    Value::Int64(_) => 4,
                    Value::UInt8(_) => 5,
                    Value::UInt16(_) => 6,
                    Value::UInt32(_) => 7,
                    Value::UInt64(_) => 8,
                    Value::Float32(_) => 9,
                    Value::Float64(_) => 10,
                    Value::Decimal(_) => 11,
                    Value::String(_) => 12,
                    Value::Bytes(_) => 13,
                    Value::Uuid(_) => 14,
                    Value::Timestamp(_) => 15,
                    Value::Date(_) => 16,
                    Value::Json(_) => 17,
                    Value::Array(_) => 18,
                    Value::Null => 19,
                };
                let other_idx = match other {
                    Value::Boolean(_) => 0,
                    Value::Int8(_) => 1,
                    Value::Int16(_) => 2,
                    Value::Int32(_) => 3,
                    Value::Int64(_) => 4,
                    Value::UInt8(_) => 5,
                    Value::UInt16(_) => 6,
                    Value::UInt32(_) => 7,
                    Value::UInt64(_) => 8,
                    Value::Float32(_) => 9,
                    Value::Float64(_) => 10,
                    Value::Decimal(_) => 11,
                    Value::String(_) => 12,
                    Value::Bytes(_) => 13,
                    Value::Uuid(_) => 14,
                    Value::Timestamp(_) => 15,
                    Value::Date(_) => 16,
                    Value::Json(_) => 17,
                    Value::Array(_) => 18,
                    Value::Null => 19,
                };
                self_idx.cmp(&other_idx)
            }
        }
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Boolean(v) => v.hash(state),
            Value::Int8(v) => v.hash(state),
            Value::Int16(v) => v.hash(state),
            Value::Int32(v) => v.hash(state),
            Value::Int64(v) => v.hash(state),
            Value::UInt8(v) => v.hash(state),
            Value::UInt16(v) => v.hash(state),
            Value::UInt32(v) => v.hash(state),
            Value::UInt64(v) => v.hash(state),
            Value::Float32(v) => v.to_bits().hash(state),
            Value::Float64(v) => v.to_bits().hash(state),
            Value::Decimal(v) => v.hash(state),
            Value::String(v) => v.hash(state),
            Value::Bytes(v) => v.hash(state),
            Value::Uuid(v) => v.hash(state),
            Value::Timestamp(v) => v.hash(state),
            Value::Date(v) => v.hash(state),
            Value::Json(v) => {
                let s = v.to_string();
                s.hash(state);
            }
            Value::Array(v) => v.hash(state),
            Value::Null => {}
        }
    }
}

impl Value {
    /// Returns true if this value is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns the type name as a string.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Boolean(_) => "boolean",
            Value::Int8(_) => "int8",
            Value::Int16(_) => "int16",
            Value::Int32(_) => "int32",
            Value::Int64(_) => "int64",
            Value::UInt8(_) => "uint8",
            Value::UInt16(_) => "uint16",
            Value::UInt32(_) => "uint32",
            Value::UInt64(_) => "uint64",
            Value::Float32(_) => "float32",
            Value::Float64(_) => "float64",
            Value::Decimal(_) => "decimal",
            Value::String(_) => "string",
            Value::Bytes(_) => "bytes",
            Value::Uuid(_) => "uuid",
            Value::Timestamp(_) => "timestamp",
            Value::Date(_) => "date",
            Value::Json(_) => "json",
            Value::Array(_) => "array",
            Value::Null => "null",
        }
    }

    /// Attempt to convert this value to an i64.
    #[inline]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int8(v) => Some(*v as i64),
            Value::Int16(v) => Some(*v as i64),
            Value::Int32(v) => Some(*v as i64),
            Value::Int64(v) => Some(*v),
            Value::UInt8(v) => Some(*v as i64),
            Value::UInt16(v) => Some(*v as i64),
            Value::UInt32(v) => Some(*v as i64),
            _ => None,
        }
    }

    /// Attempt to convert this value to a u64.
    #[inline]
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::UInt8(v) => Some(*v as u64),
            Value::UInt16(v) => Some(*v as u64),
            Value::UInt32(v) => Some(*v as u64),
            Value::UInt64(v) => Some(*v),
            Value::Int8(v) if *v >= 0 => Some(*v as u64),
            Value::Int16(v) if *v >= 0 => Some(*v as u64),
            Value::Int32(v) if *v >= 0 => Some(*v as u64),
            Value::Int64(v) if *v >= 0 => Some(*v as u64),
            _ => None,
        }
    }

    /// Attempt to convert this value to an f64.
    #[inline]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float32(v) => Some(*v as f64),
            Value::Float64(v) => Some(*v),
            Value::Int8(v) => Some(*v as f64),
            Value::Int16(v) => Some(*v as f64),
            Value::Int32(v) => Some(*v as f64),
            Value::Int64(v) => Some(*v as f64),
            Value::UInt8(v) => Some(*v as f64),
            Value::UInt16(v) => Some(*v as f64),
            Value::UInt32(v) => Some(*v as f64),
            Value::UInt64(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Attempt to convert this value to a string reference.
    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Attempt to convert this value to a bool.
    #[inline]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(v) => Some(*v),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Boolean(v) => write!(f, "{}", v),
            Value::Int8(v) => write!(f, "{}", v),
            Value::Int16(v) => write!(f, "{}", v),
            Value::Int32(v) => write!(f, "{}", v),
            Value::Int64(v) => write!(f, "{}", v),
            Value::UInt8(v) => write!(f, "{}", v),
            Value::UInt16(v) => write!(f, "{}", v),
            Value::UInt32(v) => write!(f, "{}", v),
            Value::UInt64(v) => write!(f, "{}", v),
            Value::Float32(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::Decimal(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v),
            Value::Bytes(v) => write!(f, "<{} bytes>", v.len()),
            Value::Uuid(v) => write!(f, "{}", v),
            Value::Timestamp(v) => write!(f, "{}", v),
            Value::Date(v) => write!(f, "{}", v),
            Value::Json(v) => write!(f, "{}", v),
            Value::Array(v) => {
                write!(f, "[")?;
                for (i, item) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
            Value::Null => write!(f, "NULL"),
        }
    }
}

// Conversion implementations for ergonomic Value creation
impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Boolean(v)
    }
}

impl From<i8> for Value {
    fn from(v: i8) -> Self {
        Value::Int8(v)
    }
}

impl From<i16> for Value {
    fn from(v: i16) -> Self {
        Value::Int16(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int32(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int64(v)
    }
}

impl From<u8> for Value {
    fn from(v: u8) -> Self {
        Value::UInt8(v)
    }
}

impl From<u16> for Value {
    fn from(v: u16) -> Self {
        Value::UInt16(v)
    }
}

impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Value::UInt32(v)
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::UInt64(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Float32(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float64(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(v)
    }
}

impl From<Uuid> for Value {
    fn from(v: Uuid) -> Self {
        Value::Uuid(v)
    }
}

impl From<DateTime<Utc>> for Value {
    fn from(v: DateTime<Utc>) -> Self {
        Value::Timestamp(v)
    }
}

impl From<NaiveDate> for Value {
    fn from(v: NaiveDate) -> Self {
        Value::Date(v)
    }
}

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        Value::Json(v)
    }
}

impl From<Value> for serde_json::Value {
    fn from(v: Value) -> Self {
        match v {
            Value::Boolean(b) => serde_json::Value::Bool(b),
            Value::Int8(n) => serde_json::json!(n),
            Value::Int16(n) => serde_json::json!(n),
            Value::Int32(n) => serde_json::json!(n),
            Value::Int64(n) => serde_json::json!(n),
            Value::UInt8(n) => serde_json::json!(n),
            Value::UInt16(n) => serde_json::json!(n),
            Value::UInt32(n) => serde_json::json!(n),
            Value::UInt64(n) => serde_json::json!(n),
            Value::Float32(n) => serde_json::json!(n),
            Value::Float64(n) => serde_json::json!(n),
            Value::Decimal(s) => serde_json::json!(s),
            Value::String(s) => serde_json::Value::String(s),
            Value::Bytes(b) => serde_json::json!(b),
            Value::Uuid(u) => serde_json::json!(u.to_string()),
            Value::Timestamp(t) => serde_json::json!(t.to_rfc3339()),
            Value::Date(d) => serde_json::json!(d.to_string()),
            Value::Json(j) => j,
            Value::Array(a) => {
                serde_json::Value::Array(a.into_iter().map(serde_json::Value::from).collect())
            }
            Value::Null => serde_json::Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_null() {
        let v = Value::Null;
        assert!(v.is_null());
        assert_eq!(v.type_name(), "null");
    }

    #[test]
    fn test_value_conversions() {
        assert_eq!(Value::Int64(42).as_i64(), Some(42));
        assert_eq!(Value::UInt64(42).as_u64(), Some(42));
        assert_eq!(Value::Float64(3.14).as_f64(), Some(3.14));
        assert_eq!(Value::String("hello".into()).as_str(), Some("hello"));
        assert_eq!(Value::Boolean(true).as_bool(), Some(true));
    }

    #[test]
    fn test_value_display() {
        assert_eq!(format!("{}", Value::Int64(42)), "42");
        assert_eq!(format!("{}", Value::String("hello".into())), "hello");
        assert_eq!(format!("{}", Value::Null), "NULL");
    }

    #[test]
    fn test_value_from_conversions() {
        let v: Value = 42i64.into();
        assert_eq!(v.as_i64(), Some(42));

        let v: Value = "hello".into();
        assert_eq!(v.as_str(), Some("hello"));
    }
}
