use serde::{Deserialize, Serialize};
use crate::value::Value;

/// The logical type of a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Decimal,
    String,
    Bytes,
   Uuid,
    Timestamp,
    Date,
    Json,
    Array,
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnType::Boolean => write!(f, "boolean"),
            ColumnType::Int8 => write!(f, "int8"),
            ColumnType::Int16 => write!(f, "int16"),
            ColumnType::Int32 => write!(f, "int32"),
            ColumnType::Int64 => write!(f, "int64"),
            ColumnType::UInt8 => write!(f, "uint8"),
            ColumnType::UInt16 => write!(f, "uint16"),
            ColumnType::UInt32 => write!(f, "uint32"),
            ColumnType::UInt64 => write!(f, "uint64"),
            ColumnType::Float32 => write!(f, "float32"),
            ColumnType::Float64 => write!(f, "float64"),
            ColumnType::Decimal => write!(f, "decimal"),
            ColumnType::String => write!(f, "string"),
            ColumnType::Bytes => write!(f, "bytes"),
            ColumnType::Uuid => write!(f, "uuid"),
            ColumnType::Timestamp => write!(f, "timestamp"),
            ColumnType::Date => write!(f, "date"),
            ColumnType::Json => write!(f, "json"),
            ColumnType::Array => write!(f, "array"),
        }
    }
}

/// Definition of a column in a table schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default: Option<Value>,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            nullable: false,
            primary_key: false,
            unique: false,
            default: None,
        }
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false;
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    pub fn default_value(mut self, value: Value) -> Self {
        self.default = Some(value);
        self
    }
}
