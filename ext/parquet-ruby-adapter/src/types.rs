use magnus::Value;
use std::fs::File;
use std::str::FromStr;
use tempfile::NamedTempFile;

/// Arguments for writing Parquet files
#[derive(Debug)]
pub struct ParquetWriteArgs {
    pub read_from: Value,
    pub write_to: Value,
    pub schema_value: Value,
    pub batch_size: Option<usize>,
    pub flush_threshold: Option<usize>,
    pub compression: Option<String>,
    pub sample_size: Option<usize>,
    pub logger: Option<Value>,
    pub string_cache: Option<bool>,
}

/// Arguments for creating row enumerators
pub struct RowEnumeratorArgs {
    pub rb_self: Value,
    pub to_read: Value,
    pub result_type: ParserResultType,
    pub columns: Option<Vec<String>>,
    pub strict: bool,
    pub logger: Option<Value>,
}

/// Arguments for creating column enumerators
pub struct ColumnEnumeratorArgs {
    pub rb_self: Value,
    pub to_read: Value,
    pub result_type: ParserResultType,
    pub columns: Option<Vec<String>>,
    pub batch_size: Option<usize>,
    pub strict: bool,
    pub logger: Option<Value>,
}

/// Enum to handle different writer outputs
pub enum WriterOutput {
    File(parquet_core::Writer<File>),
    TempFile(parquet_core::Writer<File>, NamedTempFile, Value), // Writer, temp file, IO object
}

/// Result type for parser output
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ParserResultType {
    Hash,
    Array,
}

impl ParserResultType {
    pub fn iter() -> impl Iterator<Item = Self> {
        [Self::Hash, Self::Array].into_iter()
    }
}

impl FromStr for ParserResultType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

impl TryFrom<&str> for ParserResultType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "hash" => Ok(ParserResultType::Hash),
            "array" => Ok(ParserResultType::Array),
            _ => Err(format!("Invalid parser result type: {}", value)),
        }
    }
}

impl TryFrom<String> for ParserResultType {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl std::fmt::Display for ParserResultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParserResultType::Hash => write!(f, "hash"),
            ParserResultType::Array => write!(f, "array"),
        }
    }
}
