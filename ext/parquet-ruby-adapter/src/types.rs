use crate::string_storage::StringStorageConfig;
use magnus::Value;
use parquet::basic::Compression;
use std::str::FromStr;

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
    /// Requested string-cache capacity; `None` means the cache is disabled.
    pub string_cache: Option<usize>,
}

/// A fully validated `Parquet.repack` request.
///
/// Every field is checked while the GVL is held, so the repack itself never has
/// to re-derive caller intent or raise Ruby-shaped errors from the GVL-free
/// phase. In particular `read_from` is guaranteed non-empty and
/// `output_file_prefix` is guaranteed to be a single plain filename component.
#[derive(Debug)]
pub struct ParquetRepackArgs {
    pub read_from: Vec<String>,
    pub output_file_prefix: String,
    pub output_dir: String,
    pub rows_per_file: Option<usize>,
    pub max_read_rows_per_chunk: Option<usize>,
    /// `None` means "keep whatever codec the inputs already use"; the concrete
    /// codec is resolved from the first input once its metadata is read.
    pub compression: Option<Compression>,
    /// Whether repack may replace an existing `{prefix}-{n}.parquet` set in
    /// `output_dir`. When false, a populated output namespace is an error.
    pub overwrite: bool,
}

/// Arguments for creating row enumerators
pub struct RowEnumeratorArgs {
    pub rb_self: Value,
    pub to_read: Value,
    pub result_type: ParserResultType,
    pub columns: Option<Vec<String>>,
    pub strict: bool,
    pub string_storage: StringStorageConfig,
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
    pub string_storage: StringStorageConfig,
    pub logger: Option<Value>,
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
