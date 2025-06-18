// Re-export all public items from submodules
mod core_types;
mod parquet_value;
mod record_types;
pub mod schema_converter;
pub mod schema_node;
mod timestamp;
pub mod type_conversion;
mod writer_types;

pub use core_types::*;
pub use parquet_value::*;
pub use record_types::*;
// Explicitly export schema-related items
pub use schema_converter::{
    infer_schema_from_first_row, legacy_schema_to_dsl, parse_legacy_schema,
};
pub use schema_node::parse_schema_node;
pub use timestamp::*;
pub use type_conversion::*;
pub use writer_types::*;

// Common imports used across the module
use arrow_array::cast::downcast_array;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
    Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    ListArray, NullArray, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, TimeUnit};
use magnus::{value::ReprValue, Error as MagnusError, IntoValue, Ruby, Value};
use parquet::data_type::Decimal;
use parquet::record::Field;
use std::{collections::HashMap, hash::BuildHasher, sync::Arc};

use crate::header_cache::StringCacheKey;

use crate::header_cache::CacheError;

use std::io;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParquetGemError {
    #[error("Failed to open file: {0}")]
    FileOpen(#[from] io::Error),
    #[error("Failed to intern headers: {0}")]
    HeaderIntern(#[from] CacheError),
    #[error("Ruby error: {0}")]
    Ruby(#[from] MagnusErrorWrapper),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("UTF-8 error: {0}")]
    Utf8Error(#[from] simdutf8::basic::Utf8Error),
    #[error("Jiff error: {0}")]
    Jiff(#[from] jiff::Error),
    #[error("Failed to cast slice to array: {0}")]
    InvalidDecimal(String),
    #[error("Failed to parse UUID: {0}")]
    UuidError(#[from] uuid::Error),
    #[error("Decimals larger than 128 bits are not supported")]
    DecimalWouldBeTruncated,
}

#[derive(Debug)]
pub struct MagnusErrorWrapper(pub MagnusError);

impl From<MagnusError> for MagnusErrorWrapper {
    fn from(err: MagnusError) -> Self {
        Self(err)
    }
}

impl std::fmt::Display for MagnusErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for MagnusErrorWrapper {}

impl From<MagnusError> for ParquetGemError {
    fn from(err: MagnusError) -> Self {
        Self::Ruby(MagnusErrorWrapper(err))
    }
}

impl From<ParquetGemError> for MagnusError {
    fn from(val: ParquetGemError) -> Self {
        match val {
            ParquetGemError::Ruby(MagnusErrorWrapper(err)) => err,
            _ => MagnusError::new(magnus::exception::runtime_error(), val.to_string()),
        }
    }
}
