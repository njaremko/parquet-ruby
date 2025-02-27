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
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float16Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, NullArray, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, TimeUnit};
use magnus::{value::ReprValue, Error as MagnusError, IntoValue, Ruby, Value};
use parquet::data_type::Decimal;
use parquet::record::Field;
use std::{collections::HashMap, hash::BuildHasher, sync::Arc};

use crate::header_cache::StringCacheKey;
