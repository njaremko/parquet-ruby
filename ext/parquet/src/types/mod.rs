// Re-export all public items from submodules
mod core_types;
mod parquet_value;
mod record_types;
mod timestamp;
mod type_conversion;
mod writer_types;

pub use core_types::*;
pub use parquet_value::*;
pub use record_types::*;
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
