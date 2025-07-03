use magnus::value::ReprValue;
use magnus::{
    scan_args::{get_kwargs, scan_args},
    Error as MagnusError, KwArgs, RArray, RHash, Ruby, Symbol, Value,
};
use parquet::basic::Compression;
use parquet_core::ParquetValue;

use crate::types::{ColumnEnumeratorArgs, ParquetWriteArgs, RowEnumeratorArgs};

/// Estimate the memory size of a ParquetValue
pub fn estimate_parquet_value_size(value: &ParquetValue) -> usize {
    match value {
        ParquetValue::Null => 1,
        ParquetValue::Boolean(_) => 1,
        ParquetValue::Int8(_) => 1,
        ParquetValue::Int16(_) => 2,
        ParquetValue::Int32(_) => 4,
        ParquetValue::Int64(_) => 8,
        ParquetValue::UInt8(_) => 1,
        ParquetValue::UInt16(_) => 2,
        ParquetValue::UInt32(_) => 4,
        ParquetValue::UInt64(_) => 8,
        ParquetValue::Float16(_) => 4,
        ParquetValue::Float32(_) => 4,
        ParquetValue::Float64(_) => 8,
        ParquetValue::String(s) => s.len() + 24, // String overhead
        ParquetValue::Bytes(b) => b.len() + 24,  // Vec overhead
        ParquetValue::Date32(_) => 4,
        ParquetValue::Date64(_) => 8,
        ParquetValue::Decimal128(_, _) => 16 + 1, // value + scale
        ParquetValue::Decimal256(_, _) => 32 + 1, // approx size for BigInt + scale
        ParquetValue::TimestampSecond(_, tz) => 8 + tz.as_ref().map_or(0, |s| s.len() + 24),
        ParquetValue::TimestampMillis(_, tz) => 8 + tz.as_ref().map_or(0, |s| s.len() + 24),
        ParquetValue::TimestampMicros(_, tz) => 8 + tz.as_ref().map_or(0, |s| s.len() + 24),
        ParquetValue::TimestampNanos(_, tz) => 8 + tz.as_ref().map_or(0, |s| s.len() + 24),
        ParquetValue::TimeMillis(_) => 4,
        ParquetValue::TimeMicros(_) => 8,
        ParquetValue::List(items) => {
            24 + items.iter().map(estimate_parquet_value_size).sum::<usize>()
        }
        ParquetValue::Map(entries) => {
            48 + entries
                .iter()
                .map(|(k, v)| estimate_parquet_value_size(k) + estimate_parquet_value_size(v))
                .sum::<usize>()
        }
        ParquetValue::Record(fields) => {
            48 + fields
                .iter()
                .map(|(k, v)| k.len() + 24 + estimate_parquet_value_size(v))
                .sum::<usize>()
        }
    }
}

/// Estimate the memory size of a row
pub fn estimate_row_size(row: &[ParquetValue]) -> usize {
    row.iter().map(estimate_parquet_value_size).sum()
}

/// Parse compression type from string
pub fn parse_compression(compression: Option<String>) -> Result<Compression, MagnusError> {
    match compression.map(|s| s.to_lowercase()).as_deref() {
        Some("none") | Some("uncompressed") => Ok(Compression::UNCOMPRESSED),
        Some("snappy") => Ok(Compression::SNAPPY),
        Some("gzip") => Ok(Compression::GZIP(parquet::basic::GzipLevel::default())),
        Some("lz4") => Ok(Compression::LZ4),
        Some("zstd") => Ok(Compression::ZSTD(parquet::basic::ZstdLevel::default())),
        Some("brotli") => Ok(Compression::BROTLI(parquet::basic::BrotliLevel::default())),
        None => Ok(Compression::SNAPPY), // Default to SNAPPY
        Some(other) => Err(MagnusError::new(
            magnus::exception::arg_error(),
            format!("Invalid compression option: '{}'. Valid options are: none, snappy, gzip, lz4, zstd, brotli", other),
        )),
    }
}

/// Parse arguments for Parquet writing
pub fn parse_parquet_write_args(
    _ruby: &Ruby,
    args: &[Value],
) -> Result<ParquetWriteArgs, MagnusError> {
    let parsed_args = scan_args::<(Value,), (), (), (), _, ()>(args)?;
    let (read_from,) = parsed_args.required;

    let kwargs = get_kwargs::<
        _,
        (Value, Value),
        (
            Option<Option<usize>>,
            Option<Option<usize>>,
            Option<Option<String>>,
            Option<Option<usize>>,
            Option<Option<Value>>,
            Option<Option<bool>>,
        ),
        (),
    >(
        parsed_args.keywords,
        &["schema", "write_to"],
        &[
            "batch_size",
            "flush_threshold",
            "compression",
            "sample_size",
            "logger",
            "string_cache",
        ],
    )?;

    Ok(ParquetWriteArgs {
        read_from,
        write_to: kwargs.required.1,
        schema_value: kwargs.required.0,
        batch_size: kwargs.optional.0.flatten(),
        flush_threshold: kwargs.optional.1.flatten(),
        compression: kwargs.optional.2.flatten(),
        sample_size: kwargs.optional.3.flatten(),
        logger: kwargs.optional.4.flatten(),
        string_cache: kwargs.optional.5.flatten(),
    })
}

/// Convert a Ruby Value to a String, handling both String and Symbol types
pub fn parse_string_or_symbol(ruby: &Ruby, value: Value) -> Result<Option<String>, MagnusError> {
    if value.is_nil() {
        Ok(None)
    } else if value.is_kind_of(ruby.class_string()) || value.is_kind_of(ruby.class_symbol()) {
        let stringed = value.to_r_string()?.to_string()?;
        Ok(Some(stringed))
    } else {
        Err(MagnusError::new(
            magnus::exception::type_error(),
            "Value must be a String or Symbol",
        ))
    }
}

/// Handle block or enumerator creation
pub fn handle_block_or_enum<F, T>(
    block_given: bool,
    create_enum: F,
) -> Result<Option<T>, MagnusError>
where
    F: FnOnce() -> Result<T, MagnusError>,
{
    if !block_given {
        let enum_value = create_enum()?;
        return Ok(Some(enum_value));
    }
    Ok(None)
}

/// Create a row enumerator
pub fn create_row_enumerator(args: RowEnumeratorArgs) -> Result<magnus::Enumerator, MagnusError> {
    let kwargs = RHash::new();
    kwargs.aset(
        Symbol::new("result_type"),
        Symbol::new(args.result_type.to_string()),
    )?;
    if let Some(columns) = args.columns {
        kwargs.aset(Symbol::new("columns"), RArray::from_vec(columns))?;
    }
    if args.strict {
        kwargs.aset(Symbol::new("strict"), true)?;
    }
    if let Some(logger) = args.logger {
        kwargs.aset(Symbol::new("logger"), logger)?;
    }
    Ok(args
        .rb_self
        .enumeratorize("each_row", (args.to_read, KwArgs(kwargs))))
}

/// Create a column enumerator
#[inline]
pub fn create_column_enumerator(
    args: ColumnEnumeratorArgs,
) -> Result<magnus::Enumerator, MagnusError> {
    let kwargs = RHash::new();
    kwargs.aset(
        Symbol::new("result_type"),
        Symbol::new(args.result_type.to_string()),
    )?;
    if let Some(columns) = args.columns {
        kwargs.aset(Symbol::new("columns"), RArray::from_vec(columns))?;
    }
    if let Some(batch_size) = args.batch_size {
        kwargs.aset(Symbol::new("batch_size"), batch_size)?;
    }
    if args.strict {
        kwargs.aset(Symbol::new("strict"), true)?;
    }
    if let Some(logger) = args.logger {
        kwargs.aset(Symbol::new("logger"), logger)?;
    }
    Ok(args
        .rb_self
        .enumeratorize("each_column", (args.to_read, KwArgs(kwargs))))
}
