use magnus::value::ReprValue;
use magnus::{
    scan_args::{get_kwargs, scan_args},
    Error as MagnusError, KwArgs, Ruby, TryConvert, Value,
};
use parquet::basic::Compression;
use parquet_core::{MAX_BATCH_SIZE, MAX_SAMPLE_SIZE};

use crate::string_cache::{DEFAULT_STRING_CACHE_CAPACITY, STRING_CACHE_CAPACITY_MAX};
use crate::string_storage::{
    StringStorageConfig, StringStorageMode, DEFAULT_SHARED_MAX_ENTRIES,
    DEFAULT_SHARED_MAX_VALUE_BYTES,
};
use crate::types::{ColumnEnumeratorArgs, ParquetWriteArgs, RowEnumeratorArgs};

/// Reconstruct the `string_storage:` kwarg value for an enumerator so a
/// block-less call round-trips losslessly: a plain symbol for the mode, or a
/// hash when a `:shared` budget differs from the default. Returns `None` for the
/// default (`:copy`) config so the kwarg is simply omitted.
fn string_storage_kwarg(
    ruby: &Ruby,
    config: StringStorageConfig,
) -> Result<Option<Value>, MagnusError> {
    if config == StringStorageConfig::default() {
        return Ok(None);
    }
    let default_budget = config.shared_max_entries == DEFAULT_SHARED_MAX_ENTRIES
        && config.shared_max_value_bytes == DEFAULT_SHARED_MAX_VALUE_BYTES;
    if config.mode == StringStorageMode::Shared && !default_budget {
        let hash = ruby.hash_new();
        hash.aset(
            ruby.to_symbol("mode"),
            ruby.to_symbol(config.mode.to_string()),
        )?;
        hash.aset(ruby.to_symbol("max_entries"), config.shared_max_entries)?;
        hash.aset(
            ruby.to_symbol("max_value_bytes"),
            config.shared_max_value_bytes,
        )?;
        Ok(Some(hash.as_value()))
    } else {
        Ok(Some(ruby.to_symbol(config.mode.to_string()).as_value()))
    }
}

/// Parse compression type from string
pub fn parse_compression(
    ruby: &Ruby,
    compression: Option<String>,
) -> Result<Compression, MagnusError> {
    match compression.map(|s| s.to_lowercase()).as_deref() {
        Some("none") | Some("uncompressed") => Ok(Compression::UNCOMPRESSED),
        Some("snappy") => Ok(Compression::SNAPPY),
        Some("gzip") => Ok(Compression::GZIP(parquet::basic::GzipLevel::default())),
        Some("lz4") => Ok(Compression::LZ4),
        Some("zstd") => Ok(Compression::ZSTD(parquet::basic::ZstdLevel::default())),
        Some("brotli") => Ok(Compression::BROTLI(parquet::basic::BrotliLevel::default())),
        None => Ok(Compression::SNAPPY), // Default to SNAPPY
        Some(other) => Err(MagnusError::new(
            ruby.exception_arg_error(),
            format!("Invalid compression option: '{}'. Valid options are: none, snappy, gzip, lz4, zstd, brotli", other),
        )),
    }
}

/// Parse arguments for Parquet writing
pub fn parse_parquet_write_args(
    ruby: &Ruby,
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
            Option<Option<Value>>,
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
        batch_size: parse_positive_bounded_usize(
            ruby,
            "batch_size",
            kwargs.optional.0.flatten(),
            MAX_BATCH_SIZE,
        )?,
        flush_threshold: kwargs.optional.1.flatten(),
        compression: kwargs.optional.2.flatten(),
        sample_size: parse_positive_bounded_usize(
            ruby,
            "sample_size",
            kwargs.optional.3.flatten(),
            MAX_SAMPLE_SIZE,
        )?,
        logger: kwargs.optional.4.flatten(),
        string_cache: parse_string_cache(ruby, kwargs.optional.5.flatten())?,
    })
}

fn parse_positive_bounded_usize(
    ruby: &Ruby,
    name: &str,
    value: Option<usize>,
    max: usize,
) -> Result<Option<usize>, MagnusError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value == 0 {
        return Err(MagnusError::new(
            ruby.exception_arg_error(),
            format!("{name} must be positive"),
        ));
    }
    if value > max {
        return Err(MagnusError::new(
            ruby.exception_arg_error(),
            format!("{name} must be at most {max}"),
        ));
    }
    Ok(Some(value))
}

/// Parse the `string_cache:` write option. `false`/`nil`/absent disables it,
/// `true` enables it with the default capacity, and a positive Integer enables
/// it with that capacity. Returns the requested capacity, or `None` when
/// disabled.
pub fn parse_string_cache(ruby: &Ruby, value: Option<Value>) -> Result<Option<usize>, MagnusError> {
    let Some(value) = value else {
        return Ok(None);
    };
    // Strict: only true/false/nil and Integer are accepted (no Ruby truthiness
    // coercion, so a stray String is a clear error rather than "enabled").
    if value.is_nil() || value.eql(ruby.qfalse())? {
        return Ok(None);
    }
    if value.eql(ruby.qtrue())? {
        return Ok(Some(DEFAULT_STRING_CACHE_CAPACITY));
    }
    if value.is_kind_of(ruby.class_integer()) {
        let capacity: usize = TryConvert::try_convert(value)?;
        if capacity == 0 {
            return Err(MagnusError::new(
                ruby.exception_arg_error(),
                "string_cache capacity must be positive",
            ));
        }
        if capacity > STRING_CACHE_CAPACITY_MAX {
            return Err(MagnusError::new(
                ruby.exception_arg_error(),
                format!(
                    "string_cache capacity must be at most {}",
                    STRING_CACHE_CAPACITY_MAX
                ),
            ));
        }
        return Ok(Some(capacity));
    }
    Err(MagnusError::new(
        ruby.exception_type_error(),
        "string_cache must be true, false, or a positive Integer",
    ))
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
            ruby.exception_type_error(),
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
pub fn create_row_enumerator(
    ruby: &Ruby,
    args: RowEnumeratorArgs,
) -> Result<magnus::Enumerator, MagnusError> {
    let kwargs = ruby.hash_new();
    kwargs.aset(
        ruby.to_symbol("result_type"),
        ruby.to_symbol(args.result_type.to_string()),
    )?;
    if let Some(columns) = args.columns {
        kwargs.aset(ruby.to_symbol("columns"), ruby.ary_from_vec(columns))?;
    }
    if args.strict {
        kwargs.aset(ruby.to_symbol("strict"), true)?;
    }
    if let Some(value) = string_storage_kwarg(ruby, args.string_storage)? {
        kwargs.aset(ruby.to_symbol("string_storage"), value)?;
    }
    if let Some(logger) = args.logger {
        kwargs.aset(ruby.to_symbol("logger"), logger)?;
    }
    Ok(args
        .rb_self
        .enumeratorize("each_row", (args.to_read, KwArgs(kwargs))))
}

/// Create a column enumerator
#[inline]
pub fn create_column_enumerator(
    ruby: &Ruby,
    args: ColumnEnumeratorArgs,
) -> Result<magnus::Enumerator, MagnusError> {
    let kwargs = ruby.hash_new();
    kwargs.aset(
        ruby.to_symbol("result_type"),
        ruby.to_symbol(args.result_type.to_string()),
    )?;
    if let Some(columns) = args.columns {
        kwargs.aset(ruby.to_symbol("columns"), ruby.ary_from_vec(columns))?;
    }
    if let Some(batch_size) = args.batch_size {
        kwargs.aset(ruby.to_symbol("batch_size"), batch_size)?;
    }
    if args.strict {
        kwargs.aset(ruby.to_symbol("strict"), true)?;
    }
    if let Some(value) = string_storage_kwarg(ruby, args.string_storage)? {
        kwargs.aset(ruby.to_symbol("string_storage"), value)?;
    }
    if let Some(logger) = args.logger {
        kwargs.aset(ruby.to_symbol("logger"), logger)?;
    }
    Ok(args
        .rb_self
        .enumeratorize("each_column", (args.to_read, KwArgs(kwargs))))
}
