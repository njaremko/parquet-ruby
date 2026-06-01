use magnus::r_hash::ForEach;
use magnus::scan_args::{get_kwargs, scan_args};
use magnus::value::ReprValue;
use magnus::{Error as MagnusError, RHash, Ruby, TryConvert, Value};
use parquet_ruby_adapter::utils::parse_string_or_symbol;
use parquet_ruby_adapter::{
    logger::RubyLogger, types::ParserResultType, utils::parse_parquet_write_args,
    StringStorageConfig, StringStorageMode, DEFAULT_SHARED_MAX_ENTRIES,
    DEFAULT_SHARED_MAX_VALUE_BYTES,
};

fn arg_error(message: impl Into<String>) -> MagnusError {
    // Only ever called while constructing an error to return to Ruby, i.e. on the
    // Ruby thread with the GVL held, so a handle is always available.
    let ruby = Ruby::get().expect("arg_error built while the Ruby GVL is held");
    MagnusError::new(ruby.exception_arg_error(), message.into())
}

/// Parse the optional `string_storage:` keyword into a config. Accepts a symbol
/// or string naming the mode (`:copy`/`:intern`/`:shared`), or a hash
/// `{ mode:, max_entries:, max_value_bytes: }` to also set the `:shared` leak
/// budget. Defaults to the historical copy-per-value behavior when absent.
fn parse_string_storage(
    ruby: &Ruby,
    value: Option<Value>,
) -> Result<StringStorageConfig, MagnusError> {
    let Some(value) = value else {
        return Ok(StringStorageConfig::default());
    };
    if value.is_kind_of(ruby.class_hash()) {
        return parse_string_storage_hash(ruby, value);
    }
    let mode = parse_storage_mode(ruby, value)?;
    Ok(StringStorageConfig::from_mode(mode))
}

fn parse_storage_mode(ruby: &Ruby, value: Value) -> Result<StringStorageMode, MagnusError> {
    parse_string_or_symbol(ruby, value)?
        .ok_or_else(|| arg_error("string_storage mode cannot be nil"))?
        .parse()
        .map_err(arg_error)
}

fn parse_string_storage_hash(
    ruby: &Ruby,
    value: Value,
) -> Result<StringStorageConfig, MagnusError> {
    let hash: RHash = TryConvert::try_convert(value)?;
    reject_unknown_string_storage_keys(ruby, hash)?;
    let mode = match hash.get(ruby.to_symbol("mode")) {
        Some(mode_value) => parse_storage_mode(ruby, mode_value)?,
        None => return Err(arg_error("string_storage hash requires a :mode")),
    };
    // The leak budget only applies to :shared. Reject it for other modes rather
    // than silently ignoring it — that also keeps every parsed config in a state
    // the symbol/hash round-trip can reproduce (only :shared carries a budget).
    if mode != StringStorageMode::Shared
        && (has_key(ruby, &hash, "max_entries") || has_key(ruby, &hash, "max_value_bytes"))
    {
        return Err(arg_error(
            "string_storage :max_entries/:max_value_bytes are only valid with mode: :shared",
        ));
    }
    Ok(StringStorageConfig {
        mode,
        shared_max_entries: positive_usize(ruby, &hash, "max_entries", DEFAULT_SHARED_MAX_ENTRIES)?,
        shared_max_value_bytes: positive_usize(
            ruby,
            &hash,
            "max_value_bytes",
            DEFAULT_SHARED_MAX_VALUE_BYTES,
        )?,
    })
}

fn reject_unknown_string_storage_keys(ruby: &Ruby, hash: RHash) -> Result<(), MagnusError> {
    hash.foreach(|key: Value, _value: Value| {
        let key_name = parse_string_or_symbol(ruby, key)?
            .ok_or_else(|| arg_error("string_storage option keys cannot be nil"))?;
        match key_name.as_str() {
            "mode" | "max_entries" | "max_value_bytes" => Ok(ForEach::Continue),
            other => Err(arg_error(format!("unknown string_storage option :{other}"))),
        }
    })?;
    Ok(())
}

fn has_key(ruby: &Ruby, hash: &RHash, key: &str) -> bool {
    hash.get(ruby.to_symbol(key))
        .is_some_and(|value| !value.is_nil())
}

/// Read a positive-integer value from `hash[:key]`, falling back to `default`
/// when the key is absent or nil.
fn positive_usize(
    ruby: &Ruby,
    hash: &RHash,
    key: &str,
    default: usize,
) -> Result<usize, MagnusError> {
    match hash.get(ruby.to_symbol(key)) {
        Some(value) if !value.is_nil() => {
            let parsed: usize = TryConvert::try_convert(value).map_err(|_| {
                arg_error(format!(
                    "string_storage :{} must be a positive Integer",
                    key
                ))
            })?;
            if parsed == 0 {
                return Err(arg_error(format!(
                    "string_storage :{} must be positive",
                    key
                )));
            }
            Ok(parsed)
        }
        _ => Ok(default),
    }
}

pub fn each_row(rb_self: Value, args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = Ruby::get().expect("Ruby FFI entry point runs while the Ruby GVL is held");

    // Parse arguments
    let parsed_args = scan_args::<(Value,), (), (), (), _, ()>(args)?;
    let (to_read,) = parsed_args.required;

    // Parse keyword arguments
    let kwargs = get_kwargs::<
        _,
        (),
        (
            Option<Option<Value>>,       // result_type
            Option<Option<Vec<String>>>, // columns
            Option<Option<bool>>,        // strict
            Option<Option<Value>>,       // string_storage
            Option<Option<Value>>,       // logger
        ),
        (),
    >(
        parsed_args.keywords,
        &[],
        &[
            "result_type",
            "columns",
            "strict",
            "string_storage",
            "logger",
        ],
    )?;

    let result_type: ParserResultType = if let Some(rt_value) = kwargs.optional.0.flatten() {
        parse_string_or_symbol(&ruby, rt_value)?
            .ok_or_else(|| {
                MagnusError::new(ruby.exception_arg_error(), "result_type cannot be nil")
            })?
            .parse()
            .map_err(|_| {
                MagnusError::new(ruby.exception_arg_error(), "Invalid result_type value")
            })?
    } else {
        ParserResultType::Hash
    };
    let columns = kwargs.optional.1.flatten();
    let strict = kwargs.optional.2.flatten().unwrap_or(true);
    let string_storage = parse_string_storage(&ruby, kwargs.optional.3.flatten())?;
    let logger = RubyLogger::new(kwargs.optional.4.flatten())?;

    // Delegate to parquet_ruby_adapter
    parquet_ruby_adapter::reader::each_row(
        &ruby,
        rb_self,
        to_read,
        result_type,
        columns,
        strict,
        string_storage,
        logger,
    )
}

pub fn each_column(rb_self: Value, args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = Ruby::get().expect("Ruby FFI entry point runs while the Ruby GVL is held");

    // Parse arguments
    let parsed_args = scan_args::<(Value,), (), (), (), _, ()>(args)?;
    let (to_read,) = parsed_args.required;

    // Parse keyword arguments
    let kwargs = get_kwargs::<
        _,
        (),
        (
            Option<Option<Value>>,       // result_type
            Option<Option<Vec<String>>>, // columns
            Option<Option<usize>>,       // batch_size
            Option<Option<bool>>,        // strict
            Option<Option<Value>>,       // string_storage
            Option<Option<Value>>,       // logger
        ),
        (),
    >(
        parsed_args.keywords,
        &[],
        &[
            "result_type",
            "columns",
            "batch_size",
            "strict",
            "string_storage",
            "logger",
        ],
    )?;

    let result_type: ParserResultType = if let Some(rt_value) = kwargs.optional.0.flatten() {
        parse_string_or_symbol(&ruby, rt_value)?
            .ok_or_else(|| {
                MagnusError::new(ruby.exception_arg_error(), "result_type cannot be nil")
            })?
            .parse()
            .map_err(|_| {
                MagnusError::new(ruby.exception_arg_error(), "Invalid result_type value")
            })?
    } else {
        ParserResultType::Hash
    };
    let columns = kwargs.optional.1.flatten();
    let batch_size = if let Some(bs) = kwargs.optional.2.flatten() {
        if bs == 0 {
            return Err(MagnusError::new(
                ruby.exception_arg_error(),
                "batch_size must be greater than 0",
            ));
        }
        Some(bs)
    } else {
        None
    };
    let strict = kwargs.optional.3.flatten().unwrap_or(true);
    let string_storage = parse_string_storage(&ruby, kwargs.optional.4.flatten())?;
    let logger = RubyLogger::new(kwargs.optional.5.flatten())?;

    // Delegate to parquet_ruby_adapter
    parquet_ruby_adapter::reader::each_column(
        &ruby,
        rb_self,
        to_read,
        result_type,
        columns,
        batch_size,
        strict,
        string_storage,
        logger,
    )
}

pub fn write_rows(args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = Ruby::get().expect("Ruby FFI entry point runs while the Ruby GVL is held");

    // Parse arguments using the new parser
    let write_args = parse_parquet_write_args(&ruby, args)?;

    // Delegate to parquet_ruby_adapter
    parquet_ruby_adapter::writer::write_rows(&ruby, write_args)
}

pub fn write_columns(args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = Ruby::get().expect("Ruby FFI entry point runs while the Ruby GVL is held");

    // Parse arguments using the new parser
    let write_args = parse_parquet_write_args(&ruby, args)?;
    reject_row_only_column_write_options(&write_args)?;

    // Delegate to parquet_ruby_adapter
    parquet_ruby_adapter::writer::write_columns(&ruby, write_args)
}

fn reject_row_only_column_write_options(
    write_args: &parquet_ruby_adapter::types::ParquetWriteArgs,
) -> Result<(), MagnusError> {
    if write_args.batch_size.is_some() {
        return Err(arg_error(
            "write_columns does not accept batch_size; split input into column batches instead",
        ));
    }
    if write_args.sample_size.is_some() {
        return Err(arg_error(
            "write_columns does not accept sample_size; sample_size only applies to write_rows",
        ));
    }
    if write_args.string_cache.is_some() {
        return Err(arg_error(
            "write_columns does not accept string_cache; string_cache only applies to write_rows",
        ));
    }
    Ok(())
}
