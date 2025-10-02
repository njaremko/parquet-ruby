use magnus::scan_args::{get_kwargs, scan_args};
use magnus::{Error as MagnusError, Ruby, Value};
use parquet_ruby_adapter::utils::parse_string_or_symbol;
use parquet_ruby_adapter::{
    logger::RubyLogger, types::ParserResultType, utils::parse_parquet_write_args,
};

use parquet_ruby_adapter::writer::{
    file_writer_close, file_writer_create, file_writer_flush_row_group, file_writer_write_rows,
};
use magnus::value::ReprValue;

pub fn each_row(rb_self: Value, args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = Ruby::get().map_err(|_| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            "Failed to get Ruby runtime",
        )
    })?;

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
            Option<Option<Vec<usize>>>,  // row_groups
            Option<Option<bool>>,        // strict
            Option<Option<Value>>,       // logger
        ),
        (),
    >(
        parsed_args.keywords,
        &[],
        &["result_type", "columns", "row_groups", "strict", "logger"],
    )?;

    let result_type: ParserResultType = if let Some(rt_value) = kwargs.optional.0.flatten() {
        parse_string_or_symbol(&ruby, rt_value)?
            .ok_or_else(|| {
                MagnusError::new(magnus::exception::arg_error(), "result_type cannot be nil")
            })?
            .parse()
            .map_err(|_| {
                MagnusError::new(magnus::exception::arg_error(), "Invalid result_type value")
            })?
    } else {
        ParserResultType::Hash
    };
    let columns = kwargs.optional.1.flatten();
    let row_groups = kwargs.optional.2.flatten();
    let strict = kwargs.optional.3.flatten().unwrap_or(true);
    let logger = RubyLogger::new(kwargs.optional.4.flatten())?;

    // Delegate to parquet_ruby_adapter
    parquet_ruby_adapter::reader::each_row(
        &ruby,
        rb_self,
        to_read,
        result_type,
        columns,
        row_groups,
        strict,
        logger,
    )
}

pub fn each_column(rb_self: Value, args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = Ruby::get().map_err(|_| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            "Failed to get Ruby runtime",
        )
    })?;

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
            Option<Option<Vec<usize>>>,  // row_groups
            Option<Option<usize>>,       // batch_size
            Option<Option<bool>>,        // strict
            Option<Option<Value>>,       // logger
        ),
        (),
    >(
        parsed_args.keywords,
        &[],
        &[
            "result_type",
            "columns",
            "row_groups",
            "batch_size",
            "strict",
            "logger",
        ],
    )?;

    let result_type: ParserResultType = if let Some(rt_value) = kwargs.optional.0.flatten() {
        parse_string_or_symbol(&ruby, rt_value)?
            .ok_or_else(|| {
                MagnusError::new(magnus::exception::arg_error(), "result_type cannot be nil")
            })?
            .parse()
            .map_err(|_| {
                MagnusError::new(magnus::exception::arg_error(), "Invalid result_type value")
            })?
    } else {
        ParserResultType::Hash
    };
    let columns = kwargs.optional.1.flatten();
    let row_groups = kwargs.optional.2.flatten();
    let batch_size = if let Some(bs) = kwargs.optional.3.flatten() {
        if bs == 0 {
            return Err(MagnusError::new(
                magnus::exception::arg_error(),
                "batch_size must be greater than 0",
            ));
        }
        Some(bs)
    } else {
        None
    };
    let strict = kwargs.optional.4.flatten().unwrap_or(true);
    let logger = RubyLogger::new(kwargs.optional.5.flatten())?;

    // Delegate to parquet_ruby_adapter
    parquet_ruby_adapter::reader::each_column(
        &ruby,
        rb_self,
        to_read,
        result_type,
        columns,
        batch_size,
        row_groups,
        strict,
        logger,
    )
}

pub fn write_rows(args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = Ruby::get().map_err(|_| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            "Failed to get Ruby runtime",
        )
    })?;

    // Parse arguments using the new parser
    let write_args = parse_parquet_write_args(&ruby, args)?;

    // Delegate to parquet_ruby_adapter
    parquet_ruby_adapter::writer::write_rows(&ruby, write_args)
}

pub fn write_columns(args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = Ruby::get().map_err(|_| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            "Failed to get Ruby runtime",
        )
    })?;

    // Parse arguments using the new parser
    let write_args = parse_parquet_write_args(&ruby, args)?;

    // Delegate to parquet_ruby_adapter
    parquet_ruby_adapter::writer::write_columns(&ruby, write_args)
}

// FileWriter FFI wrappers
pub fn fw_create(args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = Ruby::get().map_err(|_| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            "Failed to get Ruby runtime",
        )
    })?;

    let parsed_args = scan_args::<(Value, Value), (), (), (), _, ()>(args)?;
    let (schema_value, write_to) = parsed_args.required;

    // Parse keyword arguments
    let kwargs = get_kwargs::<
        _,
        (),
        (
            Option<Option<Value>>, // compression
            Option<Option<usize>>, // row_group_target_bytes
            Option<Option<usize>>, // sample_size
            Option<Option<bool>>,  // string_cache
            Option<Option<Value>>, // logger
        ),
        (),
    >(
        parsed_args.keywords,
        &[],
        &[
            "compression",
            "row_group_target_bytes",
            "sample_size",
            "string_cache",
            "logger",
        ],
    )?;

    let compression = kwargs.optional.0.flatten().map(|v| {
        parquet_ruby_adapter::utils::parse_string_or_symbol(&ruby, v)
            .unwrap()
            .unwrap()
            .to_string()
    });
    let threshold = kwargs.optional.1.flatten();
    let sample_size = kwargs.optional.2.flatten();
    let string_cache = kwargs.optional.3.flatten();
    let logger = kwargs.optional.4.flatten();

    let id = file_writer_create(
        &ruby,
        write_to,
        schema_value,
        compression,
        threshold,
        sample_size,
        string_cache,
        logger,
    )?;
    Ok(magnus::Integer::from_u64(id).as_value())
}

pub fn fw_write_rows(args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = Ruby::get().map_err(|_| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            "Failed to get Ruby runtime",
        )
    })?;
    let parsed_args = scan_args::<(Value, Value), (), (), (), (), ()>(args)?;
    let (id_val, rows) = parsed_args.required;
    let id: u64 = magnus::Integer::from_value(id_val)
        .ok_or_else(|| magnus::Error::new(magnus::exception::type_error(), "id must be Integer"))?
        .to_u64()
        .map_err(|e| magnus::Error::new(magnus::exception::range_error(), format!("{e}")))?;
    file_writer_write_rows(&ruby, id, rows)?;
    Ok(ruby.qnil().as_value())
}

pub fn fw_flush_row_group(args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = Ruby::get().map_err(|_| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            "Failed to get Ruby runtime",
        )
    })?;
    let parsed_args = scan_args::<(Value,), (), (), (), (), ()>(args)?;
    let (id_val,) = parsed_args.required;
    let id: u64 = magnus::Integer::from_value(id_val)
        .ok_or_else(|| magnus::Error::new(magnus::exception::type_error(), "id must be Integer"))?
        .to_u64()
        .map_err(|e| magnus::Error::new(magnus::exception::range_error(), format!("{e}")))?;
    file_writer_flush_row_group(&ruby, id)?;
    Ok(ruby.qnil().as_value())
}

pub fn fw_close(args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = Ruby::get().map_err(|_| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            "Failed to get Ruby runtime",
        )
    })?;
    let parsed_args = scan_args::<(Value,), (), (), (), (), ()>(args)?;
    let (id_val,) = parsed_args.required;
    let id: u64 = magnus::Integer::from_value(id_val)
        .ok_or_else(|| magnus::Error::new(magnus::exception::type_error(), "id must be Integer"))?
        .to_u64()
        .map_err(|e| magnus::Error::new(magnus::exception::range_error(), format!("{e}")))?;
    file_writer_close(&ruby, id)?;
    Ok(ruby.qnil().as_value())
}
