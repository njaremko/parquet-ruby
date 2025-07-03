use magnus::scan_args::{get_kwargs, scan_args};
use magnus::{Error as MagnusError, Ruby, Value};
use parquet_ruby_adapter::utils::parse_string_or_symbol;
use parquet_ruby_adapter::{
    logger::RubyLogger, types::ParserResultType, utils::parse_parquet_write_args,
};
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
            Option<Option<bool>>,        // strict
            Option<Option<Value>>,       // logger
        ),
        (),
    >(
        parsed_args.keywords,
        &[],
        &["result_type", "columns", "strict", "logger"],
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
    let strict = kwargs.optional.2.flatten().unwrap_or(true);
    let logger = RubyLogger::new(kwargs.optional.3.flatten())?;

    // Delegate to parquet_ruby_adapter
    parquet_ruby_adapter::reader::each_row(
        &ruby,
        rb_self,
        to_read,
        result_type,
        columns,
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
            Option<Option<usize>>,       // batch_size
            Option<Option<bool>>,        // strict
            Option<Option<Value>>,       // logger
        ),
        (),
    >(
        parsed_args.keywords,
        &[],
        &["result_type", "columns", "batch_size", "strict", "logger"],
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
    let batch_size = if let Some(bs) = kwargs.optional.2.flatten() {
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
    let strict = kwargs.optional.3.flatten().unwrap_or(true);
    let logger = RubyLogger::new(kwargs.optional.4.flatten())?;

    // Delegate to parquet_ruby_adapter
    parquet_ruby_adapter::reader::each_column(
        &ruby,
        rb_self,
        to_read,
        result_type,
        columns,
        batch_size,
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
