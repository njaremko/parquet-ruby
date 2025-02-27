use crate::ParserResultType;
use magnus::{
    scan_args::{get_kwargs, scan_args},
    value::ReprValue,
    Error, RString, Ruby, Symbol, Value,
};

/// Convert a Ruby Value to a String, handling both String and Symbol types
pub fn parse_string_or_symbol(ruby: &Ruby, value: Value) -> Result<Option<String>, Error> {
    if value.is_nil() {
        Ok(None)
    } else if value.is_kind_of(ruby.class_string()) {
        RString::from_value(value)
            .ok_or_else(|| Error::new(magnus::exception::type_error(), "Invalid string value"))?
            .to_string()
            .map(|s| Some(s))
    } else if value.is_kind_of(ruby.class_symbol()) {
        Symbol::from_value(value)
            .ok_or_else(|| Error::new(magnus::exception::type_error(), "Invalid symbol value"))?
            .funcall("to_s", ())
            .map(|s| Some(s))
    } else {
        Err(Error::new(
            magnus::exception::type_error(),
            "Value must be a String or Symbol",
        ))
    }
}

#[derive(Debug)]
pub struct ParquetRowsArgs {
    pub to_read: Value,
    pub result_type: ParserResultType,
    pub columns: Option<Vec<String>>,
    pub strict: bool,
    pub logger: Option<Value>,
}

/// Parse common arguments for parquet row iteration
pub fn parse_parquet_rows_args(ruby: &Ruby, args: &[Value]) -> Result<ParquetRowsArgs, Error> {
    let parsed_args = scan_args::<(Value,), (), (), (), _, ()>(args)?;
    let (to_read,) = parsed_args.required;

    let kwargs = get_kwargs::<
        _,
        (),
        (
            Option<Option<Value>>,
            Option<Option<Vec<String>>>,
            Option<Option<bool>>,
            Option<Option<Value>>,
        ),
        (),
    >(
        parsed_args.keywords,
        &[],
        &["result_type", "columns", "strict", "logger"],
    )?;

    let result_type: ParserResultType = match kwargs
        .optional
        .0
        .flatten()
        .map(|value| parse_string_or_symbol(ruby, value))
    {
        Some(Ok(Some(parsed))) => parsed.try_into().map_err(|e| {
            Error::new(
                magnus::exception::runtime_error(),
                format!(
                    "Invalid result type: {e}. Must be one of {}",
                    ParserResultType::iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            )
        })?,
        Some(Ok(None)) => ParserResultType::Hash,
        Some(Err(_)) => {
            return Err(Error::new(
                magnus::exception::type_error(),
                "result_type must be a String or Symbol",
            ))
        }
        None => ParserResultType::Hash,
    };

    let strict = kwargs.optional.2.flatten().unwrap_or(true);
    let logger = kwargs.optional.3.flatten();

    Ok(ParquetRowsArgs {
        to_read,
        result_type,
        columns: kwargs.optional.1.flatten(),
        strict,
        logger,
    })
}

#[derive(Debug)]
pub struct ParquetColumnsArgs {
    pub to_read: Value,
    pub result_type: ParserResultType,
    pub columns: Option<Vec<String>>,
    pub batch_size: Option<usize>,
    pub strict: bool,
    pub logger: Option<Value>,
}

/// Parse common arguments for parquet column iteration
pub fn parse_parquet_columns_args(
    ruby: &Ruby,
    args: &[Value],
) -> Result<ParquetColumnsArgs, Error> {
    let parsed_args = scan_args::<(Value,), (), (), (), _, ()>(args)?;
    let (to_read,) = parsed_args.required;

    let kwargs = get_kwargs::<
        _,
        (),
        (
            Option<Option<Value>>,
            Option<Option<Vec<String>>>,
            Option<Option<usize>>,
            Option<Option<bool>>,
            Option<Option<Value>>,
        ),
        (),
    >(
        parsed_args.keywords,
        &[],
        &["result_type", "columns", "batch_size", "strict", "logger"],
    )?;

    let result_type: ParserResultType = match kwargs
        .optional
        .0
        .flatten()
        .map(|value| parse_string_or_symbol(ruby, value))
    {
        Some(Ok(Some(parsed))) => parsed.try_into().map_err(|e| {
            Error::new(
                magnus::exception::runtime_error(),
                format!(
                    "Invalid result type: {e}. Must be one of {}",
                    ParserResultType::iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            )
        })?,
        Some(Ok(None)) => ParserResultType::Hash,
        Some(Err(_)) => {
            return Err(Error::new(
                magnus::exception::type_error(),
                "result_type must be a String or Symbol",
            ))
        }
        None => ParserResultType::Hash,
    };

    let batch_size = kwargs.optional.2.flatten();
    if let Some(sz) = batch_size {
        if sz <= 0 {
            return Err(Error::new(
                ruby.exception_arg_error(),
                format!("batch_size must be > 0, got {}", sz),
            ));
        }
    }

    let strict = kwargs.optional.3.flatten().unwrap_or(true);
    let logger = kwargs.optional.4.flatten();

    Ok(ParquetColumnsArgs {
        to_read,
        result_type,
        columns: kwargs.optional.1.flatten(),
        batch_size,
        strict,
        logger,
    })
}
