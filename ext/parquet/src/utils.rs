use magnus::{
    scan_args::{get_kwargs, scan_args},
    value::ReprValue,
    Error, RString, Ruby, Symbol, Value,
};

fn parse_string_or_symbol(ruby: &Ruby, value: Value) -> Result<Option<String>, Error> {
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
pub struct ParquetArgs {
    pub to_read: Value,
    pub result_type: String,
    pub columns: Option<Vec<String>>,
}

/// Parse common arguments for CSV parsing
pub fn parse_parquet_args(ruby: &Ruby, args: &[Value]) -> Result<ParquetArgs, Error> {
    let parsed_args = scan_args::<(Value,), (), (), (), _, ()>(args)?;
    let (to_read,) = parsed_args.required;

    let kwargs = get_kwargs::<_, (), (Option<Value>, Option<Vec<String>>), ()>(
        parsed_args.keywords,
        &[],
        &["result_type", "columns"],
    )?;

    let result_type = match kwargs
        .optional
        .0
        .map(|value| parse_string_or_symbol(ruby, value))
    {
        Some(Ok(Some(parsed))) => match parsed.as_str() {
            "hash" | "array" => parsed,
            _ => {
                return Err(Error::new(
                    magnus::exception::runtime_error(),
                    "result_type must be either 'hash' or 'array'",
                ))
            }
        },
        Some(Ok(None)) => String::from("hash"),
        Some(Err(_)) => {
            return Err(Error::new(
                magnus::exception::type_error(),
                "result_type must be a String or Symbol",
            ))
        }
        None => String::from("hash"),
    };

    Ok(ParquetArgs {
        to_read,
        result_type,
        columns: kwargs.optional.1,
    })
}
