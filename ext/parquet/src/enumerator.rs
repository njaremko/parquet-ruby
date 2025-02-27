use crate::ParserResultType;
use magnus::{value::ReprValue, Error as MagnusError, KwArgs, RArray, RHash, Symbol, Value};

pub struct RowEnumeratorArgs {
    pub rb_self: Value,
    pub to_read: Value,
    pub result_type: ParserResultType,
    pub columns: Option<Vec<String>>,
    pub strict: bool,
    pub logger: Option<Value>,
}

/// Creates an enumerator for lazy Parquet row parsing
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

pub struct ColumnEnumeratorArgs {
    pub rb_self: Value,
    pub to_read: Value,
    pub result_type: ParserResultType,
    pub columns: Option<Vec<String>>,
    pub batch_size: Option<usize>,
    pub strict: bool,
    pub logger: Option<Value>,
}

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
