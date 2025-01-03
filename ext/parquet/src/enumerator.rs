use ahash::RandomState;
use magnus::{
    block::Yield, value::ReprValue, Error as MagnusError, KwArgs, RArray, RHash, Symbol, Value,
};

use crate::{ColumnRecord, RowRecord};

pub struct RowEnumeratorArgs {
    pub rb_self: Value,
    pub to_read: Value,
    pub result_type: String,
    pub columns: Option<Vec<String>>,
}

#[inline]
pub fn create_row_enumerator(
    args: RowEnumeratorArgs,
) -> Result<Yield<Box<dyn Iterator<Item = RowRecord<RandomState>>>>, MagnusError> {
    let kwargs = RHash::new();
    kwargs.aset(Symbol::new("result_type"), Symbol::new(args.result_type))?;
    if let Some(columns) = args.columns {
        kwargs.aset(Symbol::new("columns"), RArray::from_vec(columns))?;
    }
    let enumerator = args
        .rb_self
        .enumeratorize("each_row", (args.to_read, KwArgs(kwargs)));
    Ok(Yield::Enumerator(enumerator))
}

pub struct ColumnEnumeratorArgs {
    pub rb_self: Value,
    pub to_read: Value,
    pub result_type: String,
    pub columns: Option<Vec<String>>,
    pub batch_size: Option<usize>,
}

#[inline]
pub fn create_column_enumerator(
    args: ColumnEnumeratorArgs,
) -> Result<Yield<Box<dyn Iterator<Item = ColumnRecord<RandomState>>>>, MagnusError> {
    let kwargs = RHash::new();
    kwargs.aset(Symbol::new("result_type"), Symbol::new(args.result_type))?;
    if let Some(columns) = args.columns {
        kwargs.aset(Symbol::new("columns"), RArray::from_vec(columns))?;
    }
    if let Some(batch_size) = args.batch_size {
        kwargs.aset(Symbol::new("batch_size"), batch_size)?;
    }
    let enumerator = args
        .rb_self
        .enumeratorize("each_column", (args.to_read, KwArgs(kwargs)));
    Ok(Yield::Enumerator(enumerator))
}
