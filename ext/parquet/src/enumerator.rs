use ahash::RandomState;
use magnus::{
    block::Yield, value::ReprValue, Error as MagnusError, KwArgs, RArray, RHash, Symbol, Value,
};

use crate::Record;

pub struct EnumeratorArgs {
    pub rb_self: Value,
    pub to_read: Value,
    pub result_type: String,
    pub columns: Option<Vec<String>>,
}

#[inline]
pub fn create_enumerator(
    args: EnumeratorArgs,
) -> Result<Yield<Box<dyn Iterator<Item = Record<RandomState>>>>, MagnusError> {
    let kwargs = RHash::new();
    kwargs.aset(Symbol::new("result_type"), Symbol::new(args.result_type))?;
    if let Some(columns) = args.columns {
        kwargs.aset(Symbol::new("columns"), RArray::from_vec(columns))?;
    }
    let enumerator = args
        .rb_self
        .enumeratorize("for_each", (args.to_read, KwArgs(kwargs)));
    Ok(Yield::Enumerator(enumerator))
}
