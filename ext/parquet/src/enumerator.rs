use magnus::{block::Yield, value::ReprValue, Error as MagnusError, KwArgs, RHash, Symbol, Value};
use xxhash_rust::xxh3::Xxh3Builder;

use crate::Record;

pub struct EnumeratorArgs {
    pub rb_self: Value,
    pub to_read: Value,
    pub result_type: String,
}

#[inline]
pub fn create_enumerator(
    args: EnumeratorArgs,
) -> Result<Yield<Box<dyn Iterator<Item = Record<Xxh3Builder>>>>, MagnusError> {
    let kwargs = RHash::new();
    kwargs.aset(Symbol::new("result_type"), Symbol::new(args.result_type))?;
    let enumerator = args
        .rb_self
        .enumeratorize("for_each", (args.to_read, KwArgs(kwargs)));
    Ok(Yield::Enumerator(enumerator))
}
