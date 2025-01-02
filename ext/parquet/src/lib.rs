pub mod header_cache;
mod reader;
mod ruby_reader;
mod utils;

use crate::reader::*;

use magnus::{Error, Ruby};

/// Initializes the Ruby extension and defines methods.
#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("Parquet")?;
    module.define_module_function("each_row", magnus::method!(parse_parquet, -1))?;
    Ok(())
}
