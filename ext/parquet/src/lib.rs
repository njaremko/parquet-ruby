mod allocator;
mod enumerator;
pub mod header_cache;
mod reader;
mod ruby_integration;
mod ruby_reader;
mod types;
mod utils;

use crate::enumerator::*;
use crate::reader::*;
use crate::ruby_integration::*;
use crate::types::*;

use magnus::{Error, Ruby};

/// Initializes the Ruby extension and defines methods.
#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("Parquet")?;
    module.define_module_function("each_row", magnus::method!(parse_parquet_rows, -1))?;
    module.define_module_function("each_column", magnus::method!(parse_parquet_columns, -1))?;
    Ok(())
}
