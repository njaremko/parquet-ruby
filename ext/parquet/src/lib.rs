mod allocator;
mod enumerator;
pub mod header_cache;
mod logger;
mod reader;
mod ruby_reader;
mod types;
mod utils;
mod writer;

use crate::enumerator::*;
use crate::reader::*;
use crate::types::*;

use magnus::{Error, Ruby};
use writer::write_columns;
use writer::write_rows;

/// Initializes the Ruby extension and defines methods.
#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    // Require 'time' for Time.parse method
    ruby.require("time")?;
    
    let module = ruby.define_module("Parquet")?;
    module.define_module_function("metadata", magnus::method!(reader::parse_metadata, -1))?;
    module.define_module_function("each_row", magnus::method!(parse_parquet_rows, -1))?;
    module.define_module_function("each_column", magnus::method!(parse_parquet_columns, -1))?;
    module.define_module_function("write_rows", magnus::function!(write_rows, -1))?;
    module.define_module_function("write_columns", magnus::function!(write_columns, -1))?;
    Ok(())
}
