mod adapter_ffi;
mod allocator;

use magnus::{function, method, Error, Ruby};

use crate::adapter_ffi::{each_column, each_row, write_columns, write_rows};
use parquet_ruby_adapter::metadata::parse_metadata;

/// Initializes the Ruby extension and defines methods.
#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    ruby.require("time")?;
    ruby.require("bigdecimal")?;

    let module = ruby.define_module("Parquet")?;

    module.define_module_function("metadata", function!(parse_metadata, 1))?;
    module.define_module_function("each_row", method!(each_row, -1))?;
    module.define_module_function("each_column", method!(each_column, -1))?;
    module.define_module_function("write_rows", function!(write_rows, -1))?;
    module.define_module_function("write_columns", function!(write_columns, -1))?;

    Ok(())
}
