mod adapter_ffi;
mod allocator;

use magnus::{function, method, Error, Ruby};

use crate::adapter_ffi::{each_column, each_row, repack, write_columns, write_rows};
use parquet_ruby_adapter::metadata::parse_metadata;

/// Initializes the Ruby extension and defines methods.
#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    // On Ruby 4.0 and later, the methods defined below may be called from any
    // Ractor. Earlier Rubies keep the default, which limits them to the main
    // Ractor: they crash when Enumerator#next, which the writers use to pull
    // their input, runs in parallel Ractors.
    if cfg!(ruby_gte_4) {
        unsafe { rb_sys::rb_ext_ractor_safe(true) };
    }

    // The conversions use these without requiring them, since a require from
    // any Ractor but the main one is handed over to the main Ractor.
    ruby.require("date")?;
    ruby.require("time")?;
    ruby.require("bigdecimal")?;

    let module = ruby.define_module("Parquet")?;

    module.define_module_function("metadata", function!(parse_metadata, 1))?;
    module.define_module_function("each_row", method!(each_row, -1))?;
    module.define_module_function("each_column", method!(each_column, -1))?;
    module.define_module_function("write_rows", function!(write_rows, -1))?;
    module.define_module_function("write_columns", function!(write_columns, -1))?;
    module.define_module_function("repack", function!(repack, -1))?;

    Ok(())
}
