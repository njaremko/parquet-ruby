use crate::reader::unified::{parse_parquet_unified, ParserType, UnifiedParserArgs};
use crate::utils::*;
use crate::ParquetGemError;

use magnus::{Error as MagnusError, Ruby, Value};
use std::rc::Rc;

#[inline]
pub fn parse_parquet_columns(rb_self: Value, args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };
    parse_parquet_columns_impl(Rc::new(ruby), rb_self, args).map_err(|e| {
        let z: MagnusError = e.into();
        z
    })
}

#[inline]
fn parse_parquet_columns_impl(
    ruby: Rc<Ruby>,
    rb_self: Value,
    args: &[Value],
) -> Result<Value, ParquetGemError> {
    let ParquetColumnsArgs {
        to_read,
        result_type,
        columns,
        batch_size,
        strict,
        logger,
    } = parse_parquet_columns_args(&ruby, args)?;

    // Use the unified parsing implementation
    parse_parquet_unified(
        ruby,
        rb_self,
        UnifiedParserArgs {
            to_read,
            result_type,
            columns,
            parser_type: ParserType::Column { batch_size, strict },
            logger,
        },
    )
}