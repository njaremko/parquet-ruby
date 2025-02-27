use crate::header_cache::StringCache;
use crate::logger::RubyLogger;
use crate::types::{ArrayWrapper, TryIntoValue};
use crate::{
    create_column_enumerator, utils::*, ColumnEnumeratorArgs, ColumnRecord, ParquetValueVec,
    ParserResultType,
};
use ahash::RandomState;
use either::Either;
use magnus::IntoValue;
use magnus::{Error as MagnusError, Ruby, Value};
use std::collections::HashMap;
use std::sync::OnceLock;

use super::common::{
    create_batch_reader, handle_block_or_enum, handle_empty_file, open_parquet_source,
};
use super::ReaderError;

#[inline]
pub fn parse_parquet_columns<'a>(rb_self: Value, args: &[Value]) -> Result<Value, MagnusError> {
    Ok(parse_parquet_columns_impl(rb_self, args).map_err(|e| {
        let z: MagnusError = e.into();
        z
    })?)
}

#[inline]
fn parse_parquet_columns_impl<'a>(rb_self: Value, args: &[Value]) -> Result<Value, ReaderError> {
    let ruby = unsafe { Ruby::get_unchecked() };

    let ParquetColumnsArgs {
        to_read,
        result_type,
        columns,
        batch_size,
        strict,
        logger,
    } = parse_parquet_columns_args(&ruby, args)?;

    // Initialize the logger if provided
    let ruby_logger = RubyLogger::new(&ruby, logger)?;
    if let Some(ref bs) = batch_size {
        ruby_logger.debug(|| format!("Using batch size: {}", bs))?;
    }

    // Clone values for the closure to avoid move issues
    let columns_clone = columns.clone();

    // Handle block or create enumerator
    if let Some(enum_value) = handle_block_or_enum(&ruby, ruby.block_given(), || {
        create_column_enumerator(ColumnEnumeratorArgs {
            rb_self,
            to_read,
            result_type,
            columns: columns_clone,
            batch_size,
            strict,
            logger: logger.as_ref().map(|_| to_read),
        })
        .map(|yield_enum| yield_enum.into_value_with(&ruby))
    })? {
        return Ok(enum_value);
    }

    let source = open_parquet_source(to_read)?;

    // Use the common function to create the batch reader

    let (batch_reader, schema, num_rows) = match source {
        Either::Left(file) => create_batch_reader(file, &columns, batch_size)?,
        Either::Right(readable) => create_batch_reader(readable, &columns, batch_size)?,
    };

    // Handle empty file case
    if handle_empty_file(&ruby, &schema, num_rows)? {
        return Ok(ruby.qnil().into_value_with(&ruby));
    }

    match result_type {
        ParserResultType::Hash => {
            let headers = OnceLock::new();
            let headers_clone = headers.clone();
            let iter = batch_reader.map(move |batch| {
                batch.map_err(ReaderError::Arrow).and_then(|batch| {
                    let local_headers = headers_clone
                        .get_or_init(|| {
                            let schema = batch.schema();
                            let fields = schema.fields();
                            let mut header_string = Vec::with_capacity(fields.len());
                            for field in fields {
                                header_string.push(field.name().to_owned());
                            }
                            StringCache::intern_many(&header_string)
                        })
                        .as_ref()
                        .map_err(|e| ReaderError::HeaderIntern(e.clone()))?;

                    let mut map = HashMap::with_capacity_and_hasher(
                        local_headers.len(),
                        RandomState::default(),
                    );

                    batch
                        .columns()
                        .iter()
                        .enumerate()
                        .try_for_each(|(i, column)| {
                            let header = local_headers[i];
                            let values = ParquetValueVec::try_from(ArrayWrapper {
                                array: &*column,
                                strict: strict,
                            })?;
                            map.insert(header, values.into_inner());
                            Ok::<_, ReaderError>(())
                        })?;

                    Ok(ColumnRecord::Map::<RandomState>(map))
                })
            });

            for result in iter {
                let record = result?;
                let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
            }
        }
        ParserResultType::Array => {
            let iter = batch_reader.map(|batch| {
                batch.map_err(ReaderError::Arrow).and_then(|batch| {
                    let vec = batch
                        .columns()
                        .into_iter()
                        .map(|column| {
                            let values = ParquetValueVec::try_from(ArrayWrapper {
                                array: &*column,
                                strict: strict,
                            })?;
                            Ok::<_, ReaderError>(values.into_inner())
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(ColumnRecord::Vec::<RandomState>(vec))
                })
            });

            for result in iter {
                let record = result?;
                let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
            }
        }
    }

    Ok(ruby.qnil().into_value_with(&ruby))
}
