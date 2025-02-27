use crate::header_cache::StringCache;
use crate::logger::RubyLogger;
use crate::types::TryIntoValue;
use crate::{
    create_row_enumerator, utils::*, ParquetField, ParserResultType, ReaderError,
    RowEnumeratorArgs, RowRecord,
};
use ahash::RandomState;
use either::Either;
use magnus::IntoValue;
use magnus::{Error as MagnusError, Ruby, Value};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::reader::RowIter as ParquetRowIter;
use parquet::schema::types::{Type as SchemaType, TypePtr};
use std::collections::HashMap;
use std::sync::OnceLock;

use super::common::{handle_block_or_enum, open_parquet_source};

#[inline]
pub fn parse_parquet_rows<'a>(rb_self: Value, args: &[Value]) -> Result<Value, MagnusError> {
    Ok(parse_parquet_rows_impl(rb_self, args).map_err(|e| {
        let z: MagnusError = e.into();
        z
    })?)
}

#[inline]
fn parse_parquet_rows_impl<'a>(rb_self: Value, args: &[Value]) -> Result<Value, ReaderError> {
    let ruby = unsafe { Ruby::get_unchecked() };

    let ParquetRowsArgs {
        to_read,
        result_type,
        columns,
        strict,
        logger,
    } = parse_parquet_rows_args(&ruby, args)?;

    // Initialize the logger if provided
    let ruby_logger = RubyLogger::new(&ruby, logger)?;

    // Clone values for the closure to avoid move issues
    let columns_clone = columns.clone();

    // Handle block or create enumerator
    if let Some(enum_value) = handle_block_or_enum(&ruby, ruby.block_given(), || {
        create_row_enumerator(RowEnumeratorArgs {
            rb_self,
            to_read,
            result_type,
            columns: columns_clone,
            strict,
            logger,
        })
        .map(|yield_enum| yield_enum.into_value_with(&ruby))
    })? {
        return Ok(enum_value);
    }

    let source = open_parquet_source(to_read)?;
    let reader: Box<dyn FileReader> = match source {
        Either::Left(file) => Box::new(SerializedFileReader::new(file).map_err(ReaderError::from)?),
        Either::Right(readable) => {
            Box::new(SerializedFileReader::new(readable).map_err(ReaderError::from)?)
        }
    };

    let schema = reader.metadata().file_metadata().schema().clone();
    ruby_logger.debug(|| format!("Schema loaded: {:?}", schema))?;

    let mut iter = ParquetRowIter::from_file_into(reader);
    if let Some(cols) = columns {
        ruby_logger.debug(|| format!("Projecting columns: {:?}", cols))?;
        let projection = create_projection_schema(&schema, &cols);
        iter = iter.project(Some(projection.to_owned())).map_err(|e| {
            MagnusError::new(
                ruby.exception_runtime_error(),
                format!("Failed to create projection: {}", e),
            )
        })?;
    }

    match result_type {
        ParserResultType::Hash => {
            let headers = OnceLock::new();
            let headers_clone = headers.clone();
            let iter = iter.map(move |row| {
                row.and_then(|row| {
                    let headers = headers_clone.get_or_init(|| {
                        let column_count = row.get_column_iter().count();

                        let mut header_string = Vec::with_capacity(column_count);
                        for (k, _) in row.get_column_iter() {
                            header_string.push(k.to_owned());
                        }

                        let headers = StringCache::intern_many(&header_string)
                            .expect("Failed to intern headers");

                        headers
                    });

                    let mut map =
                        HashMap::with_capacity_and_hasher(headers.len(), RandomState::default());
                    for (i, (_, v)) in row.get_column_iter().enumerate() {
                        map.insert(headers[i], ParquetField(v.clone(), strict));
                    }
                    Ok(map)
                })
                .and_then(|row| Ok(RowRecord::Map::<RandomState>(row)))
                .map_err(|e| ReaderError::from(e))
            });

            for result in iter {
                let record = result?;
                let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
            }
        }
        ParserResultType::Array => {
            let iter = iter.map(|row| {
                row.and_then(|row| {
                    let column_count = row.get_column_iter().count();
                    let mut vec = Vec::with_capacity(column_count);
                    for (_, v) in row.get_column_iter() {
                        vec.push(ParquetField(v.clone(), strict));
                    }
                    Ok(vec)
                })
                .and_then(|row| Ok(RowRecord::Vec::<RandomState>(row)))
                .map_err(|e| ReaderError::from(e))
            });

            for result in iter {
                let record = result?;
                let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
            }
        }
    }

    Ok(ruby.qnil().into_value_with(&ruby))
}

fn create_projection_schema(schema: &SchemaType, columns: &[String]) -> SchemaType {
    if let SchemaType::GroupType { fields, .. } = schema {
        let projected_fields: Vec<TypePtr> = fields
            .iter()
            .filter(|field| columns.contains(&field.name().to_string()))
            .cloned()
            .collect();

        SchemaType::GroupType {
            basic_info: schema.get_basic_info().clone(),
            fields: projected_fields,
        }
    } else {
        // Return original schema if not a group type
        schema.clone()
    }
}
