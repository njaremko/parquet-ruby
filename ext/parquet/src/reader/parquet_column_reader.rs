use crate::header_cache::StringCache;
use crate::types::{ArrayWrapper, TryIntoValue};
use crate::{
    create_column_enumerator, utils::*, ColumnEnumeratorArgs, ColumnRecord, ForgottenFileHandle,
    ParquetValueVec, ParserResultType, SeekableRubyValue,
};
use ahash::RandomState;
use magnus::rb_sys::AsRawValue;
use magnus::value::{Opaque, ReprValue};
use magnus::IntoValue;
use magnus::{Error as MagnusError, Ruby, Value};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use std::collections::HashMap;
use std::fs::File;
use std::mem::ManuallyDrop;
use std::os::fd::FromRawFd;
use std::sync::OnceLock;

use super::ReaderError;

#[inline]
pub fn parse_parquet_columns<'a>(rb_self: Value, args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };

    let ParquetColumnsArgs {
        to_read,
        result_type,
        columns,
        batch_size,
        strict,
    } = parse_parquet_columns_args(&ruby, args)?;

    if !ruby.block_given() {
        return create_column_enumerator(ColumnEnumeratorArgs {
            rb_self,
            to_read,
            result_type,
            columns,
            batch_size,
            strict,
        })
        .map(|yield_enum| yield_enum.into_value_with(&ruby));
    }

    let (batch_reader, schema, num_rows) = if to_read.is_kind_of(ruby.class_string()) {
        let path_string = to_read.to_r_string()?;
        let file_path = unsafe { path_string.as_str()? };
        let file = File::open(file_path).map_err(|e| ReaderError::FileOpen(e))?;

        let mut builder =
            ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| ReaderError::Parquet(e))?;
        let schema = builder.schema().clone();
        let num_rows = builder.metadata().file_metadata().num_rows();

        // If columns are specified, project only those columns
        if let Some(cols) = &columns {
            // Get the parquet schema
            let parquet_schema = builder.parquet_schema();

            // Create a projection mask from column names
            let projection =
                ProjectionMask::columns(parquet_schema, cols.iter().map(|s| s.as_str()));

            builder = builder.with_projection(projection);
        }

        if let Some(batch_size) = batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        let reader = builder.build().map_err(|e| ReaderError::Parquet(e))?;

        (reader, schema, num_rows)
    } else if to_read.is_kind_of(ruby.class_io()) {
        let raw_value = to_read.as_raw();
        let fd = std::panic::catch_unwind(|| unsafe { rb_sys::rb_io_descriptor(raw_value) })
            .map_err(|_| {
                ReaderError::FileDescriptor("Failed to get file descriptor".to_string())
            })?;

        if fd < 0 {
            return Err(ReaderError::InvalidFileDescriptor.into());
        }

        let file = unsafe { File::from_raw_fd(fd) };
        let file = ForgottenFileHandle(ManuallyDrop::new(file));

        let mut builder =
            ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| ReaderError::Parquet(e))?;
        let schema = builder.schema().clone();
        let num_rows = builder.metadata().file_metadata().num_rows();

        // If columns are specified, project only those columns
        if let Some(cols) = &columns {
            // Get the parquet schema
            let parquet_schema = builder.parquet_schema();

            // Create a projection mask from column names
            let projection =
                ProjectionMask::columns(parquet_schema, cols.iter().map(|s| s.as_str()));

            builder = builder.with_projection(projection);
        }

        if let Some(batch_size) = batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        let reader = builder.build().map_err(|e| ReaderError::Parquet(e))?;

        (reader, schema, num_rows)
    } else {
        let readable = SeekableRubyValue(Opaque::from(to_read));

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(readable)
            .map_err(|e| ReaderError::Parquet(e))?;
        let schema = builder.schema().clone();
        let num_rows = builder.metadata().file_metadata().num_rows();

        // If columns are specified, project only those columns
        if let Some(cols) = &columns {
            // Get the parquet schema
            let parquet_schema = builder.parquet_schema();

            // Create a projection mask from column names
            let projection =
                ProjectionMask::columns(parquet_schema, cols.iter().map(|s| s.as_str()));

            builder = builder.with_projection(projection);
        }

        if let Some(batch_size) = batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        let reader = builder.build().map_err(|e| ReaderError::Parquet(e))?;

        (reader, schema, num_rows)
    };

    if num_rows == 0 {
        let mut map =
            HashMap::with_capacity_and_hasher(schema.fields().len(), RandomState::default());
        let headers: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        let interned_headers =
            StringCache::intern_many(&headers).map_err(|e| ReaderError::HeaderIntern(e))?;
        for field in interned_headers.iter() {
            map.insert(*field, vec![]);
        }
        let record = ColumnRecord::Map(map);
        let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
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
