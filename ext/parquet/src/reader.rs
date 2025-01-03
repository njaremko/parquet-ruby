// =============================================================================
// Imports and Dependencies
// =============================================================================
use crate::header_cache::{CacheError, HeaderCacheCleanupIter, StringCache};
use crate::{
    create_column_enumerator, create_row_enumerator, utils::*, ColumnEnumeratorArgs, ColumnRecord,
    ForgottenFileHandle, ParquetField, ParquetValueVec, RowEnumeratorArgs, RowRecord,
    SeekableRubyValue,
};
use ahash::RandomState;
use magnus::rb_sys::AsRawValue;
use magnus::value::{Opaque, ReprValue};
use magnus::{block::Yield, Error as MagnusError, Ruby, Value};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::errors::ParquetError;
use parquet::file::reader::FileReader;
use parquet::file::reader::SerializedFileReader;
use parquet::record::reader::RowIter as ParquetRowIter;
use parquet::schema::types::{Type as SchemaType, TypePtr};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self};
use std::mem::ManuallyDrop;
use std::os::fd::FromRawFd;
use std::sync::OnceLock;
use thiserror::Error;

#[inline]
pub fn parse_parquet_rows<'a>(
    rb_self: Value,
    args: &[Value],
) -> Result<Yield<Box<dyn Iterator<Item = RowRecord<RandomState>>>>, MagnusError> {
    let original = unsafe { Ruby::get_unchecked() };
    let ruby: &'static Ruby = Box::leak(Box::new(original));

    let ParquetRowsArgs {
        to_read,
        result_type,
        columns,
    } = parse_parquet_rows_args(&ruby, args)?;

    if !ruby.block_given() {
        return create_row_enumerator(RowEnumeratorArgs {
            rb_self,
            to_read,
            result_type,
            columns,
        });
    }

    let (schema, mut iter) = if to_read.is_kind_of(ruby.class_string()) {
        let path_string = to_read.to_r_string()?;
        let file_path = unsafe { path_string.as_str()? };
        let file = File::open(file_path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let schema = reader.metadata().file_metadata().schema().clone();

        (schema, ParquetRowIter::from_file_into(Box::new(reader)))
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
        let reader = SerializedFileReader::new(file).unwrap();
        let schema = reader.metadata().file_metadata().schema().clone();

        (schema, ParquetRowIter::from_file_into(Box::new(reader)))
    } else {
        let readable = SeekableRubyValue(Opaque::from(to_read));
        let reader = SerializedFileReader::new(readable).unwrap();
        let schema = reader.metadata().file_metadata().schema().clone();

        (schema, ParquetRowIter::from_file_into(Box::new(reader)))
    };

    if let Some(cols) = columns {
        let projection = create_projection_schema(&schema, &cols);
        iter = iter.project(Some(projection.to_owned())).map_err(|e| {
            MagnusError::new(
                ruby.exception_runtime_error(),
                format!("Failed to create projection: {}", e),
            )
        })?;
    }

    let iter: Box<dyn Iterator<Item = RowRecord<RandomState>>> = match result_type.as_str() {
        "hash" => {
            let headers = OnceLock::new();
            let headers_clone = headers.clone();
            let iter = iter
                .filter_map(move |row| {
                    row.ok().map(|row| {
                        let headers = headers_clone.get_or_init(|| {
                            let column_count = row.get_column_iter().count();

                            let mut header_string = Vec::with_capacity(column_count);
                            for (k, _) in row.get_column_iter() {
                                header_string.push(k.to_owned());
                            }

                            let headers = StringCache::intern_many(&header_string).unwrap();

                            headers
                        });

                        let mut map =
                            HashMap::with_capacity_and_hasher(headers.len(), Default::default());
                        row.get_column_iter().enumerate().for_each(|(i, (_, v))| {
                            map.insert(headers[i], ParquetField(v.clone()));
                        });
                        map
                    })
                })
                .map(RowRecord::Map);

            Box::new(HeaderCacheCleanupIter {
                inner: iter,
                headers,
            })
        }
        "array" => Box::new(
            iter.filter_map(|row| {
                row.ok().map(|row| {
                    let column_count = row.get_column_iter().count();
                    let mut vec = Vec::with_capacity(column_count);
                    row.get_column_iter()
                        .for_each(|(_, v)| vec.push(ParquetField(v.clone())));
                    vec
                })
            })
            .map(RowRecord::Vec),
        ),
        _ => {
            return Err(MagnusError::new(
                ruby.exception_runtime_error(),
                "Invalid result type",
            ))
        }
    };

    Ok(Yield::Iter(iter))
}

#[inline]
pub fn parse_parquet_columns<'a>(
    rb_self: Value,
    args: &[Value],
) -> Result<Yield<Box<dyn Iterator<Item = ColumnRecord<RandomState>>>>, MagnusError> {
    let original = unsafe { Ruby::get_unchecked() };
    let ruby: &'static Ruby = Box::leak(Box::new(original));

    let ParquetColumnsArgs {
        to_read,
        result_type,
        columns,
        batch_size,
    } = parse_parquet_columns_args(&ruby, args)?;

    if !ruby.block_given() {
        return create_column_enumerator(ColumnEnumeratorArgs {
            rb_self,
            to_read,
            result_type,
            columns,
            batch_size,
        });
    }

    let batch_reader = if to_read.is_kind_of(ruby.class_string()) {
        let path_string = to_read.to_r_string()?;
        let file_path = unsafe { path_string.as_str()? };
        let file = File::open(file_path).map_err(|e| ReaderError::FileOpen(e))?;

        let mut builder =
            ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| ReaderError::Parquet(e))?;

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

        let reader = builder.build().unwrap();

        reader
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

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        if let Some(batch_size) = batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        // If columns are specified, project only those columns
        if let Some(cols) = &columns {
            // Get the parquet schema
            let parquet_schema = builder.parquet_schema();

            // Create a projection mask from column names
            let projection =
                ProjectionMask::columns(parquet_schema, cols.iter().map(|s| s.as_str()));

            builder = builder.with_projection(projection);
        }

        let reader = builder.build().unwrap();

        reader
    } else {
        let readable = SeekableRubyValue(Opaque::from(to_read));

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(readable).unwrap();

        if let Some(batch_size) = batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        // If columns are specified, project only those columns
        if let Some(cols) = &columns {
            // Get the parquet schema
            let parquet_schema = builder.parquet_schema();

            // Create a projection mask from column names
            let projection =
                ProjectionMask::columns(parquet_schema, cols.iter().map(|s| s.as_str()));

            builder = builder.with_projection(projection);
        }

        let reader = builder.build().unwrap();

        reader
    };

    let iter: Box<dyn Iterator<Item = ColumnRecord<RandomState>>> = match result_type.as_str() {
        "hash" => {
            let headers = OnceLock::new();
            let headers_clone = headers.clone();
            let iter = batch_reader
                .filter_map(move |batch| {
                    batch.ok().map(|batch| {
                        let headers = headers_clone.get_or_init(|| {
                            let schema = batch.schema();
                            let fields = schema.fields();
                            let mut header_string = Vec::with_capacity(fields.len());
                            for field in fields {
                                header_string.push(field.name().to_owned());
                            }
                            StringCache::intern_many(&header_string).unwrap()
                        });

                        let mut map =
                            HashMap::with_capacity_and_hasher(headers.len(), Default::default());

                        batch.columns().iter().enumerate().for_each(|(i, column)| {
                            let header = headers[i];
                            let values = ParquetValueVec::try_from(column.clone()).unwrap();
                            map.insert(header, values.into_inner());
                        });

                        map
                    })
                })
                .map(ColumnRecord::Map);

            Box::new(HeaderCacheCleanupIter {
                inner: iter,
                headers,
            })
        }
        "array" => Box::new(
            batch_reader
                .filter_map(|batch| {
                    batch.ok().map(|batch| {
                        batch
                            .columns()
                            .into_iter()
                            .map(|column| {
                                let values = ParquetValueVec::try_from(column.clone()).unwrap();
                                values.into_inner()
                            })
                            .collect()
                    })
                })
                .map(ColumnRecord::Vec),
        ),
        _ => {
            return Err(MagnusError::new(
                ruby.exception_runtime_error(),
                "Invalid result type",
            ))
        }
    };

    Ok(Yield::Iter(iter))
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

#[derive(Error, Debug)]
pub enum ReaderError {
    #[error("Failed to get file descriptor: {0}")]
    FileDescriptor(String),
    #[error("Invalid file descriptor")]
    InvalidFileDescriptor,
    #[error("Failed to open file: {0}")]
    FileOpen(#[from] io::Error),
    #[error("Failed to intern headers: {0}")]
    HeaderIntern(#[from] CacheError),
    #[error("Ruby error: {0}")]
    Ruby(String),
    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),
}

impl From<MagnusError> for ReaderError {
    fn from(err: MagnusError) -> Self {
        Self::Ruby(err.to_string())
    }
}

impl From<ReaderError> for MagnusError {
    fn from(err: ReaderError) -> Self {
        MagnusError::new(
            Ruby::get().unwrap().exception_runtime_error(),
            err.to_string(),
        )
    }
}
