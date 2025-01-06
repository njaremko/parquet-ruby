use crate::header_cache::{CacheError, HeaderCacheCleanupIter, StringCache};
use crate::{
    create_column_enumerator, utils::*, ColumnEnumeratorArgs, ColumnRecord, ForgottenFileHandle,
    ParquetValueVec, ParserResultType, SeekableRubyValue,
};
use ahash::RandomState;
use magnus::rb_sys::AsRawValue;
use magnus::value::{Opaque, ReprValue};
use magnus::{block::Yield, Error as MagnusError, Ruby, Value};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::errors::ParquetError;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::mem::ManuallyDrop;
use std::os::fd::FromRawFd;
use std::sync::OnceLock;
use thiserror::Error;

#[inline]
pub fn parse_parquet_columns<'a>(
    rb_self: Value,
    args: &[Value],
) -> Result<Yield<Box<dyn Iterator<Item = ColumnRecord<RandomState>>>>, MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };

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

        let reader = builder.build().unwrap();

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

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let schema = builder.schema().clone();
        let num_rows = builder.metadata().file_metadata().num_rows();

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

        (reader, schema, num_rows)
    } else {
        let readable = SeekableRubyValue(Opaque::from(to_read));

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(readable).unwrap();
        let schema = builder.schema().clone();
        let num_rows = builder.metadata().file_metadata().num_rows();

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

        (reader, schema, num_rows)
    };

    if num_rows == 0 {
        let mut map =
            HashMap::with_capacity_and_hasher(schema.fields().len(), RandomState::default());
        for field in schema.fields() {
            map.insert(
                StringCache::intern(field.name().to_string()).unwrap(),
                vec![],
            );
        }
        let column_record = vec![ColumnRecord::Map(map)];
        return Ok(Yield::Iter(Box::new(column_record.into_iter())));
    }

    let iter: Box<dyn Iterator<Item = ColumnRecord<RandomState>>> = match result_type {
        ParserResultType::Hash => {
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
        ParserResultType::Array => Box::new(
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
    };

    Ok(Yield::Iter(iter))
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
