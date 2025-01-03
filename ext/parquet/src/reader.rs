// =============================================================================
// Imports and Dependencies
// =============================================================================
use crate::header_cache::{CacheError, HeaderCacheCleanupIter, StringCache};
use crate::{
    create_enumerator, utils::*, EnumeratorArgs, ForgottenFileHandle, ParquetField, Record,
    SeekableRubyValue,
};
use ahash::RandomState;
use magnus::rb_sys::AsRawValue;
use magnus::value::{Opaque, ReprValue};
use magnus::{block::Yield, Error as MagnusError, Ruby, Value};
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
pub fn parse_parquet<'a>(
    rb_self: Value,
    args: &[Value],
) -> Result<Yield<Box<dyn Iterator<Item = Record<RandomState>>>>, MagnusError> {
    let original = unsafe { Ruby::get_unchecked() };
    let ruby: &'static Ruby = Box::leak(Box::new(original));

    let ParquetArgs {
        to_read,
        result_type,
        columns,
    } = parse_parquet_args(&ruby, args)?;

    if !ruby.block_given() {
        return create_enumerator(EnumeratorArgs {
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

    let iter: Box<dyn Iterator<Item = Record<RandomState>>> = match result_type.as_str() {
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
                .map(Record::Map);

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
            .map(Record::Vec),
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
