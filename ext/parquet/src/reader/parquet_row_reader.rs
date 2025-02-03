use crate::header_cache::StringCache;
use crate::types::TryIntoValue;
use crate::{
    create_row_enumerator, utils::*, ForgottenFileHandle, ParquetField, ParserResultType,
    ReaderError, RowEnumeratorArgs, RowRecord, SeekableRubyValue,
};
use ahash::RandomState;
use magnus::rb_sys::AsRawValue;
use magnus::value::{Opaque, ReprValue};
use magnus::IntoValue;
use magnus::{Error as MagnusError, Ruby, Value};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::reader::RowIter as ParquetRowIter;
use parquet::schema::types::{Type as SchemaType, TypePtr};
use std::collections::HashMap;
use std::fs::File;
use std::mem::ManuallyDrop;
use std::os::fd::FromRawFd;
use std::sync::OnceLock;

#[inline]
pub fn parse_parquet_rows<'a>(rb_self: Value, args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };

    let ParquetRowsArgs {
        to_read,
        result_type,
        columns,
        strict,
    } = parse_parquet_rows_args(&ruby, args)?;

    if !ruby.block_given() {
        return create_row_enumerator(RowEnumeratorArgs {
            rb_self,
            to_read,
            result_type,
            columns,
            strict,
        })
        .map(|yield_enum| yield_enum.into_value_with(&ruby));
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

                        let headers = StringCache::intern_many(&header_string).unwrap();

                        headers
                    });

                    let mut map =
                        HashMap::with_capacity_and_hasher(headers.len(), RandomState::default());
                    row.get_column_iter().enumerate().for_each(|(i, (_, v))| {
                        map.insert(headers[i], ParquetField(v.clone(), strict));
                    });
                    Ok(map)
                })
                .and_then(|row| Ok(RowRecord::Map::<RandomState>(row)))
                .map_err(|e| ReaderError::Parquet(e))
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
                    row.get_column_iter()
                        .for_each(|(_, v)| vec.push(ParquetField(v.clone(), strict)));
                    Ok(vec)
                })
                .and_then(|row| Ok(RowRecord::Vec::<RandomState>(row)))
                .map_err(|e| ReaderError::Parquet(e))
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
