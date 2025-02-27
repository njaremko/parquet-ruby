use ahash::RandomState;
use arrow_schema::Schema;
use either::Either;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use magnus::value::ReprValue;
use magnus::{Error as MagnusError, Value};

use crate::header_cache::StringCache;
use crate::ruby_reader::{RubyReader, ThreadSafeRubyReader};
use crate::types::TryIntoValue;
use crate::ColumnRecord;

use super::ReaderError;

/// Opens a parquet file or IO-like object for reading
///
/// This function handles both file paths (as strings) and IO-like objects,
/// returning either a File or a ThreadSafeRubyReader that can be used with
/// parquet readers.
pub fn open_parquet_source(
    to_read: Value,
) -> Result<Either<File, ThreadSafeRubyReader>, ReaderError> {
    let ruby = unsafe { magnus::Ruby::get_unchecked() };

    if to_read.is_kind_of(ruby.class_string()) {
        let path_string = to_read.to_r_string()?;
        let file_path = unsafe { path_string.as_str()? };
        let file = File::open(file_path).map_err(ReaderError::from)?;
        Ok(Either::Left(file))
    } else {
        let readable = ThreadSafeRubyReader::new(RubyReader::try_from(to_read)?);
        Ok(Either::Right(readable))
    }
}

/// Helper function to check if a block is given and create an appropriate enumerator
/// if not
pub fn handle_block_or_enum<F, T>(
    _ruby: &magnus::Ruby,
    block_given: bool,
    create_enum: F,
) -> Result<Option<T>, MagnusError>
where
    F: FnOnce() -> Result<T, MagnusError>,
{
    if !block_given {
        let enum_value = create_enum()?;
        return Ok(Some(enum_value));
    }
    Ok(None)
}

/// Creates a ParquetRecordBatchReader with the given columns and batch size configurations
pub fn create_batch_reader<T: parquet::file::reader::ChunkReader + 'static>(
    reader: T,
    columns: &Option<Vec<String>>,
    batch_size: Option<usize>,
) -> Result<(ParquetRecordBatchReader, std::sync::Arc<Schema>, i64), ReaderError> {
    let mut builder =
        ParquetRecordBatchReaderBuilder::try_new(reader).map_err(|e| ReaderError::Parquet(e))?;

    let schema = builder.schema().clone();
    let num_rows = builder.metadata().file_metadata().num_rows();

    // If columns are specified, project only those columns
    if let Some(cols) = columns {
        // Get the parquet schema
        let parquet_schema = builder.parquet_schema();

        // Create a projection mask from column names
        let projection = ProjectionMask::columns(parquet_schema, cols.iter().map(|s| s.as_str()));
        builder = builder.with_projection(projection);
    }

    if let Some(batch_size) = batch_size {
        builder = builder.with_batch_size(batch_size);
    }

    let reader = builder.build().map_err(|e| ReaderError::Parquet(e))?;
    Ok((reader, schema, num_rows))
}

/// Handles the case of an empty parquet file (no rows) by yielding a record with empty arrays
/// Returns true if the file was empty and was handled, false otherwise
pub fn handle_empty_file(
    ruby: &magnus::Ruby,
    schema: &Arc<Schema>,
    num_rows: i64,
) -> Result<bool, ReaderError> {
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
        return Ok(true);
    }
    Ok(false)
}
