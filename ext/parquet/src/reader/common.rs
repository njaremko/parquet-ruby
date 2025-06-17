use ahash::RandomState;
use arrow_schema::Schema;
use either::Either;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use std::collections::HashMap;
use std::fs::File;
use std::rc::Rc;
use std::sync::Arc;

use magnus::value::ReprValue;
use magnus::{Error as MagnusError, Ruby, Value};

use crate::header_cache::StringCache;
use crate::logger::RubyLogger;
use crate::ruby_reader::{RubyReader, ThreadSafeRubyReader};
use crate::types::{ParquetGemError, TryIntoValue};
use crate::ColumnRecord;

use super::format_detector::{detect_file_format, detect_format_from_extension, FileFormat};

/// Represents the different data sources we can open
pub enum DataSource {
    Parquet(Either<File, ThreadSafeRubyReader>),
    Arrow(Either<File, ThreadSafeRubyReader>),
}

/// Opens a data file (Parquet or Arrow) for reading, automatically detecting the format
pub fn open_data_source(
    ruby: Rc<Ruby>,
    to_read: Value,
    ruby_logger: &RubyLogger,
) -> Result<DataSource, ParquetGemError> {
    if to_read.is_kind_of(ruby.class_string()) {
        let path_string = to_read.to_r_string()?;
        let file_path = unsafe { path_string.as_str()? };

        // Try to detect format from extension first
        let format_hint = detect_format_from_extension(file_path);

        let mut file = File::open(file_path).map_err(ParquetGemError::from)?;

        // Detect actual format from file content
        let format = detect_file_format(&mut file)?;

        // Warn if extension doesn't match content
        if let Some(hint) = format_hint {
            if hint != format {
                ruby_logger.warn(|| {
                    format!(
                        "Extension implied format {:?} but actual format is {:?}",
                        hint, format
                    )
                })?;
            }
        }

        match format {
            FileFormat::Parquet => Ok(DataSource::Parquet(Either::Left(file))),
            FileFormat::Arrow => Ok(DataSource::Arrow(Either::Left(file))),
        }
    } else {
        // For IO-like objects, we need to use a temporary file
        use std::io::{Read, Write};
        use tempfile::NamedTempFile;

        let mut readable = RubyReader::new(ruby.clone(), to_read)?;
        let mut temp_file = NamedTempFile::new().map_err(ParquetGemError::from)?;

        // Copy the entire content to the temporary file
        let mut buffer = vec![0u8; 8192];
        loop {
            let bytes_read = readable.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            temp_file.write_all(&buffer[..bytes_read])?;
        }
        temp_file.flush()?;

        // Detect format from the temporary file
        let mut file = temp_file.reopen()?;
        let format = detect_file_format(&mut file)?;

        // Use the temporary file as the source
        match format {
            FileFormat::Parquet => Ok(DataSource::Parquet(Either::Left(file))),
            FileFormat::Arrow => Ok(DataSource::Arrow(Either::Left(file))),
        }
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
) -> Result<(ParquetRecordBatchReader, std::sync::Arc<Schema>, i64), ParquetGemError> {
    let mut builder =
        ParquetRecordBatchReaderBuilder::try_new(reader).map_err(ParquetGemError::Parquet)?;

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

    let reader = builder.build().map_err(ParquetGemError::Parquet)?;
    Ok((reader, schema, num_rows))
}

/// Handles the case of an empty parquet file (no rows) by yielding a record with empty arrays
/// Returns true if the file was empty and was handled, false otherwise
pub fn handle_empty_file(
    ruby: &magnus::Ruby,
    schema: &Arc<Schema>,
    num_rows: i64,
) -> Result<bool, ParquetGemError> {
    if num_rows == 0 {
        let mut map =
            HashMap::with_capacity_and_hasher(schema.fields().len(), RandomState::default());
        let headers: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        let interned_headers =
            StringCache::intern_many(&headers).map_err(ParquetGemError::HeaderIntern)?;
        for field in interned_headers.iter() {
            map.insert(*field, vec![]);
        }
        let record = ColumnRecord::Map(map);
        let _: Value = ruby.yield_value(record.try_into_value_with(ruby)?)?;
        return Ok(true);
    }
    Ok(false)
}
