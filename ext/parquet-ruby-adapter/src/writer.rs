use magnus::value::ReprValue;
use magnus::{Error as MagnusError, Ruby, TryConvert, Value};
use parquet::file::properties::WriterProperties;
use parquet_core::Schema;
use std::io::{BufReader, BufWriter, Write};
use tempfile::NamedTempFile;

use crate::io::RubyIOWriter;
use crate::types::WriterOutput;
use crate::utils::parse_compression;
use crate::{
    logger::RubyLogger,
    schema::{extract_field_schemas, process_schema_value, ruby_schema_to_parquet},
};

/// Create a writer based on the output type (file path or IO object)
pub fn create_writer(
    ruby: &Ruby,
    write_to: Value,
    schema: Schema,
    compression: Option<String>,
) -> Result<WriterOutput, MagnusError> {
    let compression_setting = parse_compression(compression)?;
    let props = WriterProperties::builder()
        .set_compression(compression_setting)
        .build();

    if write_to.is_kind_of(ruby.class_string()) {
        // Direct file path
        let path_str: String = TryConvert::try_convert(write_to)?;
        let file = std::fs::File::create(&path_str)
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
        let writer = parquet_core::writer::Writer::new_with_properties(file, schema, props)
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
        Ok(WriterOutput::File(writer))
    } else {
        // IO-like object - create temporary file
        let temp_file = NamedTempFile::new().map_err(|e| {
            MagnusError::new(
                ruby.exception_runtime_error(),
                format!("Failed to create temporary file: {}", e),
            )
        })?;

        // Clone the file handle for the writer
        let file = temp_file.reopen().map_err(|e| {
            MagnusError::new(
                ruby.exception_runtime_error(),
                format!("Failed to reopen temporary file: {}", e),
            )
        })?;

        let writer = parquet_core::writer::Writer::new_with_properties(file, schema, props)
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

        Ok(WriterOutput::TempFile(writer, temp_file, write_to))
    }
}

/// Finalize the writer and copy temp file to IO if needed
pub fn finalize_writer(writer_output: WriterOutput) -> Result<(), MagnusError> {
    match writer_output {
        WriterOutput::File(writer) => writer
            .close()
            .map_err(|e| MagnusError::new(magnus::exception::runtime_error(), e.to_string())),
        WriterOutput::TempFile(writer, temp_file, io_object) => {
            // Close the writer first
            writer
                .close()
                .map_err(|e| MagnusError::new(magnus::exception::runtime_error(), e.to_string()))?;

            // Copy temp file to IO object
            copy_temp_file_to_io(temp_file, io_object)
        }
    }
}

/// Copy temporary file contents to Ruby IO object
fn copy_temp_file_to_io(temp_file: NamedTempFile, io_object: Value) -> Result<(), MagnusError> {
    let file = temp_file.reopen().map_err(|e| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            format!("Failed to reopen temporary file: {}", e),
        )
    })?;

    let mut buf_reader = BufReader::new(file);
    let ruby_io_writer = RubyIOWriter::new(io_object);
    let mut buf_writer = BufWriter::new(ruby_io_writer);

    std::io::copy(&mut buf_reader, &mut buf_writer).map_err(|e| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            format!("Failed to copy temp file to IO object: {}", e),
        )
    })?;

    buf_writer.flush().map_err(|e| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            format!("Failed to flush IO object: {}", e),
        )
    })?;

    // The temporary file will be automatically deleted when temp_file is dropped
    Ok(())
}

/// Write data in row format to a parquet file
pub fn write_rows(
    ruby: &Ruby,
    write_args: crate::types::ParquetWriteArgs,
) -> Result<Value, MagnusError> {
    use crate::batch_manager::BatchSizeManager;
    use crate::converter::RubyValueConverter;
    use crate::logger::RubyLogger;
    use crate::schema::{extract_field_schemas, process_schema_value, ruby_schema_to_parquet};
    use crate::string_cache::StringCache;
    use crate::utils::estimate_row_size;
    use magnus::{RArray, TryConvert};

    // Convert data to array if it isn't already
    let data_array = if write_args.read_from.is_kind_of(ruby.class_array()) {
        TryConvert::try_convert(write_args.read_from)?
    } else if write_args.read_from.respond_to("to_a", false)? {
        let array_value: Value = write_args.read_from.funcall("to_a", ())?;
        TryConvert::try_convert(array_value)?
    } else {
        return Err(MagnusError::new(
            ruby.exception_type_error(),
            "data must be an array or respond to 'to_a'",
        ));
    };

    let data_array: RArray = data_array;

    // Process schema value
    let schema_hash = process_schema_value(ruby, write_args.schema_value, Some(&data_array))
        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

    // Create schema
    let schema = ruby_schema_to_parquet(schema_hash)
        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

    // Extract field schemas for conversion hints
    let field_schemas = extract_field_schemas(&schema);

    // Create writer
    let mut writer_output = create_writer(
        ruby,
        write_args.write_to,
        schema.clone(),
        write_args.compression,
    )?;

    // Create logger
    let logger = RubyLogger::new(write_args.logger)?;
    let _ = logger.info(|| "Starting to write parquet file".to_string());

    // Create batch size manager
    let mut batch_manager = BatchSizeManager::new(
        write_args.batch_size,
        write_args.flush_threshold,
        write_args.sample_size,
    );

    let _ = logger.debug(|| {
        format!(
            "Batch sizing: fixed_size={:?}, memory_threshold={}, sample_size={}",
            batch_manager.fixed_batch_size,
            batch_manager.memory_threshold,
            batch_manager.sample_size
        )
    });

    // Create converter with string cache if enabled
    let mut converter = if write_args.string_cache.unwrap_or(false) {
        let _ = logger.debug(|| "String cache enabled".to_string());
        RubyValueConverter::with_string_cache(StringCache::new(true))
    } else {
        RubyValueConverter::new()
    };

    // Collect rows in batches
    let mut batch = Vec::new();
    let mut batch_memory_size = 0usize;
    let mut total_rows = 0u64;

    for row_value in data_array.into_iter() {
        // Convert Ruby row to ParquetValue vector
        let row = if row_value.is_kind_of(ruby.class_array()) {
            let array: RArray = TryConvert::try_convert(row_value)?;
            let mut values = Vec::with_capacity(array.len());

            for (idx, item) in array.into_iter().enumerate() {
                let schema_hint = field_schemas.get(idx);
                let pq_value = converter
                    .to_parquet_with_schema_hint(item, schema_hint)
                    .map_err(|e| {
                        let error_msg = e.to_string();
                        // Check if this is an encoding error
                        if error_msg.contains("EncodingError")
                            || error_msg.contains("invalid utf-8")
                        {
                            // Extract the actual encoding error message
                            if let Some(pos) = error_msg.find("EncodingError: ") {
                                let encoding_msg = error_msg[pos + 15..].to_string();
                                MagnusError::new(ruby.exception_encoding_error(), encoding_msg)
                            } else {
                                MagnusError::new(ruby.exception_encoding_error(), error_msg)
                            }
                        } else {
                            MagnusError::new(ruby.exception_runtime_error(), error_msg)
                        }
                    })?;
                values.push(pq_value);
            }
            values
        } else {
            return Err(MagnusError::new(
                ruby.exception_type_error(),
                "each row must be an array",
            ));
        };

        // Record row size for dynamic batch sizing
        let row_size = estimate_row_size(&row);
        batch_manager.record_row_size(row_size);
        batch_memory_size += row_size;

        batch.push(row);
        total_rows += 1;

        // Log sampling progress
        if batch_manager.row_size_samples.len() <= batch_manager.sample_size
            && batch_manager.row_size_samples.len() % 10 == 0
        {
            let _ = logger.debug(|| {
                format!(
                    "Sampled {} rows, avg size: {} bytes, current batch size: {}",
                    batch_manager.row_size_samples.len(),
                    batch_manager.average_row_size(),
                    batch_manager.current_batch_size
                )
            });
        }

        // Write batch if it reaches threshold
        if batch_manager.should_flush(batch.len(), batch_memory_size) {
            let _ = logger.info(|| format!("Writing batch of {} rows", batch.len()));
            let _ = logger.debug(|| format!(
                "Batch details: recent avg row size: {} bytes, current batch size: {}, actual memory: {} bytes",
                batch_manager.recent_average_size(),
                batch_manager.current_batch_size,
                batch_memory_size
            ));
            match &mut writer_output {
                WriterOutput::File(writer) | WriterOutput::TempFile(writer, _, _) => {
                    writer.write_rows(std::mem::take(&mut batch)).map_err(|e| {
                        MagnusError::new(ruby.exception_runtime_error(), e.to_string())
                    })?;
                }
            }
            batch_memory_size = 0;
        }
    }

    // Write remaining rows
    if !batch.is_empty() {
        let _ = logger.info(|| format!("Writing batch of {} rows", batch.len()));
        let _ = logger.debug(|| format!("Final batch: {} rows", batch.len()));
        match &mut writer_output {
            WriterOutput::File(writer) | WriterOutput::TempFile(writer, _, _) => {
                writer
                    .write_rows(batch)
                    .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
            }
        }
    }

    let _ = logger.info(|| format!("Finished writing {} rows to parquet file", total_rows));

    // Log string cache statistics if enabled
    if let Some(stats) = converter.string_cache_stats() {
        let _ = logger.info(|| {
            format!(
                "String cache stats: {} unique strings, {} hits ({:.1}% hit rate)",
                stats.size,
                stats.hits,
                stats.hit_rate * 100.0
            )
        });
    }

    // Finalize the writer
    finalize_writer(writer_output)?;

    Ok(ruby.qnil().as_value())
}

/// Write data in column format to a parquet file
pub fn write_columns(
    ruby: &Ruby,
    write_args: crate::types::ParquetWriteArgs,
) -> Result<Value, MagnusError> {
    use crate::converter::RubyValueConverter;
    use crate::schema::{extract_field_schemas, process_schema_value, ruby_schema_to_parquet};
    use magnus::{RArray, TryConvert};

    // Convert data to array for processing
    let data_array = if write_args.read_from.is_kind_of(ruby.class_array()) {
        TryConvert::try_convert(write_args.read_from)?
    } else if write_args.read_from.respond_to("to_a", false)? {
        let array_value: Value = write_args.read_from.funcall("to_a", ())?;
        TryConvert::try_convert(array_value)?
    } else {
        return Err(MagnusError::new(
            ruby.exception_type_error(),
            "data must be an array or respond to 'to_a'",
        ));
    };

    let data_array: RArray = data_array;

    // Process schema value
    let schema_hash = process_schema_value(ruby, write_args.schema_value, Some(&data_array))
        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

    // Create schema
    let schema = ruby_schema_to_parquet(schema_hash)
        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

    // Extract field schemas for conversion hints
    let field_schemas = extract_field_schemas(&schema);

    // Create writer
    let mut writer_output = create_writer(
        ruby,
        write_args.write_to,
        schema.clone(),
        write_args.compression,
    )?;

    // Get column names from schema
    let column_names: Vec<String> =
        if let parquet_core::SchemaNode::Struct { fields, .. } = &schema.root {
            fields.iter().map(|f| f.name().to_string()).collect()
        } else {
            return Err(MagnusError::new(
                ruby.exception_runtime_error(),
                "Schema root must be a struct",
            ));
        };

    // Convert data to columns format
    let mut all_columns: Vec<(String, Vec<parquet_core::ParquetValue>)> = Vec::new();

    // Process batches
    for (batch_idx, batch) in data_array.into_iter().enumerate() {
        if !batch.is_kind_of(ruby.class_array()) {
            return Err(MagnusError::new(
                ruby.exception_type_error(),
                "each batch must be an array of column values",
            ));
        }

        let batch_array: RArray = TryConvert::try_convert(batch)?;

        // Verify batch has the right number of columns
        if batch_array.len() != column_names.len() {
            return Err(MagnusError::new(
                ruby.exception_runtime_error(),
                format!(
                    "Batch has {} columns but schema has {}",
                    batch_array.len(),
                    column_names.len()
                ),
            ));
        }

        // Process each column in the batch
        for (col_idx, column_values) in batch_array.into_iter().enumerate() {
            if !column_values.is_kind_of(ruby.class_array()) {
                return Err(MagnusError::new(
                    ruby.exception_type_error(),
                    format!("Column {} values must be an array", col_idx),
                ));
            }

            let values_array: RArray = TryConvert::try_convert(column_values)?;

            // Initialize column vector on first batch
            if batch_idx == 0 {
                all_columns.push((column_names[col_idx].clone(), Vec::new()));
            }

            // Convert and append values
            let mut converter = RubyValueConverter::new();
            let schema_hint = field_schemas.get(col_idx);

            for value in values_array.into_iter() {
                let pq_value = converter
                    .to_parquet_with_schema_hint(value, schema_hint)
                    .map_err(|e| {
                        let error_msg = e.to_string();
                        // Check if this is an encoding error
                        if error_msg.contains("EncodingError")
                            || error_msg.contains("invalid utf-8")
                        {
                            // Extract the actual encoding error message
                            if let Some(pos) = error_msg.find("EncodingError: ") {
                                let encoding_msg = error_msg[pos + 15..].to_string();
                                MagnusError::new(ruby.exception_encoding_error(), encoding_msg)
                            } else {
                                MagnusError::new(ruby.exception_encoding_error(), error_msg)
                            }
                        } else {
                            MagnusError::new(ruby.exception_runtime_error(), error_msg)
                        }
                    })?;
                all_columns[col_idx].1.push(pq_value);
            }
        }
    }

    // Write the columns
    match &mut writer_output {
        WriterOutput::File(writer) | WriterOutput::TempFile(writer, _, _) => {
            writer
                .write_columns(all_columns)
                .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
        }
    }

    // Finalize the writer
    finalize_writer(writer_output)?;

    Ok(ruby.qnil().as_value())
}

// =========================
// Incremental File Writer
// =========================

use crate::batch_manager::BatchSizeManager;
use crate::converter::RubyValueConverter;
use crate::string_cache::StringCache;
use magnus::RArray;
use parquet_core::SchemaNode;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct IncrementalWriter {
    writer_output: WriterOutput,
    field_schemas: Vec<SchemaNode>,
    converter: RubyValueConverter,
    batch_manager: BatchSizeManager,
    batch: Vec<Vec<parquet_core::ParquetValue>>,
    batch_memory_size: usize,
    closed: bool,
}

impl IncrementalWriter {
    fn write_batch_to_core(&mut self) -> Result<(), MagnusError> {
        if self.batch.is_empty() {
            return Ok(());
        }
        match &mut self.writer_output {
            WriterOutput::File(writer) | WriterOutput::TempFile(writer, _, _) => {
                writer
                    .write_rows(std::mem::take(&mut self.batch))
                    .map_err(|e| MagnusError::new(magnus::exception::runtime_error(), e.to_string()))?;
            }
        }
        self.batch_memory_size = 0;
        Ok(())
    }
}

thread_local! {
    static FW_REGISTRY: RefCell<HashMap<u64, IncrementalWriter>> = RefCell::new(HashMap::new());
}
static FW_NEXT_ID: AtomicU64 = AtomicU64::new(1);

pub fn file_writer_create(
    ruby: &Ruby,
    write_to: Value,
    schema_value: Value,
    compression: Option<String>,
    row_group_target_bytes: Option<usize>,
    sample_size: Option<usize>,
    string_cache: Option<bool>,
    logger: Option<Value>,
) -> Result<u64, MagnusError> {
    // Build schema from Ruby value
    let schema_hash = process_schema_value(ruby, schema_value, None)
        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
    let schema = ruby_schema_to_parquet(schema_hash)
        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
    let field_schemas = extract_field_schemas(&schema);

    let writer_output = create_writer(ruby, write_to, schema.clone(), compression)?;

    let use_cache = string_cache.unwrap_or(false);
    let converter = if use_cache {
        RubyValueConverter::with_string_cache(StringCache::new(true))
    } else {
        RubyValueConverter::new()
    };

    // Batch manager: row_group_target_bytes maps to memory_threshold
    let batch_manager = BatchSizeManager::new(None, row_group_target_bytes, sample_size);

    // Optional log
    let logger = RubyLogger::new(logger)?;
    let _ = logger.info(|| format!(
        "FileWriter created: target_bytes={:?} sample_size={:?}",
        row_group_target_bytes, sample_size
    ));

    let writer = IncrementalWriter {
        writer_output,
        field_schemas,
        converter,
        batch_manager,
        batch: Vec::new(),
        batch_memory_size: 0,
        closed: false,
    };

    let id = FW_NEXT_ID.fetch_add(1, Ordering::SeqCst);
    FW_REGISTRY.with(|reg| {
        reg.borrow_mut().insert(id, writer);
    });
    Ok(id)
}

pub fn file_writer_write_rows(ruby: &Ruby, id: u64, rows: Value) -> Result<(), MagnusError> {
    let mut_opt = FW_REGISTRY.with(|reg| reg.borrow_mut().remove(&id));
    let mut writer = if let Some(w) = mut_opt { w } else {
        return Err(MagnusError::new(
            magnus::exception::runtime_error(),
            format!("Invalid writer id {id}"),
        ));
    };
    if writer.closed {
        return Err(MagnusError::new(
            magnus::exception::runtime_error(),
            "Writer is closed",
        ));
    }

    let rows_ary: RArray = if rows.is_kind_of(ruby.class_array()) {
        magnus::TryConvert::try_convert(rows)?
    } else if rows.respond_to("to_a", false)? {
        let array_value: Value = rows.funcall("to_a", ())?;
        magnus::TryConvert::try_convert(array_value)?
    } else {
        return Err(MagnusError::new(
            ruby.exception_type_error(),
            "rows must be an array or respond to 'to_a'",
        ));
    };

    for row_value in rows_ary.into_iter() {
        if !row_value.is_kind_of(ruby.class_array()) {
            return Err(MagnusError::new(
                ruby.exception_type_error(),
                "each row must be an array",
            ));
        }
        let row_array: RArray = magnus::TryConvert::try_convert(row_value)?;
        let mut values = Vec::with_capacity(row_array.len());
        for (idx, item) in row_array.into_iter().enumerate() {
            let schema_hint = writer.field_schemas.get(idx);
            let pq_value = writer.converter.to_parquet_with_schema_hint(item, schema_hint).map_err(|e| {
                let msg = e.to_string();
                if msg.contains("EncodingError") || msg.contains("invalid utf-8") {
                    if let Some(pos) = msg.find("EncodingError: ") {
                        let encoding_msg = msg[pos + 15..].to_string();
                        MagnusError::new(ruby.exception_encoding_error(), encoding_msg)
                    } else {
                        MagnusError::new(ruby.exception_encoding_error(), msg)
                    }
                } else {
                    MagnusError::new(ruby.exception_runtime_error(), msg)
                }
            })?;
            values.push(pq_value);
        }

        // Estimate size
        let row_size = crate::utils::estimate_row_size(&values);
        writer.batch_manager.record_row_size(row_size);
        writer.batch_memory_size += row_size;
        writer.batch.push(values);

        if writer
            .batch_manager
            .should_flush(writer.batch.len(), writer.batch_memory_size)
        {
            // Flush to core and seal row group
            writer.write_batch_to_core()?;
            match &mut writer.writer_output {
                WriterOutput::File(w) | WriterOutput::TempFile(w, _, _) => {
                    w.flush()
                        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
                }
            }
        }
    }

    // Put back
    FW_REGISTRY.with(|reg| {
        reg.borrow_mut().insert(id, writer);
    });
    Ok(())
}

pub fn file_writer_flush_row_group(_ruby: &Ruby, id: u64) -> Result<(), MagnusError> {
    let mut_opt = FW_REGISTRY.with(|reg| reg.borrow_mut().remove(&id));
    let mut writer = if let Some(w) = mut_opt { w } else {
        return Err(MagnusError::new(
            magnus::exception::runtime_error(),
            format!("Invalid writer id {id}"),
        ));
    };
    if writer.closed {
        // Put back in case of duplicate calls
        FW_REGISTRY.with(|reg| {
            reg.borrow_mut().insert(id, writer);
        });
        return Err(MagnusError::new(magnus::exception::runtime_error(), "Writer is closed"));
    }

    writer.write_batch_to_core()?;
    match &mut writer.writer_output {
        WriterOutput::File(w) | WriterOutput::TempFile(w, _, _) => {
            w.flush()
                .map_err(|e| MagnusError::new(magnus::exception::runtime_error(), e.to_string()))?;
        }
    }

    // Put back
    FW_REGISTRY.with(|reg| {
        reg.borrow_mut().insert(id, writer);
    });
    Ok(())
}

pub fn file_writer_close(_ruby: &Ruby, id: u64) -> Result<(), MagnusError> {
    let mut writer = FW_REGISTRY
        .with(|reg| reg.borrow_mut().remove(&id))
        .ok_or_else(|| {
            MagnusError::new(
                magnus::exception::runtime_error(),
                format!("Invalid writer id {id}"),
            )
        })?;
    if writer.closed {
        return Ok(());
    }

    // Flush remaining
    writer.write_batch_to_core()?;

    // Finalize
    finalize_writer(writer.writer_output)?;
    writer.closed = true;
    Ok(())
}
