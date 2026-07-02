use magnus::value::ReprValue;
use magnus::{Enumerator, Error as MagnusError, RArray, Ruby, TryConvert, Value};
use parquet_core::writer::WriterBuilder;
use parquet_core::Schema;
use std::io::{BufReader, BufWriter, Write};
use tempfile::NamedTempFile;

use crate::io::RubyIOWriter;
use crate::types::WriterOutput;
use crate::utils::parse_compression;

/// Rows pulled per slice when streaming rows from an Enumerable without a
/// user-provided batch_size. Mirrors the core writer's default batch size so
/// the Ruby-side slice and the core row buffer stay in the same range.
const DEFAULT_ROW_SLICE_SIZE: usize = 1000;

/// How the writer batches rows before flushing. All batch sizing is owned by the
/// core `Writer`; the adapter only forwards the user's options.
#[derive(Debug, Default, Clone, Copy)]
pub struct BatchSizingOptions {
    pub batch_size: Option<usize>,
    pub flush_threshold: Option<usize>,
    pub sample_size: Option<usize>,
}

/// Create a writer based on the output type (file path or IO object), forwarding
/// the batch-sizing options to the core writer (the single source of truth).
pub fn create_writer(
    ruby: &Ruby,
    write_to: Value,
    schema: Schema,
    compression: Option<String>,
    options: BatchSizingOptions,
) -> Result<WriterOutput, MagnusError> {
    let mut builder = WriterBuilder::new().with_compression(parse_compression(ruby, compression)?);
    if let Some(size) = options.batch_size {
        builder = builder.with_batch_size(size);
    }
    if let Some(threshold) = options.flush_threshold {
        builder = builder.with_memory_threshold(threshold);
    }
    if let Some(size) = options.sample_size {
        builder = builder.with_sample_size(size);
    }

    if write_to.is_kind_of(ruby.class_string()) {
        // Direct file path
        let path_str: String = TryConvert::try_convert(write_to)?;
        let file = std::fs::File::create(&path_str)
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
        let writer = builder
            .build(file, schema)
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

        let writer = builder
            .build(file, schema)
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

        Ok(WriterOutput::TempFile(writer, temp_file, write_to))
    }
}

/// Finalize the writer and copy temp file to IO if needed
pub fn finalize_writer(ruby: &Ruby, writer_output: WriterOutput) -> Result<(), MagnusError> {
    match writer_output {
        WriterOutput::File(writer) => writer
            .close()
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string())),
        WriterOutput::TempFile(writer, temp_file, io_object) => {
            // Close the writer first
            writer
                .close()
                .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

            // Copy temp file to IO object
            copy_temp_file_to_io(ruby, temp_file, io_object)
        }
    }
}

/// Copy temporary file contents to Ruby IO object
fn copy_temp_file_to_io(
    ruby: &Ruby,
    temp_file: NamedTempFile,
    io_object: Value,
) -> Result<(), MagnusError> {
    let file = temp_file.reopen().map_err(|e| {
        MagnusError::new(
            ruby.exception_runtime_error(),
            format!("Failed to reopen temporary file: {}", e),
        )
    })?;

    let mut buf_reader = BufReader::new(file);
    let ruby_io_writer = RubyIOWriter::new(io_object);
    let mut buf_writer = BufWriter::new(ruby_io_writer);

    std::io::copy(&mut buf_reader, &mut buf_writer).map_err(|e| {
        MagnusError::new(
            ruby.exception_runtime_error(),
            format!("Failed to copy temp file to IO object: {}", e),
        )
    })?;

    buf_writer.flush().map_err(|e| {
        MagnusError::new(
            ruby.exception_runtime_error(),
            format!("Failed to flush IO object: {}", e),
        )
    })?;

    // The temporary file will be automatically deleted when temp_file is dropped
    Ok(())
}

/// The rows to write, either already materialized or pulled lazily from an
/// Enumerable in bounded slices so the whole input is never resident at once.
enum RowSource {
    Materialized(RArray),
    Streamed {
        first_slice: Option<RArray>,
        remaining: Enumerator,
    },
}

/// Classify `read_from` without draining it. Arrays are used as-is; anything
/// else that supports `each_slice` (Enumerator, any Enumerable) is streamed in
/// bounded slices; objects that only support `to_a` keep the legacy
/// materialize-first behavior.
fn row_source(
    ruby: &Ruby,
    read_from: Value,
    batch_size: Option<usize>,
) -> Result<RowSource, MagnusError> {
    if read_from.is_kind_of(ruby.class_array()) {
        return Ok(RowSource::Materialized(TryConvert::try_convert(read_from)?));
    }

    if read_from.respond_to("each_slice", false)? {
        // One fiber switch per slice keeps Enumerator overhead negligible
        // while bounding how many Ruby rows are in flight at once.
        let slice_size = batch_size.unwrap_or(DEFAULT_ROW_SLICE_SIZE);
        let mut remaining = read_from.enumeratorize("each_slice", (slice_size,));
        let first_slice = match remaining.next() {
            Some(slice) => Some(TryConvert::try_convert(slice?)?),
            None => None,
        };
        return Ok(RowSource::Streamed {
            first_slice,
            remaining,
        });
    }

    if read_from.respond_to("to_a", false)? {
        let array_value: Value = read_from.funcall("to_a", ())?;
        return Ok(RowSource::Materialized(TryConvert::try_convert(
            array_value,
        )?));
    }

    Err(MagnusError::new(
        ruby.exception_type_error(),
        "data must be an array or respond to 'to_a'",
    ))
}

/// Map a value-conversion failure to the Ruby exception the write API raises:
/// encoding problems surface as EncodingError, everything else RuntimeError.
fn conversion_error(ruby: &Ruby, error_msg: String) -> MagnusError {
    if error_msg.contains("EncodingError") || error_msg.contains("invalid utf-8") {
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
}

/// Convert one slice of Ruby rows and write them through the core writer.
/// Returns the number of rows written.
///
/// `release_consumed` should be true only when `rows` is an array this module
/// created itself (an `each_slice` slice): each element is dropped from the
/// slice once converted, so large rows become collectable while the rest of
/// the slice is still being written. User-provided arrays must not be mutated.
fn write_row_slice(
    ruby: &Ruby,
    writer_output: &mut WriterOutput,
    converter: &mut crate::converter::RubyValueConverter,
    field_schemas: &[parquet_core::SchemaNode],
    rows: RArray,
    release_consumed: bool,
) -> Result<u64, MagnusError> {
    let mut rows_written = 0u64;

    for (row_idx, row_value) in rows.into_iter().enumerate() {
        // Convert Ruby row to ParquetValue vector
        let row = if row_value.is_kind_of(ruby.class_array()) {
            let array: RArray = TryConvert::try_convert(row_value)?;
            let mut values = Vec::with_capacity(array.len());

            for (idx, item) in array.into_iter().enumerate() {
                let schema_hint = field_schemas.get(idx);
                let pq_value = converter
                    .to_parquet_with_schema_hint(item, schema_hint)
                    .map_err(|e| conversion_error(ruby, e.to_string()))?;
                values.push(pq_value);
            }
            values
        } else {
            return Err(MagnusError::new(
                ruby.exception_type_error(),
                "each row must be an array",
            ));
        };

        if release_consumed {
            rows.store(row_idx as isize, ruby.qnil())?;
        }

        match writer_output {
            WriterOutput::File(writer) | WriterOutput::TempFile(writer, _, _) => {
                writer
                    .write_row(row)
                    .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
            }
        }
        rows_written += 1;
    }

    Ok(rows_written)
}

/// Write data in row format to a parquet file
pub fn write_rows(
    ruby: &Ruby,
    write_args: crate::types::ParquetWriteArgs,
) -> Result<Value, MagnusError> {
    use crate::converter::RubyValueConverter;
    use crate::logger::RubyLogger;
    use crate::schema::{extract_field_schemas, process_schema_value, ruby_schema_to_parquet};
    use crate::string_cache::StringCache;

    // Read rows lazily where possible: draining an Enumerator up front would
    // hold the entire dataset in memory for the duration of the write.
    let source = row_source(ruby, write_args.read_from, write_args.batch_size)?;

    // Schema inference (used when `schema` is nil/empty) only inspects the
    // first row, so the first slice is enough.
    let empty_rows;
    let inference_rows = match &source {
        RowSource::Materialized(rows) => rows,
        RowSource::Streamed {
            first_slice: Some(rows),
            ..
        } => rows,
        RowSource::Streamed {
            first_slice: None, ..
        } => {
            empty_rows = ruby.ary_new();
            &empty_rows
        }
    };

    // Process schema value
    let schema_hash = process_schema_value(ruby, write_args.schema_value, Some(inference_rows))
        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

    // Create schema
    let schema = ruby_schema_to_parquet(schema_hash)
        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

    // Extract field schemas for conversion hints
    let field_schemas = extract_field_schemas(&schema);

    // Create writer. All batch sizing and flushing is owned by the core writer;
    // the user's batch_size/flush_threshold/sample_size are forwarded to it.
    let mut writer_output = create_writer(
        ruby,
        write_args.write_to,
        schema.clone(),
        write_args.compression,
        BatchSizingOptions {
            batch_size: write_args.batch_size,
            flush_threshold: write_args.flush_threshold,
            sample_size: write_args.sample_size,
        },
    )?;

    // Create logger
    let logger = RubyLogger::new(write_args.logger)?;
    let _ = logger.info(|| "Starting to write parquet file".to_string());

    // Create converter with string cache if enabled. `string_cache` is the
    // requested capacity (None = disabled).
    let mut converter = if let Some(capacity) = write_args.string_cache {
        let _ = logger.debug(|| format!("String cache enabled (capacity {})", capacity));
        RubyValueConverter::with_string_cache(StringCache::new(capacity))
    } else {
        RubyValueConverter::new()
    };

    // Stream each row to the core writer, which buffers and flushes internally
    // according to its (now sole) batch-sizing policy. Completed row groups
    // reach the destination while later rows are still being produced.
    let mut total_rows = 0u64;

    match source {
        RowSource::Materialized(rows) => {
            total_rows += write_row_slice(
                ruby,
                &mut writer_output,
                &mut converter,
                &field_schemas,
                rows,
                false,
            )?;
        }
        RowSource::Streamed {
            first_slice,
            remaining,
        } => {
            if let Some(rows) = first_slice {
                total_rows += write_row_slice(
                    ruby,
                    &mut writer_output,
                    &mut converter,
                    &field_schemas,
                    rows,
                    true,
                )?;
            }
            for slice in remaining {
                let rows: RArray = TryConvert::try_convert(slice?)?;
                total_rows += write_row_slice(
                    ruby,
                    &mut writer_output,
                    &mut converter,
                    &field_schemas,
                    rows,
                    true,
                )?;
            }
        }
    }

    // The core writer flushes any remaining buffered rows when closed by
    // finalize_writer below.
    let _ = logger.info(|| format!("Finished writing {} rows to parquet file", total_rows));

    // Log string cache statistics if enabled. `misses` is exact even after the
    // bounded cache fills; exact distinct cardinality would require an unbounded
    // side table, so the log labels it as misses rather than unique strings.
    if let Some(stats) = converter.string_cache_stats() {
        let _ = logger.info(|| {
            format!(
                "String cache stats: {} cache misses, {} hits ({:.1}% hit rate)",
                stats.misses,
                stats.hits,
                stats.hit_rate * 100.0
            )
        });
    }

    // Finalize the writer
    finalize_writer(ruby, writer_output)?;

    Ok(ruby.qnil().as_value())
}

/// The column batches to write, either already materialized or pulled lazily
/// from an Enumerable one batch at a time.
enum BatchSource {
    Materialized(RArray),
    Streamed {
        first_batch: Option<Value>,
        remaining: Enumerator,
    },
}

/// Classify `read_from` without draining it, mirroring `row_source`. Batches
/// are chunky (whole columns), so streamed sources are pulled one batch per
/// fiber switch via `each`.
fn batch_source(ruby: &Ruby, read_from: Value) -> Result<BatchSource, MagnusError> {
    if read_from.is_kind_of(ruby.class_array()) {
        return Ok(BatchSource::Materialized(TryConvert::try_convert(
            read_from,
        )?));
    }

    if read_from.respond_to("each", false)? {
        let mut remaining = read_from.enumeratorize("each", ());
        let first_batch = remaining.next().transpose()?;
        return Ok(BatchSource::Streamed {
            first_batch,
            remaining,
        });
    }

    if read_from.respond_to("to_a", false)? {
        let array_value: Value = read_from.funcall("to_a", ())?;
        return Ok(BatchSource::Materialized(TryConvert::try_convert(
            array_value,
        )?));
    }

    Err(MagnusError::new(
        ruby.exception_type_error(),
        "data must be an array or respond to 'to_a'",
    ))
}

/// Convert one Ruby batch (an array of per-column value arrays) and write it
/// through the core writer as a record batch. Returns the batch's row count.
fn write_column_batch(
    ruby: &Ruby,
    writer_output: &mut WriterOutput,
    field_schemas: &[parquet_core::SchemaNode],
    column_names: &[String],
    batch: Value,
) -> Result<usize, MagnusError> {
    use crate::converter::RubyValueConverter;

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

    let mut batch_columns: Vec<(String, Vec<parquet_core::ParquetValue>)> =
        Vec::with_capacity(column_names.len());

    // Process each column in the batch
    for (col_idx, column_values) in batch_array.into_iter().enumerate() {
        if !column_values.is_kind_of(ruby.class_array()) {
            return Err(MagnusError::new(
                ruby.exception_type_error(),
                format!("Column {} values must be an array", col_idx),
            ));
        }

        let values_array: RArray = TryConvert::try_convert(column_values)?;

        // Convert and append values
        let mut converter = RubyValueConverter::new();
        let schema_hint = field_schemas.get(col_idx);

        let mut values = Vec::with_capacity(values_array.len());
        for value in values_array.into_iter() {
            let pq_value = converter
                .to_parquet_with_schema_hint(value, schema_hint)
                .map_err(|e| conversion_error(ruby, e.to_string()))?;
            values.push(pq_value);
        }
        batch_columns.push((column_names[col_idx].clone(), values));
    }

    let batch_rows = batch_columns
        .first()
        .map(|(_name, values)| values.len())
        .unwrap_or(0);

    // Write this batch immediately; the core writer flushes completed row
    // groups to the destination once its in-progress buffer exceeds the
    // flush threshold.
    match writer_output {
        WriterOutput::File(writer) | WriterOutput::TempFile(writer, _, _) => {
            writer
                .write_columns(batch_columns)
                .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
        }
    }

    Ok(batch_rows)
}

/// Write data in column format to a parquet file
pub fn write_columns(
    ruby: &Ruby,
    write_args: crate::types::ParquetWriteArgs,
) -> Result<Value, MagnusError> {
    use crate::logger::RubyLogger;
    use crate::schema::{extract_field_schemas, process_schema_value, ruby_schema_to_parquet};

    let logger = RubyLogger::new(write_args.logger)?;

    // Read batches lazily where possible: draining an Enumerator up front
    // would hold every batch in memory for the duration of the write.
    let source = batch_source(ruby, write_args.read_from)?;

    // Schema inference (used when `schema` is nil/empty) only inspects the
    // first batch, so one batch is enough.
    let first_batch_holder;
    let inference_batches = match &source {
        BatchSource::Materialized(batches) => batches,
        BatchSource::Streamed { first_batch, .. } => {
            first_batch_holder = ruby.ary_new();
            if let Some(batch) = first_batch {
                first_batch_holder.push(*batch)?;
            }
            &first_batch_holder
        }
    };

    // Process schema value
    let schema_hash = process_schema_value(ruby, write_args.schema_value, Some(inference_batches))
        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

    // Create schema
    let schema = ruby_schema_to_parquet(schema_hash)
        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

    // Extract field schemas for conversion hints
    let field_schemas = extract_field_schemas(&schema);

    // Create writer. The columnar path writes one record batch per write_columns
    // call, so row batch-sizing options are rejected before this point.
    let mut writer_output = create_writer(
        ruby,
        write_args.write_to,
        schema.clone(),
        write_args.compression,
        BatchSizingOptions {
            batch_size: None,
            flush_threshold: write_args.flush_threshold,
            sample_size: None,
        },
    )?;
    let _ = logger.info(|| "Starting to write parquet file columns".to_string());

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

    // Convert and write each batch as it arrives; completed row groups are
    // flushed to the destination instead of accumulating every batch first.
    let mut total_rows: usize = 0;

    match source {
        BatchSource::Materialized(batches) => {
            for batch in batches.into_iter() {
                total_rows += write_column_batch(
                    ruby,
                    &mut writer_output,
                    &field_schemas,
                    &column_names,
                    batch,
                )?;
            }
        }
        BatchSource::Streamed {
            first_batch,
            remaining,
        } => {
            if let Some(batch) = first_batch {
                total_rows += write_column_batch(
                    ruby,
                    &mut writer_output,
                    &field_schemas,
                    &column_names,
                    batch,
                )?;
            }
            for batch in remaining {
                total_rows += write_column_batch(
                    ruby,
                    &mut writer_output,
                    &field_schemas,
                    &column_names,
                    batch?,
                )?;
            }
        }
    }

    let _ = logger.info(|| format!("Finished writing {total_rows} rows to parquet file columns"));

    // Finalize the writer
    finalize_writer(ruby, writer_output)?;

    Ok(ruby.qnil().as_value())
}
