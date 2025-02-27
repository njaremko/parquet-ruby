use super::{
    build_column_collectors_from_dsl, copy_temp_file_to_io_like, create_writer,
    parse_parquet_write_args, DEFAULT_MEMORY_THRESHOLD, INITIAL_BATCH_SIZE, MIN_BATCH_SIZE,
    MIN_SAMPLES_FOR_ESTIMATE, SAMPLE_SIZE,
};
use crate::{
    logger::RubyLogger,
    types::{
        schema_node::build_arrow_schema, ColumnCollector, ParquetGemError, ParquetSchemaType,
        PrimitiveType, WriterOutput,
    },
    IoLikeValue, ParquetWriteArgs,
};
use arrow_array::{Array, RecordBatch};
use magnus::{
    value::ReprValue, Error as MagnusError, RArray, RHash, Ruby, Symbol, TryConvert, Value,
};
use rand::Rng;
use std::sync::Arc;

#[inline]
pub fn write_rows(args: &[Value]) -> Result<(), MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };
    write_rows_impl(Arc::new(ruby), args).map_err(|e| {
        let z: MagnusError = e.into();
        z
    })?;
    Ok(())
}

#[inline]
fn write_rows_impl(ruby: Arc<Ruby>, args: &[Value]) -> Result<(), ParquetGemError> {
    let ParquetWriteArgs {
        read_from,
        write_to,
        schema,
        batch_size: user_batch_size,
        compression,
        flush_threshold,
        sample_size: user_sample_size,
        logger,
    } = parse_parquet_write_args(&ruby, args)?;

    let logger = RubyLogger::new(&ruby, logger)?;
    let flush_threshold = flush_threshold.unwrap_or(DEFAULT_MEMORY_THRESHOLD);

    // Get the Arrow schema from the SchemaNode (we only have DSL schema now, since legacy is converted)
    let arrow_schema = build_arrow_schema(&schema, &logger).map_err(|e| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            format!("Failed to build Arrow schema from DSL schema: {}", e),
        )
    })?;

    // Create the writer
    let mut writer = create_writer(&ruby, &write_to, arrow_schema.clone(), compression)?;

    if read_from.is_kind_of(ruby.class_enumerator()) {
        // Build column collectors - we only have DSL schema now
        let mut column_collectors =
            build_column_collectors_from_dsl(&ruby, &arrow_schema, &schema)?;

        let mut rows_in_batch = 0;
        let mut total_rows = 0;
        let mut rng = rand::rng();
        let sample_size = user_sample_size.unwrap_or(SAMPLE_SIZE);
        let mut size_samples = Vec::with_capacity(sample_size);
        let mut current_batch_size = user_batch_size.unwrap_or(INITIAL_BATCH_SIZE);

        loop {
            match read_from.funcall::<_, _, Value>("next", ()) {
                Ok(row) => {
                    // Process the row
                    process_row(&ruby, row, &mut column_collectors)?;

                    // Update row sampling for dynamic batch sizing
                    if size_samples.len() < sample_size {
                        // estimate row size
                        let row_array = RArray::from_value(row).ok_or_else(|| {
                            MagnusError::new(ruby.exception_type_error(), "Row must be an array")
                        })?;
                        let row_size = estimate_single_row_size(&row_array, &column_collectors)?;
                        size_samples.push(row_size);
                    } else if rng.random_range(0..=total_rows) < sample_size as usize {
                        let idx = rng.random_range(0..sample_size as usize);
                        let row_array = RArray::from_value(row).ok_or_else(|| {
                            MagnusError::new(ruby.exception_type_error(), "Row must be an array")
                        })?;
                        let row_size = estimate_single_row_size(&row_array, &column_collectors)?;
                        size_samples[idx] = row_size;
                    }

                    rows_in_batch += 1;
                    total_rows += 1;

                    // Calculate batch size progressively once we have minimum samples
                    if user_batch_size.is_none() && size_samples.len() >= MIN_SAMPLES_FOR_ESTIMATE {
                        current_batch_size =
                            update_batch_size(&size_samples, flush_threshold, MIN_BATCH_SIZE);
                    }

                    // When we reach batch size, write the batch
                    if rows_in_batch >= current_batch_size {
                        write_batch(&mut writer, &mut column_collectors, flush_threshold)?;
                        rows_in_batch = 0;
                    }
                }
                Err(e) => {
                    if e.is_kind_of(ruby.exception_stop_iteration()) {
                        // Write any remaining rows
                        if rows_in_batch > 0 {
                            write_batch(&mut writer, &mut column_collectors, flush_threshold)?;
                        }
                        break;
                    }
                    return Err(e)?;
                }
            }
        }
    } else {
        return Err(MagnusError::new(
            magnus::exception::type_error(),
            "read_from must be an Enumerator".to_string(),
        ))?;
    }

    // Ensure everything is written and get the temp file if it exists
    if let Some(temp_file) = writer.close()? {
        // If we got a temp file back, we need to copy its contents to the IO-like object
        copy_temp_file_to_io_like(temp_file, IoLikeValue(write_to))?;
    }

    Ok(())
}

// Processes a single data row and adds values to the corresponding column collectors
// This function is called for each row of input data when writing in row-wise mode.
// It performs important validation to ensure the row structure matches the schema:
// - Verifies that the number of columns in the row matches the schema
// - Distributes each value to the appropriate ColumnCollector
//
// Each ColumnCollector handles type conversion and accumulation for its specific column,
// allowing this function to focus on row-level validation and distribution.
fn process_row(
    ruby: &Ruby,
    row: Value,
    column_collectors: &mut [ColumnCollector],
) -> Result<(), MagnusError> {
    let row_array = RArray::from_value(row)
        .ok_or_else(|| MagnusError::new(ruby.exception_type_error(), "Row must be an array"))?;

    // Validate row length matches schema
    if row_array.len() != column_collectors.len() {
        return Err(MagnusError::new(
            magnus::exception::runtime_error(),
            format!(
                "Row length ({}) does not match schema length ({}). Schema expects columns: {:?}",
                row_array.len(),
                column_collectors.len(),
                column_collectors
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
            ),
        ));
    }

    // Process each value in the row
    for (collector, value) in column_collectors.iter_mut().zip(row_array) {
        collector.push_value(value)?;
    }

    Ok(())
}

// Converts all accumulated data from ColumnCollectors into an Arrow RecordBatch
// and writes it to the Parquet file/output. This is a crucial function that bridges
// between our Ruby-oriented data collectors and the Arrow/Parquet ecosystem.
//
// The function:
// 1. Takes all collected values from each ColumnCollector and converts them to Arrow arrays
// 2. Creates a RecordBatch from these arrays (column-oriented data format)
// 3. Writes the batch to the ParquetWriter
// 4. Flushes the writer if the accumulated memory exceeds the threshold
//
// This approach enables efficient batch-wise writing while controlling memory usage.
fn write_batch(
    writer: &mut WriterOutput,
    collectors: &mut [ColumnCollector],
    flush_threshold: usize,
) -> Result<(), ParquetGemError> {
    // Convert columns to Arrow arrays
    let arrow_arrays: Vec<(String, Arc<dyn Array>)> = collectors
        .iter_mut()
        .map(|c| {
            let arr = c.take_array()?;
            Ok((c.name.clone(), arr))
        })
        .collect::<Result<_, ParquetGemError>>()?;

    let record_batch = RecordBatch::try_from_iter(arrow_arrays.clone()).map_err(|e| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            format!("Failed to create RecordBatch: {}", e),
        )
    })?;

    writer.write(&record_batch)?;

    // Check if we need to flush based on memory usage thresholds
    match writer {
        WriterOutput::File(w) | WriterOutput::TempFile(w, _) => {
            if w.in_progress_size() >= flush_threshold || w.memory_size() >= flush_threshold {
                w.flush()?;
            }
        }
    }
    Ok(())
}

// Estimates the memory size of a single row by examining each value
// This is used for dynamic batch sizing to optimize memory usage during writes
// by adapting batch sizes based on the actual data being processed.
pub fn estimate_single_row_size(
    row_array: &RArray,
    collectors: &[ColumnCollector],
) -> Result<usize, MagnusError> {
    let mut size = 0;
    for (idx, val) in row_array.into_iter().enumerate() {
        let col_type = &collectors[idx].type_;
        // Calculate size based on the type-specific estimation
        size += estimate_value_size(val, col_type)?;
    }
    Ok(size)
}

// Estimates the memory footprint of a single value based on its schema type
// This provides type-specific size estimates that help with dynamic batch sizing
// For complex types like lists, maps, and structs, we use reasonable approximations
pub fn estimate_value_size(
    value: Value,
    schema_type: &ParquetSchemaType,
) -> Result<usize, MagnusError> {
    use ParquetSchemaType as PST;
    if value.is_nil() {
        return Ok(0); // nil => minimal
    }
    match schema_type {
        PST::Primitive(PrimitiveType::Int8) | PST::Primitive(PrimitiveType::UInt8) => Ok(1),
        PST::Primitive(PrimitiveType::Int16) | PST::Primitive(PrimitiveType::UInt16) => Ok(2),
        PST::Primitive(PrimitiveType::Int32)
        | PST::Primitive(PrimitiveType::UInt32)
        | PST::Primitive(PrimitiveType::Float32) => Ok(4),
        PST::Primitive(PrimitiveType::Int64)
        | PST::Primitive(PrimitiveType::UInt64)
        | PST::Primitive(PrimitiveType::Float64) => Ok(8),
        PST::Primitive(PrimitiveType::Boolean) => Ok(1),
        PST::Primitive(PrimitiveType::Date32)
        | PST::Primitive(PrimitiveType::TimestampMillis)
        | PST::Primitive(PrimitiveType::TimestampMicros) => Ok(8),
        PST::Primitive(PrimitiveType::String) | PST::Primitive(PrimitiveType::Binary) => {
            if let Ok(s) = String::try_convert(value) {
                // Account for string length plus Rust String's capacity+pointer overhead
                Ok(s.len() + std::mem::size_of::<usize>() * 3)
            } else {
                // Try to convert the value to a string using to_s for non-string types
                // This handles numeric values that will be converted to strings later
                match value.funcall::<_, _, Value>("to_s", ()) {
                    Ok(str_val) => {
                        if let Ok(s) = String::try_convert(str_val) {
                            Ok(s.len() + std::mem::size_of::<usize>() * 3)
                        } else {
                            // If to_s conversion fails, just use a reasonable default
                            Ok(8) // Reasonable size estimate for small values
                        }
                    }
                    Err(_) => {
                        // If to_s method fails, use a default size
                        Ok(8) // Reasonable size estimate for small values
                    }
                }
            }
        }
        PST::List(item_type) => {
            if let Ok(arr) = RArray::try_convert(value) {
                let len = arr.len();

                // Base overhead for the array structure (pointer, length, capacity)
                let base_size = std::mem::size_of::<usize>() * 3;

                // If empty, just return the base size
                if len == 0 {
                    return Ok(base_size);
                }

                // Sample up to 5 elements to get average element size
                let sample_count = std::cmp::min(len, 5);
                let mut total_sample_size = 0;

                for i in 0..sample_count {
                    let element = arr.entry(i as isize)?;
                    let element_size = estimate_value_size(element, &item_type.item_type)?;
                    total_sample_size += element_size;
                }

                // If we couldn't sample any elements properly, that's an error
                if sample_count > 0 && total_sample_size == 0 {
                    return Err(MagnusError::new(
                        magnus::exception::runtime_error(),
                        "Failed to estimate size of list elements",
                    ));
                }

                // Calculate average element size from samples
                let avg_element_size = if sample_count > 0 {
                    total_sample_size as f64 / sample_count as f64
                } else {
                    return Err(MagnusError::new(
                        magnus::exception::runtime_error(),
                        "Failed to sample list elements for size estimation",
                    ));
                };

                // Estimate total size based on average element size * length + base overhead
                Ok(base_size + (avg_element_size as usize * len))
            } else {
                // Instead of assuming it's a small list, return an error
                Err(MagnusError::new(
                    magnus::exception::runtime_error(),
                    format!("Expected array for List type but got: {:?}", value),
                ))
            }
        }
        PST::Map(map_field) => {
            if let Ok(hash) = RHash::try_convert(value) {
                let size_estimate = hash.funcall::<_, _, usize>("size", ())?;

                // Base overhead for the hash structure
                let base_size = std::mem::size_of::<usize>() * 4;

                // If empty, just return the base size
                if size_estimate == 0 {
                    return Ok(base_size);
                }

                // Sample up to 5 key-value pairs to estimate average sizes
                let mut key_sample_size = 0;
                let mut value_sample_size = 0;
                let mut sample_count = 0;

                // Get an enumerator for the hash
                let enumerator = hash.funcall::<_, _, Value>("to_enum", ())?;

                // Sample up to 5 entries
                for _ in 0..std::cmp::min(size_estimate, 5) {
                    match enumerator.funcall::<_, _, Value>("next", ()) {
                        Ok(pair) => {
                            if let Ok(pair_array) = RArray::try_convert(pair) {
                                if pair_array.len() == 2 {
                                    let key = pair_array.entry(0)?;
                                    let val = pair_array.entry(1)?;

                                    key_sample_size +=
                                        estimate_value_size(key, &map_field.key_type)?;
                                    value_sample_size +=
                                        estimate_value_size(val, &map_field.value_type)?;
                                    sample_count += 1;
                                }
                            }
                        }
                        Err(_) => break, // Stop if we reach the end
                    }
                }

                // If we couldn't sample any pairs, return an error
                if size_estimate > 0 && sample_count == 0 {
                    return Err(MagnusError::new(
                        magnus::exception::runtime_error(),
                        "Failed to sample map entries for size estimation",
                    ));
                }

                // Calculate average key and value sizes
                let (avg_key_size, avg_value_size) = if sample_count > 0 {
                    (
                        key_sample_size as f64 / sample_count as f64,
                        value_sample_size as f64 / sample_count as f64,
                    )
                } else {
                    return Err(MagnusError::new(
                        magnus::exception::runtime_error(),
                        "Failed to sample hash key-value pairs for size estimation",
                    ));
                };

                // Each entry has overhead (node pointers, etc.) in a hash map
                let entry_overhead = std::mem::size_of::<usize>() * 2;

                // Estimate total size:
                // base size + (key_size + value_size + entry_overhead) * count
                Ok(base_size
                    + ((avg_key_size + avg_value_size + entry_overhead as f64) as usize
                        * size_estimate))
            } else {
                // Instead of assuming a small map, return an error
                Err(MagnusError::new(
                    magnus::exception::runtime_error(),
                    format!("Expected hash for Map type but got: {:?}", value),
                ))
            }
        }
        PST::Struct(struct_field) => {
            if let Ok(hash) = RHash::try_convert(value) {
                // Base overhead for the struct
                let base_size = std::mem::size_of::<usize>() * 3;

                // Estimate size for each field
                let mut total_fields_size = 0;

                for field in &struct_field.fields {
                    // Try to get the field value from the hash
                    match hash.get(Symbol::new(&field.name)) {
                        Some(field_value) => {
                            total_fields_size += estimate_value_size(field_value, &field.type_)?;
                        }
                        None => {
                            if let Some(field_value) = hash.get(&*field.name) {
                                total_fields_size +=
                                    estimate_value_size(field_value, &field.type_)?;
                            } else {
                                if field.nullable {
                                    total_fields_size += 0;
                                } else {
                                    return Err(MagnusError::new(
                                        magnus::exception::runtime_error(),
                                        format!("Missing field: {} in hash {:?}", field.name, hash),
                                    ));
                                }
                            }
                        }
                    }
                }

                // We no longer error on missing fields during size estimation
                Ok(base_size + total_fields_size)
            } else {
                // Instead of trying instance_variables or assuming a default, return an error
                Err(MagnusError::new(
                    magnus::exception::runtime_error(),
                    format!("Expected hash for Struct type but got: {:?}", value),
                ))
            }
        }
    }
}

// Dynamically calculates an optimal batch size based on estimated row sizes
// and memory constraints. This function enables the writer to adapt to different
// data characteristics for optimal performance.
//
// The algorithm:
// 1. Requires a minimum number of samples to make a reliable estimate
// 2. Calculates the average row size from the samples
// 3. Determines a batch size that would consume approximately the target memory threshold
// 4. Ensures the batch size doesn't go below a minimum value for efficiency
//
// This approach balances memory usage with processing efficiency by targeting
// a specific memory footprint per batch.
fn update_batch_size(
    size_samples: &[usize],
    flush_threshold: usize,
    min_batch_size: usize,
) -> usize {
    if size_samples.len() < MIN_SAMPLES_FOR_ESTIMATE {
        return min_batch_size;
    }

    let total_size = size_samples.iter().sum::<usize>();
    // Safe because we know we have at least MIN_SAMPLES_FOR_ESTIMATE samples
    let avg_row_size = total_size as f64 / size_samples.len() as f64;
    let avg_row_size = avg_row_size.max(1.0); // Ensure we don't divide by zero
    let suggested_batch_size = (flush_threshold as f64 / avg_row_size).floor() as usize;
    suggested_batch_size.max(min_batch_size)
}
