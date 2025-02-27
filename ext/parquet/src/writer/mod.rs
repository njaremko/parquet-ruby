use std::{
    fs::File,
    io::{self, BufReader, BufWriter},
    sync::Arc,
};

use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Schema, TimeUnit};
use itertools::Itertools;
use magnus::{
    scan_args::{get_kwargs, scan_args},
    value::ReprValue,
    Error as MagnusError, RArray, RHash, Ruby, Symbol, TryConvert, Value,
};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use rand::Rng;
use tempfile::NamedTempFile;

use crate::{
    convert_ruby_array_to_arrow,
    logger::RubyLogger,
    reader::ReaderError,
    types::{
        schema_node::build_arrow_schema, // ADDED - we need to reference the DSL's build_arrow_schema
        ColumnCollector,
        ParquetSchemaType,
        WriterOutput,
    },
    utils::parse_string_or_symbol,
    IoLikeValue, ParquetSchemaType as PST, ParquetWriteArgs, SchemaField, SendableWrite,
};
use crate::{types::PrimitiveType, SchemaNode}; // ADDED - ensure we import SchemaNode

const MIN_SAMPLES_FOR_ESTIMATE: usize = 10;
const SAMPLE_SIZE: usize = 100;
const MIN_BATCH_SIZE: usize = 10;
const INITIAL_BATCH_SIZE: usize = 100;
const DEFAULT_MEMORY_THRESHOLD: usize = 64 * 1024 * 1024;

// -----------------------------------------------------------------------------
// HELPER to invert arrow DataType back to our ParquetSchemaType
// Converts Arrow DataType to our internal ParquetSchemaType representation.
// This is essential for mapping Arrow types back to our schema representation
// when working with column collections and schema validation.
// -----------------------------------------------------------------------------
fn arrow_data_type_to_parquet_schema_type(dt: &DataType) -> Result<ParquetSchemaType, MagnusError> {
    match dt {
        DataType::Boolean => Ok(PST::Boolean),
        DataType::Int8 => Ok(PST::Int8),
        DataType::Int16 => Ok(PST::Int16),
        DataType::Int32 => Ok(PST::Int32),
        DataType::Int64 => Ok(PST::Int64),
        DataType::UInt8 => Ok(PST::UInt8),
        DataType::UInt16 => Ok(PST::UInt16),
        DataType::UInt32 => Ok(PST::UInt32),
        DataType::UInt64 => Ok(PST::UInt64),
        DataType::Float16 => {
            // We do not have a direct ParquetSchemaType::Float16, we treat it as Float
            Ok(PST::Float)
        }
        DataType::Float32 => Ok(PST::Float),
        DataType::Float64 => Ok(PST::Double),
        DataType::Date32 => Ok(PST::Date32),
        DataType::Date64 => {
            // Our code typically uses Date32 or Timestamp for 64. But Arrow has Date64
            // We can store it as PST::Date64 if we want. If we don't have that, consider PST::Date32 or an error.
            // If your existing code only handles Date32, you can error. But let's do PST::Date32 as fallback:
            // Or define a new variant if you have one in your code. We'll show a fallback approach:
            Err(MagnusError::new(
                magnus::exception::runtime_error(),
                "Arrow Date64 not directly supported in current ParquetSchemaType (use date32?).",
            ))
        }
        DataType::Timestamp(TimeUnit::Second, _tz) => {
            // We'll treat this as PST::TimestampMillis, or define PST::TimestampSecond
            // For simplicity, let's map "second" to PST::TimestampMillis with a note:
            Ok(PST::TimestampMillis)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _tz) => Ok(PST::TimestampMillis),
        DataType::Timestamp(TimeUnit::Microsecond, _tz) => Ok(PST::TimestampMicros),
        DataType::Timestamp(TimeUnit::Nanosecond, _tz) => {
            // If you have a PST::TimestampNanos variant, use it. Otherwise, degrade to micros
            // for demonstration:
            Err(MagnusError::new(
                magnus::exception::runtime_error(),
                "TimestampNanos not supported, please adjust your schema or code.",
            ))
        }
        DataType::Utf8 => Ok(PST::String),
        DataType::Binary => Ok(PST::Binary),
        DataType::LargeUtf8 => {
            // If not supported, degrade or error. We'll degrade to PST::String
            Ok(PST::String)
        }
        DataType::LargeBinary => Ok(PST::Binary),
        DataType::List(child_field) => {
            // Recursively handle the item type
            let child_type = arrow_data_type_to_parquet_schema_type(child_field.data_type())?;
            Ok(PST::List(Box::new(crate::types::ListField {
                item_type: child_type,
                format: None,
                nullable: true,
            })))
        }
        DataType::Map(entry_field, _keys_sorted) => {
            // Arrow's Map -> a struct<key, value> inside
            let entry_type = entry_field.data_type();
            if let DataType::Struct(fields) = entry_type {
                if fields.len() == 2 {
                    let key_type = arrow_data_type_to_parquet_schema_type(fields[0].data_type())?;
                    let value_type = arrow_data_type_to_parquet_schema_type(fields[1].data_type())?;
                    Ok(PST::Map(Box::new(crate::types::MapField {
                        key_type,
                        value_type,
                        key_format: None,
                        value_format: None,
                        value_nullable: true,
                    })))
                } else {
                    Err(MagnusError::new(
                        magnus::exception::type_error(),
                        "Map field must have exactly 2 child fields (key, value)",
                    ))
                }
            } else {
                Err(MagnusError::new(
                    magnus::exception::type_error(),
                    "Map field is not a struct? Unexpected Arrow schema layout",
                ))
            }
        }
        DataType::Struct(arrow_fields) => {
            // We treat this as PST::Struct. We'll recursively handle subfields
            // but for top-level collecting we only store them as one column
            // so the user data must pass a Ruby Hash or something for that field.
            let mut schema_fields = vec![];
            for f in arrow_fields {
                let sub_type = arrow_data_type_to_parquet_schema_type(f.data_type())?;
                schema_fields.push(SchemaField {
                    name: f.name().clone(),
                    type_: sub_type,
                    format: None, // We can't see the 'format' from Arrow
                    nullable: f.is_nullable(),
                });
            }
            Ok(PST::Struct(Box::new(crate::types::StructField {
                fields: schema_fields,
            })))
        }
        _ => Err(MagnusError::new(
            magnus::exception::runtime_error(),
            format!("Unsupported or unhandled Arrow DataType: {:?}", dt),
        )),
    }
}

// -----------------------------------------------------------------------------
// HELPER to build ColumnCollectors for the DSL variant
// This function converts a SchemaNode (from our DSL) into a collection of ColumnCollectors
// that can accumulate values for each column in the schema.
// - arrow_schema: The Arrow schema corresponding to our DSL schema
// - root_node: The root SchemaNode (expected to be a Struct node) from which to build collectors
// -----------------------------------------------------------------------------
fn build_column_collectors_from_dsl<'a>(
    ruby: &'a Ruby,
    arrow_schema: &'a Arc<Schema>,
    root_node: &'a SchemaNode,
) -> Result<Vec<ColumnCollector<'a>>, MagnusError> {
    // We expect the top-level schema node to be a Struct so that arrow_schema
    // lines up with root_node.fields. If the user gave a top-level primitive, it would be 1 field, but
    // our code calls build_arrow_schema under the assumption "top-level must be Struct."
    let fields = match root_node {
        SchemaNode::Struct { fields, .. } => fields,
        _ => {
            return Err(MagnusError::new(
                ruby.exception_runtime_error(),
                "Top-level schema for DSL must be a struct",
            ))
        }
    };

    if fields.len() != arrow_schema.fields().len() {
        return Err(MagnusError::new(
            ruby.exception_runtime_error(),
            format!(
                "Mismatch between DSL field count ({}) and Arrow fields ({})",
                fields.len(),
                arrow_schema.fields().len()
            ),
        ));
    }

    let mut collectors = Vec::with_capacity(fields.len());
    for (arrow_field, schema_field_node) in arrow_schema.fields().iter().zip(fields) {
        let name = arrow_field.name().clone();
        let parquet_type = arrow_data_type_to_parquet_schema_type(arrow_field.data_type())?;

        // Extract the optional format from the schema node
        let format = extract_format_from_schema_node(schema_field_node);

        // Build the ColumnCollector
        collectors.push(ColumnCollector::new(
            name,
            parquet_type,
            format,
            arrow_field.is_nullable(),
        ));
    }
    Ok(collectors)
}

// Helper to extract the format from a SchemaNode if available
fn extract_format_from_schema_node(node: &SchemaNode) -> Option<String> {
    match node {
        SchemaNode::Primitive {
            format: f,
            parquet_type: _,
            ..
        } => f.clone(),
        // For struct, list, map, etc. there's no single "format." We ignore it.
        _ => None,
    }
}

/// Parse arguments for Parquet writing
pub fn parse_parquet_write_args(args: &[Value]) -> Result<ParquetWriteArgs, MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };
    let parsed_args = scan_args::<(Value,), (), (), (), _, ()>(args)?;
    let (read_from,) = parsed_args.required;

    let kwargs = get_kwargs::<
        _,
        (Value, Value),
        (
            Option<Option<usize>>,
            Option<Option<usize>>,
            Option<Option<String>>,
            Option<Option<usize>>,
            Option<Option<Value>>,
        ),
        (),
    >(
        parsed_args.keywords,
        &["schema", "write_to"],
        &[
            "batch_size",
            "flush_threshold",
            "compression",
            "sample_size",
            "logger",
        ],
    )?;

    // The schema value could be one of:
    // 1. An array of hashes (legacy format)
    // 2. A hash with type: :struct (new DSL format)
    // 3. nil (infer from data)
    let schema_value = kwargs.required.0;

    // Check if it's the new DSL format (a hash with type: :struct)
    // We need to handle both direct hash objects and objects created via Parquet::Schema.define

    // First, try to convert it to a Hash if it's not already a Hash
    // This handles the case where schema_value is a Schema object from Parquet::Schema.define
    let schema_hash = if schema_value.is_kind_of(ruby.class_hash()) {
        RHash::from_value(schema_value).ok_or_else(|| {
            MagnusError::new(magnus::exception::type_error(), "Schema must be a hash")
        })?
    } else {
        // Try to convert the object to a hash with to_h
        match schema_value.respond_to("to_h", false) {
            Ok(true) => {
                match schema_value.funcall::<_, _, Value>("to_h", ()) {
                    Ok(hash_val) => match RHash::from_value(hash_val) {
                        Some(hash) => hash,
                        None => {
                            // Not a hash, continue to normal handling
                            RHash::new()
                        }
                    },
                    Err(_) => {
                        // couldn't call to_h, continue to normal handling
                        RHash::new()
                    }
                }
            }
            _ => {
                // Doesn't respond to to_h, continue to normal handling
                RHash::new()
            }
        }
    };

    // Now check if it's a schema hash with a type: :struct field
    let type_val = schema_hash.get(Symbol::new("type"));

    if let Some(type_val) = type_val {
        // If it has a type: :struct, it's the new DSL format
        // Use parse_string_or_symbol to handle both String and Symbol values
        let ttype = parse_string_or_symbol(&ruby, type_val)?;
        if let Some(ref type_str) = ttype {
            if type_str == "struct" {
                // Parse using the new schema approach
                let schema_node = crate::parse_schema_node(&ruby, schema_value)?;

                validate_schema_node(&ruby, &schema_node)?;

                return Ok(ParquetWriteArgs {
                    read_from,
                    write_to: kwargs.required.1,
                    schema: schema_node,
                    batch_size: kwargs.optional.0.flatten(),
                    flush_threshold: kwargs.optional.1.flatten(),
                    compression: kwargs.optional.2.flatten(),
                    sample_size: kwargs.optional.3.flatten(),
                    logger: kwargs.optional.4.flatten(),
                });
            }
        }
    }

    // If it's not a hash with type: :struct, handle as legacy format
    let schema_fields = if schema_value.is_nil()
        || (schema_value.is_kind_of(ruby.class_array())
            && RArray::from_value(schema_value)
                .ok_or_else(|| {
                    MagnusError::new(
                        magnus::exception::type_error(),
                        "Schema fields must be an array",
                    )
                })?
                .len()
                == 0)
    {
        // If schema is nil or an empty array, we need to peek at the first value to determine column count
        let first_value = read_from.funcall::<_, _, Value>("peek", ())?;
        // Default to nullable:true for auto-inferred fields
        crate::infer_schema_from_first_row(&ruby, first_value, true)?
    } else {
        // Legacy array format - use our centralized parser
        crate::parse_legacy_schema(&ruby, schema_value)?
    };

    // Convert the legacy schema fields to SchemaNode (DSL format)
    let schema_node = crate::legacy_schema_to_dsl(&ruby, schema_fields)?;

    validate_schema_node(&ruby, &schema_node)?;

    Ok(ParquetWriteArgs {
        read_from,
        write_to: kwargs.required.1,
        schema: schema_node,
        batch_size: kwargs.optional.0.flatten(),
        flush_threshold: kwargs.optional.1.flatten(),
        compression: kwargs.optional.2.flatten(),
        sample_size: kwargs.optional.3.flatten(),
        logger: kwargs.optional.4.flatten(),
    })
}

// Validates a SchemaNode to ensure it meets Parquet schema requirements
// Currently checks for duplicate field names at the root level, which would
// cause problems when writing Parquet files. Additional validation rules
// could be added here in the future.
//
// This validation is important because schema errors are difficult to debug
// once they reach the Parquet/Arrow layer, so we check proactively before
// any data processing begins.
fn validate_schema_node(ruby: &Ruby, schema_node: &SchemaNode) -> Result<(), MagnusError> {
    if let SchemaNode::Struct { fields, .. } = &schema_node {
        // if any root level schema fields have the same name, we raise an error
        let field_names = fields
            .iter()
            .map(|f| match f {
                SchemaNode::Struct { name, .. } => name.as_str(),
                SchemaNode::List { name, .. } => name.as_str(),
                SchemaNode::Map { name, .. } => name.as_str(),
                SchemaNode::Primitive { name, .. } => name.as_str(),
            })
            .collect::<Vec<_>>();
        let unique_field_names = field_names.iter().unique().collect::<Vec<_>>();
        if field_names.len() != unique_field_names.len() {
            return Err(MagnusError::new(
                ruby.exception_arg_error(),
                format!(
                    "Duplicate field names in root level schema: {:?}",
                    field_names
                ),
            ));
        }
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

#[inline]
pub fn write_rows(args: &[Value]) -> Result<(), MagnusError> {
    write_rows_impl(args).map_err(|e| {
        let z: MagnusError = e.into();
        z
    })?;
    Ok(())
}

#[inline]
fn write_rows_impl(args: &[Value]) -> Result<(), ReaderError> {
    let ruby = unsafe { Ruby::get_unchecked() };

    let ParquetWriteArgs {
        read_from,
        write_to,
        schema,
        batch_size: user_batch_size,
        compression,
        flush_threshold,
        sample_size: user_sample_size,
        logger,
    } = parse_parquet_write_args(args)?;

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

#[inline]
pub fn write_columns(args: &[Value]) -> Result<(), MagnusError> {
    write_columns_impl(args).map_err(|e| {
        let z: MagnusError = e.into();
        z
    })?;
    Ok(())
}

#[inline]
fn write_columns_impl(args: &[Value]) -> Result<(), ReaderError> {
    let ruby = unsafe { Ruby::get_unchecked() };

    let ParquetWriteArgs {
        read_from,
        write_to,
        schema,
        batch_size: _,
        compression,
        flush_threshold,
        sample_size: _,
        logger,
    } = parse_parquet_write_args(args)?;

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
        loop {
            match read_from.funcall::<_, _, Value>("next", ()) {
                Ok(batch) => {
                    let batch_array = RArray::from_value(batch).ok_or_else(|| {
                        MagnusError::new(ruby.exception_type_error(), "Batch must be an array")
                    })?;

                    // Batch array must be an array of arrays. Check that the first value in `batch_array` is an array.
                    batch_array.entry::<RArray>(0).map_err(|_| {
                        MagnusError::new(
                            ruby.exception_type_error(),
                            "When writing columns, data must be formatted as batches of columns: [[batch1_col1, batch1_col2], [batch2_col1, batch2_col2]].",
                        )
                    })?;

                    // Validate batch length matches schema
                    // Get schema length and field names - we only have DSL schema now
                    let (schema_len, field_names): (usize, Vec<&str>) = {
                        let fields = match &schema {
                            SchemaNode::Struct { fields, .. } => fields,
                            _ => {
                                return Err(MagnusError::new(
                                    magnus::exception::type_error(),
                                    "Root schema node must be a struct type",
                                ))?
                            }
                        };
                        (
                            fields.len(),
                            fields
                                .iter()
                                .map(|f| match f {
                                    SchemaNode::Primitive { name, .. } => name.as_str(),
                                    SchemaNode::List { name, .. } => name.as_str(),
                                    SchemaNode::Map { name, .. } => name.as_str(),
                                    SchemaNode::Struct { name, .. } => name.as_str(),
                                })
                                .to_owned()
                                .collect(),
                        )
                    };

                    if batch_array.len() != schema_len {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!(
                                "Batch column count ({}) does not match schema length ({}). Schema expects columns: {:?}",
                                batch_array.len(),
                                schema_len,
                                field_names
                            ),
                        ))?;
                    }

                    // Convert each column in the batch to Arrow arrays
                    let arrow_arrays: Vec<(String, Arc<dyn Array>)> = {
                        // Process each field in the DSL schema
                        let fields = arrow_schema.fields();
                        let top_fields =
                            match &schema {
                                SchemaNode::Struct { fields, .. } => fields,
                                _ => return Err(MagnusError::new(
                                    magnus::exception::runtime_error(),
                                    "Top-level DSL schema must be a struct for columns approach",
                                ))?,
                            };
                        if top_fields.len() != fields.len() {
                            return Err(MagnusError::new(
                                magnus::exception::runtime_error(),
                                "Mismatch top-level DSL fields vs Arrow fields",
                            ))?;
                        }

                        let mut out = vec![];
                        for ((arrow_f, dsl_f), col_val) in
                            fields.iter().zip(top_fields.iter()).zip(batch_array)
                        {
                            let col_arr = RArray::from_value(col_val).ok_or_else(|| {
                                MagnusError::new(
                                    magnus::exception::type_error(),
                                    format!("Column '{}' must be an array", arrow_f.name()),
                                )
                            })?;
                            // Get appropriate parquet_type
                            let ptype = match dsl_f {
                                SchemaNode::Primitive {
                                    parquet_type,
                                    // Format is handled internally now
                                    ..
                                } => match parquet_type {
                                    &PrimitiveType::Int8 => PST::Int8,
                                    &PrimitiveType::Int16 => PST::Int16,
                                    &PrimitiveType::Int32 => PST::Int32,
                                    &PrimitiveType::Int64 => PST::Int64,
                                    &PrimitiveType::UInt8 => PST::UInt8,
                                    &PrimitiveType::UInt16 => PST::UInt16,
                                    &PrimitiveType::UInt32 => PST::UInt32,
                                    &PrimitiveType::UInt64 => PST::UInt64,
                                    &PrimitiveType::Float32 => PST::Float,
                                    &PrimitiveType::Float64 => PST::Double,
                                    &PrimitiveType::String => PST::String,
                                    &PrimitiveType::Binary => PST::Binary,
                                    &PrimitiveType::Boolean => PST::Boolean,
                                    &PrimitiveType::Date32 => PST::Date32,
                                    &PrimitiveType::TimestampMillis => PST::TimestampMillis,
                                    &PrimitiveType::TimestampMicros => PST::TimestampMicros,
                                },
                                SchemaNode::List { .. }
                                | SchemaNode::Map { .. }
                                | SchemaNode::Struct { .. } => {
                                    // For nested, we just do a single "column" as well
                                    arrow_data_type_to_parquet_schema_type(arrow_f.data_type())?
                                }
                            };
                            out.push((
                                arrow_f.name().clone(),
                                convert_ruby_array_to_arrow(col_arr, &ptype)?,
                            ));
                        }
                        out
                    };

                    // Create and write record batch
                    let record_batch = RecordBatch::try_from_iter(arrow_arrays).map_err(|e| {
                        MagnusError::new(
                            magnus::exception::runtime_error(),
                            format!("Failed to create record batch: {}", e),
                        )
                    })?;

                    writer.write(&record_batch)?;

                    match &mut writer {
                        WriterOutput::File(w) | WriterOutput::TempFile(w, _) => {
                            if w.in_progress_size() >= flush_threshold {
                                w.flush()?;
                            }
                        }
                    }
                }
                Err(e) => {
                    if e.is_kind_of(ruby.exception_stop_iteration()) {
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

// Creates an appropriate Parquet writer based on the output target and compression settings
// This function handles two main output scenarios:
// 1. Writing directly to a file path (string)
// 2. Writing to a Ruby IO-like object (using a temporary file as an intermediate buffer)
//
// For IO-like objects, the function creates a temporary file that is later copied to the
// IO object when writing is complete. This approach is necessary because Parquet requires
// random file access to write its footer after the data.
//
// The function also configures compression based on the user's preferences, with
// several options available (none, snappy, gzip, lz4, zstd).
fn create_writer(
    ruby: &Ruby,
    write_to: &Value,
    schema: Arc<Schema>,
    compression: Option<String>,
) -> Result<WriterOutput, ReaderError> {
    // Create writer properties with compression based on the option
    let props = WriterProperties::builder()
        .set_compression(match compression.as_deref() {
            Some("none") | Some("uncompressed") => Compression::UNCOMPRESSED,
            Some("snappy") => Compression::SNAPPY,
            Some("gzip") => Compression::GZIP(GzipLevel::default()),
            Some("lz4") => Compression::LZ4,
            Some("zstd") => Compression::ZSTD(ZstdLevel::default()),
            _ => Compression::UNCOMPRESSED,
        })
        .build();

    if write_to.is_kind_of(ruby.class_string()) {
        let path = write_to.to_r_string()?.to_string()?;
        let file: Box<dyn SendableWrite> = Box::new(File::create(path)?);
        let writer = ArrowWriter::try_new(file, schema, Some(props))?;
        Ok(WriterOutput::File(writer))
    } else {
        // Create a temporary file to write to instead of directly to the IoLikeValue
        let temp_file = NamedTempFile::new().map_err(|e| {
            MagnusError::new(
                magnus::exception::runtime_error(),
                format!("Failed to create temporary file: {}", e),
            )
        })?;
        let file: Box<dyn SendableWrite> = Box::new(temp_file.reopen().map_err(|e| {
            MagnusError::new(
                magnus::exception::runtime_error(),
                format!("Failed to reopen temporary file: {}", e),
            )
        })?);
        let writer = ArrowWriter::try_new(file, schema, Some(props))?;
        Ok(WriterOutput::TempFile(writer, temp_file))
    }
}

// Copies the contents of a temporary file to a Ruby IO-like object
// This function is necessary because Parquet writing requires random file access
// (especially for writing the footer after all data), but Ruby IO objects may not
// support seeking. The solution is to:
//
// 1. Write the entire Parquet file to a temporary file first
// 2. Once writing is complete, copy the entire contents to the Ruby IO object
//
// This approach enables support for a wide range of Ruby IO objects like StringIO,
// network streams, etc., but does require enough disk space for the temporary file
// and involves a second full-file read/write operation at the end.
fn copy_temp_file_to_io_like(
    temp_file: NamedTempFile,
    io_like: IoLikeValue,
) -> Result<(), MagnusError> {
    let file = temp_file.reopen().map_err(|e| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            format!("Failed to reopen temporary file: {}", e),
        )
    })?;
    let mut buf_reader = BufReader::new(file);
    let mut buf_writer = BufWriter::new(io_like);

    io::copy(&mut buf_reader, &mut buf_writer).map_err(|e| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            format!("Failed to copy temp file to io_like: {}", e),
        )
    })?;

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
        PST::Int8 | PST::UInt8 => Ok(1),
        PST::Int16 | PST::UInt16 => Ok(2),
        PST::Int32 | PST::UInt32 | PST::Float => Ok(4),
        PST::Int64 | PST::UInt64 | PST::Double => Ok(8),
        PST::Boolean => Ok(1),
        PST::Date32 | PST::TimestampMillis | PST::TimestampMicros => Ok(8),
        PST::String | PST::Binary => {
            if let Ok(s) = String::try_convert(value) {
                // Account for string length plus Rust String's capacity+pointer overhead
                Ok(s.len() + std::mem::size_of::<usize>() * 3)
            } else {
                // Try to convert the value to a string using to_s for non-string types
                // This handles numeric values that will be converted to strings later
                let _ruby = unsafe { Ruby::get_unchecked() };
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
) -> Result<(), ReaderError> {
    // Convert columns to Arrow arrays
    let arrow_arrays: Vec<(String, Arc<dyn Array>)> = collectors
        .iter_mut()
        .map(|c| {
            let arr = c.take_array()?;
            Ok((c.name.clone(), arr))
        })
        .collect::<Result<_, ReaderError>>()?;

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
