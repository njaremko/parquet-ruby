mod write_columns;
mod write_rows;

use arrow_schema::{DataType, Schema, TimeUnit};
use itertools::Itertools;
use magnus::{
    scan_args::{get_kwargs, scan_args},
    value::ReprValue,
    Error as MagnusError, RArray, RHash, Ruby, Symbol, Value,
};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use std::{
    fs::File,
    io::{self, BufReader, BufWriter},
    sync::Arc,
};
use tempfile::NamedTempFile;
pub use write_columns::write_columns;
pub use write_rows::write_rows;

use crate::{types::PrimitiveType, SchemaNode};
use crate::{
    types::{ColumnCollector, ParquetGemError, ParquetSchemaType, WriterOutput},
    utils::parse_string_or_symbol,
    IoLikeValue, ParquetSchemaType as PST, ParquetWriteArgs, SchemaField, SendableWrite,
};

const SAMPLE_SIZE: usize = 100;
const MIN_BATCH_SIZE: usize = 10;
const INITIAL_BATCH_SIZE: usize = 100;
const DEFAULT_MEMORY_THRESHOLD: usize = 64 * 1024 * 1024;

/// Parse arguments for Parquet writing
pub fn parse_parquet_write_args(
    ruby: &Ruby,
    args: &[Value],
) -> Result<ParquetWriteArgs, MagnusError> {
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
        let ttype = parse_string_or_symbol(ruby, type_val)?;
        if let Some(ref type_str) = ttype {
            if type_str == "struct" {
                // Parse using the new schema approach
                let schema_node = crate::parse_schema_node(ruby, schema_value)?;

                validate_schema_node(ruby, &schema_node)?;

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
                .is_empty())
    {
        // If schema is nil or an empty array, we need to peek at the first value to determine column count
        let first_value = read_from.funcall::<_, _, Value>("peek", ())?;
        // Default to nullable:true for auto-inferred fields
        crate::infer_schema_from_first_row(ruby, first_value, true)?
    } else {
        // Legacy array format - use our centralized parser
        crate::parse_legacy_schema(ruby, schema_value)?
    };

    // Convert the legacy schema fields to SchemaNode (DSL format)
    let schema_node = crate::legacy_schema_to_dsl(ruby, schema_fields)?;

    validate_schema_node(ruby, &schema_node)?;

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

// -----------------------------------------------------------------------------
// HELPER to invert arrow DataType back to our ParquetSchemaType
// Converts Arrow DataType to our internal ParquetSchemaType representation.
// This is essential for mapping Arrow types back to our schema representation
// when working with column collections and schema validation.
// -----------------------------------------------------------------------------
fn arrow_data_type_to_parquet_schema_type(dt: &DataType) -> Result<ParquetSchemaType, MagnusError> {
    match dt {
        DataType::Boolean => Ok(PST::Primitive(PrimitiveType::Boolean)),
        DataType::Int8 => Ok(PST::Primitive(PrimitiveType::Int8)),
        DataType::Int16 => Ok(PST::Primitive(PrimitiveType::Int16)),
        DataType::Int32 => Ok(PST::Primitive(PrimitiveType::Int32)),
        DataType::Int64 => Ok(PST::Primitive(PrimitiveType::Int64)),
        DataType::UInt8 => Ok(PST::Primitive(PrimitiveType::UInt8)),
        DataType::UInt16 => Ok(PST::Primitive(PrimitiveType::UInt16)),
        DataType::UInt32 => Ok(PST::Primitive(PrimitiveType::UInt32)),
        DataType::UInt64 => Ok(PST::Primitive(PrimitiveType::UInt64)),
        DataType::Float16 => {
            // We do not have a direct ParquetSchemaType::Float16, we treat it as Float
            Ok(PST::Primitive(PrimitiveType::Float32))
        }
        DataType::Float32 => Ok(PST::Primitive(PrimitiveType::Float32)),
        DataType::Float64 => Ok(PST::Primitive(PrimitiveType::Float64)),
        DataType::Decimal128(precision, scale) => Ok(PST::Primitive(PrimitiveType::Decimal128(
            *precision, *scale,
        ))),
        DataType::Decimal256(precision, scale) => Ok(PST::Primitive(PrimitiveType::Decimal256(
            *precision, *scale,
        ))),
        DataType::Date32 => Ok(PST::Primitive(PrimitiveType::Date32)),
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
            Ok(PST::Primitive(PrimitiveType::TimestampMillis))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _tz) => {
            Ok(PST::Primitive(PrimitiveType::TimestampMillis))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _tz) => {
            Ok(PST::Primitive(PrimitiveType::TimestampMicros))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _tz) => {
            // If you have a PST::TimestampNanos variant, use it. Otherwise, degrade to micros
            // for demonstration:
            Err(MagnusError::new(
                magnus::exception::runtime_error(),
                "TimestampNanos not supported, please adjust your schema or code.",
            ))
        }
        DataType::Time32(TimeUnit::Millisecond) => Ok(PST::Primitive(PrimitiveType::TimeMillis)),
        DataType::Time64(TimeUnit::Microsecond) => Ok(PST::Primitive(PrimitiveType::TimeMicros)),
        DataType::Time32(_) => Err(MagnusError::new(
            magnus::exception::runtime_error(),
            "Time32 only supports millisecond unit",
        )),
        DataType::Time64(_) => Err(MagnusError::new(
            magnus::exception::runtime_error(),
            "Time64 only supports microsecond unit",
        )),
        DataType::Utf8 => Ok(PST::Primitive(PrimitiveType::String)),
        DataType::Binary => Ok(PST::Primitive(PrimitiveType::Binary)),
        DataType::LargeUtf8 => {
            // If not supported, degrade or error. We'll degrade to PST::String
            Ok(PST::Primitive(PrimitiveType::String))
        }
        DataType::LargeBinary => Ok(PST::Primitive(PrimitiveType::Binary)),
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
            ruby,
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
) -> Result<WriterOutput, ParquetGemError> {
    // Create writer properties with compression based on the option
    let compression_setting = match compression.map(|s| s.to_lowercase()).as_deref() {
        Some("none") | Some("uncompressed") => Ok(Compression::UNCOMPRESSED),
        Some("snappy") => Ok(Compression::SNAPPY),
        Some("gzip") => Ok(Compression::GZIP(GzipLevel::default())),
        Some("lz4") => Ok(Compression::LZ4),
        Some("zstd") => Ok(Compression::ZSTD(ZstdLevel::default())),
        None => Ok(Compression::UNCOMPRESSED),
        other => Err(MagnusError::new(
            magnus::exception::arg_error(),
            format!("Invalid compression option: {:?}", other),
        )),
    }?;

    let props = WriterProperties::builder()
        .set_compression(compression_setting)
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
