use super::{
    arrow_data_type_to_parquet_schema_type, copy_temp_file_to_io_like, create_writer,
    parse_parquet_write_args, DEFAULT_MEMORY_THRESHOLD,
};
use crate::{
    convert_ruby_array_to_arrow,
    logger::RubyLogger,
    types::{schema_node::build_arrow_schema, ParquetGemError, WriterOutput},
    IoLikeValue, ParquetSchemaType as PST, ParquetWriteArgs,
};
use crate::{types::PrimitiveType, SchemaNode};
use arrow_array::{Array, RecordBatch};
use magnus::{value::ReprValue, Error as MagnusError, RArray, Ruby, Value};
use std::{rc::Rc, sync::Arc};

#[inline]
pub fn write_columns(args: &[Value]) -> Result<(), MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };
    write_columns_impl(Rc::new(ruby), args).map_err(|e| {
        let z: MagnusError = e.into();
        z
    })?;
    Ok(())
}

#[inline]
fn write_columns_impl(ruby: Rc<Ruby>, args: &[Value]) -> Result<(), ParquetGemError> {
    let ParquetWriteArgs {
        read_from,
        write_to,
        schema,
        batch_size: _,
        compression,
        flush_threshold,
        sample_size: _,
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
                        Err(MagnusError::new(
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
                            Err(MagnusError::new(
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
                                } => match *parquet_type {
                                    PrimitiveType::Int8 => PST::Primitive(PrimitiveType::Int8),
                                    PrimitiveType::Int16 => PST::Primitive(PrimitiveType::Int16),
                                    PrimitiveType::Int32 => PST::Primitive(PrimitiveType::Int32),
                                    PrimitiveType::Int64 => PST::Primitive(PrimitiveType::Int64),
                                    PrimitiveType::UInt8 => PST::Primitive(PrimitiveType::UInt8),
                                    PrimitiveType::UInt16 => PST::Primitive(PrimitiveType::UInt16),
                                    PrimitiveType::UInt32 => PST::Primitive(PrimitiveType::UInt32),
                                    PrimitiveType::UInt64 => PST::Primitive(PrimitiveType::UInt64),
                                    PrimitiveType::Float32 => {
                                        PST::Primitive(PrimitiveType::Float32)
                                    }
                                    PrimitiveType::Float64 => {
                                        PST::Primitive(PrimitiveType::Float64)
                                    }
                                    PrimitiveType::Decimal128(precision, scale) => {
                                        PST::Primitive(PrimitiveType::Decimal128(precision, scale))
                                    }
                                    PrimitiveType::String => PST::Primitive(PrimitiveType::String),
                                    PrimitiveType::Binary => PST::Primitive(PrimitiveType::Binary),
                                    PrimitiveType::Boolean => {
                                        PST::Primitive(PrimitiveType::Boolean)
                                    }
                                    PrimitiveType::Date32 => PST::Primitive(PrimitiveType::Date32),
                                    PrimitiveType::TimestampMillis => {
                                        PST::Primitive(PrimitiveType::TimestampMillis)
                                    }
                                    PrimitiveType::TimestampMicros => {
                                        PST::Primitive(PrimitiveType::TimestampMicros)
                                    }
                                    PrimitiveType::TimeMillis => {
                                        PST::Primitive(PrimitiveType::TimeMillis)
                                    }
                                    PrimitiveType::TimeMicros => {
                                        PST::Primitive(PrimitiveType::TimeMicros)
                                    }
                                    PrimitiveType::Decimal256(precision, scale) => {
                                        PST::Primitive(PrimitiveType::Decimal256(precision, scale))
                                    }
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
                                convert_ruby_array_to_arrow(&ruby, col_arr, &ptype)?,
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
                    Err(e)?;
                }
            }
        }
    } else {
        Err(MagnusError::new(
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
