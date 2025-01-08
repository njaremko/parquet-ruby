use std::{fs::File, io::Write, str::FromStr, sync::Arc};

use ahash::RandomState;
use arrow_array::{
    Array, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use magnus::{
    scan_args::{get_kwargs, scan_args},
    try_convert::TryConvertOwned,
    value::ReprValue,
    Error as MagnusError, RArray, RString, Ruby, Symbol, TryConvert, Value,
};
use parquet::{arrow::ArrowWriter, errors::ParquetError};

const DEFAULT_BATCH_SIZE: usize = 1000;

// Wrapper type to handle Parquet errors
struct ParquetErrorWrapper(ParquetError);

impl From<ParquetErrorWrapper> for MagnusError {
    fn from(err: ParquetErrorWrapper) -> Self {
        MagnusError::new(
            magnus::exception::runtime_error(),
            format!("Parquet error: {}", err.0),
        )
    }
}

#[derive(Debug, Clone)]
pub enum ParquetSchemaType {
    Int32,
    Int64,
    Float,
    Double,
    String,
    Struct,
}

#[derive(Debug)]
pub struct SchemaField {
    pub name: String,
    pub type_: ParquetSchemaType,
}

#[derive(Debug)]
pub struct ParquetWriteArgs {
    pub read_from: Value,
    pub write_to: Value,
    pub schema: Vec<SchemaField>,
    pub batch_size: Option<usize>,
}

/// Parse arguments for Parquet writing
pub fn parse_parquet_write_args(args: &[Value]) -> Result<ParquetWriteArgs, MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };
    let parsed_args = scan_args::<(Value,), (), (), (), _, ()>(args)?;
    let (read_from,) = parsed_args.required;

    let kwargs = get_kwargs::<_, (Value, Value), (Option<usize>,), ()>(
        parsed_args.keywords,
        &["schema", "write_to"],
        &["batch_size"],
    )?;

    let schema_array = RArray::from_value(kwargs.required.0).ok_or_else(|| {
        MagnusError::new(
            magnus::exception::type_error(),
            "schema must be an array of hashes",
        )
    })?;

    let mut schema = Vec::with_capacity(schema_array.len());

    for (idx, field_hash) in schema_array.into_iter().enumerate() {
        if !field_hash.is_kind_of(ruby.class_hash()) {
            return Err(MagnusError::new(
                magnus::exception::type_error(),
                format!("schema[{}] must be a hash", idx),
            ));
        }

        let entries: Vec<(Value, Value)> = field_hash.funcall("to_a", ())?;
        if entries.len() != 1 {
            return Err(MagnusError::new(
                magnus::exception::type_error(),
                format!("schema[{}] must contain exactly one key-value pair", idx),
            ));
        }

        let (name, type_str) = &entries[0];
        let name = String::try_convert(name.clone())?;
        let type_ = ParquetSchemaType::try_convert(type_str.clone())?;

        schema.push(SchemaField { name, type_ });
    }

    Ok(ParquetWriteArgs {
        read_from,
        write_to: kwargs.required.1,
        schema,
        batch_size: kwargs.optional.0,
    })
}

#[inline]
pub fn write_rows(rb_self: Value, args: &[Value]) -> Result<(), MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };

    let ParquetWriteArgs {
        read_from,
        write_to,
        schema,
        batch_size,
    } = parse_parquet_write_args(args)?;

    let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

    // Convert schema to Arrow schema
    let arrow_fields: Vec<Field> = schema
        .iter()
        .map(|field| {
            Field::new(
                &field.name,
                match field.type_ {
                    ParquetSchemaType::Int32 => DataType::Int32,
                    ParquetSchemaType::Int64 => DataType::Int64,
                    ParquetSchemaType::Float => DataType::Float32,
                    ParquetSchemaType::Double => DataType::Float64,
                    ParquetSchemaType::String => DataType::Utf8,
                    ParquetSchemaType::Struct => unimplemented!("Struct type not yet supported"),
                },
                true,
            )
        })
        .collect();
    let arrow_schema = Arc::new(Schema::new(arrow_fields));

    // Create the writer
    let mut writer = create_writer(&write_to, arrow_schema.clone())?;

    if read_from.is_kind_of(ruby.class_enumerator()) {
        // Create collectors for each column
        let mut column_collectors: Vec<ColumnCollector> = schema
            .into_iter()
            .map(|field| ColumnCollector::new(field.name, field.type_))
            .collect();

        let mut rows_in_batch = 0;

        loop {
            match read_from.funcall::<_, _, Value>("next", ()) {
                Ok(row) => {
                    let row_array = RArray::from_value(row).ok_or_else(|| {
                        MagnusError::new(ruby.exception_type_error(), "Row must be an array")
                    })?;

                    // Validate row length matches schema
                    if row_array.len() != column_collectors.len() {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!(
                                "Row length ({}) does not match schema length ({}). Schema expects columns: {:?}",
                                row_array.len(),
                                column_collectors.len(),
                                column_collectors.iter().map(|c| c.name.as_str()).collect::<Vec<_>>()
                            ),
                        ));
                    }

                    // Process each value in the row immediately
                    for (collector, value) in column_collectors.iter_mut().zip(row_array) {
                        collector.push_value(value)?;
                    }

                    rows_in_batch += 1;

                    // When we reach batch size, write the batch
                    if rows_in_batch >= batch_size {
                        write_batch(&mut writer, &mut column_collectors)?;
                        rows_in_batch = 0;
                    }
                }
                Err(e) => {
                    if e.is_kind_of(ruby.exception_stop_iteration()) {
                        // Write any remaining rows
                        if rows_in_batch > 0 {
                            write_batch(&mut writer, &mut column_collectors)?;
                        }
                        break;
                    }
                    return Err(e);
                }
            }
        }
    } else {
        return Err(MagnusError::new(
            magnus::exception::type_error(),
            "read_from must be an Enumerator",
        ));
    }

    // Ensure everything is written
    writer.close().map_err(|e| ParquetErrorWrapper(e))?;
    Ok(())
}

#[inline]
pub fn write_columns(rb_self: Value, args: &[Value]) -> Result<(), MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };

    let ParquetWriteArgs {
        read_from,
        write_to,
        schema,
        batch_size: _, // Batch size is determined by the input
    } = parse_parquet_write_args(args)?;

    // Convert schema to Arrow schema
    let arrow_fields: Vec<Field> = schema
        .iter()
        .map(|field| {
            Field::new(
                &field.name,
                match field.type_ {
                    ParquetSchemaType::Int32 => DataType::Int32,
                    ParquetSchemaType::Int64 => DataType::Int64,
                    ParquetSchemaType::Float => DataType::Float32,
                    ParquetSchemaType::Double => DataType::Float64,
                    ParquetSchemaType::String => DataType::Utf8,
                    ParquetSchemaType::Struct => unimplemented!("Struct type not yet supported"),
                },
                true,
            )
        })
        .collect();
    let arrow_schema = Arc::new(Schema::new(arrow_fields));

    // Create the writer
    let mut writer = create_writer(&write_to, arrow_schema.clone())?;

    if read_from.is_kind_of(ruby.class_enumerator()) {
        loop {
            match read_from.funcall::<_, _, Value>("next", ()) {
                Ok(batch) => {
                    let batch_array = RArray::from_value(batch).ok_or_else(|| {
                        MagnusError::new(ruby.exception_type_error(), "Batch must be an array")
                    })?;

                    // Validate batch length matches schema
                    if batch_array.len() != schema.len() {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!(
                                "Batch column count ({}) does not match schema length ({}). Schema expects columns: {:?}",
                                batch_array.len(),
                                schema.len(),
                                schema.iter().map(|f| f.name.as_str()).collect::<Vec<_>>()
                            ),
                        ));
                    }

                    // Convert each column in the batch to Arrow arrays
                    let arrow_arrays: Vec<(String, Arc<dyn Array>)> = schema
                        .iter()
                        .zip(batch_array)
                        .map(|(field, column)| {
                            let column_array = RArray::from_value(column).ok_or_else(|| {
                                MagnusError::new(
                                    magnus::exception::type_error(),
                                    format!("Column '{}' must be an array", field.name),
                                )
                            })?;

                            let array = match field.type_ {
                                ParquetSchemaType::Int32 => {
                                    let values: Vec<i32> = column_array
                                        .each()
                                        .map(|v| {
                                            let v = v?;
                                            if v.is_kind_of(ruby.class_string()) {
                                                let s = String::try_convert(v)?;
                                                s.trim().parse::<i32>().map_err(|e| {
                                                    MagnusError::new(
                                                        magnus::exception::type_error(),
                                                        format!(
                                                            "Failed to parse '{}' as int32: {}",
                                                            s, e
                                                        ),
                                                    )
                                                })
                                            } else {
                                                i32::try_convert(v)
                                            }
                                        })
                                        .collect::<Result<_, MagnusError>>()?;
                                    Arc::new(Int32Array::from_iter_values(values)) as Arc<dyn Array>
                                }
                                ParquetSchemaType::Int64 => {
                                    let values: Vec<i64> = column_array
                                        .each()
                                        .map(|v| {
                                            let v = v?;
                                            if v.is_kind_of(ruby.class_string()) {
                                                let s = String::try_convert(v)?;
                                                s.trim().parse::<i64>().map_err(|e| {
                                                    MagnusError::new(
                                                        magnus::exception::type_error(),
                                                        format!(
                                                            "Failed to parse '{}' as int64: {}",
                                                            s, e
                                                        ),
                                                    )
                                                })
                                            } else {
                                                i64::try_convert(v)
                                            }
                                        })
                                        .collect::<Result<_, MagnusError>>()?;
                                    Arc::new(Int64Array::from_iter_values(values)) as Arc<dyn Array>
                                }
                                ParquetSchemaType::Float => {
                                    let values: Vec<f32> = column_array
                                        .each()
                                        .map(|v| {
                                            let v = v?;
                                            if v.is_kind_of(ruby.class_string()) {
                                                let s = String::try_convert(v)?;
                                                s.trim().parse::<f32>().map_err(|e| {
                                                    MagnusError::new(
                                                        magnus::exception::type_error(),
                                                        format!(
                                                            "Failed to parse '{}' as float: {}",
                                                            s, e
                                                        ),
                                                    )
                                                })
                                            } else {
                                                f32::try_convert(v)
                                            }
                                        })
                                        .collect::<Result<_, MagnusError>>()?;
                                    Arc::new(Float32Array::from_iter_values(values))
                                        as Arc<dyn Array>
                                }
                                ParquetSchemaType::Double => {
                                    let values: Vec<f64> = column_array
                                        .each()
                                        .map(|v| {
                                            let v = v?;
                                            if v.is_kind_of(ruby.class_string()) {
                                                let s = String::try_convert(v)?;
                                                s.trim().parse::<f64>().map_err(|e| {
                                                    MagnusError::new(
                                                        magnus::exception::type_error(),
                                                        format!(
                                                            "Failed to parse '{}' as double: {}",
                                                            s, e
                                                        ),
                                                    )
                                                })
                                            } else {
                                                f64::try_convert(v)
                                            }
                                        })
                                        .collect::<Result<_, MagnusError>>()?;
                                    Arc::new(Float64Array::from_iter_values(values))
                                        as Arc<dyn Array>
                                }
                                ParquetSchemaType::String => {
                                    let values: Vec<String> = column_array
                                        .each()
                                        .map(|v| {
                                            let v = v?;
                                            String::try_convert(v)
                                        })
                                        .collect::<Result<_, MagnusError>>()?;
                                    Arc::new(StringArray::from_iter_values(values))
                                        as Arc<dyn Array>
                                }
                                ParquetSchemaType::Struct => {
                                    unimplemented!("Struct type not yet supported")
                                }
                            };

                            Ok((field.name.clone(), array))
                        })
                        .collect::<Result<Vec<_>, MagnusError>>()?;

                    // Create and write record batch
                    let record_batch = RecordBatch::try_from_iter(arrow_arrays).map_err(|e| {
                        MagnusError::new(
                            magnus::exception::runtime_error(),
                            format!("Failed to create record batch: {}", e),
                        )
                    })?;

                    writer
                        .write(&record_batch)
                        .map_err(|e| ParquetErrorWrapper(e))?;
                }
                Err(e) => {
                    if e.is_kind_of(ruby.exception_stop_iteration()) {
                        break;
                    }
                    return Err(e);
                }
            }
        }
    } else {
        return Err(MagnusError::new(
            magnus::exception::type_error(),
            "read_from must be an Enumerator",
        ));
    }

    // Ensure everything is written
    writer.close().map_err(|e| ParquetErrorWrapper(e))?;
    Ok(())
}

// Helper struct to collect values for each column
struct ColumnCollector {
    name: String,
    type_: ParquetSchemaType,
    values: Vec<Value>,
}

impl ColumnCollector {
    fn new(name: String, type_: ParquetSchemaType) -> Self {
        Self {
            name,
            type_,
            values: Vec::new(),
        }
    }

    fn push_value(&mut self, value: Value) -> Result<(), MagnusError> {
        self.values.push(value);
        Ok(())
    }

    fn take_array(&mut self) -> Result<Arc<dyn Array>, MagnusError> {
        let values = std::mem::take(&mut self.values);
        let ruby = unsafe { Ruby::get_unchecked() };

        match self.type_ {
            ParquetSchemaType::Int32 => {
                let values: Vec<i32> = values
                    .into_iter()
                    .map(|v| {
                        if v.is_kind_of(ruby.class_string()) {
                            let s = String::try_convert(v)?;
                            s.trim().parse::<i32>().map_err(|e| {
                                MagnusError::new(
                                    magnus::exception::type_error(),
                                    format!("Failed to parse '{}' as int32: {}", s, e),
                                )
                            })
                        } else {
                            i32::try_convert(v)
                        }
                    })
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Int32Array::from_iter_values(values)))
            }
            ParquetSchemaType::Int64 => {
                let values: Vec<i64> = values
                    .into_iter()
                    .map(|v| {
                        if v.is_kind_of(ruby.class_string()) {
                            let s = String::try_convert(v)?;
                            s.trim().parse::<i64>().map_err(|e| {
                                MagnusError::new(
                                    magnus::exception::type_error(),
                                    format!("Failed to parse '{}' as int64: {}", s, e),
                                )
                            })
                        } else {
                            i64::try_convert(v)
                        }
                    })
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Int64Array::from_iter_values(values)))
            }
            ParquetSchemaType::Float => {
                let values: Vec<f32> = values
                    .into_iter()
                    .map(|v| {
                        if v.is_kind_of(ruby.class_string()) {
                            let s = String::try_convert(v)?;
                            s.trim().parse::<f32>().map_err(|e| {
                                MagnusError::new(
                                    magnus::exception::type_error(),
                                    format!("Failed to parse '{}' as float: {}", s, e),
                                )
                            })
                        } else {
                            f32::try_convert(v)
                        }
                    })
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Float32Array::from_iter_values(values)))
            }
            ParquetSchemaType::Double => {
                let values: Vec<f64> = values
                    .into_iter()
                    .map(|v| {
                        if v.is_kind_of(ruby.class_string()) {
                            let s = String::try_convert(v)?;
                            s.trim().parse::<f64>().map_err(|e| {
                                MagnusError::new(
                                    magnus::exception::type_error(),
                                    format!("Failed to parse '{}' as double: {}", s, e),
                                )
                            })
                        } else {
                            f64::try_convert(v)
                        }
                    })
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Float64Array::from_iter_values(values)))
            }
            ParquetSchemaType::String => {
                let values: Vec<String> = values
                    .into_iter()
                    .map(String::try_convert)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(StringArray::from_iter_values(values)))
            }
            ParquetSchemaType::Struct => unimplemented!("Struct type not yet supported"),
        }
    }
}

trait SendableWrite: Send + Write {}
impl<T: Send + Write> SendableWrite for T {}

fn create_writer(
    write_to: &Value,
    schema: Arc<Schema>,
) -> Result<ArrowWriter<Box<dyn SendableWrite>>, MagnusError> {
    let path = write_to.to_r_string()?.to_string()?;
    let file: Box<dyn SendableWrite> = Box::new(File::create(path).unwrap());
    ArrowWriter::try_new(file, schema, None).map_err(|e| ParquetErrorWrapper(e).into())
}

fn write_batch(
    writer: &mut ArrowWriter<Box<dyn SendableWrite>>,
    collectors: &mut [ColumnCollector],
) -> Result<(), MagnusError> {
    // Convert columns to Arrow arrays
    let arrow_arrays: Vec<(String, Arc<dyn Array>)> = collectors
        .iter_mut()
        .map(|collector| Ok((collector.name.clone(), collector.take_array()?)))
        .collect::<Result<_, MagnusError>>()?;

    // Create and write record batch
    let record_batch = RecordBatch::try_from_iter(arrow_arrays).map_err(|e| {
        MagnusError::new(
            magnus::exception::runtime_error(),
            format!("Failed to create record batch: {}", e),
        )
    })?;

    writer
        .write(&record_batch)
        .map_err(|e| ParquetErrorWrapper(e))?;

    Ok(())
}

impl FromStr for ParquetSchemaType {
    type Err = MagnusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "int32" => Ok(ParquetSchemaType::Int32),
            "int64" => Ok(ParquetSchemaType::Int64),
            "float" => Ok(ParquetSchemaType::Float),
            "double" => Ok(ParquetSchemaType::Double),
            "string" => Ok(ParquetSchemaType::String),
            _ => Err(MagnusError::new(
                magnus::exception::runtime_error(),
                "Invalid schema type",
            )),
        }
    }
}

impl TryConvert for ParquetSchemaType {
    fn try_convert(value: Value) -> Result<Self, MagnusError> {
        let ruby = unsafe { Ruby::get_unchecked() };
        let schema_type = parse_string_or_symbol(&ruby, value)?;

        schema_type.unwrap().parse()
    }
}

unsafe impl TryConvertOwned for ParquetSchemaType {}

fn parse_string_or_symbol(ruby: &Ruby, value: Value) -> Result<Option<String>, MagnusError> {
    if value.is_nil() {
        Ok(None)
    } else if value.is_kind_of(ruby.class_string()) {
        RString::from_value(value)
            .ok_or_else(|| {
                MagnusError::new(magnus::exception::type_error(), "Invalid string value")
            })?
            .to_string()
            .map(|s| Some(s))
    } else if value.is_kind_of(ruby.class_symbol()) {
        Symbol::from_value(value)
            .ok_or_else(|| {
                MagnusError::new(magnus::exception::type_error(), "Invalid symbol value")
            })?
            .funcall("to_s", ())
            .map(|s| Some(s))
    } else {
        Err(MagnusError::new(
            magnus::exception::type_error(),
            "Value must be a String or Symbol",
        ))
    }
}
