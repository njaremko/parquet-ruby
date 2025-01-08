use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufReader, BufWriter, Write},
    str::FromStr,
    sync::Arc,
};

use arrow_array::builder::{ListBuilder, StringBuilder, StructBuilder};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float16Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray,
    RecordBatch, StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use magnus::{
    scan_args::{get_kwargs, scan_args},
    try_convert::TryConvertOwned,
    value::ReprValue,
    Error as MagnusError, RArray, RString, Ruby, Symbol, TryConvert, Value,
};
use parquet::{arrow::ArrowWriter, errors::ParquetError};
use tempfile::NamedTempFile;

mod type_conversion {
    use jiff::tz::{Offset, TimeZone};

    use super::*;

    pub struct NumericConverter<T> {
        _phantom: std::marker::PhantomData<T>,
    }

    impl<T> NumericConverter<T>
    where
        T: TryConvert + FromStr,
        <T as FromStr>::Err: std::fmt::Display,
    {
        pub fn convert_with_string_fallback(value: Value) -> Result<T, MagnusError> {
            let ruby = unsafe { Ruby::get_unchecked() };
            if value.is_kind_of(ruby.class_string()) {
                let s = String::try_convert(value)?;
                s.trim().parse::<T>().map_err(|e| {
                    MagnusError::new(
                        magnus::exception::type_error(),
                        format!("Failed to parse '{}' as numeric: {}", s, e),
                    )
                })
            } else {
                T::try_convert(value)
            }
        }
    }

    pub fn convert_to_date32(value: Value) -> Result<i32, MagnusError> {
        let ruby = unsafe { Ruby::get_unchecked() };
        if value.is_kind_of(ruby.class_string()) {
            let s = String::try_convert(value)?;
            // Parse string into Timestamp using jiff
            let date: jiff::civil::Date = s.parse().map_err(|e| {
                MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Failed to parse '{}' as date32: {}", s, e),
                )
            })?;

            let timestamp = date.at(0, 0, 0, 0);

            let x = timestamp
                .to_zoned(TimeZone::fixed(Offset::constant(0)))
                .unwrap()
                .timestamp();

            // Convert to epoch days
            Ok((x.as_second() as i64 / 86400) as i32)
        } else if value.is_kind_of(ruby.class_time()) {
            // Convert Time object to epoch days
            let secs = i64::try_convert(value.funcall::<_, _, Value>("to_i", ()).unwrap())?;
            Ok(((secs as f64) / 86400.0) as i32)
        } else {
            Err(MagnusError::new(
                magnus::exception::type_error(),
                format!("Cannot convert {} to date32", unsafe { value.classname() }),
            ))
        }
    }

    pub fn convert_to_timestamp_millis(value: Value) -> Result<i64, MagnusError> {
        let ruby = unsafe { Ruby::get_unchecked() };
        if value.is_kind_of(ruby.class_string()) {
            let s = String::try_convert(value)?;
            // Parse string into Timestamp using jiff
            let timestamp: jiff::Timestamp = s.parse().map_err(|e| {
                MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Failed to parse '{}' as timestamp_millis: {}", s, e),
                )
            })?;
            // Convert to milliseconds
            Ok(timestamp.as_millisecond())
        } else if value.is_kind_of(ruby.class_time()) {
            // Convert Time object to milliseconds
            let secs = i64::try_convert(value.funcall::<_, _, Value>("to_i", ()).unwrap())?;
            let usecs = i64::try_convert(value.funcall::<_, _, Value>("usec", ()).unwrap())?;
            Ok(secs * 1000 + (usecs / 1000))
        } else {
            Err(MagnusError::new(
                magnus::exception::type_error(),
                format!("Cannot convert {} to timestamp_millis", unsafe {
                    value.classname()
                }),
            ))
        }
    }

    pub fn convert_to_timestamp_micros(value: Value) -> Result<i64, MagnusError> {
        let ruby = unsafe { Ruby::get_unchecked() };
        if value.is_kind_of(ruby.class_string()) {
            let s = String::try_convert(value)?;
            // Parse string into Timestamp using jiff
            let timestamp: jiff::Timestamp = s.parse().map_err(|e| {
                MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Failed to parse '{}' as timestamp_micros: {}", s, e),
                )
            })?;
            // Convert to microseconds
            Ok(timestamp.as_microsecond())
        } else if value.is_kind_of(ruby.class_time()) {
            // Convert Time object to microseconds
            let secs = i64::try_convert(value.funcall::<_, _, Value>("to_i", ()).unwrap())?;
            let usecs = i64::try_convert(value.funcall::<_, _, Value>("usec", ()).unwrap())?;
            Ok(secs * 1_000_000 + usecs)
        } else {
            Err(MagnusError::new(
                magnus::exception::type_error(),
                format!("Cannot convert {} to timestamp_micros", unsafe {
                    value.classname()
                }),
            ))
        }
    }

    pub fn convert_to_binary(value: Value) -> Result<Vec<u8>, MagnusError> {
        Ok(unsafe { value.to_r_string()?.as_slice() }.to_vec())
    }

    pub fn convert_to_boolean(value: Value) -> Result<bool, MagnusError> {
        let ruby = unsafe { Ruby::get_unchecked() };
        if value.is_kind_of(ruby.class_string()) {
            let s = String::try_convert(value)?;
            s.trim().parse::<bool>().map_err(|e| {
                MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Failed to parse '{}' as boolean: {}", s, e),
                )
            })
        } else {
            bool::try_convert(value)
        }
    }

    pub fn convert_to_list(value: Value) -> Result<Vec<Value>, MagnusError> {
        let ruby = unsafe { Ruby::get_unchecked() };
        if value.is_kind_of(ruby.class_array()) {
            let array = RArray::from_value(value).ok_or_else(|| {
                MagnusError::new(magnus::exception::type_error(), "Invalid list format")
            })?;
            Ok(array.into_iter().collect())
        } else {
            Err(MagnusError::new(
                magnus::exception::type_error(),
                "Invalid list format",
            ))
        }
    }

    pub fn convert_to_map(value: Value) -> Result<HashMap<String, String>, MagnusError> {
        let ruby = unsafe { Ruby::get_unchecked() };
        if value.is_kind_of(ruby.class_array()) {
            let array = RArray::from_value(value).ok_or_else(|| {
                MagnusError::new(magnus::exception::type_error(), "Invalid map format")
            })?;

            let mut map = HashMap::new();
            for entry in array.into_iter() {
                let entry_array = RArray::from_value(entry).ok_or_else(|| {
                    MagnusError::new(magnus::exception::type_error(), "Invalid map entry format")
                })?;

                if entry_array.len() != 2 {
                    return Err(MagnusError::new(
                        magnus::exception::type_error(),
                        "Invalid map entry format",
                    ));
                }

                let key = entry_array.into_iter().next().ok_or_else(|| {
                    MagnusError::new(magnus::exception::type_error(), "Invalid map key format")
                })?;

                let value = entry_array.into_iter().nth(1).ok_or_else(|| {
                    MagnusError::new(magnus::exception::type_error(), "Invalid map value format")
                })?;

                let key_str = String::try_convert(key).map_err(|e| {
                    MagnusError::new(
                        magnus::exception::type_error(),
                        format!("Failed to convert map key: {}", e),
                    )
                })?;
                let value_str = String::try_convert(value).map_err(|e| {
                    MagnusError::new(
                        magnus::exception::type_error(),
                        format!("Failed to convert map value: {}", e),
                    )
                })?;

                map.insert(key_str, value_str);
            }
            Ok(map)
        } else {
            Err(MagnusError::new(
                magnus::exception::type_error(),
                "Invalid map format",
            ))
        }
    }

    pub fn convert_array_to_arrow(
        values: Vec<Value>,
        type_: &ParquetSchemaType,
    ) -> Result<Arc<dyn Array>, MagnusError> {
        match type_ {
            ParquetSchemaType::Int8 => {
                let values: Vec<i8> = values
                    .into_iter()
                    .map(NumericConverter::<i8>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Int8Array::from_iter_values(values)))
            }
            ParquetSchemaType::Int16 => {
                let values: Vec<i16> = values
                    .into_iter()
                    .map(NumericConverter::<i16>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Int16Array::from_iter_values(values)))
            }
            ParquetSchemaType::Int32 => {
                let values: Vec<i32> = values
                    .into_iter()
                    .map(NumericConverter::<i32>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Int32Array::from_iter_values(values)))
            }
            ParquetSchemaType::Int64 => {
                let values: Vec<i64> = values
                    .into_iter()
                    .map(NumericConverter::<i64>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Int64Array::from_iter_values(values)))
            }
            ParquetSchemaType::UInt8 => {
                let values: Vec<u8> = values
                    .into_iter()
                    .map(NumericConverter::<u8>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(UInt8Array::from_iter_values(values)))
            }
            ParquetSchemaType::UInt16 => {
                let values: Vec<u16> = values
                    .into_iter()
                    .map(NumericConverter::<u16>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(UInt16Array::from_iter_values(values)))
            }
            ParquetSchemaType::UInt32 => {
                let values: Vec<u32> = values
                    .into_iter()
                    .map(NumericConverter::<u32>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(UInt32Array::from_iter_values(values)))
            }
            ParquetSchemaType::UInt64 => {
                let values: Vec<u64> = values
                    .into_iter()
                    .map(NumericConverter::<u64>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(UInt64Array::from_iter_values(values)))
            }
            ParquetSchemaType::Float => {
                let values: Vec<f32> = values
                    .into_iter()
                    .map(NumericConverter::<f32>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Float32Array::from_iter_values(values)))
            }
            ParquetSchemaType::Double => {
                let values: Vec<f64> = values
                    .into_iter()
                    .map(NumericConverter::<f64>::convert_with_string_fallback)
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
            ParquetSchemaType::Binary => {
                let values: Vec<Vec<u8>> = values
                    .into_iter()
                    .map(convert_to_binary)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(BinaryArray::from_iter_values(values)))
            }
            ParquetSchemaType::Boolean => {
                let values: Vec<bool> = values
                    .into_iter()
                    .map(convert_to_boolean)
                    .collect::<Result<_, _>>()?;
                let x: BooleanArray = values.into();
                Ok(Arc::new(x))
            }
            ParquetSchemaType::Date32 => {
                let values: Vec<i32> = values
                    .into_iter()
                    .map(convert_to_date32)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Date32Array::from_iter_values(values)))
            }
            ParquetSchemaType::TimestampMillis => {
                let values: Vec<i64> = values
                    .into_iter()
                    .map(convert_to_timestamp_millis)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(TimestampMillisecondArray::from_iter_values(
                    values,
                )))
            }
            ParquetSchemaType::TimestampMicros => {
                let values: Vec<i64> = values
                    .into_iter()
                    .map(convert_to_timestamp_micros)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(TimestampMicrosecondArray::from_iter_values(
                    values,
                )))
            }
            ParquetSchemaType::List => {
                panic!("List type not yet supported");
            }
            ParquetSchemaType::Map => {
                panic!("Map type not yet supported");
            }
            ParquetSchemaType::Struct => unimplemented!("Struct type not yet supported"),
        }
    }
}

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
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float,
    Double,
    String,
    Binary,
    Boolean,
    Date32,
    TimestampMillis,
    TimestampMicros,
    List,
    Map,
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
pub fn write_rows(args: &[Value]) -> Result<(), MagnusError> {
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
                    ParquetSchemaType::Int8 => DataType::Int8,
                    ParquetSchemaType::Int16 => DataType::Int16,
                    ParquetSchemaType::Int32 => DataType::Int32,
                    ParquetSchemaType::Int64 => DataType::Int64,
                    ParquetSchemaType::UInt8 => DataType::UInt8,
                    ParquetSchemaType::UInt16 => DataType::UInt16,
                    ParquetSchemaType::UInt32 => DataType::UInt32,
                    ParquetSchemaType::UInt64 => DataType::UInt64,
                    ParquetSchemaType::Float => DataType::Float32,
                    ParquetSchemaType::Double => DataType::Float64,
                    ParquetSchemaType::String => DataType::Utf8,
                    ParquetSchemaType::Binary => DataType::Binary,
                    ParquetSchemaType::Boolean => DataType::Boolean,
                    ParquetSchemaType::Date32 => DataType::Date32,
                    ParquetSchemaType::TimestampMillis => {
                        DataType::Timestamp(TimeUnit::Millisecond, None)
                    }
                    ParquetSchemaType::TimestampMicros => {
                        DataType::Timestamp(TimeUnit::Microsecond, None)
                    }
                    ParquetSchemaType::List => unimplemented!("List type not yet supported"),
                    ParquetSchemaType::Map => unimplemented!("Map type not yet supported"),
                    ParquetSchemaType::Struct => unimplemented!("Struct type not yet supported"),
                },
                true,
            )
        })
        .collect();
    let arrow_schema = Arc::new(Schema::new(arrow_fields));

    // Create the writer
    let mut writer = create_writer(&ruby, &write_to, arrow_schema.clone())?;

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

    // Ensure everything is written and get the temp file if it exists
    if let Some(temp_file) = writer.close().map_err(|e| ParquetErrorWrapper(e))? {
        // If we got a temp file back, we need to copy its contents to the IO-like object
        copy_temp_file_to_io_like(temp_file, IoLikeValue(write_to))?;
    }

    Ok(())
}

#[inline]
pub fn write_columns(args: &[Value]) -> Result<(), MagnusError> {
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
                    ParquetSchemaType::Int8 => DataType::Int8,
                    ParquetSchemaType::Int16 => DataType::Int16,
                    ParquetSchemaType::Int32 => DataType::Int32,
                    ParquetSchemaType::Int64 => DataType::Int64,
                    ParquetSchemaType::UInt8 => DataType::UInt8,
                    ParquetSchemaType::UInt16 => DataType::UInt16,
                    ParquetSchemaType::UInt32 => DataType::UInt32,
                    ParquetSchemaType::UInt64 => DataType::UInt64,
                    ParquetSchemaType::Float => DataType::Float32,
                    ParquetSchemaType::Double => DataType::Float64,
                    ParquetSchemaType::String => DataType::Utf8,
                    ParquetSchemaType::Binary => DataType::Binary,
                    ParquetSchemaType::Boolean => DataType::Boolean,
                    ParquetSchemaType::Date32 => DataType::Date32,
                    ParquetSchemaType::TimestampMillis => {
                        DataType::Timestamp(TimeUnit::Millisecond, None)
                    }
                    ParquetSchemaType::TimestampMicros => {
                        DataType::Timestamp(TimeUnit::Microsecond, None)
                    }
                    ParquetSchemaType::List => unimplemented!("List type not yet supported"),
                    ParquetSchemaType::Map => unimplemented!("Map type not yet supported"),
                    ParquetSchemaType::Struct => unimplemented!("Struct type not yet supported"),
                },
                true,
            )
        })
        .collect();
    let arrow_schema = Arc::new(Schema::new(arrow_fields));

    // Create the writer
    let mut writer = create_writer(&ruby, &write_to, arrow_schema.clone())?;

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

                            let values: Vec<Value> = column_array.into_iter().collect();
                            Ok((
                                field.name.clone(),
                                type_conversion::convert_array_to_arrow(values, &field.type_)?,
                            ))
                        })
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

    // Ensure everything is written and get the temp file if it exists
    if let Some(temp_file) = writer.close().map_err(|e| ParquetErrorWrapper(e))? {
        // If we got a temp file back, we need to copy its contents to the IO-like object
        copy_temp_file_to_io_like(temp_file, IoLikeValue(write_to))?;
    }

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
        type_conversion::convert_array_to_arrow(values, &self.type_)
    }
}

pub trait SendableWrite: Send + Write {}
impl<T: Send + Write> SendableWrite for T {}

pub enum WriterOutput {
    File(ArrowWriter<Box<dyn SendableWrite>>),
    TempFile(ArrowWriter<Box<dyn SendableWrite>>, NamedTempFile),
}

impl WriterOutput {
    fn write(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        match self {
            WriterOutput::File(writer) | WriterOutput::TempFile(writer, _) => writer.write(batch),
        }
    }

    fn close(self) -> Result<Option<NamedTempFile>, ParquetError> {
        match self {
            WriterOutput::File(writer) => {
                writer.close()?;
                Ok(None)
            }
            WriterOutput::TempFile(writer, temp_file) => {
                writer.close()?;
                Ok(Some(temp_file))
            }
        }
    }
}

fn create_writer(
    ruby: &Ruby,
    write_to: &Value,
    schema: Arc<Schema>,
) -> Result<WriterOutput, MagnusError> {
    if write_to.is_kind_of(ruby.class_string()) {
        let path = write_to.to_r_string()?.to_string()?;
        let file: Box<dyn SendableWrite> = Box::new(File::create(path).unwrap());
        let writer =
            ArrowWriter::try_new(file, schema, None).map_err(|e| ParquetErrorWrapper(e))?;
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
        let writer =
            ArrowWriter::try_new(file, schema, None).map_err(|e| ParquetErrorWrapper(e))?;
        Ok(WriterOutput::TempFile(writer, temp_file))
    }
}

// Helper function to copy temp file contents to IoLikeValue
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

fn write_batch(
    writer: &mut WriterOutput,
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
            "int8" => Ok(ParquetSchemaType::Int8),
            "int16" => Ok(ParquetSchemaType::Int16),
            "int32" => Ok(ParquetSchemaType::Int32),
            "int64" => Ok(ParquetSchemaType::Int64),
            "uint8" => Ok(ParquetSchemaType::UInt8),
            "uint16" => Ok(ParquetSchemaType::UInt16),
            "uint32" => Ok(ParquetSchemaType::UInt32),
            "uint64" => Ok(ParquetSchemaType::UInt64),
            "float" | "float32" => Ok(ParquetSchemaType::Float),
            "double" | "float64" => Ok(ParquetSchemaType::Double),
            "string" | "utf8" => Ok(ParquetSchemaType::String),
            "binary" => Ok(ParquetSchemaType::Binary),
            "boolean" | "bool" => Ok(ParquetSchemaType::Boolean),
            "date32" => Ok(ParquetSchemaType::Date32),
            "timestamp_millis" => Ok(ParquetSchemaType::TimestampMillis),
            "timestamp_micros" => Ok(ParquetSchemaType::TimestampMicros),
            "list" => Ok(ParquetSchemaType::List),
            "map" => Ok(ParquetSchemaType::Map),
            "struct" => Ok(ParquetSchemaType::Struct),
            _ => Err(MagnusError::new(
                magnus::exception::runtime_error(),
                format!("Invalid schema type: {}", s),
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

struct IoLikeValue(Value);

impl Write for IoLikeValue {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        let ruby_bytes = RString::from_slice(buf);

        let bytes_written = self
            .0
            .funcall::<_, _, usize>("write", (ruby_bytes,))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        Ok(bytes_written)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        self.0
            .funcall::<_, _, Value>("flush", ())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        Ok(())
    }
}
