use std::{
    io::{self, Write},
    str::FromStr,
    sync::Arc,
};

use arrow_array::{Array, RecordBatch};
use magnus::{value::ReprValue, Error as MagnusError, RString, Ruby, Symbol, TryConvert, Value};
use parquet::{arrow::ArrowWriter, errors::ParquetError};
use tempfile::NamedTempFile;

use crate::types::{ListField, MapField, ParquetSchemaType};

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

pub trait SendableWrite: Send + Write {}
impl<T: Send + Write> SendableWrite for T {}

pub struct IoLikeValue(pub(crate) Value);

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
            "list" => Ok(ParquetSchemaType::List(Box::new(ListField {
                item_type: ParquetSchemaType::Int8,
            }))),
            "map" => Ok(ParquetSchemaType::Map(Box::new(MapField {
                key_type: ParquetSchemaType::String,
                value_type: ParquetSchemaType::Int8,
            }))),
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

// We know this type is safe to move between threads because it's just an enum
// with simple primitive types and strings
unsafe impl Send for ParquetSchemaType {}

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

pub enum WriterOutput {
    File(ArrowWriter<Box<dyn SendableWrite>>),
    TempFile(ArrowWriter<Box<dyn SendableWrite>>, NamedTempFile),
}

impl WriterOutput {
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        match self {
            WriterOutput::File(writer) | WriterOutput::TempFile(writer, _) => writer.write(batch),
        }
    }

    pub fn close(self) -> Result<Option<NamedTempFile>, ParquetError> {
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

pub struct ParquetErrorWrapper(pub ParquetError);

impl From<ParquetErrorWrapper> for MagnusError {
    fn from(err: ParquetErrorWrapper) -> Self {
        MagnusError::new(
            magnus::exception::runtime_error(),
            format!("Parquet error: {}", err.0),
        )
    }
}

pub struct ColumnCollector {
    pub name: String,
    pub type_: ParquetSchemaType,
    pub values: Vec<crate::types::ParquetValue>,
}

impl ColumnCollector {
    pub fn new(name: String, type_: ParquetSchemaType) -> Self {
        Self {
            name,
            type_,
            values: Vec::new(),
        }
    }

    pub fn push_value(&mut self, value: Value) -> Result<(), MagnusError> {
        use crate::types::ParquetValue;
        use crate::{
            convert_to_binary, convert_to_boolean, convert_to_date32, convert_to_list,
            convert_to_map, convert_to_timestamp_micros, convert_to_timestamp_millis,
            NumericConverter,
        };

        if value.is_nil() {
            self.values.push(ParquetValue::Null);
            return Ok(());
        }

        let parquet_value = match &self.type_ {
            ParquetSchemaType::Int8 => {
                let v = NumericConverter::<i8>::convert_with_string_fallback(value)?;
                ParquetValue::Int8(v)
            }
            ParquetSchemaType::Int16 => {
                let v = NumericConverter::<i16>::convert_with_string_fallback(value)?;
                ParquetValue::Int16(v)
            }
            ParquetSchemaType::Int32 => {
                let v = NumericConverter::<i32>::convert_with_string_fallback(value)?;
                ParquetValue::Int32(v)
            }
            ParquetSchemaType::Int64 => {
                let v = NumericConverter::<i64>::convert_with_string_fallback(value)?;
                ParquetValue::Int64(v)
            }
            ParquetSchemaType::UInt8 => {
                let v = NumericConverter::<u8>::convert_with_string_fallback(value)?;
                ParquetValue::UInt8(v)
            }
            ParquetSchemaType::UInt16 => {
                let v = NumericConverter::<u16>::convert_with_string_fallback(value)?;
                ParquetValue::UInt16(v)
            }
            ParquetSchemaType::UInt32 => {
                let v = NumericConverter::<u32>::convert_with_string_fallback(value)?;
                ParquetValue::UInt32(v)
            }
            ParquetSchemaType::UInt64 => {
                let v = NumericConverter::<u64>::convert_with_string_fallback(value)?;
                ParquetValue::UInt64(v)
            }
            ParquetSchemaType::Float => {
                let v = NumericConverter::<f32>::convert_with_string_fallback(value)?;
                ParquetValue::Float32(v)
            }
            ParquetSchemaType::Double => {
                let v = NumericConverter::<f64>::convert_with_string_fallback(value)?;
                ParquetValue::Float64(v)
            }
            ParquetSchemaType::String => {
                let v = String::try_convert(value)?;
                ParquetValue::String(v)
            }
            ParquetSchemaType::Binary => {
                let v = convert_to_binary(value)?;
                ParquetValue::Bytes(v)
            }
            ParquetSchemaType::Boolean => {
                let v = convert_to_boolean(value)?;
                ParquetValue::Boolean(v)
            }
            ParquetSchemaType::Date32 => {
                let v = convert_to_date32(value, None)?;
                ParquetValue::Date32(v)
            }
            ParquetSchemaType::TimestampMillis => {
                let v = convert_to_timestamp_millis(value, None)?;
                ParquetValue::TimestampMillis(v, None)
            }
            ParquetSchemaType::TimestampMicros => {
                let v = convert_to_timestamp_micros(value, None)?;
                ParquetValue::TimestampMicros(v, None)
            }
            ParquetSchemaType::List(list_field) => {
                let values = convert_to_list(value, list_field)?;
                ParquetValue::List(values)
            }
            ParquetSchemaType::Map(map_field) => {
                let map = convert_to_map(value, map_field)?;
                ParquetValue::Map(map)
            }
        };
        self.values.push(parquet_value);
        Ok(())
    }

    pub fn take_array(&mut self) -> Result<Arc<dyn Array>, MagnusError> {
        let values = std::mem::take(&mut self.values);
        crate::convert_parquet_values_to_arrow(values, &self.type_)
    }
}
