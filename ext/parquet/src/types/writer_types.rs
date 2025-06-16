use super::{core_types::SchemaNode, ParquetGemError, PrimitiveType};
use crate::{
    types::{ListField, MapField, ParquetSchemaType},
    utils::parse_string_or_symbol,
};
use arrow_array::{Array, RecordBatch};
use magnus::{value::ReprValue, Error as MagnusError, RString, Ruby, TryConvert, Value};
use parquet::{arrow::ArrowWriter, errors::ParquetError};
use std::{
    io::{self, Write},
    str::FromStr,
    sync::Arc,
};
use tempfile::NamedTempFile;

#[derive(Debug, Clone)]
pub struct SchemaField<'a> {
    pub name: String,
    pub type_: ParquetSchemaType<'a>,
    pub format: Option<String>,
    pub nullable: bool,
}

#[derive(Debug)]
pub struct ParquetWriteArgs {
    pub read_from: Value,
    pub write_to: Value,
    pub schema: SchemaNode,
    pub batch_size: Option<usize>,
    pub flush_threshold: Option<usize>,
    pub compression: Option<String>,
    pub sample_size: Option<usize>,
    pub logger: Option<Value>,
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

impl FromStr for ParquetSchemaType<'_> {
    type Err = MagnusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Check if it's a list type
        if let Some(inner_type_str) = s.strip_prefix("list<").and_then(|s| s.strip_suffix(">")) {
            let inner_type = inner_type_str.parse::<ParquetSchemaType>()?;
            return Ok(ParquetSchemaType::List(Box::new(ListField {
                item_type: inner_type,
                format: None,
                nullable: true,
            })));
        }

        // Check if it's a map type
        if let Some(kv_types_str) = s.strip_prefix("map<").and_then(|s| s.strip_suffix(">")) {
            let parts: Vec<&str> = kv_types_str.splitn(2, ',').collect();
            if parts.len() != 2 {
                return Err(MagnusError::new(
                    magnus::exception::runtime_error(),
                    format!(
                        "Invalid map format. Expected 'map<keyType,valueType>', got '{}'",
                        s
                    ),
                ));
            }

            let key_type = parts[0].trim().parse::<ParquetSchemaType>()?;
            let value_type = parts[1].trim().parse::<ParquetSchemaType>()?;

            return Ok(ParquetSchemaType::Map(Box::new(MapField {
                key_type,
                value_type,
                key_format: None,
                value_format: None,
                value_nullable: true,
            })));
        }

        // Check if it's a decimal type with precision and scale
        if let Some(decimal_params) = s.strip_prefix("decimal(").and_then(|s| s.strip_suffix(")")) {
            let parts: Vec<&str> = decimal_params.split(',').collect();

            // Handle both single parameter (precision only) and two parameters (precision and scale)
            if parts.len() == 1 {
                // Only precision provided, scale defaults to 0
                let precision = parts[0].trim().parse::<u8>().map_err(|_| {
                    MagnusError::new(
                        magnus::exception::runtime_error(),
                        format!("Invalid precision value in decimal type: {}", parts[0]),
                    )
                })?;

                return Ok(ParquetSchemaType::Primitive(PrimitiveType::Decimal128(
                    precision, 0,
                )));
            } else if parts.len() == 2 {
                // Both precision and scale provided
                let precision = parts[0].trim().parse::<u8>().map_err(|_| {
                    MagnusError::new(
                        magnus::exception::runtime_error(),
                        format!("Invalid precision value in decimal type: {}", parts[0]),
                    )
                })?;

                let scale = parts[1].trim().parse::<i8>().map_err(|_| {
                    MagnusError::new(
                        magnus::exception::runtime_error(),
                        format!("Invalid scale value in decimal type: {}", parts[1]),
                    )
                })?;

                return Ok(ParquetSchemaType::Primitive(PrimitiveType::Decimal128(
                    precision, scale,
                )));
            } else {
                return Err(MagnusError::new(
                    magnus::exception::runtime_error(),
                    format!(
                        "Invalid decimal format. Expected 'decimal(precision)' or 'decimal(precision,scale)', got '{}'",
                        s
                    ),
                ));
            }
        }

        // Check if it's a decimal256 type with precision and scale
        if let Some(decimal_params) = s.strip_prefix("decimal256(").and_then(|s| s.strip_suffix(")")) {
            let parts: Vec<&str> = decimal_params.split(',').collect();

            // Handle both single parameter (precision only) and two parameters (precision and scale)
            if parts.len() == 1 {
                // Only precision provided, scale defaults to 0
                let precision = parts[0].trim().parse::<u8>().map_err(|_| {
                    MagnusError::new(
                        magnus::exception::runtime_error(),
                        format!("Invalid precision value in decimal256 type: {}", parts[0]),
                    )
                })?;

                return Ok(ParquetSchemaType::Primitive(PrimitiveType::Decimal256(
                    precision, 0,
                )));
            } else if parts.len() == 2 {
                // Both precision and scale provided
                let precision = parts[0].trim().parse::<u8>().map_err(|_| {
                    MagnusError::new(
                        magnus::exception::runtime_error(),
                        format!("Invalid precision value in decimal256 type: {}", parts[0]),
                    )
                })?;

                let scale = parts[1].trim().parse::<i8>().map_err(|_| {
                    MagnusError::new(
                        magnus::exception::runtime_error(),
                        format!("Invalid scale value in decimal256 type: {}", parts[1]),
                    )
                })?;

                return Ok(ParquetSchemaType::Primitive(PrimitiveType::Decimal256(
                    precision, scale,
                )));
            } else {
                return Err(MagnusError::new(
                    magnus::exception::runtime_error(),
                    format!(
                        "Invalid decimal256 format. Expected 'decimal256(precision)' or 'decimal256(precision,scale)', got '{}'",
                        s
                    ),
                ));
            }
        }

        // Handle primitive types
        match s {
            "int8" => Ok(ParquetSchemaType::Primitive(PrimitiveType::Int8)),
            "int16" => Ok(ParquetSchemaType::Primitive(PrimitiveType::Int16)),
            "int32" => Ok(ParquetSchemaType::Primitive(PrimitiveType::Int32)),
            "int64" => Ok(ParquetSchemaType::Primitive(PrimitiveType::Int64)),
            "uint8" => Ok(ParquetSchemaType::Primitive(PrimitiveType::UInt8)),
            "uint16" => Ok(ParquetSchemaType::Primitive(PrimitiveType::UInt16)),
            "uint32" => Ok(ParquetSchemaType::Primitive(PrimitiveType::UInt32)),
            "uint64" => Ok(ParquetSchemaType::Primitive(PrimitiveType::UInt64)),
            "float" | "float32" => Ok(ParquetSchemaType::Primitive(PrimitiveType::Float32)),
            "double" | "float64" => Ok(ParquetSchemaType::Primitive(PrimitiveType::Float64)),
            "string" | "utf8" => Ok(ParquetSchemaType::Primitive(PrimitiveType::String)),
            "binary" => Ok(ParquetSchemaType::Primitive(PrimitiveType::Binary)),
            "boolean" | "bool" => Ok(ParquetSchemaType::Primitive(PrimitiveType::Boolean)),
            "date32" => Ok(ParquetSchemaType::Primitive(PrimitiveType::Date32)),
            "timestamp_millis" => Ok(ParquetSchemaType::Primitive(PrimitiveType::TimestampMillis)),
            "timestamp_micros" => Ok(ParquetSchemaType::Primitive(PrimitiveType::TimestampMicros)),
            "decimal" => Ok(ParquetSchemaType::Primitive(PrimitiveType::Decimal128(
                38, 0,
            ))),
            "decimal256" => Ok(ParquetSchemaType::Primitive(PrimitiveType::Decimal256(
                38, 0,
            ))),
            "list" => Ok(ParquetSchemaType::List(Box::new(ListField {
                item_type: ParquetSchemaType::Primitive(PrimitiveType::String),
                format: None,
                nullable: true,
            }))),
            _ => Err(MagnusError::new(
                magnus::exception::runtime_error(),
                format!("Invalid schema type: {}", s),
            )),
        }
    }
}

impl TryConvert for ParquetSchemaType<'_> {
    fn try_convert(value: Value) -> Result<Self, MagnusError> {
        let ruby = unsafe { Ruby::get_unchecked() };
        let schema_type = parse_string_or_symbol(&ruby, value)?;

        schema_type
            .ok_or_else(|| {
                MagnusError::new(magnus::exception::type_error(), "Invalid schema type")
            })?
            .parse()
    }
}

// We know this type is safe to move between threads because it's just an enum
// with simple primitive types and strings
unsafe impl Send for ParquetSchemaType<'_> {}

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

pub struct ColumnCollector<'a> {
    pub ruby: &'a Ruby,
    pub name: String,
    pub type_: ParquetSchemaType<'a>,
    pub format: Option<String>,
    pub nullable: bool,
    pub values: Vec<crate::types::ParquetValue>,
}

impl<'a> ColumnCollector<'a> {
    pub fn new(
        ruby: &'a Ruby,
        name: String,
        type_: ParquetSchemaType<'a>,
        format: Option<String>,
        nullable: bool,
    ) -> Self {
        Self {
            ruby,
            name,
            type_,
            format,
            nullable,
            values: Vec::new(),
        }
    }

    pub fn push_value(&mut self, value: Value) -> Result<(), MagnusError> {
        use crate::types::ParquetValue;

        if value.is_nil() && !self.nullable {
            // For non-nullable fields, raise an error
            return Err(MagnusError::new(
                magnus::exception::runtime_error(),
                "Cannot write nil value for non-nullable field",
            ));
        }

        // For all other types, proceed as normal
        let parquet_value =
            ParquetValue::from_value(self.ruby, value, &self.type_, self.format.as_deref())?;
        self.values.push(parquet_value);
        Ok(())
    }

    pub fn take_array(&mut self) -> Result<Arc<dyn Array>, ParquetGemError> {
        let values = std::mem::take(&mut self.values);
        crate::convert_parquet_values_to_arrow(values, &self.type_)
    }
}
