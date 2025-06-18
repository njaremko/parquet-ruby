use std::str::FromStr;
use std::sync::Arc;

use super::*;
use arrow_array::builder::MapFieldNames;
use arrow_array::builder::*;
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use jiff::tz::{Offset, TimeZone};
use magnus::{RArray, RString, TryConvert};

pub struct NumericConverter<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> NumericConverter<T>
where
    T: TryConvert + FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    pub fn convert_with_string_fallback(ruby: &Ruby, value: Value) -> Result<T, MagnusError> {
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

pub fn convert_to_date32(
    ruby: &Ruby,
    value: Value,
    format: Option<&str>,
) -> Result<i32, MagnusError> {
    if value.is_kind_of(ruby.class_string()) {
        let s = String::try_convert(value)?;
        // Parse string into Date using jiff
        let date = if let Some(fmt) = format {
            jiff::civil::Date::strptime(fmt, &s).or_else(|e1| {
                // Try parsing as DateTime and convert to Date with zero offset
                jiff::civil::DateTime::strptime(fmt, &s)
                    .and_then(|dt| dt.to_zoned(TimeZone::fixed(Offset::constant(0))))
                    .map(|dt| dt.date())
                    .map_err(|e2| {
                        MagnusError::new(
                            magnus::exception::type_error(),
                            format!(
                                "Failed to parse '{}' with format '{}' as date32: {} (and as datetime: {})",
                                s, fmt, e1, e2
                            ),
                        )
                    })
            })?
        } else {
            s.parse().map_err(|e| {
                MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Failed to parse '{}' as date32: {}", s, e),
                )
            })?
        };

        let timestamp = date.at(0, 0, 0, 0);

        let x = timestamp
            .to_zoned(TimeZone::fixed(Offset::constant(0)))
            .map_err(|e| {
                MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Failed to convert date32 to timestamp: {}", e),
                )
            })?
            .timestamp();

        // Convert to epoch days
        Ok((x.as_second() / 86400) as i32)
    } else if value.is_kind_of(ruby.class_time()) {
        // Convert Time object to epoch days
        let secs = i64::try_convert(value.funcall::<_, _, Value>("to_i", ())?)?;
        Ok(((secs as f64) / 86400.0) as i32)
    } else {
        Err(MagnusError::new(
            magnus::exception::type_error(),
            format!("Cannot convert {} to date32", unsafe { value.classname() }),
        ))
    }
}

pub fn convert_to_timestamp_millis(
    ruby: &Ruby,
    value: Value,
    format: Option<&str>,
) -> Result<i64, MagnusError> {
    if value.is_kind_of(ruby.class_string()) {
        let s = String::try_convert(value)?;
        // Parse string into Timestamp using jiff
        let timestamp = if let Some(fmt) = format {
            jiff::Timestamp::strptime(fmt, &s)
                .or_else(|e1| {
                    // Try parsing as DateTime and convert to Timestamp with zero offset
                    jiff::civil::DateTime::strptime(fmt, &s)
                        .and_then(|dt| dt.to_zoned(TimeZone::fixed(Offset::constant(0))))
                        .map(|dt| dt.timestamp())
                        .map_err(|e2| {
                            MagnusError::new(
                                magnus::exception::type_error(),
                                format!(
                                    "Failed to parse '{}' with format '{}' as timestamp_millis: {} (and as datetime: {})",
                                    s, fmt, e1, e2
                                ),
                            )
                        })
                })?
        } else {
            s.parse().map_err(|e| {
                MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Failed to parse '{}' as timestamp_millis: {}", s, e),
                )
            })?
        };
        // Convert to milliseconds
        Ok(timestamp.as_millisecond())
    } else if value.is_kind_of(ruby.class_time()) {
        // Convert Time object to milliseconds
        let secs = i64::try_convert(value.funcall::<_, _, Value>("to_i", ())?)?;
        let usecs = i64::try_convert(value.funcall::<_, _, Value>("usec", ())?)?;
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

pub fn convert_to_timestamp_micros(
    ruby: &Ruby,
    value: Value,
    format: Option<&str>,
) -> Result<i64, MagnusError> {
    if value.is_kind_of(ruby.class_string()) {
        let s = String::try_convert(value)?;
        // Parse string into Timestamp using jiff
        let timestamp = if let Some(fmt) = format {
            jiff::Timestamp::strptime(fmt, &s).or_else(|e1| {
                // Try parsing as DateTime and convert to Timestamp with zero offset
                jiff::civil::DateTime::strptime(fmt, &s).and_then(|dt| {
                    dt.to_zoned(TimeZone::fixed(Offset::constant(0)))
                })
                .map(|dt| dt.timestamp())
                .map_err(|e2| {
                    MagnusError::new(
                        magnus::exception::type_error(),
                        format!(
                            "Failed to parse '{}' with format '{}' as timestamp_micros: {} (and as datetime: {})",
                            s, fmt, e1, e2
                        ),
                    )
                })
            })?
        } else {
            s.parse().map_err(|e| {
                MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Failed to parse '{}' as timestamp_micros: {}", s, e),
                )
            })?
        };
        // Convert to microseconds
        Ok(timestamp.as_microsecond())
    } else if value.is_kind_of(ruby.class_time()) {
        // Convert Time object to microseconds
        let secs = i64::try_convert(value.funcall::<_, _, Value>("to_i", ())?)?;
        let usecs = i64::try_convert(value.funcall::<_, _, Value>("usec", ())?)?;
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

pub fn convert_to_boolean(ruby: &Ruby, value: Value) -> Result<bool, MagnusError> {
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

pub fn convert_to_string(value: Value) -> Result<String, MagnusError> {
    String::try_convert(value).or_else(|_| {
        if value.respond_to("to_s", false)? {
            value.funcall::<_, _, RString>("to_s", ())?.to_string()
        } else if value.respond_to("to_str", false)? {
            value.funcall::<_, _, RString>("to_str", ())?.to_string()
        } else {
            Err(MagnusError::new(
                magnus::exception::type_error(),
                format!("Not able to convert {:?} to String", value),
            ))
        }
    })
}

/// Converts our custom `ParquetSchemaType` into an Arrow `DataType`.
/// This ensures proper nullability settings for nested types.
/// Converts a ParquetSchemaType to an Arrow DataType
pub fn parquet_schema_type_to_arrow_data_type(
    schema_type: &ParquetSchemaType,
) -> Result<DataType, MagnusError> {
    Ok(match schema_type {
        ParquetSchemaType::Primitive(primative) => match primative {
            PrimitiveType::Int8 => DataType::Int8,
            PrimitiveType::Int16 => DataType::Int16,
            PrimitiveType::Int32 => DataType::Int32,
            PrimitiveType::Int64 => DataType::Int64,
            PrimitiveType::UInt8 => DataType::UInt8,
            PrimitiveType::UInt16 => DataType::UInt16,
            PrimitiveType::UInt32 => DataType::UInt32,
            PrimitiveType::UInt64 => DataType::UInt64,
            PrimitiveType::Float32 => DataType::Float32,
            PrimitiveType::Float64 => DataType::Float64,
            PrimitiveType::Decimal128(precision, scale) => DataType::Decimal128(*precision, *scale),
            PrimitiveType::Decimal256(precision, scale) => DataType::Decimal256(*precision, *scale),
            PrimitiveType::String => DataType::Utf8,
            PrimitiveType::Binary => DataType::Binary,
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Date32 => DataType::Date32,
            PrimitiveType::TimestampMillis => DataType::Timestamp(TimeUnit::Millisecond, None),
            PrimitiveType::TimestampMicros => DataType::Timestamp(TimeUnit::Microsecond, None),
            PrimitiveType::TimeMillis => DataType::Time32(TimeUnit::Millisecond),
            PrimitiveType::TimeMicros => DataType::Time64(TimeUnit::Microsecond),
        },
        // For a List<T>, create a standard List in Arrow with nullable items
        ParquetSchemaType::List(list_field) => {
            let child_type = parquet_schema_type_to_arrow_data_type(&list_field.item_type)?;
            // For a list, use empty field name to match expectations for schema_dsl test
            // This is the critical fix for the schema_dsl test which expects an empty field name
            // Use empty field name for all list field items - this is crucial for compatibility
            DataType::List(Arc::new(Field::new(
                "item",
                child_type,
                list_field.nullable,
            )))
        }

        // For a Map<K, V>, ensure entries field is non-nullable and key field is non-nullable
        ParquetSchemaType::Map(map_field) => {
            let key_arrow_type = parquet_schema_type_to_arrow_data_type(&map_field.key_type)?;
            let value_arrow_type = parquet_schema_type_to_arrow_data_type(&map_field.value_type)?;
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", key_arrow_type, false), // key must be non-null
                        Field::new("value", value_arrow_type, true), // value can be null
                    ])),
                    /*nullable=*/ false, // crucial: entries must be non-nullable
                )),
                /*keys_sorted=*/ false,
            )
        }
        ParquetSchemaType::Struct(struct_field) => {
            if struct_field.fields.is_empty() {
                return Err(MagnusError::new(
                    magnus::exception::runtime_error(),
                    "Cannot create a struct with zero subfields (empty struct).",
                ));
            }

            // Build arrow fields
            let mut arrow_fields = Vec::with_capacity(struct_field.fields.len());

            for field in &struct_field.fields {
                let field_type = parquet_schema_type_to_arrow_data_type(&field.type_)?;
                arrow_fields.push(Field::new(&field.name, field_type, true)); // All fields are nullable by default
            }

            DataType::Struct(Fields::from(arrow_fields))
        }
    })
}

#[macro_export]
macro_rules! impl_timestamp_array_conversion {
    ($column:expr, $array_type:ty, $variant:ident, $tz:expr) => {{
        let array = downcast_array::<$array_type>($column);
        Ok(ParquetValueVec(if array.is_nullable() {
            array
                .values()
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    if array.is_null(i) {
                        ParquetValue::Null
                    } else {
                        ParquetValue::$variant(*x, $tz.clone().map(|s| s.into()))
                    }
                })
                .collect()
        } else {
            array
                .values()
                .iter()
                .map(|x| ParquetValue::$variant(*x, $tz.clone().map(|s| s.into())))
                .collect()
        }))
    }};
}

// Create the appropriate Arrow builder for a given ParquetSchemaType.
// We return a Box<dyn ArrayBuilder> so we can dynamically downcast.
fn create_arrow_builder_for_type(
    type_: &ParquetSchemaType,
    capacity: Option<usize>,
) -> Result<Box<dyn ArrayBuilder>, ParquetGemError> {
    let cap = capacity.unwrap_or(1); // Default to at least capacity 1 to avoid empty builders
    match type_ {
        ParquetSchemaType::Primitive(PrimitiveType::Int8) => {
            Ok(Box::new(Int8Builder::with_capacity(cap)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::Int16) => {
            Ok(Box::new(Int16Builder::with_capacity(cap)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::Int32) => {
            Ok(Box::new(Int32Builder::with_capacity(cap)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::Int64) => {
            Ok(Box::new(Int64Builder::with_capacity(cap)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::UInt8) => {
            Ok(Box::new(UInt8Builder::with_capacity(cap)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::UInt16) => {
            Ok(Box::new(UInt16Builder::with_capacity(cap)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::UInt32) => {
            Ok(Box::new(UInt32Builder::with_capacity(cap)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::UInt64) => {
            Ok(Box::new(UInt64Builder::with_capacity(cap)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::Float32) => {
            Ok(Box::new(Float32Builder::with_capacity(cap)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::Float64) => {
            Ok(Box::new(Float64Builder::with_capacity(cap)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::Decimal128(precision, scale)) => {
            // Create a Decimal128Builder with specific precision and scale
            let builder = Decimal128Builder::with_capacity(cap);

            // Set precision and scale for the decimal and return the new builder
            let builder_with_precision = builder
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| {
                    MagnusError::new(
                        magnus::exception::runtime_error(),
                        format!("Failed to set precision and scale: {}", e),
                    )
                })?;

            Ok(Box::new(builder_with_precision))
        }
        ParquetSchemaType::Primitive(PrimitiveType::Decimal256(precision, scale)) => {
            // Create a Decimal128Builder since we're truncating Decimal256 to Decimal128
            let builder = Decimal256Builder::with_capacity(cap);

            // Set precision and scale for the decimal and return the new builder
            let builder_with_precision = builder
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| {
                    MagnusError::new(
                        magnus::exception::runtime_error(),
                        format!("Failed to set precision and scale: {}", e),
                    )
                })?;

            Ok(Box::new(builder_with_precision))
        }
        ParquetSchemaType::Primitive(PrimitiveType::String) => {
            Ok(Box::new(StringBuilder::with_capacity(cap, cap * 32)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::Binary) => {
            Ok(Box::new(BinaryBuilder::with_capacity(cap, cap * 32)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::Boolean) => {
            Ok(Box::new(BooleanBuilder::with_capacity(cap)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::Date32) => {
            Ok(Box::new(Date32Builder::with_capacity(cap)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::TimestampMillis) => {
            Ok(Box::new(TimestampMillisecondBuilder::with_capacity(cap)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::TimestampMicros) => {
            Ok(Box::new(TimestampMicrosecondBuilder::with_capacity(cap)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::TimeMillis) => {
            Ok(Box::new(Time32MillisecondBuilder::with_capacity(cap)))
        }
        ParquetSchemaType::Primitive(PrimitiveType::TimeMicros) => {
            Ok(Box::new(Time64MicrosecondBuilder::with_capacity(cap)))
        }
        ParquetSchemaType::List(list_field) => {
            // For a list, we create a ListBuilder whose child builder is determined by item_type.
            // Pass through capacity to ensure consistent sizing
            let child_builder = create_arrow_builder_for_type(&list_field.item_type, Some(cap))?;

            // Ensure consistent builder capacity for lists
            Ok(Box::new(ListBuilder::<Box<dyn ArrayBuilder>>::new(
                child_builder,
            )))
        }
        ParquetSchemaType::Map(map_field) => {
            // A Map is physically a list<struct<key:..., value:...>> in Arrow.
            // Pass through capacity to ensure consistent sizing
            let key_builder = create_arrow_builder_for_type(&map_field.key_type, Some(cap))?;
            let value_builder = create_arrow_builder_for_type(&map_field.value_type, Some(cap))?;

            // Create a MapBuilder with explicit field names to ensure compatibility
            Ok(Box::new(MapBuilder::<
                Box<dyn ArrayBuilder>,
                Box<dyn ArrayBuilder>,
            >::new(
                Some(MapFieldNames {
                    entry: "entries".to_string(),
                    key: "key".to_string(),
                    value: "value".to_string(),
                }),
                key_builder,
                value_builder,
            )))
        }
        ParquetSchemaType::Struct(struct_field) => {
            // Check for empty struct immediately
            if struct_field.fields.is_empty() {
                Err(MagnusError::new(
                    magnus::exception::runtime_error(),
                    "Cannot build a struct with zero fields - Parquet doesn't support empty structs".to_string(),
                ))?;
            }

            // Create a child builder for each field in the struct
            let mut child_field_builders = Vec::with_capacity(struct_field.fields.len());

            // Get struct data type first to ensure field compatibility
            let data_type = parquet_schema_type_to_arrow_data_type(type_)?;

            // Make sure the data type is a struct
            let arrow_fields = if let DataType::Struct(ref fields) = data_type {
                fields.clone()
            } else {
                return Err(MagnusError::new(
                    magnus::exception::type_error(),
                    "Expected struct data type".to_string(),
                ))?;
            };

            // Create builders for each child field with consistent capacity
            for child in &struct_field.fields {
                let sub_builder = create_arrow_builder_for_type(&child.type_, Some(cap))?;
                child_field_builders.push(sub_builder);
            }

            // Make sure we have the right number of builders
            if child_field_builders.len() != arrow_fields.len() {
                Err(MagnusError::new(
                    magnus::exception::runtime_error(),
                    format!(
                        "Number of field builders ({}) doesn't match number of arrow fields ({})",
                        child_field_builders.len(),
                        arrow_fields.len()
                    ),
                ))?;
            }

            // Create the StructBuilder with the fields and child builders
            Ok(Box::new(StructBuilder::new(
                arrow_fields,
                child_field_builders,
            )))
        }
    }
}

// Fill primitive scalar Int8 values
fn fill_int8_builder(
    builder: &mut dyn ArrayBuilder,
    values: &[ParquetValue],
) -> Result<(), MagnusError> {
    let typed_builder = builder
        .as_any_mut()
        .downcast_mut::<Int8Builder>()
        .expect("Builder mismatch: expected Int8Builder");
    for val in values {
        match val {
            ParquetValue::Int8(i) => typed_builder.append_value(*i),
            // Handle Int64 that could be an Int8
            ParquetValue::Int64(i) => {
                if *i < i8::MIN as i64 || *i > i8::MAX as i64 {
                    return Err(MagnusError::new(
                        magnus::exception::range_error(),
                        format!("Integer {} is out of range for Int8", i),
                    ));
                }
                typed_builder.append_value(*i as i8)
            }
            ParquetValue::Null => typed_builder.append_null(),
            other => {
                return Err(MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Expected Int8, got {:?}", other),
                ))
            }
        }
    }
    Ok(())
}

// Fill primitive scalar Int16 values
fn fill_int16_builder(
    builder: &mut dyn ArrayBuilder,
    values: &[ParquetValue],
) -> Result<(), MagnusError> {
    let typed_builder = builder
        .as_any_mut()
        .downcast_mut::<Int16Builder>()
        .expect("Builder mismatch: expected Int16Builder");
    for val in values {
        match val {
            ParquetValue::Int16(i) => typed_builder.append_value(*i),
            // Handle Int64 that could be an Int16
            ParquetValue::Int64(i) => {
                if *i < i16::MIN as i64 || *i > i16::MAX as i64 {
                    return Err(MagnusError::new(
                        magnus::exception::range_error(),
                        format!("Integer {} is out of range for Int16", i),
                    ));
                }
                typed_builder.append_value(*i as i16)
            }
            ParquetValue::Null => typed_builder.append_null(),
            other => {
                return Err(MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Expected Int16, got {:?}", other),
                ))
            }
        }
    }
    Ok(())
}

// Fill list values by recursively filling child items
fn fill_list_builder(
    builder: &mut dyn ArrayBuilder,
    item_type: &ParquetSchemaType,
    values: &[ParquetValue],
) -> Result<(), MagnusError> {
    // We need to use a more specific type for ListBuilder to help Rust's type inference
    let lb = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
        .expect("Builder mismatch: expected ListBuilder");

    for val in values {
        if let ParquetValue::Null = val {
            // null list
            lb.append(false);
        } else if let ParquetValue::List(list_items) = val {
            // First fill the child builder with the items
            let values_builder = lb.values();
            fill_builder(values_builder, item_type, list_items)?;
            // Then finalize the list by calling append(true)
            lb.append(true);
        } else {
            return Err(MagnusError::new(
                magnus::exception::type_error(),
                format!("Expected ParquetValue::List(...) or Null, got {:?}", val),
            ));
        }
    }

    Ok(())
}

// Fill map values by recursively filling key and value items
fn fill_map_builder(
    builder: &mut dyn ArrayBuilder,
    key_type: &ParquetSchemaType,
    value_type: &ParquetSchemaType,
    values: &[ParquetValue],
) -> Result<(), MagnusError> {
    let mb = builder
        .as_any_mut()
        .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
        .expect("Builder mismatch: expected MapBuilder");

    for val in values {
        match val {
            ParquetValue::Null => {
                // null map
                mb.append(false).map_err(|e| {
                    MagnusError::new(
                        magnus::exception::runtime_error(),
                        format!("Failed to append null to map: {}", e),
                    )
                })?;
            }
            ParquetValue::Map(map_entries) => {
                // First append all key-value pairs to the child arrays
                for (k, v) in map_entries {
                    // Note: Arrow expects field names "key" and "value" (singular)
                    fill_builder(mb.keys(), key_type, &[k.clone()])?;
                    fill_builder(mb.values(), value_type, &[v.clone()])?;
                }
                // Then finalize the map by calling append(true)
                mb.append(true).map_err(|e| {
                    MagnusError::new(
                        magnus::exception::runtime_error(),
                        format!("Failed to append map entry: {}", e),
                    )
                })?;
            }
            other => {
                return Err(MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Expected ParquetValue::Map(...) or Null, got {:?}", other),
                ))
            }
        }
    }

    Ok(())
}

// Append an entire slice of ParquetValue into the given Arrow builder.
// We do a `match` on the type for each item, recursing for nested list/map.
fn fill_builder(
    builder: &mut dyn ArrayBuilder,
    type_: &ParquetSchemaType,
    values: &[ParquetValue],
) -> Result<(), MagnusError> {
    match type_ {
        // ------------------
        // PRIMITIVE SCALARS - delegated to specialized helpers
        // ------------------
        ParquetSchemaType::Primitive(PrimitiveType::Int8) => fill_int8_builder(builder, values),
        ParquetSchemaType::Primitive(PrimitiveType::Int16) => fill_int16_builder(builder, values),
        ParquetSchemaType::Primitive(PrimitiveType::Int32) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .expect("Builder mismatch: expected Int32Builder");
            for val in values {
                match val {
                    ParquetValue::Int32(i) => typed_builder.append_value(*i),
                    ParquetValue::Date32(d) => typed_builder.append_value(*d), // if you allow date->int
                    // Handle the case where we have an Int64 in an Int32 field (common with Ruby Integers)
                    ParquetValue::Int64(i) => {
                        if *i < i32::MIN as i64 || *i > i32::MAX as i64 {
                            return Err(MagnusError::new(
                                magnus::exception::range_error(),
                                format!("Integer {} is out of range for Int32", i),
                            ));
                        }
                        typed_builder.append_value(*i as i32)
                    }
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected Int32, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::Int64) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .expect("Builder mismatch: expected Int64Builder");
            for val in values {
                match val {
                    ParquetValue::Int64(i) => typed_builder.append_value(*i),
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected Int64, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::UInt8) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<UInt8Builder>()
                .expect("Builder mismatch: expected UInt8Builder");
            for val in values {
                match val {
                    ParquetValue::UInt8(u) => typed_builder.append_value(*u),
                    // Handle Int64 that could be a UInt8
                    ParquetValue::Int64(i) => {
                        if *i < 0 || *i > u8::MAX as i64 {
                            return Err(MagnusError::new(
                                magnus::exception::range_error(),
                                format!("Integer {} is out of range for UInt8", i),
                            ));
                        }
                        typed_builder.append_value(*i as u8)
                    }
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected UInt8, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::UInt16) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<UInt16Builder>()
                .expect("Builder mismatch: expected UInt16Builder");
            for val in values {
                match val {
                    ParquetValue::UInt16(u) => typed_builder.append_value(*u),
                    // Handle Int64 that could be a UInt16
                    ParquetValue::Int64(i) => {
                        if *i < 0 || *i > u16::MAX as i64 {
                            return Err(MagnusError::new(
                                magnus::exception::range_error(),
                                format!("Integer {} is out of range for UInt16", i),
                            ));
                        }
                        typed_builder.append_value(*i as u16)
                    }
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected UInt16, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::UInt32) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<UInt32Builder>()
                .expect("Builder mismatch: expected UInt32Builder");
            for val in values {
                match val {
                    ParquetValue::UInt32(u) => typed_builder.append_value(*u),
                    // Handle Int64 that could be a UInt32
                    ParquetValue::Int64(i) => {
                        if *i < 0 || *i > u32::MAX as i64 {
                            return Err(MagnusError::new(
                                magnus::exception::range_error(),
                                format!("Integer {} is out of range for UInt32", i),
                            ));
                        }
                        typed_builder.append_value(*i as u32)
                    }
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected UInt32, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::UInt64) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .expect("Builder mismatch: expected UInt64Builder");
            for val in values {
                match val {
                    ParquetValue::UInt64(u) => typed_builder.append_value(*u),
                    // Handle Int64 that could be a UInt64
                    ParquetValue::Int64(i) => {
                        if *i < 0 {
                            return Err(MagnusError::new(
                                magnus::exception::range_error(),
                                format!("Integer {} is out of range for UInt64", i),
                            ));
                        }
                        typed_builder.append_value(*i as u64)
                    }
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected UInt64, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::Float32) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .expect("Builder mismatch: expected Float32Builder");
            for val in values {
                match val {
                    ParquetValue::Float32(f) => typed_builder.append_value(*f),
                    ParquetValue::Float16(fh) => typed_builder.append_value(*fh),
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected Float32, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::Float64) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .expect("Builder mismatch: expected Float64Builder");
            for val in values {
                match val {
                    ParquetValue::Float64(f) => typed_builder.append_value(*f),
                    // If you want to allow f32 => f64, do so:
                    ParquetValue::Float32(flo) => typed_builder.append_value(*flo as f64),
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected Float64, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::Decimal128(_precision, scale)) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<Decimal128Builder>()
                .expect("Builder mismatch: expected Float64Builder");

            for val in values {
                match val {
                    ParquetValue::Decimal128(d, _scale) => typed_builder.append_value(*d),
                    ParquetValue::Float64(f) => {
                        // Scale the float to the desired precision and scale
                        let scaled_value = (*f * 10_f64.powi(*scale as i32)) as i128;
                        typed_builder.append_value(scaled_value)
                    }
                    ParquetValue::Float32(flo) => {
                        // Scale the float to the desired precision and scale
                        let scaled_value = (*flo as f64 * 10_f64.powi(*scale as i32)) as i128;
                        typed_builder.append_value(scaled_value)
                    }
                    ParquetValue::Int64(i) => {
                        // Scale the integer to the desired scale
                        let scaled_value = (*i as i128) * 10_i128.pow(*scale as u32);
                        typed_builder.append_value(scaled_value)
                    }
                    ParquetValue::Int32(i) => {
                        // Scale the integer to the desired scale
                        let scaled_value = (*i as i128) * 10_i128.pow(*scale as u32);
                        typed_builder.append_value(scaled_value)
                    }
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected Float64, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::Decimal256(_precision, scale)) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<Decimal256Builder>()
                .expect("Builder mismatch: expected Decimal256Builder for Decimal256");

            for val in values {
                match val {
                    ParquetValue::Decimal256(d, _scale) => typed_builder.append_value(*d),
                    ParquetValue::Decimal128(d, _scale) => {
                        // Convert i128 to i256
                        typed_builder.append_value(arrow_buffer::i256::from_i128(*d))
                    }
                    ParquetValue::Float64(f) => {
                        // Scale the float to the desired precision and scale
                        // For large values, use BigInt to avoid overflow
                        let scaled = *f * 10_f64.powi(*scale as i32);
                        if scaled >= i128::MIN as f64 && scaled <= i128::MAX as f64 {
                            let scaled_value = scaled as i128;
                            typed_builder.append_value(arrow_buffer::i256::from_i128(scaled_value))
                        } else {
                            // Use BigInt for values that don't fit in i128
                            use num::{BigInt, FromPrimitive};
                            let bigint = BigInt::from_f64(scaled).ok_or_else(|| {
                                MagnusError::new(
                                    magnus::exception::type_error(),
                                    format!("Failed to convert float {} to BigInt", f),
                                )
                            })?;
                            let bytes = bigint.to_signed_bytes_le();
                            if bytes.len() <= 32 {
                                let mut buf = if bigint.sign() == num::bigint::Sign::Minus {
                                    [0xff; 32]
                                } else {
                                    [0; 32]
                                };
                                buf[..bytes.len()].copy_from_slice(&bytes);
                                typed_builder.append_value(arrow_buffer::i256::from_le_bytes(buf))
                            } else {
                                return Err(MagnusError::new(
                                    magnus::exception::type_error(),
                                    format!(
                                        "Float value {} scaled to {} is too large for Decimal256",
                                        f, scaled
                                    ),
                                ));
                            }
                        }
                    }
                    ParquetValue::Float32(flo) => {
                        // Scale the float to the desired precision and scale
                        let scaled = (*flo as f64) * 10_f64.powi(*scale as i32);
                        if scaled >= i128::MIN as f64 && scaled <= i128::MAX as f64 {
                            let scaled_value = scaled as i128;
                            typed_builder.append_value(arrow_buffer::i256::from_i128(scaled_value))
                        } else {
                            // Use BigInt for values that don't fit in i128
                            use num::{BigInt, FromPrimitive};
                            let bigint = BigInt::from_f64(scaled).ok_or_else(|| {
                                MagnusError::new(
                                    magnus::exception::type_error(),
                                    format!("Failed to convert float {} to BigInt", flo),
                                )
                            })?;
                            let bytes = bigint.to_signed_bytes_le();
                            if bytes.len() <= 32 {
                                let mut buf = if bigint.sign() == num::bigint::Sign::Minus {
                                    [0xff; 32]
                                } else {
                                    [0; 32]
                                };
                                buf[..bytes.len()].copy_from_slice(&bytes);
                                typed_builder.append_value(arrow_buffer::i256::from_le_bytes(buf))
                            } else {
                                return Err(MagnusError::new(
                                    magnus::exception::type_error(),
                                    format!(
                                        "Float value {} scaled is too large for Decimal256",
                                        flo
                                    ),
                                ));
                            }
                        }
                    }
                    ParquetValue::Int64(i) => {
                        // Scale the integer to the desired scale
                        let base = arrow_buffer::i256::from_i128(*i as i128);
                        if *scale <= 38 {
                            // Can use i128 multiplication for scale <= 38
                            let scale_factor =
                                arrow_buffer::i256::from_i128(10_i128.pow(*scale as u32));
                            match base.checked_mul(scale_factor) {
                                Some(scaled) => typed_builder.append_value(scaled),
                                None => {
                                    return Err(MagnusError::new(
                                        magnus::exception::type_error(),
                                        format!(
                                            "Integer {} scaled by {} overflows Decimal256",
                                            i, scale
                                        ),
                                    ));
                                }
                            }
                        } else {
                            // For very large scales, use BigInt
                            use num::BigInt;
                            let bigint = BigInt::from(*i) * BigInt::from(10).pow(*scale as u32);
                            let bytes = bigint.to_signed_bytes_le();
                            if bytes.len() <= 32 {
                                let mut buf = if bigint.sign() == num::bigint::Sign::Minus {
                                    [0xff; 32]
                                } else {
                                    [0; 32]
                                };
                                buf[..bytes.len()].copy_from_slice(&bytes);
                                typed_builder.append_value(arrow_buffer::i256::from_le_bytes(buf))
                            } else {
                                return Err(MagnusError::new(
                                    magnus::exception::type_error(),
                                    format!(
                                        "Integer {} scaled by {} is too large for Decimal256",
                                        i, scale
                                    ),
                                ));
                            }
                        }
                    }
                    ParquetValue::Int32(i) => {
                        // Scale the integer to the desired scale
                        let base = arrow_buffer::i256::from_i128(*i as i128);
                        if *scale <= 38 {
                            // Can use i128 multiplication for scale <= 38
                            let scale_factor =
                                arrow_buffer::i256::from_i128(10_i128.pow(*scale as u32));
                            match base.checked_mul(scale_factor) {
                                Some(scaled) => typed_builder.append_value(scaled),
                                None => {
                                    return Err(MagnusError::new(
                                        magnus::exception::type_error(),
                                        format!(
                                            "Integer {} scaled by {} overflows Decimal256",
                                            i, scale
                                        ),
                                    ));
                                }
                            }
                        } else {
                            // For very large scales, use BigInt
                            use num::BigInt;
                            let bigint = BigInt::from(*i) * BigInt::from(10).pow(*scale as u32);
                            let bytes = bigint.to_signed_bytes_le();
                            if bytes.len() <= 32 {
                                let mut buf = if bigint.sign() == num::bigint::Sign::Minus {
                                    [0xff; 32]
                                } else {
                                    [0; 32]
                                };
                                buf[..bytes.len()].copy_from_slice(&bytes);
                                typed_builder.append_value(arrow_buffer::i256::from_le_bytes(buf))
                            } else {
                                return Err(MagnusError::new(
                                    magnus::exception::type_error(),
                                    format!(
                                        "Integer {} scaled by {} is too large for Decimal256",
                                        i, scale
                                    ),
                                ));
                            }
                        }
                    }
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected numeric value for Decimal256, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::Boolean) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .expect("Builder mismatch: expected BooleanBuilder");
            for val in values {
                match val {
                    ParquetValue::Boolean(b) => typed_builder.append_value(*b),
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected Boolean, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::Date32) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .expect("Builder mismatch: expected Date32Builder");
            for val in values {
                match val {
                    ParquetValue::Date32(d) => typed_builder.append_value(*d),
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected Date32, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::TimestampMillis) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<TimestampMillisecondBuilder>()
                .expect("Builder mismatch: expected TimestampMillisecondBuilder");
            for val in values {
                match val {
                    ParquetValue::TimestampMillis(ts, _tz) => typed_builder.append_value(*ts),
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected TimestampMillis, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::TimestampMicros) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .expect("Builder mismatch: expected TimestampMicrosecondBuilder");
            for val in values {
                match val {
                    ParquetValue::TimestampMicros(ts, _tz) => typed_builder.append_value(*ts),
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected TimestampMicros, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::TimeMillis) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<Time32MillisecondBuilder>()
                .expect("Builder mismatch: expected Time32MillisecondBuilder");
            for val in values {
                match val {
                    ParquetValue::TimeMillis(t) => typed_builder.append_value(*t),
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected TimeMillis, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::TimeMicros) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<Time64MicrosecondBuilder>()
                .expect("Builder mismatch: expected Time64MicrosecondBuilder");
            for val in values {
                match val {
                    ParquetValue::TimeMicros(t) => typed_builder.append_value(*t),
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected TimeMicros, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }

        // ------------------
        // NESTED LIST - using helper function
        // ------------------
        ParquetSchemaType::List(list_field) => {
            fill_list_builder(builder, &list_field.item_type, values)
        }

        // ------------------
        // NESTED MAP - using helper function
        // ------------------
        ParquetSchemaType::Map(map_field) => {
            fill_map_builder(builder, &map_field.key_type, &map_field.value_type, values)
        }

        // ------------------
        // OTHER TYPES - keep as is for now
        // ------------------
        ParquetSchemaType::Primitive(PrimitiveType::String) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .expect("Builder mismatch: expected StringBuilder");
            for val in values {
                match val {
                    ParquetValue::String(s) => typed_builder.append_value(s),
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected String, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Primitive(PrimitiveType::Binary) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .expect("Builder mismatch: expected BinaryBuilder");
            for val in values {
                match val {
                    ParquetValue::Bytes(b) => typed_builder.append_value(b),
                    ParquetValue::Null => typed_builder.append_null(),
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected Binary, got {:?}", other),
                        ))
                    }
                }
            }
            Ok(())
        }
        ParquetSchemaType::Struct(struct_field) => {
            let typed_builder = builder
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .expect("Builder mismatch: expected StructBuilder");

            for val in values {
                match val {
                    ParquetValue::Null => {
                        // null struct
                        typed_builder.append(false);
                    }
                    ParquetValue::Map(map_data) => {
                        for (i, field) in struct_field.fields.iter().enumerate() {
                            let field_key = ParquetValue::String(field.name.clone());
                            if let Some(field_val) = map_data.get(&field_key) {
                                match field_val {
                                    ParquetValue::Int8(x) => typed_builder
                                        .field_builder::<Int8Builder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into Int8Builder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::Int16(x) => typed_builder
                                        .field_builder::<Int16Builder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into Int16Builder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::Int32(x) => typed_builder
                                        .field_builder::<Int32Builder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into Int32Builder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::Int64(x) => typed_builder
                                        .field_builder::<Int64Builder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into Int64Builder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::UInt8(x) => typed_builder
                                        .field_builder::<UInt8Builder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into UInt8Builder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::UInt16(x) => typed_builder
                                        .field_builder::<UInt16Builder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into UInt16Builder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::UInt32(x) => typed_builder
                                        .field_builder::<UInt32Builder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into UInt32Builder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::UInt64(x) => typed_builder
                                        .field_builder::<UInt64Builder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into UInt64Builder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::Float16(_) => {
                                        return Err(MagnusError::new(
                                            magnus::exception::runtime_error(),
                                            "Float16 not supported",
                                        ))
                                    }
                                    ParquetValue::Float32(x) => typed_builder
                                        .field_builder::<Float32Builder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into Float32Builder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::Float64(x) => typed_builder
                                        .field_builder::<Float64Builder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into Float64Builder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::Boolean(x) => typed_builder
                                        .field_builder::<BooleanBuilder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into BooleanBuilder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::String(x) => typed_builder
                                        .field_builder::<StringBuilder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into StringBuilder",
                                            )
                                        })?
                                        .append_value(x),
                                    ParquetValue::Bytes(bytes) => typed_builder
                                        .field_builder::<BinaryBuilder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into BinaryBuilder",
                                            )
                                        })?
                                        .append_value(bytes),
                                    ParquetValue::Decimal128(x, _scale) => typed_builder
                                        .field_builder::<Decimal128Builder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into Decimal128Builder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::Decimal256(x, _scale) => typed_builder
                                        .field_builder::<Decimal256Builder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into Decimal256Builder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::Date32(x) => typed_builder
                                        .field_builder::<Date32Builder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into Date32Builder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::Date64(x) => typed_builder
                                        .field_builder::<Date64Builder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into Date64Builder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::TimestampSecond(x, _tz) => typed_builder
                                        .field_builder::<TimestampSecondBuilder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into TimestampSecondBuilder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::TimestampMillis(x, _tz) => typed_builder
                                        .field_builder::<TimestampMillisecondBuilder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into TimestampMillisecondBuilder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::TimestampMicros(x, _tz) => typed_builder
                                        .field_builder::<TimestampMicrosecondBuilder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into TimestampMicrosecondBuilder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::TimestampNanos(x, _tz) => typed_builder
                                        .field_builder::<TimestampNanosecondBuilder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into TimestampNanosecondBuilder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::TimeMillis(x) => typed_builder
                                        .field_builder::<Time32MillisecondBuilder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into Time32MillisecondBuilder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::TimeMicros(x) => typed_builder
                                        .field_builder::<Time64MicrosecondBuilder>(i)
                                        .ok_or_else(|| {
                                            MagnusError::new(
                                                magnus::exception::type_error(),
                                                "Failed to coerce into Time64MicrosecondBuilder",
                                            )
                                        })?
                                        .append_value(*x),
                                    ParquetValue::List(items) => {
                                        let list_builder = typed_builder
                                            .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into ListBuilder",
                                                )
                                            })?;
                                        fill_builder(
                                            list_builder.values(),
                                            &struct_field.fields[i].type_,
                                            items,
                                        )?;
                                        list_builder.append(true);
                                    }
                                    ParquetValue::Map(map_data) => {
                                        let maybe_map_builder = typed_builder
                                            .field_builder::<MapBuilder<
                                                Box<dyn ArrayBuilder>,
                                                Box<dyn ArrayBuilder>,
                                            >>(i);

                                        if let Some(map_builder) = maybe_map_builder {
                                            fill_builder(
                                                map_builder,
                                                &struct_field.fields[i].type_,
                                                &[ParquetValue::Map(map_data.clone())],
                                            )?;
                                            map_builder.append(true).map_err(|e| {
                                                MagnusError::new(
                                                    magnus::exception::runtime_error(),
                                                    format!("Failed to append map: {}", e),
                                                )
                                            })?;
                                        } else {
                                            let child_struct_builder = typed_builder
                                                .field_builder::<StructBuilder>(i)
                                                .ok_or_else(|| {
                                                    MagnusError::new(
                                                        magnus::exception::type_error(),
                                                        "Failed to coerce into StructBuilder",
                                                    )
                                                })?;
                                            fill_builder(
                                                child_struct_builder,
                                                &struct_field.fields[i].type_,
                                                &[ParquetValue::Map(map_data.clone())],
                                            )?;
                                        }
                                    }
                                    ParquetValue::Null => match struct_field.fields[i].type_ {
                                        ParquetSchemaType::Primitive(PrimitiveType::Int8) => typed_builder
                                            .field_builder::<Int8Builder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into Int8Builder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::Int16) => typed_builder
                                            .field_builder::<Int16Builder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into Int16Builder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::Int32) => typed_builder
                                            .field_builder::<Int32Builder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into Int32Builder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::Int64) => typed_builder
                                            .field_builder::<Int64Builder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into Int64Builder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::UInt8) => typed_builder
                                            .field_builder::<UInt8Builder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into UInt8Builder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::UInt16) => typed_builder
                                            .field_builder::<UInt16Builder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into UInt16Builder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::UInt32) => typed_builder
                                            .field_builder::<UInt32Builder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into UInt32Builder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::UInt64) => typed_builder
                                            .field_builder::<UInt64Builder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into UInt64Builder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::Float32) => typed_builder
                                            .field_builder::<Float32Builder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into Float32Builder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::Float64) => typed_builder
                                            .field_builder::<Float64Builder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into Float64Builder",
                                                )
                                            })?
                                            .append_null(),
                                            ParquetSchemaType::Primitive(PrimitiveType::Decimal128(_, _)) => typed_builder
                                            .field_builder::<Decimal128Builder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into Decimal128Builder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::Decimal256(_, _)) => typed_builder
                                            .field_builder::<Decimal256Builder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into Decimal256Builder for Decimal256",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::String) => typed_builder
                                            .field_builder::<StringBuilder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into StringBuilder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::Binary) => typed_builder
                                            .field_builder::<BinaryBuilder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into BinaryBuilder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::Boolean) => typed_builder
                                            .field_builder::<BooleanBuilder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into BooleanBuilder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::Date32) => typed_builder
                                            .field_builder::<Date32Builder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into Date32Builder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::TimestampMillis) => typed_builder
                                            .field_builder::<TimestampMillisecondBuilder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into TimestampMillisecondBuilder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::TimestampMicros) => typed_builder
                                            .field_builder::<TimestampMicrosecondBuilder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into TimestampMicrosecondBuilder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::TimeMillis) => typed_builder
                                            .field_builder::<Time32MillisecondBuilder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into Time32MillisecondBuilder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::Primitive(PrimitiveType::TimeMicros) => typed_builder
                                            .field_builder::<Time64MicrosecondBuilder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into Time64MicrosecondBuilder",
                                                )
                                            })?
                                            .append_null(),
                                        ParquetSchemaType::List(_) => typed_builder
                                            .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into ListBuilder",
                                                )
                                            })?
                                            .append(false),
                                        ParquetSchemaType::Map(_) => {
                                            typed_builder
                                                .field_builder::<MapBuilder<
                                                    Box<dyn ArrayBuilder>,
                                                    Box<dyn ArrayBuilder>,
                                                >>(i)
                                                .ok_or_else(|| {
                                                    MagnusError::new(
                                                        magnus::exception::type_error(),
                                                        "Failed to coerce into MapBuilder",
                                                    )
                                                })?
                                                .append(false)
                                                .map_err(|e| {
                                                    MagnusError::new(
                                                        magnus::exception::runtime_error(),
                                                        format!("Failed to append map: {}", e),
                                                    )
                                                })?;
                                        }
                                        ParquetSchemaType::Struct(_) => typed_builder
                                            .field_builder::<StructBuilder>(i)
                                            .ok_or_else(|| {
                                                MagnusError::new(
                                                    magnus::exception::type_error(),
                                                    "Failed to coerce into StructBuilder",
                                                )
                                            })?
                                            .append_null(),
                                    },
                                }
                            } else {
                                return Err(MagnusError::new(
                                    magnus::exception::type_error(),
                                    format!("Field {} not found in map", i),
                                ));
                            }
                        }
                        typed_builder.append(true);
                    }
                    other => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected ParquetValue::Map(...) or Null, got {:?}", other),
                        ));
                    }
                }
            }
            Ok(())
        }
    }
}

/// Creates a final Arrow array from a list of ParquetValues and a schema type.
/// This is your "unified" way to handle any nesting level.
pub fn convert_parquet_values_to_arrow(
    values: Vec<ParquetValue>,
    type_: &ParquetSchemaType,
) -> Result<Arc<dyn Array>, ParquetGemError> {
    // Make sure we always have at least capacity 1 to avoid empty builders
    let capacity = if values.is_empty() { 1 } else { values.len() };
    let mut builder = create_arrow_builder_for_type(type_, Some(capacity))?;

    fill_builder(&mut builder, type_, &values)?;

    // Finish building the array
    let array = builder.finish();

    Ok(Arc::new(array))
}

pub fn convert_ruby_array_to_arrow(
    ruby: &Ruby,
    values: RArray,
    type_: &ParquetSchemaType,
) -> Result<Arc<dyn Array>, ParquetGemError> {
    let mut parquet_values = Vec::with_capacity(values.len());
    for value in values {
        if value.is_nil() {
            parquet_values.push(ParquetValue::Null);
            continue;
        }
        let parquet_value = ParquetValue::from_value(ruby, value, type_, None)?;
        parquet_values.push(parquet_value);
    }
    convert_parquet_values_to_arrow(parquet_values, type_)
}

pub fn convert_to_time_millis(
    ruby: &Ruby,
    value: Value,
    format: Option<&str>,
) -> Result<i32, MagnusError> {
    if value.is_kind_of(ruby.class_time()) {
        // Extract time components
        let hour = i32::try_convert(value.funcall::<_, _, Value>("hour", ())?)?;
        let min = i32::try_convert(value.funcall::<_, _, Value>("min", ())?)?;
        let sec = i32::try_convert(value.funcall::<_, _, Value>("sec", ())?)?;
        let usec = i32::try_convert(value.funcall::<_, _, Value>("usec", ())?)?;

        // Convert to milliseconds since midnight
        Ok(hour * 3600000 + min * 60000 + sec * 1000 + usec / 1000)
    } else if value.is_kind_of(ruby.class_string()) {
        let s = String::try_convert(value)?;

        if let Some(fmt) = format {
            // Parse using the provided format
            match jiff::civil::Time::strptime(fmt, &s) {
                Ok(time) => {
                    let millis = time.hour() as i32 * 3600000
                        + time.minute() as i32 * 60000
                        + time.second() as i32 * 1000
                        + time.millisecond() as i32;
                    Ok(millis)
                }
                Err(e) => Err(MagnusError::new(
                    magnus::exception::type_error(),
                    format!(
                        "Failed to parse '{}' with format '{}' as time: {}",
                        s, fmt, e
                    ),
                )),
            }
        } else {
            // Try to parse as standard time format
            match s.parse::<jiff::civil::Time>() {
                Ok(time) => {
                    let millis = time.hour() as i32 * 3600000
                        + time.minute() as i32 * 60000
                        + time.second() as i32 * 1000
                        + time.millisecond() as i32;
                    Ok(millis)
                }
                Err(e) => Err(MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Failed to parse '{}' as time: {}", s, e),
                )),
            }
        }
    } else {
        Err(MagnusError::new(
            magnus::exception::type_error(),
            format!("Cannot convert {} to time_millis", unsafe {
                value.classname()
            }),
        ))
    }
}

pub fn convert_to_time_micros(
    ruby: &Ruby,
    value: Value,
    format: Option<&str>,
) -> Result<i64, MagnusError> {
    if value.is_kind_of(ruby.class_time()) {
        // Extract time components
        let hour = i64::try_convert(value.funcall::<_, _, Value>("hour", ())?)?;
        let min = i64::try_convert(value.funcall::<_, _, Value>("min", ())?)?;
        let sec = i64::try_convert(value.funcall::<_, _, Value>("sec", ())?)?;
        let usec = i64::try_convert(value.funcall::<_, _, Value>("usec", ())?)?;

        // Convert to microseconds since midnight
        Ok(hour * 3600000000 + min * 60000000 + sec * 1000000 + usec)
    } else if value.is_kind_of(ruby.class_string()) {
        let s = String::try_convert(value)?;

        if let Some(fmt) = format {
            // Parse using the provided format
            match jiff::civil::Time::strptime(fmt, &s) {
                Ok(time) => {
                    let micros = time.hour() as i64 * 3600000000
                        + time.minute() as i64 * 60000000
                        + time.second() as i64 * 1000000
                        + time.microsecond() as i64;
                    Ok(micros)
                }
                Err(e) => Err(MagnusError::new(
                    magnus::exception::type_error(),
                    format!(
                        "Failed to parse '{}' with format '{}' as time: {}",
                        s, fmt, e
                    ),
                )),
            }
        } else {
            // Try to parse as standard time format
            match s.parse::<jiff::civil::Time>() {
                Ok(time) => {
                    let micros = time.hour() as i64 * 3600000000
                        + time.minute() as i64 * 60000000
                        + time.second() as i64 * 1000000
                        + time.microsecond() as i64;
                    Ok(micros)
                }
                Err(e) => Err(MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Failed to parse '{}' as time: {}", s, e),
                )),
            }
        }
    } else {
        Err(MagnusError::new(
            magnus::exception::type_error(),
            format!("Cannot convert {} to time_micros", unsafe {
                value.classname()
            }),
        ))
    }
}
