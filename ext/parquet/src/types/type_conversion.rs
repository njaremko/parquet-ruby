use std::str::FromStr;

use super::*;
use arrow_array::builder::*;
use jiff::tz::{Offset, TimeZone};
use magnus::{RArray, TryConvert};

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

pub fn convert_to_date32(value: Value, format: Option<&str>) -> Result<i32, MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };
    if value.is_kind_of(ruby.class_string()) {
        let s = String::try_convert(value)?;
        // Parse string into Date using jiff
        let date = if let Some(fmt) = format {
            jiff::civil::Date::strptime(&fmt, &s).or_else(|e1| {
                // Try parsing as DateTime and convert to Date with zero offset
                jiff::civil::DateTime::strptime(&fmt, &s)
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

pub fn convert_to_timestamp_millis(value: Value, format: Option<&str>) -> Result<i64, MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };
    if value.is_kind_of(ruby.class_string()) {
        let s = String::try_convert(value)?;
        // Parse string into Timestamp using jiff
        let timestamp = if let Some(fmt) = format {
            jiff::Timestamp::strptime(&fmt, &s)
                .or_else(|e1| {
                    // Try parsing as DateTime and convert to Timestamp with zero offset
                    jiff::civil::DateTime::strptime(&fmt, &s)
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

pub fn convert_to_timestamp_micros(value: Value, format: Option<&str>) -> Result<i64, MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };
    if value.is_kind_of(ruby.class_string()) {
        let s = String::try_convert(value)?;
        // Parse string into Timestamp using jiff
        let timestamp = if let Some(fmt) = format {
            jiff::Timestamp::strptime(&fmt, &s).or_else(|e1| {
                // Try parsing as DateTime and convert to Timestamp with zero offset
                jiff::civil::DateTime::strptime(&fmt, &s).and_then(|dt| {
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

pub fn convert_to_list(
    value: Value,
    list_field: &ListField,
) -> Result<Vec<ParquetValue>, MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };
    if value.is_kind_of(ruby.class_array()) {
        let array = RArray::from_value(value).ok_or_else(|| {
            MagnusError::new(magnus::exception::type_error(), "Invalid list format")
        })?;

        let mut values = Vec::with_capacity(array.len());
        for item_value in array.into_iter() {
            let converted = match &list_field.item_type {
                ParquetSchemaType::Int8 => {
                    let v = NumericConverter::<i8>::convert_with_string_fallback(item_value)?;
                    ParquetValue::Int8(v)
                }
                ParquetSchemaType::Int16 => {
                    let v = NumericConverter::<i16>::convert_with_string_fallback(item_value)?;
                    ParquetValue::Int16(v)
                }
                ParquetSchemaType::Int32 => {
                    let v = NumericConverter::<i32>::convert_with_string_fallback(item_value)?;
                    ParquetValue::Int32(v)
                }
                ParquetSchemaType::Int64 => {
                    let v = NumericConverter::<i64>::convert_with_string_fallback(item_value)?;
                    ParquetValue::Int64(v)
                }
                ParquetSchemaType::UInt8 => {
                    let v = NumericConverter::<u8>::convert_with_string_fallback(item_value)?;
                    ParquetValue::UInt8(v)
                }
                ParquetSchemaType::UInt16 => {
                    let v = NumericConverter::<u16>::convert_with_string_fallback(item_value)?;
                    ParquetValue::UInt16(v)
                }
                ParquetSchemaType::UInt32 => {
                    let v = NumericConverter::<u32>::convert_with_string_fallback(item_value)?;
                    ParquetValue::UInt32(v)
                }
                ParquetSchemaType::UInt64 => {
                    let v = NumericConverter::<u64>::convert_with_string_fallback(item_value)?;
                    ParquetValue::UInt64(v)
                }
                ParquetSchemaType::Float => {
                    let v = NumericConverter::<f32>::convert_with_string_fallback(item_value)?;
                    ParquetValue::Float32(v)
                }
                ParquetSchemaType::Double => {
                    let v = NumericConverter::<f64>::convert_with_string_fallback(item_value)?;
                    ParquetValue::Float64(v)
                }
                ParquetSchemaType::String => {
                    let v = String::try_convert(item_value)?;
                    ParquetValue::String(v)
                }
                ParquetSchemaType::Binary => {
                    let v = convert_to_binary(item_value)?;
                    ParquetValue::Bytes(v)
                }
                ParquetSchemaType::Boolean => {
                    let v = convert_to_boolean(item_value)?;
                    ParquetValue::Boolean(v)
                }
                ParquetSchemaType::Date32 => {
                    let v = convert_to_date32(item_value, list_field.format)?;
                    ParquetValue::Date32(v)
                }
                ParquetSchemaType::TimestampMillis => {
                    let v = convert_to_timestamp_millis(item_value, list_field.format)?;
                    ParquetValue::TimestampMillis(v, None)
                }
                ParquetSchemaType::TimestampMicros => {
                    let v = convert_to_timestamp_micros(item_value, list_field.format)?;
                    ParquetValue::TimestampMicros(v, None)
                }
                ParquetSchemaType::List(_) | ParquetSchemaType::Map(_) => {
                    return Err(MagnusError::new(
                        magnus::exception::type_error(),
                        "Nested lists and maps are not supported",
                    ))
                }
            };
            values.push(converted);
        }
        Ok(values)
    } else {
        Err(MagnusError::new(
            magnus::exception::type_error(),
            "Invalid list format",
        ))
    }
}

pub fn convert_to_map(
    value: Value,
    map_field: &MapField,
) -> Result<HashMap<ParquetValue, ParquetValue>, MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };
    if value.is_kind_of(ruby.class_hash()) {
        let mut map = HashMap::new();
        let entries: Vec<(Value, Value)> = value.funcall("to_a", ())?;

        for (key, value) in entries {
            let key_value = match &map_field.key_type {
                ParquetSchemaType::String => {
                    let v = String::try_convert(key)?;
                    ParquetValue::String(v)
                }
                _ => {
                    return Err(MagnusError::new(
                        magnus::exception::type_error(),
                        "Map keys must be strings",
                    ))
                }
            };

            let value_value = match &map_field.value_type {
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
                    let v = convert_to_date32(value, map_field.format)?;
                    ParquetValue::Date32(v)
                }
                ParquetSchemaType::TimestampMillis => {
                    let v = convert_to_timestamp_millis(value, map_field.format)?;
                    ParquetValue::TimestampMillis(v, None)
                }
                ParquetSchemaType::TimestampMicros => {
                    let v = convert_to_timestamp_micros(value, map_field.format)?;
                    ParquetValue::TimestampMicros(v, None)
                }
                ParquetSchemaType::List(_) | ParquetSchemaType::Map(_) => {
                    return Err(MagnusError::new(
                        magnus::exception::type_error(),
                        "Map values cannot be lists or maps",
                    ))
                }
            };

            map.insert(key_value, value_value);
        }
        Ok(map)
    } else {
        Err(MagnusError::new(
            magnus::exception::type_error(),
            "Invalid map format",
        ))
    }
}

macro_rules! impl_timestamp_to_arrow_conversion {
    ($values:expr, $builder_type:ty, $variant:ident) => {{
        let mut builder = <$builder_type>::with_capacity($values.len());
        for value in $values {
            match value {
                ParquetValue::$variant(v, _tz) => builder.append_value(v),
                ParquetValue::Null => builder.append_null(),
                _ => {
                    return Err(MagnusError::new(
                        magnus::exception::type_error(),
                        format!("Expected {}, got {:?}", stringify!($variant), value),
                    ))
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
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

#[macro_export]
macro_rules! impl_array_conversion {
    ($values:expr, $builder_type:ty, $variant:ident) => {{
        let mut builder = <$builder_type>::with_capacity($values.len());
        for value in $values {
            match value {
                ParquetValue::$variant(v) => builder.append_value(v),
                ParquetValue::Null => builder.append_null(),
                _ => {
                    return Err(MagnusError::new(
                        magnus::exception::type_error(),
                        format!("Expected {}, got {:?}", stringify!($variant), value),
                    ))
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
    ($values:expr, $builder_type:ty, $variant:ident, $capacity:expr) => {{
        let mut builder = <$builder_type>::with_capacity($values.len(), $capacity);
        for value in $values {
            match value {
                ParquetValue::$variant(v) => builder.append_value(v),
                ParquetValue::Null => builder.append_null(),
                _ => {
                    return Err(MagnusError::new(
                        magnus::exception::type_error(),
                        format!("Expected {}, got {:?}", stringify!($variant), value),
                    ))
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

#[macro_export]
macro_rules! append_list_value {
    ($list_builder:expr, $item_type:path, $value:expr, $builder_type:ty, $value_variant:path) => {
        match (&$item_type, &$value) {
            ($item_type, $value_variant(v)) => {
                $list_builder
                    .values()
                    .as_any_mut()
                    .downcast_mut::<$builder_type>()
                    .unwrap()
                    .append_value(v.clone());
            }
            (_, ParquetValue::Null) => {
                $list_builder.append_null();
            }
            _ => {
                return Err(MagnusError::new(
                    magnus::exception::type_error(),
                    format!(
                        "Type mismatch in list: expected {:?}, got {:?}",
                        $item_type, $value
                    ),
                ))
            }
        }
    };
}

#[macro_export]
macro_rules! append_list_value_copy {
    ($list_builder:expr, $item_type:path, $value:expr, $builder_type:ty, $value_variant:path) => {
        match (&$item_type, &$value) {
            ($item_type, $value_variant(v)) => {
                $list_builder
                    .values()
                    .as_any_mut()
                    .downcast_mut::<$builder_type>()
                    .unwrap()
                    .append_value(*v);
            }
            (_, ParquetValue::Null) => {
                $list_builder.append_null();
            }
            _ => {
                return Err(MagnusError::new(
                    magnus::exception::type_error(),
                    format!(
                        "Type mismatch in list: expected {:?}, got {:?}",
                        $item_type, $value
                    ),
                ))
            }
        }
    };
}

#[macro_export]
macro_rules! append_timestamp_list_value {
    ($list_builder:expr, $item_type:path, $value:expr, $builder_type:ty, $value_variant:path) => {
        match (&$item_type, &$value) {
            ($item_type, $value_variant(v, _tz)) => {
                $list_builder
                    .values()
                    .as_any_mut()
                    .downcast_mut::<$builder_type>()
                    .unwrap()
                    .append_value(*v);
            }
            (_, ParquetValue::Null) => {
                $list_builder.append_null();
            }
            _ => {
                return Err(MagnusError::new(
                    magnus::exception::type_error(),
                    format!(
                        "Type mismatch in list: expected {:?}, got {:?}",
                        $item_type, $value
                    ),
                ))
            }
        }
    };
}

pub fn convert_parquet_values_to_arrow(
    values: Vec<ParquetValue>,
    type_: &ParquetSchemaType,
) -> Result<Arc<dyn Array>, MagnusError> {
    match type_ {
        ParquetSchemaType::Int8 => impl_array_conversion!(values, Int8Builder, Int8),
        ParquetSchemaType::Int16 => impl_array_conversion!(values, Int16Builder, Int16),
        ParquetSchemaType::Int32 => impl_array_conversion!(values, Int32Builder, Int32),
        ParquetSchemaType::Int64 => impl_array_conversion!(values, Int64Builder, Int64),
        ParquetSchemaType::UInt8 => impl_array_conversion!(values, UInt8Builder, UInt8),
        ParquetSchemaType::UInt16 => impl_array_conversion!(values, UInt16Builder, UInt16),
        ParquetSchemaType::UInt32 => impl_array_conversion!(values, UInt32Builder, UInt32),
        ParquetSchemaType::UInt64 => impl_array_conversion!(values, UInt64Builder, UInt64),
        ParquetSchemaType::Float => impl_array_conversion!(values, Float32Builder, Float32),
        ParquetSchemaType::Double => impl_array_conversion!(values, Float64Builder, Float64),
        ParquetSchemaType::String => {
            impl_array_conversion!(values, StringBuilder, String, values.len() * 32)
        }
        ParquetSchemaType::Binary => {
            impl_array_conversion!(values, BinaryBuilder, Bytes, values.len() * 32)
        }
        ParquetSchemaType::Boolean => impl_array_conversion!(values, BooleanBuilder, Boolean),
        ParquetSchemaType::Date32 => impl_array_conversion!(values, Date32Builder, Date32),
        ParquetSchemaType::TimestampMillis => {
            impl_timestamp_to_arrow_conversion!(
                values,
                TimestampMillisecondBuilder,
                TimestampMillis
            )
        }
        ParquetSchemaType::TimestampMicros => {
            impl_timestamp_to_arrow_conversion!(
                values,
                TimestampMicrosecondBuilder,
                TimestampMicros
            )
        }
        ParquetSchemaType::List(list_field) => {
            let value_builder = match list_field.item_type {
                ParquetSchemaType::Int8 => Box::new(Int8Builder::new()) as Box<dyn ArrayBuilder>,
                ParquetSchemaType::Int16 => Box::new(Int16Builder::new()) as Box<dyn ArrayBuilder>,
                ParquetSchemaType::Int32 => Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>,
                ParquetSchemaType::Int64 => Box::new(Int64Builder::new()) as Box<dyn ArrayBuilder>,
                ParquetSchemaType::UInt8 => Box::new(UInt8Builder::new()) as Box<dyn ArrayBuilder>,
                ParquetSchemaType::UInt16 => {
                    Box::new(UInt16Builder::new()) as Box<dyn ArrayBuilder>
                }
                ParquetSchemaType::UInt32 => {
                    Box::new(UInt32Builder::new()) as Box<dyn ArrayBuilder>
                }
                ParquetSchemaType::UInt64 => {
                    Box::new(UInt64Builder::new()) as Box<dyn ArrayBuilder>
                }
                ParquetSchemaType::Float => {
                    Box::new(Float32Builder::new()) as Box<dyn ArrayBuilder>
                }
                ParquetSchemaType::Double => {
                    Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>
                }
                ParquetSchemaType::String => {
                    Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>
                }
                ParquetSchemaType::Binary => {
                    Box::new(BinaryBuilder::new()) as Box<dyn ArrayBuilder>
                }
                ParquetSchemaType::Boolean => {
                    Box::new(BooleanBuilder::new()) as Box<dyn ArrayBuilder>
                }
                ParquetSchemaType::Date32 => {
                    Box::new(Date32Builder::new()) as Box<dyn ArrayBuilder>
                }
                ParquetSchemaType::TimestampMillis => {
                    Box::new(TimestampMillisecondBuilder::new()) as Box<dyn ArrayBuilder>
                }
                ParquetSchemaType::TimestampMicros => {
                    Box::new(TimestampMicrosecondBuilder::new()) as Box<dyn ArrayBuilder>
                }
                ParquetSchemaType::List(_) | ParquetSchemaType::Map(_) => {
                    return Err(MagnusError::new(
                        magnus::exception::type_error(),
                        "Nested lists and maps are not supported",
                    ))
                }
            };

            let mut list_builder = ListBuilder::new(value_builder);

            for value in values {
                match value {
                    ParquetValue::List(items) => {
                        for item in items {
                            match &list_field.item_type {
                                ParquetSchemaType::Int8 => append_list_value_copy!(
                                    list_builder,
                                    ParquetSchemaType::Int8,
                                    item,
                                    Int8Builder,
                                    ParquetValue::Int8
                                ),
                                ParquetSchemaType::Int16 => append_list_value_copy!(
                                    list_builder,
                                    ParquetSchemaType::Int16,
                                    item,
                                    Int16Builder,
                                    ParquetValue::Int16
                                ),
                                ParquetSchemaType::Int32 => append_list_value_copy!(
                                    list_builder,
                                    ParquetSchemaType::Int32,
                                    item,
                                    Int32Builder,
                                    ParquetValue::Int32
                                ),
                                ParquetSchemaType::Int64 => append_list_value_copy!(
                                    list_builder,
                                    ParquetSchemaType::Int64,
                                    item,
                                    Int64Builder,
                                    ParquetValue::Int64
                                ),
                                ParquetSchemaType::UInt8 => append_list_value_copy!(
                                    list_builder,
                                    ParquetSchemaType::UInt8,
                                    item,
                                    UInt8Builder,
                                    ParquetValue::UInt8
                                ),
                                ParquetSchemaType::UInt16 => append_list_value_copy!(
                                    list_builder,
                                    ParquetSchemaType::UInt16,
                                    item,
                                    UInt16Builder,
                                    ParquetValue::UInt16
                                ),
                                ParquetSchemaType::UInt32 => append_list_value_copy!(
                                    list_builder,
                                    ParquetSchemaType::UInt32,
                                    item,
                                    UInt32Builder,
                                    ParquetValue::UInt32
                                ),
                                ParquetSchemaType::UInt64 => append_list_value_copy!(
                                    list_builder,
                                    ParquetSchemaType::UInt64,
                                    item,
                                    UInt64Builder,
                                    ParquetValue::UInt64
                                ),
                                ParquetSchemaType::Float => append_list_value_copy!(
                                    list_builder,
                                    ParquetSchemaType::Float,
                                    item,
                                    Float32Builder,
                                    ParquetValue::Float32
                                ),
                                ParquetSchemaType::Double => append_list_value_copy!(
                                    list_builder,
                                    ParquetSchemaType::Double,
                                    item,
                                    Float64Builder,
                                    ParquetValue::Float64
                                ),
                                ParquetSchemaType::String => append_list_value!(
                                    list_builder,
                                    ParquetSchemaType::String,
                                    item,
                                    StringBuilder,
                                    ParquetValue::String
                                ),
                                ParquetSchemaType::Binary => append_list_value!(
                                    list_builder,
                                    ParquetSchemaType::Binary,
                                    item,
                                    BinaryBuilder,
                                    ParquetValue::Bytes
                                ),
                                ParquetSchemaType::Boolean => append_list_value_copy!(
                                    list_builder,
                                    ParquetSchemaType::Boolean,
                                    item,
                                    BooleanBuilder,
                                    ParquetValue::Boolean
                                ),
                                ParquetSchemaType::Date32 => append_list_value_copy!(
                                    list_builder,
                                    ParquetSchemaType::Date32,
                                    item,
                                    Date32Builder,
                                    ParquetValue::Date32
                                ),
                                ParquetSchemaType::TimestampMillis => append_timestamp_list_value!(
                                    list_builder,
                                    ParquetSchemaType::TimestampMillis,
                                    item,
                                    TimestampMillisecondBuilder,
                                    ParquetValue::TimestampMillis
                                ),
                                ParquetSchemaType::TimestampMicros => append_timestamp_list_value!(
                                    list_builder,
                                    ParquetSchemaType::TimestampMicros,
                                    item,
                                    TimestampMicrosecondBuilder,
                                    ParquetValue::TimestampMicros
                                ),
                                ParquetSchemaType::List(_) | ParquetSchemaType::Map(_) => {
                                    return Err(MagnusError::new(
                                        magnus::exception::type_error(),
                                        "Nested lists and maps are not supported",
                                    ))
                                }
                            }
                        }
                    }
                    ParquetValue::Null => list_builder.append_null(),
                    _ => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            format!("Expected List, got {:?}", value),
                        ))
                    }
                }
            }
            Ok(Arc::new(list_builder.finish()))
        }
        ParquetSchemaType::Map(_map_field) => {
            unimplemented!("Writing maps is not yet supported")
        }
    }
}

pub fn convert_ruby_array_to_arrow(
    values: RArray,
    type_: &ParquetSchemaType,
) -> Result<Arc<dyn Array>, MagnusError> {
    let mut parquet_values = Vec::with_capacity(values.len());
    for value in values {
        if value.is_nil() {
            parquet_values.push(ParquetValue::Null);
            continue;
        }
        let parquet_value = ParquetValue::from_value(value, type_)?;
        parquet_values.push(parquet_value);
    }
    convert_parquet_values_to_arrow(parquet_values, type_)
}
