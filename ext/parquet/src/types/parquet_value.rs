use crate::{impl_date_conversion, impl_timestamp_array_conversion, impl_timestamp_conversion};

use super::record_types::{format_decimal_with_i8_scale, format_i256_decimal_with_scale};
use super::*;
use arrow_array::MapArray;
use magnus::{RArray, RString};

#[derive(Debug, Clone)]
pub enum ParquetValue {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float16(f32), // f16 converted to f32
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    String(String),
    Bytes(Vec<u8>),
    Date32(i32),
    Date64(i64),
    Decimal128(i128, i8),
    Decimal256(arrow_buffer::i256, i8),
    TimestampSecond(i64, Option<Arc<str>>),
    TimestampMillis(i64, Option<Arc<str>>),
    TimestampMicros(i64, Option<Arc<str>>),
    TimestampNanos(i64, Option<Arc<str>>),
    TimeMillis(i32),         // Time of day in milliseconds since midnight
    TimeMicros(i64),         // Time of day in microseconds since midnight
    List(Vec<ParquetValue>), // A list of values (can be empty or have null items)
    // We're not using a separate NilList type anymore - we'll handle nil lists elsewhere
    Map(HashMap<ParquetValue, ParquetValue>),
    Null,
}

impl PartialEq for ParquetValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ParquetValue::Int8(a), ParquetValue::Int8(b)) => a == b,
            (ParquetValue::Int16(a), ParquetValue::Int16(b)) => a == b,
            (ParquetValue::Int32(a), ParquetValue::Int32(b)) => a == b,
            (ParquetValue::Int64(a), ParquetValue::Int64(b)) => a == b,
            (ParquetValue::UInt8(a), ParquetValue::UInt8(b)) => a == b,
            (ParquetValue::UInt16(a), ParquetValue::UInt16(b)) => a == b,
            (ParquetValue::UInt32(a), ParquetValue::UInt32(b)) => a == b,
            (ParquetValue::UInt64(a), ParquetValue::UInt64(b)) => a == b,
            (ParquetValue::Float16(a), ParquetValue::Float16(b)) => a == b,
            (ParquetValue::Float32(a), ParquetValue::Float32(b)) => a == b,
            (ParquetValue::Float64(a), ParquetValue::Float64(b)) => a == b,
            (ParquetValue::Boolean(a), ParquetValue::Boolean(b)) => a == b,
            (ParquetValue::String(a), ParquetValue::String(b)) => a == b,
            (ParquetValue::Bytes(a), ParquetValue::Bytes(b)) => a == b,
            (ParquetValue::Date32(a), ParquetValue::Date32(b)) => a == b,
            (ParquetValue::Date64(a), ParquetValue::Date64(b)) => a == b,
            (ParquetValue::Decimal128(a, scale_a), ParquetValue::Decimal128(b, scale_b)) => {
                if scale_a == scale_b {
                    // Same scale, compare directly
                    a == b
                } else {
                    // Different scales, need to adjust for proper comparison
                    let mut a_val = *a;
                    let mut b_val = *b;

                    // Adjust to the same scale for proper comparison
                    if scale_a < scale_b {
                        // Scale up a to match b's scale
                        let scale_diff = (*scale_b - *scale_a) as u32;
                        if scale_diff <= 38 {
                            // Limit to avoid overflow
                            a_val *= 10_i128.pow(scale_diff);
                        } else {
                            // For large scale differences, use BigInt for the comparison
                            let a_big = num::BigInt::from(*a)
                                * num::BigInt::from(10_i128.pow(scale_diff.min(38)));
                            let b_big = num::BigInt::from(*b);
                            return a_big == b_big;
                        }
                    } else {
                        // Scale up b to match a's scale
                        let scale_diff = (*scale_a - *scale_b) as u32;
                        if scale_diff <= 38 {
                            // Limit to avoid overflow
                            b_val *= 10_i128.pow(scale_diff);
                        } else {
                            // For large scale differences, use BigInt for the comparison
                            let a_big = num::BigInt::from(*a);
                            let b_big = num::BigInt::from(*b)
                                * num::BigInt::from(10_i128.pow(scale_diff.min(38)));
                            return a_big == b_big;
                        }
                    }

                    a_val == b_val
                }
            }
            (ParquetValue::Decimal256(a, scale_a), ParquetValue::Decimal256(b, scale_b)) => {
                if scale_a == scale_b {
                    // Same scale, compare directly
                    a == b
                } else {
                    // TODO: Implement decimal256 comparison
                    todo!("decimal256 comparison");
                }
            }
            (ParquetValue::TimestampSecond(a, _), ParquetValue::TimestampSecond(b, _)) => a == b,
            (ParquetValue::TimestampMillis(a, _), ParquetValue::TimestampMillis(b, _)) => a == b,
            (ParquetValue::TimestampMicros(a, _), ParquetValue::TimestampMicros(b, _)) => a == b,
            (ParquetValue::TimestampNanos(a, _), ParquetValue::TimestampNanos(b, _)) => a == b,
            (ParquetValue::TimeMillis(a), ParquetValue::TimeMillis(b)) => a == b,
            (ParquetValue::TimeMicros(a), ParquetValue::TimeMicros(b)) => a == b,
            (ParquetValue::List(a), ParquetValue::List(b)) => a == b,
            (ParquetValue::Null, ParquetValue::Null) => true,
            _ => false,
        }
    }
}

impl Eq for ParquetValue {}

impl std::hash::Hash for ParquetValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            ParquetValue::Int8(i) => i.hash(state),
            ParquetValue::Int16(i) => i.hash(state),
            ParquetValue::Int32(i) => i.hash(state),
            ParquetValue::Int64(i) => i.hash(state),
            ParquetValue::UInt8(i) => i.hash(state),
            ParquetValue::UInt16(i) => i.hash(state),
            ParquetValue::UInt32(i) => i.hash(state),
            ParquetValue::UInt64(i) => i.hash(state),
            ParquetValue::Float16(f) => f.to_bits().hash(state),
            ParquetValue::Float32(f) => f.to_bits().hash(state),
            ParquetValue::Float64(f) => f.to_bits().hash(state),
            ParquetValue::Boolean(b) => b.hash(state),
            ParquetValue::String(s) => s.hash(state),
            ParquetValue::Bytes(b) => b.hash(state),
            ParquetValue::Date32(d) => d.hash(state),
            ParquetValue::Date64(d) => d.hash(state),
            ParquetValue::Decimal128(d, scale) => {
                d.hash(state);
                scale.hash(state);
            }
            ParquetValue::Decimal256(d, scale) => {
                d.hash(state);
                scale.hash(state);
            }
            ParquetValue::TimestampSecond(ts, tz) => {
                ts.hash(state);
                tz.hash(state);
            }
            ParquetValue::TimestampMillis(ts, tz) => {
                ts.hash(state);
                tz.hash(state);
            }
            ParquetValue::TimestampMicros(ts, tz) => {
                ts.hash(state);
                tz.hash(state);
            }
            ParquetValue::TimestampNanos(ts, tz) => {
                ts.hash(state);
                tz.hash(state);
            }
            ParquetValue::TimeMillis(t) => t.hash(state),
            ParquetValue::TimeMicros(t) => t.hash(state),
            ParquetValue::List(l) => l.hash(state),
            ParquetValue::Map(m) => {
                for (k, v) in m {
                    k.hash(state);
                    v.hash(state);
                }
            }
            ParquetValue::Null => 0_i32.hash(state),
        }
    }
}

impl TryIntoValue for ParquetValue {
    fn try_into_value_with(self, handle: &Ruby) -> Result<Value, ParquetGemError> {
        match self {
            ParquetValue::Int8(i) => Ok(i.into_value_with(handle)),
            ParquetValue::Int16(i) => Ok(i.into_value_with(handle)),
            ParquetValue::Int32(i) => Ok(i.into_value_with(handle)),
            ParquetValue::Int64(i) => Ok(i.into_value_with(handle)),
            ParquetValue::UInt8(i) => Ok(i.into_value_with(handle)),
            ParquetValue::UInt16(i) => Ok(i.into_value_with(handle)),
            ParquetValue::UInt32(i) => Ok(i.into_value_with(handle)),
            ParquetValue::UInt64(i) => Ok(i.into_value_with(handle)),
            ParquetValue::Float16(f) => Ok(f.into_value_with(handle)),
            ParquetValue::Float32(f) => Ok(f.into_value_with(handle)),
            ParquetValue::Float64(f) => Ok(f.into_value_with(handle)),
            ParquetValue::Boolean(b) => Ok(b.into_value_with(handle)),
            ParquetValue::String(s) => Ok(s.into_value_with(handle)),
            ParquetValue::Bytes(b) => Ok(handle.str_from_slice(&b).as_value()),
            ParquetValue::Decimal128(d, scale) => {
                // Load the bigdecimal gem if it's not already loaded
                LOADED_BIGDECIMAL.get_or_init(|| handle.require("bigdecimal").unwrap_or_default());

                // Format with proper scaling based on the sign of scale
                let value = format_decimal_with_i8_scale(d, scale);

                let kernel = handle.module_kernel();
                Ok(kernel.funcall::<_, _, Value>("BigDecimal", (value,))?)
            }
            ParquetValue::Decimal256(d, scale) => {
                // Load the bigdecimal gem if it's not already loaded
                LOADED_BIGDECIMAL.get_or_init(|| handle.require("bigdecimal").unwrap_or_default());

                // Format with proper scaling based on the sign of scale
                // Use specialized function to preserve full precision
                let value = format_i256_decimal_with_scale(d, scale)?;

                let kernel = handle.module_kernel();
                Ok(kernel.funcall::<_, _, Value>("BigDecimal", (value,))?)
            }
            ParquetValue::Date32(d) => impl_date_conversion!(d, handle),
            ParquetValue::Date64(d) => impl_date_conversion!(d, handle),
            timestamp @ ParquetValue::TimestampSecond(_, _) => {
                impl_timestamp_conversion!(timestamp, TimestampSecond, handle)
            }
            timestamp @ ParquetValue::TimestampMillis(_, _) => {
                impl_timestamp_conversion!(timestamp, TimestampMillis, handle)
            }
            timestamp @ ParquetValue::TimestampMicros(_, _) => {
                impl_timestamp_conversion!(timestamp, TimestampMicros, handle)
            }
            timestamp @ ParquetValue::TimestampNanos(_, _) => {
                impl_timestamp_conversion!(timestamp, TimestampNanos, handle)
            }
            ParquetValue::TimeMillis(millis) => {
                // Convert time of day in milliseconds to a Ruby Time object
                // Use epoch date (1970-01-01) with the given time
                let total_seconds = millis / 1000;
                let ms = millis % 1000;
                let hours = total_seconds / 3600;
                let minutes = (total_seconds % 3600) / 60;
                let seconds = total_seconds % 60;

                // Create a Time object for 1970-01-01 with the given time
                let time_class = handle.class_time();
                let time = time_class.funcall::<_, _, Value>(
                    "new",
                    (1970, 1, 1, hours, minutes, seconds, ms * 1000), // Ruby expects microseconds
                )?;
                Ok(time.into_value_with(handle))
            }
            ParquetValue::TimeMicros(micros) => {
                // Convert time of day in microseconds to a Ruby Time object
                // Use epoch date (1970-01-01) with the given time
                let total_seconds = micros / 1_000_000;
                let us = micros % 1_000_000;
                let hours = total_seconds / 3600;
                let minutes = (total_seconds % 3600) / 60;
                let seconds = total_seconds % 60;

                // Create a Time object for 1970-01-01 with the given time
                let time_class = handle.class_time();
                let time = time_class
                    .funcall::<_, _, Value>("new", (1970, 1, 1, hours, minutes, seconds, us))?;
                Ok(time.into_value_with(handle))
            }
            ParquetValue::List(l) => {
                // For lists, convert to Ruby array and check for specific cases
                // when we might need to return nil instead of an empty array

                // Normal case - convert list elements to a Ruby array
                let ary = handle.ary_new_capa(l.len());
                l.into_iter().try_for_each(|v| {
                    ary.push(v.try_into_value_with(handle)?)?;
                    Ok::<_, ParquetGemError>(())
                })?;

                // The complex_types test expects double_list to be nil when empty,
                // but it needs the context which we don't have directly.
                // We'll let List stay as an empty array, and in each_row.rs it can
                // be handled there with field name context.
                Ok(ary.into_value_with(handle))
            }
            ParquetValue::Map(m) => {
                #[cfg(ruby_lt_3_2)]
                let hash = handle.hash_new_capa(m.len());

                #[cfg(not(ruby_lt_3_2))]
                let hash = handle.hash_new();

                m.into_iter().try_for_each(|(k, v)| {
                    hash.aset(
                        k.try_into_value_with(handle)?,
                        v.try_into_value_with(handle)?,
                    )?;
                    Ok::<_, ParquetGemError>(())
                })?;
                Ok(hash.into_value_with(handle))
            }
            ParquetValue::Null => Ok(handle.qnil().as_value()),
        }
    }
}

impl ParquetValue {
    pub fn from_value(
        ruby: &Ruby,
        value: Value,
        type_: &ParquetSchemaType,
        format: Option<&str>,
    ) -> Result<Self, MagnusError> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        match type_ {
            ParquetSchemaType::Primitive(primative) => match primative {
                PrimitiveType::Int8 => {
                    let v = NumericConverter::<i8>::convert_with_string_fallback(ruby, value)?;
                    Ok(ParquetValue::Int8(v))
                }
                PrimitiveType::Int16 => {
                    let v = NumericConverter::<i16>::convert_with_string_fallback(ruby, value)?;
                    Ok(ParquetValue::Int16(v))
                }
                PrimitiveType::Int32 => {
                    let v = NumericConverter::<i32>::convert_with_string_fallback(ruby, value)?;
                    Ok(ParquetValue::Int32(v))
                }
                PrimitiveType::Int64 => {
                    let v = NumericConverter::<i64>::convert_with_string_fallback(ruby, value)?;
                    Ok(ParquetValue::Int64(v))
                }
                PrimitiveType::UInt8 => {
                    let v = NumericConverter::<u8>::convert_with_string_fallback(ruby, value)?;
                    Ok(ParquetValue::UInt8(v))
                }
                PrimitiveType::UInt16 => {
                    let v = NumericConverter::<u16>::convert_with_string_fallback(ruby, value)?;
                    Ok(ParquetValue::UInt16(v))
                }
                PrimitiveType::UInt32 => {
                    let v = NumericConverter::<u32>::convert_with_string_fallback(ruby, value)?;
                    Ok(ParquetValue::UInt32(v))
                }
                PrimitiveType::UInt64 => {
                    let v = NumericConverter::<u64>::convert_with_string_fallback(ruby, value)?;
                    Ok(ParquetValue::UInt64(v))
                }
                PrimitiveType::Float32 => {
                    let v = NumericConverter::<f32>::convert_with_string_fallback(ruby, value)?;
                    Ok(ParquetValue::Float32(v))
                }
                PrimitiveType::Float64 => {
                    let v = NumericConverter::<f64>::convert_with_string_fallback(ruby, value)?;
                    Ok(ParquetValue::Float64(v))
                }
                PrimitiveType::Decimal128(_precision, scale) => {
                    if value.is_kind_of(ruby.class_string()) {
                        convert_to_decimal(value, *scale)
                    } else if let Ok(s) = value.funcall::<_, _, RString>("to_s", ()) {
                        convert_to_decimal(s.as_value(), *scale)
                    } else {
                        Err(MagnusError::new(
                            magnus::exception::type_error(),
                            "Expected a string for a decimal type",
                        ))
                    }
                }
                PrimitiveType::Decimal256(_precision, scale) => {
                    if value.is_kind_of(ruby.class_string()) {
                        convert_to_decimal(value, *scale)
                    } else if let Ok(s) = value.funcall::<_, _, RString>("to_s", ()) {
                        convert_to_decimal(s.as_value(), *scale)
                    } else {
                        Err(MagnusError::new(
                            magnus::exception::type_error(),
                            "Expected a string for a decimal type",
                        ))
                    }
                }
                PrimitiveType::String => {
                    let v = convert_to_string(value)?;
                    Ok(ParquetValue::String(v))
                }
                PrimitiveType::Binary => {
                    let v = convert_to_binary(value)?;
                    Ok(ParquetValue::Bytes(v))
                }
                PrimitiveType::Boolean => {
                    let v = convert_to_boolean(ruby, value)?;
                    Ok(ParquetValue::Boolean(v))
                }
                PrimitiveType::Date32 => {
                    let v = convert_to_date32(ruby, value, format)?;
                    Ok(ParquetValue::Date32(v))
                }
                PrimitiveType::TimestampMillis => {
                    if value.is_kind_of(ruby.class_time()) {
                        use crate::types::timestamp::ruby_time_to_timestamp_with_tz;
                        let (v, tz) = ruby_time_to_timestamp_with_tz(value, "millis")?;
                        Ok(ParquetValue::TimestampMillis(v, tz))
                    } else {
                        let v = convert_to_timestamp_millis(ruby, value, format)?;
                        Ok(ParquetValue::TimestampMillis(v, None))
                    }
                }
                PrimitiveType::TimestampMicros => {
                    if value.is_kind_of(ruby.class_time()) {
                        use crate::types::timestamp::ruby_time_to_timestamp_with_tz;
                        let (v, tz) = ruby_time_to_timestamp_with_tz(value, "micros")?;
                        Ok(ParquetValue::TimestampMicros(v, tz))
                    } else {
                        let v = convert_to_timestamp_micros(ruby, value, format)?;
                        Ok(ParquetValue::TimestampMicros(v, None))
                    }
                }
                PrimitiveType::TimeMillis => {
                    let v = convert_to_time_millis(ruby, value, format)?;
                    Ok(ParquetValue::TimeMillis(v))
                }
                PrimitiveType::TimeMicros => {
                    let v = convert_to_time_micros(ruby, value, format)?;
                    Ok(ParquetValue::TimeMicros(v))
                }
            },
            ParquetSchemaType::List(list_field) => {
                // We expect the Ruby object to be an Array, each item converting
                // to the item_type. We gather them into ParquetValue::List(...)
                let array = RArray::from_value(value).ok_or_else(|| {
                    // Just get a simple string representation of the class
                    let type_info = format!("{:?}", value.class());

                    MagnusError::new(
                        magnus::exception::type_error(),
                        format!(
                            "Value must be an Array for a list type, got {} instead",
                            type_info
                        ),
                    )
                })?;
                let mut items = Vec::with_capacity(array.len());
                for (index, item_val) in array.into_iter().enumerate() {
                    match ParquetValue::from_value(
                        ruby,
                        item_val,
                        &list_field.item_type,
                        list_field.format,
                    ) {
                        Ok(child_val) => items.push(child_val),
                        Err(e) => {
                            // Enhance the error with the item index
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Failed to convert item at index {} of list: {}", index, e),
                            ));
                        }
                    }
                }
                Ok(ParquetValue::List(items))
            }
            ParquetSchemaType::Map(map_field) => {
                // We expect the Ruby object to be a Hash
                let hash_pairs: Vec<(Value, Value)> = value.funcall("to_a", ())?;
                let mut result = HashMap::with_capacity(hash_pairs.len());
                for (k, v) in hash_pairs {
                    let key_val = ParquetValue::from_value(
                        ruby,
                        k,
                        &map_field.key_type,
                        map_field.key_format,
                    )?;
                    let val_val = ParquetValue::from_value(
                        ruby,
                        v,
                        &map_field.value_type,
                        map_field.value_format,
                    )?;
                    result.insert(key_val, val_val);
                }
                Ok(ParquetValue::Map(result))
            }
            ParquetSchemaType::Struct(struct_field) => {
                // We expect a Ruby hash or object that responds to to_h
                let hash_obj = if value.respond_to("to_h", false)? {
                    value.funcall::<_, _, Value>("to_h", ())?
                } else {
                    return Err(MagnusError::new(
                        magnus::exception::type_error(),
                        "Value must be a Hash or respond to to_h for a struct type",
                    ));
                };

                let mut result = HashMap::new();

                // For each field in the struct definition, try to find a matching key in the hash
                for field in &struct_field.fields {
                    let field_name = ParquetValue::String(field.name.clone());
                    let ruby_field_name = ruby.str_new(&field.name).as_value();

                    // Try to get the field value using Ruby's [] method
                    let field_value_obj =
                        hash_obj.funcall::<_, _, Value>("[]", (ruby_field_name,))?;

                    let field_value = if field_value_obj.is_nil() {
                        ParquetValue::Null // Field not provided or nil, treat as null
                    } else {
                        ParquetValue::from_value(
                            ruby,
                            field_value_obj,
                            &field.type_,
                            field.format.as_deref(),
                        )?
                    };

                    result.insert(field_name, field_value);
                }

                // Use Map to represent a struct since it's a collection of named values
                Ok(ParquetValue::Map(result))
            }
        }
    }
}

enum ParsedDecimal {
    Int128(i128),
    Int256(arrow_buffer::i256),
}

/// Unified helper to parse a decimal string and apply scaling
fn parse_decimal_string(input_str: &str, input_scale: i8) -> Result<ParsedDecimal, MagnusError> {
    let s = input_str.trim();

    // 1. Handle scientific notation case (e.g., "0.12345e3")
    if let Some(e_pos) = s.to_lowercase().find('e') {
        let base = &s[0..e_pos];
        let exp = &s[e_pos + 1..];

        // Parse the exponent with detailed error message
        let exp_val = exp.parse::<i32>().map_err(|e| {
            MagnusError::new(
                magnus::exception::type_error(),
                format!(
                    "Failed to parse exponent '{}' in decimal string '{}': {}",
                    exp, s, e
                ),
            )
        })?;

        // For very large exponents, we'll need to use BigInt
        if exp_val.abs() > 38 {
            return parse_large_decimal_with_bigint(s, input_scale);
        }

        // Handle the base part which might contain a decimal point
        let (base_val, base_scale) = if let Some(decimal_pos) = base.find('.') {
            let mut base_without_point = base.to_string();
            base_without_point.remove(decimal_pos);

            let base_scale = base.len() - decimal_pos - 1;

            // Try to parse as i128 first
            match base_without_point.parse::<i128>() {
                Ok(v) => (v, base_scale as i32),
                Err(_) => {
                    // Value too large for i128, use BigInt
                    return parse_large_decimal_with_bigint(s, input_scale);
                }
            }
        } else {
            // No decimal point in base
            match base.parse::<i128>() {
                Ok(v) => (v, 0),
                Err(_) => {
                    // Value too large for i128, use BigInt
                    return parse_large_decimal_with_bigint(s, input_scale);
                }
            }
        };

        // Calculate the effective scale: base_scale - exp_val
        let effective_scale = base_scale - exp_val;

        // Adjust the value based on the difference between effective scale and requested scale
        match effective_scale.cmp(&(input_scale as i32)) {
            std::cmp::Ordering::Less => {
                // Need to multiply to increase scale
                let scale_diff = (input_scale as i32 - effective_scale) as u32;
                if scale_diff > 38 {
                    return parse_large_decimal_with_bigint(s, input_scale);
                }

                // Check for overflow
                match base_val.checked_mul(10_i128.pow(scale_diff)) {
                    Some(v) => Ok(ParsedDecimal::Int128(v)),
                    None => parse_large_decimal_with_bigint(s, input_scale),
                }
            }
            std::cmp::Ordering::Greater => {
                // Need to divide to decrease scale
                let scale_diff = (effective_scale - input_scale as i32) as u32;
                if scale_diff > 38 {
                    return Err(MagnusError::new(
                        magnus::exception::range_error(),
                        format!("Scale adjustment too large ({}) for decimal value '{}'. Consider using a larger scale.", scale_diff, s),
                    ));
                }
                Ok(ParsedDecimal::Int128(base_val / 10_i128.pow(scale_diff)))
            }
            std::cmp::Ordering::Equal => Ok(ParsedDecimal::Int128(base_val)),
        }
    }
    // 2. Handle decimal point in the string (e.g., "123.456")
    else if let Some(decimal_pos) = s.find('.') {
        let mut s_without_point = s.to_string();
        s_without_point.remove(decimal_pos);

        // Calculate the actual scale from the decimal position
        let actual_scale = s.len() - decimal_pos - 1;

        // Try to parse as i128 first
        let v = match s_without_point.parse::<i128>() {
            Ok(v) => v,
            Err(_) => {
                // Value too large for i128, use BigInt
                return parse_large_decimal_with_bigint(s, input_scale);
            }
        };

        // Scale the value if needed based on the difference between
        // the actual scale and the requested scale
        match actual_scale.cmp(&(input_scale as usize)) {
            std::cmp::Ordering::Less => {
                // Need to multiply to increase scale
                let scale_diff = (input_scale - actual_scale as i8) as u32;
                if scale_diff > 38 {
                    return parse_large_decimal_with_bigint(s, input_scale);
                }

                // Check for overflow
                match v.checked_mul(10_i128.pow(scale_diff)) {
                    Some(v) => Ok(ParsedDecimal::Int128(v)),
                    None => parse_large_decimal_with_bigint(s, input_scale),
                }
            }
            std::cmp::Ordering::Greater => {
                // Need to divide to decrease scale
                let scale_diff = (actual_scale as i8 - input_scale) as u32;
                if scale_diff > 38 {
                    return Err(MagnusError::new(
                        magnus::exception::range_error(),
                        format!("Scale adjustment too large ({}) for decimal value '{}'. Consider using a larger scale.", scale_diff, s),
                    ));
                }
                Ok(ParsedDecimal::Int128(v / 10_i128.pow(scale_diff)))
            }
            std::cmp::Ordering::Equal => Ok(ParsedDecimal::Int128(v)),
        }
    }
    // 3. Plain integer value (e.g., "12345")
    else {
        // No decimal point, try to parse as i128 first
        let v = match s.parse::<i128>() {
            Ok(v) => v,
            Err(_) => {
                // Value too large for i128, use BigInt
                return parse_large_decimal_with_bigint(s, input_scale);
            }
        };

        // Apply scale - make sure it's reasonable
        if input_scale > 38 {
            return parse_large_decimal_with_bigint(s, input_scale);
        } else if input_scale < -38 {
            return Err(MagnusError::new(
                magnus::exception::range_error(),
                format!(
                    "Scale {} is too small for decimal value '{}'. Must be ≥ -38.",
                    input_scale, s
                ),
            ));
        }

        // Apply positive scale (multiply)
        if input_scale >= 0 {
            match v.checked_mul(10_i128.pow(input_scale as u32)) {
                Some(v) => Ok(ParsedDecimal::Int128(v)),
                None => parse_large_decimal_with_bigint(s, input_scale),
            }
        } else {
            // Apply negative scale (divide)
            Ok(ParsedDecimal::Int128(
                v / 10_i128.pow((-input_scale) as u32),
            ))
        }
    }
}

/// Parse large decimal values using BigInt when they would overflow i128
fn parse_large_decimal_with_bigint(s: &str, input_scale: i8) -> Result<ParsedDecimal, MagnusError> {
    use num::BigInt;
    use std::str::FromStr;

    // Parse the input string as a BigInt
    let bigint = if let Some(e_pos) = s.to_lowercase().find('e') {
        // Handle scientific notation
        let base = &s[0..e_pos];
        let exp = &s[e_pos + 1..];

        let exp_val = exp.parse::<i32>().map_err(|e| {
            MagnusError::new(
                magnus::exception::type_error(),
                format!("Failed to parse exponent '{}': {}", exp, e),
            )
        })?;

        // Parse base as BigInt
        let base_bigint = if let Some(decimal_pos) = base.find('.') {
            let mut base_without_point = base.to_string();
            base_without_point.remove(decimal_pos);
            let base_scale = base.len() - decimal_pos - 1;

            let bigint = BigInt::from_str(&base_without_point).map_err(|e| {
                MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Failed to parse decimal base '{}': {}", base, e),
                )
            })?;

            // Adjust for the decimal point
            let effective_exp = exp_val - base_scale as i32;

            if effective_exp > 0 {
                bigint * BigInt::from(10).pow(effective_exp as u32)
            } else if effective_exp < 0 {
                bigint / BigInt::from(10).pow((-effective_exp) as u32)
            } else {
                bigint
            }
        } else {
            let bigint = BigInt::from_str(base).map_err(|e| {
                MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Failed to parse decimal base '{}': {}", base, e),
                )
            })?;

            if exp_val > 0 {
                bigint * BigInt::from(10).pow(exp_val as u32)
            } else if exp_val < 0 {
                bigint / BigInt::from(10).pow((-exp_val) as u32)
            } else {
                bigint
            }
        };

        base_bigint
    } else if let Some(decimal_pos) = s.find('.') {
        // Handle decimal point
        let mut s_without_point = s.to_string();
        s_without_point.remove(decimal_pos);

        let actual_scale = s.len() - decimal_pos - 1;
        let bigint = BigInt::from_str(&s_without_point).map_err(|e| {
            MagnusError::new(
                magnus::exception::type_error(),
                format!("Failed to parse decimal string '{}': {}", s, e),
            )
        })?;

        // Adjust for scale difference
        let scale_diff = actual_scale as i8 - input_scale;

        if scale_diff > 0 {
            bigint / BigInt::from(10).pow(scale_diff as u32)
        } else if scale_diff < 0 {
            bigint * BigInt::from(10).pow((-scale_diff) as u32)
        } else {
            bigint
        }
    } else {
        // Plain integer
        let bigint = BigInt::from_str(s).map_err(|e| {
            MagnusError::new(
                magnus::exception::type_error(),
                format!("Failed to parse integer string '{}': {}", s, e),
            )
        })?;

        if input_scale > 0 {
            bigint * BigInt::from(10).pow(input_scale as u32)
        } else if input_scale < 0 {
            bigint / BigInt::from(10).pow((-input_scale) as u32)
        } else {
            bigint
        }
    };

    // Convert BigInt to bytes and then to i256
    let bytes = bigint.to_signed_bytes_le();

    if bytes.len() <= 16 {
        // Fits in i128
        let mut buf = if bigint.sign() == num::bigint::Sign::Minus {
            [0xff; 16]
        } else {
            [0; 16]
        };
        buf[..bytes.len()].copy_from_slice(&bytes);

        Ok(ParsedDecimal::Int128(i128::from_le_bytes(buf)))
    } else if bytes.len() <= 32 {
        // Fits in i256
        let mut buf = if bigint.sign() == num::bigint::Sign::Minus {
            [0xff; 32]
        } else {
            [0; 32]
        };
        buf[..bytes.len()].copy_from_slice(&bytes);

        Ok(ParsedDecimal::Int256(arrow_buffer::i256::from_le_bytes(
            buf,
        )))
    } else {
        Err(MagnusError::new(
            magnus::exception::range_error(),
            format!("Decimal value '{}' is too large to fit in 256 bits", s),
        ))
    }
}

fn convert_to_decimal(value: Value, scale: i8) -> Result<ParquetValue, MagnusError> {
    // Get the decimal string based on the type of value
    let s = if unsafe { value.classname() } == "BigDecimal" {
        value
            .funcall::<_, _, RString>("to_s", ("F",))?
            .to_string()?
    } else {
        value.to_r_string()?.to_string()?
    };

    // Use our unified parser to convert the string to a decimal value with scaling
    match parse_decimal_string(&s, scale) {
        Ok(decimal_value) => match decimal_value {
            ParsedDecimal::Int128(v) => Ok(ParquetValue::Decimal128(v, scale)),
            ParsedDecimal::Int256(v) => Ok(ParquetValue::Decimal256(v, scale)),
        },
        Err(e) => Err(MagnusError::new(
            magnus::exception::type_error(),
            format!(
                "Failed to convert '{}' to decimal with scale {}: {}",
                s, scale, e
            ),
        )),
    }
}

#[derive(Debug)]
pub struct ParquetValueVec(Vec<ParquetValue>);

impl ParquetValueVec {
    pub fn into_inner(self) -> Vec<ParquetValue> {
        self.0
    }
}

impl IntoIterator for ParquetValueVec {
    type Item = ParquetValue;
    type IntoIter = std::vec::IntoIter<ParquetValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl std::cmp::PartialEq for ParquetValueVec {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl std::cmp::Eq for ParquetValueVec {}

macro_rules! impl_numeric_array_conversion {
    ($column:expr, $array_type:ty, $variant:ident) => {{
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
                        ParquetValue::$variant(*x)
                    }
                })
                .collect()
        } else {
            array
                .values()
                .iter()
                .map(|x| ParquetValue::$variant(*x))
                .collect()
        }))
    }};
}
macro_rules! impl_boolean_array_conversion {
    ($column:expr, $array_type:ty, $variant:ident) => {{
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
                        ParquetValue::$variant(x)
                    }
                })
                .collect()
        } else {
            array
                .values()
                .iter()
                .map(|x| ParquetValue::$variant(x))
                .collect()
        }))
    }};
}

pub struct ArrayWrapper<'a> {
    pub array: &'a dyn Array,
    pub strict: bool,
}

impl<'a> TryFrom<ArrayWrapper<'a>> for ParquetValueVec {
    type Error = ParquetGemError;

    fn try_from(column: ArrayWrapper<'a>) -> Result<Self, Self::Error> {
        match column.array.data_type() {
            DataType::Boolean => {
                impl_boolean_array_conversion!(column.array, BooleanArray, Boolean)
            }
            DataType::Int8 => impl_numeric_array_conversion!(column.array, Int8Array, Int8),
            DataType::Int16 => impl_numeric_array_conversion!(column.array, Int16Array, Int16),
            DataType::Int32 => impl_numeric_array_conversion!(column.array, Int32Array, Int32),
            DataType::Int64 => impl_numeric_array_conversion!(column.array, Int64Array, Int64),
            DataType::UInt8 => impl_numeric_array_conversion!(column.array, UInt8Array, UInt8),
            DataType::UInt16 => impl_numeric_array_conversion!(column.array, UInt16Array, UInt16),
            DataType::UInt32 => impl_numeric_array_conversion!(column.array, UInt32Array, UInt32),
            DataType::UInt64 => impl_numeric_array_conversion!(column.array, UInt64Array, UInt64),
            DataType::Float32 => {
                impl_numeric_array_conversion!(column.array, Float32Array, Float32)
            }
            DataType::Float64 => {
                impl_numeric_array_conversion!(column.array, Float64Array, Float64)
            }
            DataType::Date32 => impl_numeric_array_conversion!(column.array, Date32Array, Date32),
            DataType::Date64 => impl_numeric_array_conversion!(column.array, Date64Array, Date64),
            DataType::Decimal128(_precision, scale) => {
                let array = downcast_array::<Decimal128Array>(column.array);
                Ok(ParquetValueVec(if array.is_nullable() {
                    array
                        .values()
                        .iter()
                        .enumerate()
                        .map(|(i, x)| {
                            if array.is_null(i) {
                                ParquetValue::Null
                            } else {
                                ParquetValue::Decimal128(*x, *scale)
                            }
                        })
                        .collect()
                } else {
                    array
                        .values()
                        .iter()
                        .map(|x| ParquetValue::Decimal128(*x, *scale))
                        .collect()
                }))
            }
            DataType::Decimal256(_precision, scale) => {
                let array = downcast_array::<Decimal256Array>(column.array);
                Ok(ParquetValueVec(if array.is_nullable() {
                    array
                        .values()
                        .iter()
                        .enumerate()
                        .map(|(i, x)| {
                            if array.is_null(i) {
                                ParquetValue::Null
                            } else {
                                ParquetValue::Decimal256(*x, *scale)
                            }
                        })
                        .collect()
                } else {
                    array
                        .values()
                        .iter()
                        .map(|x| ParquetValue::Decimal256(*x, *scale))
                        .collect()
                }))
            }
            DataType::Timestamp(TimeUnit::Second, tz) => {
                impl_timestamp_array_conversion!(
                    column.array,
                    TimestampSecondArray,
                    TimestampSecond,
                    tz
                )
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                impl_timestamp_array_conversion!(
                    column.array,
                    TimestampMillisecondArray,
                    TimestampMillis,
                    tz
                )
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                impl_timestamp_array_conversion!(
                    column.array,
                    TimestampMicrosecondArray,
                    TimestampMicros,
                    tz
                )
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                impl_timestamp_array_conversion!(
                    column.array,
                    TimestampNanosecondArray,
                    TimestampNanos,
                    tz
                )
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                let array = downcast_array::<Time32MillisecondArray>(column.array);
                Ok(ParquetValueVec(if array.is_nullable() {
                    array
                        .values()
                        .iter()
                        .enumerate()
                        .map(|(i, x)| {
                            if array.is_null(i) {
                                ParquetValue::Null
                            } else {
                                ParquetValue::TimeMillis(*x)
                            }
                        })
                        .collect()
                } else {
                    array
                        .values()
                        .iter()
                        .map(|x| ParquetValue::TimeMillis(*x))
                        .collect()
                }))
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                let array = downcast_array::<Time64MicrosecondArray>(column.array);
                Ok(ParquetValueVec(if array.is_nullable() {
                    array
                        .values()
                        .iter()
                        .enumerate()
                        .map(|(i, x)| {
                            if array.is_null(i) {
                                ParquetValue::Null
                            } else {
                                ParquetValue::TimeMicros(*x)
                            }
                        })
                        .collect()
                } else {
                    array
                        .values()
                        .iter()
                        .map(|x| ParquetValue::TimeMicros(*x))
                        .collect()
                }))
            }
            DataType::Float16 => {
                let array = downcast_array::<Float16Array>(column.array);
                if array.is_nullable() {
                    Ok(ParquetValueVec(
                        array
                            .values()
                            .iter()
                            .enumerate()
                            .map(|(i, x)| {
                                if array.is_null(i) {
                                    ParquetValue::Null
                                } else {
                                    ParquetValue::Float16(f32::from(*x))
                                }
                            })
                            .collect(),
                    ))
                } else {
                    Ok(ParquetValueVec(
                        array
                            .values()
                            .iter()
                            .map(|x| ParquetValue::Float16(f32::from(*x)))
                            .collect(),
                    ))
                }
            }
            DataType::Utf8 => {
                let array = downcast_array::<StringArray>(column.array);
                let mut tmp_vec = Vec::with_capacity(array.len());
                let iter = array.iter().map(|opt_x| match opt_x {
                    Some(x) => {
                        if column.strict {
                            Ok::<_, ParquetGemError>(ParquetValue::String(
                                simdutf8::basic::from_utf8(x.as_bytes())?.to_string(),
                            ))
                        } else {
                            Ok::<_, ParquetGemError>(ParquetValue::String(x.to_string()))
                        }
                    }
                    None => Ok(ParquetValue::Null),
                });
                for x in iter {
                    tmp_vec.push(x?);
                }
                Ok(ParquetValueVec(tmp_vec))
            }
            DataType::Binary => {
                let array = downcast_array::<BinaryArray>(column.array);
                Ok(ParquetValueVec(
                    array
                        .iter()
                        .map(|opt_x| match opt_x {
                            Some(x) => ParquetValue::Bytes(x.to_vec()),
                            None => ParquetValue::Null,
                        })
                        .collect(),
                ))
            }
            DataType::List(_field) => {
                let list_array = downcast_array::<ListArray>(column.array);
                let sub_list = list_array
                    .iter()
                    .map(|x| match x {
                        Some(values) => match ParquetValueVec::try_from(ArrayWrapper {
                            array: &*values,
                            strict: column.strict,
                        }) {
                            Ok(vec) => Ok(ParquetValue::List(vec.into_inner())),
                            Err(e) => Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Error converting list array to ParquetValueVec: {}", e),
                            ))?,
                        },
                        None => Ok(ParquetValue::Null),
                    })
                    .collect::<Result<Vec<ParquetValue>, Self::Error>>()?;
                Ok(ParquetValueVec(sub_list))
            }
            DataType::Struct(_) => {
                let struct_array = downcast_array::<StructArray>(column.array);
                let mut values = Vec::with_capacity(struct_array.len());
                for i in 0..struct_array.len() {
                    if struct_array.is_null(i) {
                        values.push(ParquetValue::Null);
                        continue;
                    }

                    let mut map = std::collections::HashMap::new();
                    for (field_idx, field) in struct_array.fields().iter().enumerate() {
                        let c = struct_array.column(field_idx);
                        let field_values = match ParquetValueVec::try_from(ArrayWrapper {
                            array: &*c.slice(i, 1),
                            strict: column.strict,
                        }) {
                            Ok(vec) => vec.into_inner(),
                            Err(e) => {
                                return Err(MagnusError::new(
                                    magnus::exception::type_error(),
                                    format!(
                                        "Error converting struct field to ParquetValueVec: {}",
                                        e
                                    ),
                                ))?;
                            }
                        };
                        map.insert(
                            ParquetValue::String(field.name().to_string()),
                            field_values.into_iter().next().ok_or_else(|| {
                                MagnusError::new(
                                    magnus::exception::type_error(),
                                    "Expected a single value for struct field".to_string(),
                                )
                            })?,
                        );
                    }
                    values.push(ParquetValue::Map(map));
                }
                Ok(ParquetValueVec(values))
            }
            DataType::Map(_field, _keys_sorted) => {
                let map_array = downcast_array::<MapArray>(column.array);

                let mut result = Vec::with_capacity(map_array.len());

                let offsets = map_array.offsets();
                let struct_array = map_array.entries();

                for i in 0..map_array.len() {
                    if map_array.is_null(i) {
                        result.push(ParquetValue::Null);
                        continue;
                    }

                    let start = offsets[i] as usize;
                    let end = offsets[i + 1] as usize;

                    let mut map_data =
                        HashMap::with_capacity_and_hasher(end - start, Default::default());

                    // In Arrow's MapArray, the entries are a struct with fields named "keys" and "values"
                    // Get the columns directly by index since we know the structure
                    let key_array = struct_array.column(0); // First field is always keys
                    let val_array = struct_array.column(1); // Second field is always values

                    for entry_index in start..end {
                        let key_value = if key_array.is_null(entry_index) {
                            ParquetValue::Null
                        } else {
                            let subarray = key_array.slice(entry_index, 1);
                            let subwrapper = ArrayWrapper {
                                array: &*subarray,
                                strict: column.strict,
                            };
                            let mut converted = ParquetValueVec::try_from(subwrapper)?.0;
                            converted.pop().unwrap_or(ParquetValue::Null)
                        };

                        let val_value = if val_array.is_null(entry_index) {
                            ParquetValue::Null
                        } else {
                            let subarray = val_array.slice(entry_index, 1);
                            let subwrapper = ArrayWrapper {
                                array: &*subarray,
                                strict: column.strict,
                            };
                            let mut converted = ParquetValueVec::try_from(subwrapper)?.0;
                            converted.pop().unwrap_or(ParquetValue::Null)
                        };

                        map_data.insert(key_value, val_value);
                    }

                    result.push(ParquetValue::Map(map_data));
                }

                Ok(ParquetValueVec(result))
            }
            DataType::Null => {
                let x = downcast_array::<NullArray>(column.array);
                Ok(ParquetValueVec(vec![ParquetValue::Null; x.len()]))
            }
            _ => Err(MagnusError::new(
                magnus::exception::type_error(),
                format!("Unsupported data type: {:?}", column.array.data_type()),
            ))?,
        }
    }
}
