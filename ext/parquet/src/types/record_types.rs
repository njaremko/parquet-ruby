use std::sync::OnceLock;

use itertools::Itertools;
use parquet::{
    basic::{ConvertedType, LogicalType},
    data_type::AsBytes,
};

use super::*;

pub static LOADED_BIGDECIMAL: OnceLock<bool> = OnceLock::new();

/// Format decimal value with appropriate scale for BigDecimal conversion
/// Handles positive and negative scales correctly for i8 scale
pub fn format_decimal_with_i8_scale<T: std::fmt::Display>(value: T, scale: i8) -> String {
    if scale >= 0 {
        // Positive scale means divide (move decimal point left)
        format!("{}e-{}", value, scale)
    } else {
        // Negative scale means multiply (move decimal point right)
        format!("{}e{}", value, -scale)
    }
}

/// Format i256 decimal value with appropriate scale for BigDecimal conversion
/// Uses bytes conversion to preserve full precision
pub fn format_i256_decimal_with_scale(
    value: arrow_buffer::i256,
    scale: i8,
) -> Result<String, ParquetGemError> {
    // Convert i256 to big-endian bytes
    let bytes = value.to_be_bytes();

    // Use the existing bytes_to_decimal function which handles full precision
    bytes_to_decimal(&bytes, scale as i32)
}

/// Format decimal value with appropriate scale for BigDecimal conversion
/// Handles positive and negative scales correctly for i32 scale
pub fn format_decimal_with_i32_scale<T: std::fmt::Display>(value: T, scale: i32) -> String {
    if scale >= 0 {
        // Positive scale means divide (move decimal point left)
        format!("{}e-{}", value, scale)
    } else {
        // Negative scale means multiply (move decimal point right)
        format!("{}e{}", value, -scale)
    }
}

/// Convert arbitrary-length big-endian byte array to decimal string
/// Supports byte arrays from 1 to 32 bytes in length
fn bytes_to_decimal(bytes: &[u8], scale: i32) -> Result<String, ParquetGemError> {
    match bytes.len() {
        0 => Err(ParquetGemError::InvalidDecimal(
            "Empty byte array for decimal".to_string(),
        )),
        1 => {
            // For 1 byte, use i8
            let value = bytes[0] as i8;
            Ok(format_decimal_with_i32_scale(value, scale))
        }
        2 => {
            // For 2 bytes, use i16
            let mut value: i16 = 0;
            let is_negative = bytes[0] & 0x80 != 0;

            for &byte in bytes {
                value = (value << 8) | (byte as i16);
            }

            // Sign extend if negative
            if is_negative {
                let shift = 16 - (bytes.len() * 8);
                value = (value << shift) >> shift;
            }

            Ok(format_decimal_with_i32_scale(value, scale))
        }
        3..=4 => {
            // For 3-4 bytes, use i32
            let mut value: i32 = 0;
            let is_negative = bytes[0] & 0x80 != 0;

            for &byte in bytes {
                value = (value << 8) | (byte as i32);
            }

            // Sign extend if negative
            if is_negative {
                let shift = 32 - (bytes.len() * 8);
                value = (value << shift) >> shift;
            }

            Ok(format_decimal_with_i32_scale(value, scale))
        }
        5..=8 => {
            // For 5-8 bytes, use i64
            let mut value: i64 = 0;
            let is_negative = bytes[0] & 0x80 != 0;

            for &byte in bytes {
                value = (value << 8) | (byte as i64);
            }

            // Sign extend if negative
            if is_negative {
                let shift = 64 - (bytes.len() * 8);
                value = (value << shift) >> shift;
            }

            Ok(format_decimal_with_i32_scale(value, scale))
        }
        9..=16 => {
            // For 9-16 bytes, use i128
            let mut value: i128 = 0;
            let is_negative = bytes[0] & 0x80 != 0;

            for &byte in bytes {
                value = (value << 8) | (byte as i128);
            }

            // Sign extend if negative
            if is_negative {
                let shift = 128 - (bytes.len() * 8);
                value = (value << shift) >> shift;
            }

            Ok(format_decimal_with_i32_scale(value, scale))
        }
        17..=32 => {
            // For 17-32 bytes, we need arbitrary precision handling
            // Check if the number is negative (MSB of first byte)
            let is_negative = bytes[0] & 0x80 != 0;

            if is_negative {
                // For negative numbers, we need to compute two's complement
                // First, invert all bits
                let mut inverted = Vec::with_capacity(bytes.len());
                for &byte in bytes {
                    inverted.push(!byte);
                }

                // Then add 1
                let mut carry = 1u8;
                for i in (0..inverted.len()).rev() {
                    let (sum, new_carry) = inverted[i].overflowing_add(carry);
                    inverted[i] = sum;
                    carry = if new_carry { 1 } else { 0 };
                }

                // Convert to decimal string
                let mut result = String::new();
                let mut remainder = inverted;

                // Repeatedly divide by 10 to get decimal digits
                while !remainder.iter().all(|&b| b == 0) {
                    let mut carry = 0u16;
                    for i in 0..remainder.len() {
                        let temp = (carry << 8) | (remainder[i] as u16);
                        remainder[i] = (temp / 10) as u8;
                        carry = temp % 10;
                    }
                    result.push_str(&carry.to_string());
                }

                // The digits are in reverse order
                if result.is_empty() {
                    result = "0".to_string();
                } else {
                    result = result.chars().rev().collect();
                }

                // Add negative sign and format with scale
                Ok(format_decimal_with_i32_scale(format!("-{}", result), scale))
            } else {
                // For positive numbers, direct conversion
                let mut result = String::new();
                let mut remainder = bytes.to_vec();

                // Repeatedly divide by 10 to get decimal digits
                while !remainder.iter().all(|&b| b == 0) {
                    let mut carry = 0u16;
                    for i in 0..remainder.len() {
                        let temp = (carry << 8) | (remainder[i] as u16);
                        remainder[i] = (temp / 10) as u8;
                        carry = temp % 10;
                    }
                    result.push_str(&carry.to_string());
                }

                // The digits are in reverse order
                if result.is_empty() {
                    result = "0".to_string();
                } else {
                    result = result.chars().rev().collect();
                }

                Ok(format_decimal_with_i32_scale(result, scale))
            }
        }
        _ => Err(ParquetGemError::InvalidDecimal(format!(
            "Unsupported decimal byte array size: {} (maximum 32 bytes)",
            bytes.len()
        ))),
    }
}

#[derive(Debug)]
pub enum RowRecord<S: BuildHasher + Default> {
    Vec(Vec<ParquetField>),
    Map(HashMap<StringCacheKey, ParquetField, S>),
}

#[derive(Debug)]
pub enum ColumnRecord<S: BuildHasher + Default> {
    Vec(Vec<Vec<ParquetValue>>),
    Map(HashMap<StringCacheKey, Vec<ParquetValue>, S>),
}

#[derive(Debug)]
pub struct ParquetField {
    pub field: Field,
    #[allow(dead_code)]
    pub converted_type: ConvertedType,
    pub logical_type: Option<LogicalType>,
    pub strict: bool,
}

impl<S: BuildHasher + Default> TryIntoValue for RowRecord<S> {
    fn try_into_value_with(self, handle: &Ruby) -> Result<Value, ParquetGemError> {
        match self {
            RowRecord::Vec(vec) => {
                let ary = handle.ary_new_capa(vec.len());
                vec.into_iter().try_for_each(|v| {
                    ary.push(v.try_into_value_with(handle)?)?;
                    Ok::<_, ParquetGemError>(())
                })?;
                Ok(handle.into_value(ary))
            }
            RowRecord::Map(map) => {
                #[cfg(ruby_lt_3_2)]
                let hash = handle.hash_new_capa(map.len());

                #[cfg(not(ruby_lt_3_2))]
                let hash = handle.hash_new();

                let mut values: [Value; 128] = [handle.qnil().as_value(); 128];
                let mut i = 0;

                for chunk in &map.into_iter().chunks(64) {
                    // Reduced to 64 to ensure space for pairs
                    for (k, v) in chunk {
                        if i + 1 >= values.len() {
                            // Bulk insert current batch if array is full
                            hash.bulk_insert(&values[..i])?;
                            values[..i].fill(handle.qnil().as_value());
                            i = 0;
                        }
                        values[i] = handle.into_value(k);
                        values[i + 1] = v.try_into_value_with(handle)?;
                        i += 2;
                    }
                    // Insert any remaining pairs
                    if i > 0 {
                        hash.bulk_insert(&values[..i])?;
                        values[..i].fill(handle.qnil().as_value());
                        i = 0;
                    }
                }

                Ok(hash.into_value_with(handle))
            }
        }
    }
}

impl<S: BuildHasher + Default> TryIntoValue for ColumnRecord<S> {
    fn try_into_value_with(self, handle: &Ruby) -> Result<Value, ParquetGemError> {
        match self {
            ColumnRecord::Vec(vec) => {
                let ary = handle.ary_new_capa(vec.len());
                vec.into_iter().try_for_each(|v| {
                    let nested_ary = handle.ary_new_capa(v.len());
                    v.into_iter().try_for_each(|v| {
                        nested_ary.push(v.try_into_value_with(handle)?)?;
                        Ok::<_, ParquetGemError>(())
                    })?;
                    ary.push(nested_ary.into_value_with(handle))?;
                    Ok::<_, ParquetGemError>(())
                })?;
                Ok(ary.into_value_with(handle))
            }
            ColumnRecord::Map(map) => {
                #[cfg(ruby_lt_3_2)]
                let hash = handle.hash_new_capa(map.len());

                #[cfg(not(ruby_lt_3_2))]
                let hash = handle.hash_new();

                let mut values: [Value; 128] = [handle.qnil().as_value(); 128];
                let mut i = 0;

                for chunk in &map.into_iter().chunks(64) {
                    // Reduced to 64 to ensure space for pairs
                    for (k, v) in chunk {
                        if i + 1 >= values.len() {
                            // Bulk insert current batch if array is full
                            hash.bulk_insert(&values[..i])?;
                            values[..i].fill(handle.qnil().as_value());
                            i = 0;
                        }
                        values[i] = handle.into_value(k);
                        let ary = handle.ary_new_capa(v.len());
                        v.into_iter().try_for_each(|v| {
                            ary.push(v.try_into_value_with(handle)?)?;
                            Ok::<_, ParquetGemError>(())
                        })?;
                        values[i + 1] = handle.into_value(ary);
                        i += 2;
                    }
                    // Insert any remaining pairs
                    if i > 0 {
                        hash.bulk_insert(&values[..i])?;
                        values[..i].fill(handle.qnil().as_value());
                        i = 0;
                    }
                }

                Ok(hash.into_value_with(handle))
            }
        }
    }
}

pub trait TryIntoValue {
    fn try_into_value_with(self, handle: &Ruby) -> Result<Value, ParquetGemError>;
}

impl TryIntoValue for ParquetField {
    fn try_into_value_with(self, handle: &Ruby) -> Result<Value, ParquetGemError> {
        match self.field {
            Field::Null => Ok(handle.qnil().as_value()),
            Field::Bool(b) => Ok(b.into_value_with(handle)),
            Field::Short(s) => Ok(s.into_value_with(handle)),
            Field::Int(i) => Ok(i.into_value_with(handle)),
            Field::Long(l) => Ok(l.into_value_with(handle)),
            Field::UByte(ub) => Ok(ub.into_value_with(handle)),
            Field::UShort(us) => Ok(us.into_value_with(handle)),
            Field::UInt(ui) => Ok(ui.into_value_with(handle)),
            Field::ULong(ul) => Ok(ul.into_value_with(handle)),
            Field::Float16(f) => Ok(f32::from(f).into_value_with(handle)),
            Field::Float(f) => Ok(f.into_value_with(handle)),
            Field::Double(d) => Ok(d.into_value_with(handle)),
            Field::Str(s) => {
                if self.strict {
                    Ok(simdutf8::basic::from_utf8(s.as_bytes())
                        .map_err(ParquetGemError::Utf8Error)
                        .map(|s| s.into_value_with(handle))?)
                } else {
                    let s = String::from_utf8_lossy(s.as_bytes());
                    Ok(s.into_value_with(handle))
                }
            }
            Field::Byte(b) => Ok(b.into_value_with(handle)),
            Field::Bytes(b) => {
                if matches!(self.logical_type, Some(parquet::basic::LogicalType::Uuid)) {
                    let bytes = b.as_bytes();
                    let uuid = uuid::Uuid::from_slice(bytes)?;
                    Ok(uuid.to_string().into_value_with(handle))
                } else {
                    Ok(handle.str_from_slice(b.data()).as_value())
                }
            }
            Field::Date(d) => {
                let ts = jiff::Timestamp::from_second((d as i64) * 86400)?;
                let formatted = ts.strftime("%Y-%m-%d").to_string();
                Ok(formatted.into_value_with(handle))
            }
            Field::TimeMillis(ts) => {
                let ts = jiff::Timestamp::from_millisecond(ts as i64)?;
                let time_class = handle.class_time();
                Ok(time_class
                    .funcall::<_, _, Value>("parse", (ts.to_string(),))?
                    .into_value_with(handle))
            }
            Field::TimestampMillis(ts) => {
                let ts = jiff::Timestamp::from_millisecond(ts)?;
                let time_class = handle.class_time();
                Ok(time_class
                    .funcall::<_, _, Value>("parse", (ts.to_string(),))?
                    .into_value_with(handle))
            }
            Field::TimestampMicros(ts) | Field::TimeMicros(ts) => {
                let ts = jiff::Timestamp::from_microsecond(ts)?;
                let time_class = handle.class_time();
                Ok(time_class
                    .funcall::<_, _, Value>("parse", (ts.to_string(),))?
                    .into_value_with(handle))
            }
            Field::ListInternal(list) => {
                let elements = list.elements();
                let ary = handle.ary_new_capa(elements.len());
                elements.iter().try_for_each(|e| {
                    ary.push(
                        ParquetField {
                            field: e.clone(),
                            logical_type: e.to_logical_type(),
                            converted_type: e.to_converted_type(),
                            strict: self.strict,
                        }
                        .try_into_value_with(handle)?,
                    )?;
                    Ok::<_, ParquetGemError>(())
                })?;
                Ok(ary.into_value_with(handle))
            }
            Field::MapInternal(map) => {
                #[cfg(ruby_lt_3_2)]
                let hash = handle.hash_new_capa(map.len());

                #[cfg(not(ruby_lt_3_2))]
                let hash = handle.hash_new();

                map.entries().iter().try_for_each(|(k, v)| {
                    hash.aset(
                        ParquetField {
                            field: k.clone(),
                            converted_type: k.to_converted_type(),
                            logical_type: k.to_logical_type(),
                            strict: self.strict,
                        }
                        .try_into_value_with(handle)?,
                        ParquetField {
                            field: v.clone(),
                            converted_type: v.to_converted_type(),
                            logical_type: v.to_logical_type(),
                            strict: self.strict,
                        }
                        .try_into_value_with(handle)?,
                    )?;
                    Ok::<_, ParquetGemError>(())
                })?;
                Ok(hash.into_value_with(handle))
            }
            Field::Decimal(d) => {
                let value = match d {
                    Decimal::Int32 { value, scale, .. } => {
                        let unscaled = i32::from_be_bytes(value);
                        format_decimal_with_i32_scale(unscaled, scale)
                    }
                    Decimal::Int64 { value, scale, .. } => {
                        let unscaled = i64::from_be_bytes(value);
                        format_decimal_with_i32_scale(unscaled, scale)
                    }
                    Decimal::Bytes { value, scale, .. } => {
                        bytes_to_decimal(value.as_bytes(), scale)?
                    }
                };

                // Load the bigdecimal gem if it's not already loaded
                LOADED_BIGDECIMAL.get_or_init(|| handle.require("bigdecimal").unwrap_or_default());

                let kernel = handle.module_kernel();
                Ok(kernel.funcall::<_, _, Value>("BigDecimal", (value,))?)
            }
            Field::Group(row) => {
                let hash = handle.hash_new();
                row.get_column_iter().try_for_each(|(k, v)| {
                    hash.aset(
                        k.clone().into_value_with(handle),
                        ParquetField {
                            field: v.clone(),
                            converted_type: v.to_converted_type(),
                            logical_type: v.to_logical_type(),
                            strict: self.strict,
                        }
                        .try_into_value_with(handle)?,
                    )?;
                    Ok::<_, ParquetGemError>(())
                })?;
                Ok(hash.into_value_with(handle))
            }
        }
    }
}

trait ToTypeInfo {
    fn to_converted_type(&self) -> ConvertedType;
    fn to_logical_type(&self) -> Option<LogicalType>;
}

impl ToTypeInfo for &parquet::record::Field {
    fn to_converted_type(&self) -> ConvertedType {
        match self {
            Field::Null => ConvertedType::NONE,
            Field::Bool(_) => ConvertedType::INT_8,
            Field::Byte(_) => ConvertedType::INT_8,
            Field::Short(_) => ConvertedType::INT_16,
            Field::Int(_) => ConvertedType::INT_32,
            Field::Long(_) => ConvertedType::INT_64,
            Field::UByte(_) => ConvertedType::UINT_8,
            Field::UShort(_) => ConvertedType::UINT_16,
            Field::UInt(_) => ConvertedType::UINT_32,
            Field::ULong(_) => ConvertedType::UINT_64,
            Field::Float16(_) => ConvertedType::NONE,
            Field::Float(_) => ConvertedType::NONE,
            Field::Double(_) => ConvertedType::NONE,
            Field::Decimal(_) => ConvertedType::DECIMAL,
            Field::Str(_) => ConvertedType::UTF8,
            Field::Bytes(_) => ConvertedType::LIST,
            Field::Date(_) => ConvertedType::DATE,
            Field::TimeMillis(_) => ConvertedType::TIME_MILLIS,
            Field::TimeMicros(_) => ConvertedType::TIMESTAMP_MICROS,
            Field::TimestampMillis(_) => ConvertedType::TIMESTAMP_MILLIS,
            Field::TimestampMicros(_) => ConvertedType::TIMESTAMP_MICROS,
            Field::Group(_) => ConvertedType::NONE,
            Field::ListInternal(_) => ConvertedType::LIST,
            Field::MapInternal(_) => ConvertedType::MAP,
        }
    }
    fn to_logical_type(&self) -> Option<LogicalType> {
        Some(match self {
            Field::Null => LogicalType::Unknown,
            Field::Bool(_) => LogicalType::Integer {
                bit_width: 1,
                is_signed: false,
            },
            Field::Byte(_) => LogicalType::Integer {
                bit_width: 8,
                is_signed: false,
            },
            Field::Short(_) => LogicalType::Integer {
                bit_width: 16,
                is_signed: true,
            },
            Field::Int(_) => LogicalType::Integer {
                bit_width: 32,
                is_signed: true,
            },
            Field::Long(_) => LogicalType::Integer {
                bit_width: 64,
                is_signed: true,
            },
            Field::UByte(_) => LogicalType::Integer {
                bit_width: 8,
                is_signed: false,
            },
            Field::UShort(_) => LogicalType::Integer {
                bit_width: 16,
                is_signed: false,
            },
            Field::UInt(_) => LogicalType::Integer {
                bit_width: 32,
                is_signed: false,
            },
            Field::ULong(_) => LogicalType::Integer {
                bit_width: 64,
                is_signed: false,
            },
            Field::Float16(_) => LogicalType::Float16,
            Field::Float(_) => LogicalType::Decimal {
                scale: 7,
                precision: 7,
            },
            Field::Double(_) => LogicalType::Decimal {
                scale: 15,
                precision: 15,
            },
            Field::Decimal(decimal) => LogicalType::Decimal {
                scale: decimal.scale(),
                precision: decimal.precision(),
            },
            Field::Str(_) => LogicalType::String,
            Field::Bytes(b) => {
                if b.data().len() == 16 && uuid::Uuid::from_slice(b.as_bytes()).is_ok() {
                    LogicalType::Uuid
                } else {
                    LogicalType::Unknown
                }
            }
            Field::Date(_) => LogicalType::Date,
            Field::TimeMillis(_) => LogicalType::Time {
                is_adjusted_to_u_t_c: true,
                unit: parquet::basic::TimeUnit::MILLIS(parquet::format::MilliSeconds {}),
            },
            Field::TimeMicros(_) => LogicalType::Time {
                is_adjusted_to_u_t_c: true,
                unit: parquet::basic::TimeUnit::MICROS(parquet::format::MicroSeconds {}),
            },
            Field::TimestampMillis(_) => LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: parquet::basic::TimeUnit::MILLIS(parquet::format::MilliSeconds {}),
            },
            Field::TimestampMicros(_) => LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: parquet::basic::TimeUnit::MICROS(parquet::format::MicroSeconds {}),
            },
            Field::Group(_) => LogicalType::Unknown,
            Field::ListInternal(_) => LogicalType::List,
            Field::MapInternal(_) => LogicalType::Map,
        })
    }
}
