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
            // For 17-32 bytes, treat as a signed 256-bit integer in big-endian, left-padded as needed.
            // Do not use BigInt.

            if bytes.len() > 32 {
                return Err(ParquetGemError::InvalidDecimal(format!(
                    "Decimal byte array too large: {} bytes (maximum 32 bytes for 256 bits)",
                    bytes.len()
                )));
            }

            // Pad to 32 bytes (big-endian, left pad)
            let mut buf = [0u8; 32];
            let offset = 32 - bytes.len();
            buf[offset..].copy_from_slice(bytes);

            // Determine sign from the first byte of the input (not the padded buffer)
            let is_negative = bytes[0] & 0x80 != 0;

            // If negative, sign-extend the padding bytes
            if is_negative {
                for b in &mut buf[..offset] {
                    *b = 0xFF;
                }
            }

            // Now buf is a 32-byte two's complement signed integer, big-endian

            // Helper: convert [u8; 32] two's complement to absolute value and sign
            fn twos_complement_to_abs(buf: &[u8; 32]) -> (bool, [u8; 32]) {
                let negative = buf[0] & 0x80 != 0;
                if !negative {
                    (false, *buf)
                } else {
                    // Two's complement: invert and add 1
                    let mut out = [0u8; 32];
                    let mut carry = true;
                    for (i, b) in buf.iter().rev().enumerate() {
                        let inv = !b;
                        let (val, c) = if carry {
                            inv.overflowing_add(1)
                        } else {
                            (inv, false)
                        };
                        out[31 - i] = val;
                        carry = c;
                    }
                    (true, out)
                }
            }

            let (negative, abs_bytes) = twos_complement_to_abs(&buf);

            // Helper: divide a 256-bit unsigned integer by u64, returning quotient and remainder
            fn div_rem_u256_by_u64(n: &[u8; 32], divisor: u64) -> ([u8; 32], u64) {
                let mut quotient = [0u8; 32];
                let mut rem: u128 = 0;
                for (i, &b) in n.iter().enumerate() {
                    rem = (rem << 8) | (b as u128);
                    let q = rem / (divisor as u128);
                    rem = rem % (divisor as u128);
                    quotient[i] = q as u8;
                }
                (quotient, rem as u64)
            }

            // Helper: check if a 256-bit value is zero
            fn is_zero_u256(n: &[u8; 32]) -> bool {
                n.iter().all(|&b| b == 0)
            }

            // Convert 10^scale to u64 (safe for scale <= 18)
            let pow10 = 10u64.checked_pow(scale as u32).ok_or_else(|| {
                ParquetGemError::InvalidDecimal(format!(
                    "Decimal scale too large for 256-bit decimal: {}",
                    scale
                ))
            })?;

            // Get integer part and fractional remainder
            let (int_bytes, rem) = div_rem_u256_by_u64(&abs_bytes, pow10);

            // Convert integer part to decimal string
            let mut int_digits = Vec::new();
            let mut tmp = int_bytes;
            while !is_zero_u256(&tmp) {
                let (q, r) = div_rem_u256_by_u64(&tmp, 10);
                int_digits.push((r as u8) + b'0');
                tmp = q;
            }
            if int_digits.is_empty() {
                int_digits.push(b'0');
            }
            int_digits.reverse();
            let int_str = String::from_utf8(int_digits).unwrap();

            // Convert fractional part to decimal string, left pad with zeros if needed
            let mut frac_digits = vec![b'0'; scale as usize];
            let mut frac = rem;
            for i in (0..scale as usize).rev() {
                frac_digits[i] = ((frac % 10) as u8) + b'0';
                frac /= 10;
            }
            let frac_str = String::from_utf8(frac_digits).unwrap();

            // Compose the final string
            let result = if scale == 0 {
                if negative {
                    format!("-{}", int_str)
                } else {
                    int_str
                }
            } else {
                if negative {
                    format!("-{}.{}", int_str, frac_str)
                } else {
                    format!("{}.{}", int_str, frac_str)
                }
            };

            Ok(result)
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
