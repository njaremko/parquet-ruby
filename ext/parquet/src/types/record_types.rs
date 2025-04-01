use std::sync::OnceLock;

use itertools::Itertools;
use parquet::data_type::AsBytes;

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
pub struct ParquetField(pub Field, pub bool);

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
        match self.0 {
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
                if self.1 {
                    Ok(simdutf8::basic::from_utf8(s.as_bytes())
                        .map_err(ParquetGemError::Utf8Error)
                        .map(|s| s.into_value_with(handle))?)
                } else {
                    let s = String::from_utf8_lossy(s.as_bytes());
                    Ok(s.into_value_with(handle))
                }
            }
            Field::Byte(b) => Ok(b.into_value_with(handle)),
            Field::Bytes(b) => Ok(handle.str_from_slice(b.data()).as_value()),
            Field::Date(d) => {
                let ts = jiff::Timestamp::from_second((d as i64) * 86400)?;
                let formatted = ts.strftime("%Y-%m-%d").to_string();
                Ok(formatted.into_value_with(handle))
            }
            Field::TimestampMillis(ts) => {
                let ts = jiff::Timestamp::from_millisecond(ts)?;
                let time_class = handle.class_time();
                Ok(time_class
                    .funcall::<_, _, Value>("parse", (ts.to_string(),))?
                    .into_value_with(handle))
            }
            Field::TimestampMicros(ts) => {
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
                    ary.push(ParquetField(e.clone(), self.1).try_into_value_with(handle)?)?;
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
                        ParquetField(k.clone(), self.1).try_into_value_with(handle)?,
                        ParquetField(v.clone(), self.1).try_into_value_with(handle)?,
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
                        // value is a byte array containing the bytes for an i128 value in big endian order
                        let casted = value.as_bytes()[..16].try_into()?;
                        let unscaled = i128::from_be_bytes(casted);
                        format_decimal_with_i32_scale(unscaled, scale)
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
                        ParquetField(v.clone(), self.1).try_into_value_with(handle)?,
                    )?;
                    Ok::<_, ParquetGemError>(())
                })?;
                Ok(hash.into_value_with(handle))
            }
        }
    }
}
