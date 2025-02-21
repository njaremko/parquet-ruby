use crate::{
    impl_date_conversion, impl_timestamp_array_conversion, impl_timestamp_conversion,
    reader::ReaderError,
};

use super::*;

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
    TimestampSecond(i64, Option<Arc<str>>),
    TimestampMillis(i64, Option<Arc<str>>),
    TimestampMicros(i64, Option<Arc<str>>),
    TimestampNanos(i64, Option<Arc<str>>),
    List(Vec<ParquetValue>),
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
            (ParquetValue::TimestampSecond(a, _), ParquetValue::TimestampSecond(b, _)) => a == b,
            (ParquetValue::TimestampMillis(a, _), ParquetValue::TimestampMillis(b, _)) => a == b,
            (ParquetValue::TimestampMicros(a, _), ParquetValue::TimestampMicros(b, _)) => a == b,
            (ParquetValue::TimestampNanos(a, _), ParquetValue::TimestampNanos(b, _)) => a == b,
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
            ParquetValue::List(l) => l.hash(state),
            ParquetValue::Map(_m) => panic!("Map is not hashable"),
            ParquetValue::Null => 0_i32.hash(state),
        }
    }
}

impl TryIntoValue for ParquetValue {
    fn try_into_value_with(self, handle: &Ruby) -> Result<Value, ReaderError> {
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
            ParquetValue::List(l) => {
                let ary = handle.ary_new_capa(l.len());
                l.into_iter().try_for_each(|v| {
                    ary.push(v.try_into_value_with(handle)?)?;
                    Ok::<_, ReaderError>(())
                })?;
                Ok(ary.into_value_with(handle))
            }
            ParquetValue::Map(m) => {
                let hash = handle.hash_new_capa(m.len());
                m.into_iter().try_for_each(|(k, v)| {
                    hash.aset(
                        k.try_into_value_with(handle)?,
                        v.try_into_value_with(handle)?,
                    )
                })?;
                Ok(hash.into_value_with(handle))
            }
            ParquetValue::Null => Ok(handle.qnil().as_value()),
        }
    }
}

impl ParquetValue {
    pub fn from_value(value: Value, type_: &ParquetSchemaType) -> Result<Self, MagnusError> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        match type_ {
            ParquetSchemaType::Int8 => {
                let v = NumericConverter::<i8>::convert_with_string_fallback(value)?;
                Ok(ParquetValue::Int8(v))
            }
            ParquetSchemaType::Int16 => {
                let v = NumericConverter::<i16>::convert_with_string_fallback(value)?;
                Ok(ParquetValue::Int16(v))
            }
            ParquetSchemaType::Int32 => {
                let v = NumericConverter::<i32>::convert_with_string_fallback(value)?;
                Ok(ParquetValue::Int32(v))
            }
            ParquetSchemaType::Int64 => {
                let v = NumericConverter::<i64>::convert_with_string_fallback(value)?;
                Ok(ParquetValue::Int64(v))
            }
            ParquetSchemaType::UInt8 => {
                let v = NumericConverter::<u8>::convert_with_string_fallback(value)?;
                Ok(ParquetValue::UInt8(v))
            }
            ParquetSchemaType::UInt16 => {
                let v = NumericConverter::<u16>::convert_with_string_fallback(value)?;
                Ok(ParquetValue::UInt16(v))
            }
            ParquetSchemaType::UInt32 => {
                let v = NumericConverter::<u32>::convert_with_string_fallback(value)?;
                Ok(ParquetValue::UInt32(v))
            }
            ParquetSchemaType::UInt64 => {
                let v = NumericConverter::<u64>::convert_with_string_fallback(value)?;
                Ok(ParquetValue::UInt64(v))
            }
            ParquetSchemaType::Float => {
                let v = NumericConverter::<f32>::convert_with_string_fallback(value)?;
                Ok(ParquetValue::Float32(v))
            }
            ParquetSchemaType::Double => {
                let v = NumericConverter::<f64>::convert_with_string_fallback(value)?;
                Ok(ParquetValue::Float64(v))
            }
            ParquetSchemaType::String => {
                let v = convert_to_string(value)?;
                Ok(ParquetValue::String(v))
            }
            ParquetSchemaType::Binary => {
                let v = convert_to_binary(value)?;
                Ok(ParquetValue::Bytes(v))
            }
            ParquetSchemaType::Boolean => {
                let v = convert_to_boolean(value)?;
                Ok(ParquetValue::Boolean(v))
            }
            ParquetSchemaType::Date32 => {
                let v = convert_to_date32(value, None)?;
                Ok(ParquetValue::Date32(v))
            }
            ParquetSchemaType::TimestampMillis => {
                let v = convert_to_timestamp_millis(value, None)?;
                Ok(ParquetValue::TimestampMillis(v, None))
            }
            ParquetSchemaType::TimestampMicros => {
                let v = convert_to_timestamp_micros(value, None)?;
                Ok(ParquetValue::TimestampMicros(v, None))
            }
            ParquetSchemaType::List(_) | ParquetSchemaType::Map(_) => Err(MagnusError::new(
                magnus::exception::type_error(),
                "Nested lists and maps are not supported",
            )),
        }
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
    type Error = ReaderError;

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
                            Ok::<_, ReaderError>(ParquetValue::String(
                                simdutf8::basic::from_utf8(x.as_bytes())?.to_string(),
                            ))
                        } else {
                            Ok::<_, ReaderError>(ParquetValue::String(x.to_string()))
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
                Ok(ParquetValueVec(
                    list_array
                        .iter()
                        .map(|x| match x {
                            Some(values) => match ParquetValueVec::try_from(ArrayWrapper {
                                array: &*values,
                                strict: column.strict,
                            }) {
                                Ok(vec) => ParquetValue::List(vec.into_inner()),
                                Err(e) => {
                                    panic!("Error converting list array to ParquetValueVec: {}", e)
                                }
                            },
                            None => ParquetValue::Null,
                        })
                        .collect(),
                ))
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
                                panic!("Error converting struct field to ParquetValueVec: {}", e)
                            }
                        };
                        map.insert(
                            ParquetValue::String(field.name().to_string()),
                            field_values.into_iter().next().unwrap(),
                        );
                    }
                    values.push(ParquetValue::Map(map));
                }
                Ok(ParquetValueVec(values))
            }
            DataType::Null => {
                let x = downcast_array::<NullArray>(column.array);
                Ok(ParquetValueVec(vec![ParquetValue::Null; x.len()]))
            }
            _ => {
                return Err(ReaderError::Ruby(format!(
                    "Unsupported data type: {:?}",
                    column.array.data_type()
                )));
            }
        }
    }
}
