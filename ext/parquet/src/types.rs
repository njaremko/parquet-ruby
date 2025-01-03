use std::{borrow::Cow, collections::HashMap, hash::BuildHasher, sync::Arc};

use arrow_array::cast::downcast_array;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float16Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, TimeUnit};
use itertools::Itertools;
use magnus::{value::ReprValue, IntoValue, Ruby, Value};
use parquet::record::Field;

use crate::header_cache::StringCacheKey;

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

impl<S: BuildHasher + Default> IntoValue for RowRecord<S> {
    fn into_value_with(self, handle: &Ruby) -> Value {
        match self {
            RowRecord::Vec(vec) => {
                let ary = handle.ary_new_capa(vec.len());
                vec.into_iter().try_for_each(|v| ary.push(v)).unwrap();
                handle.into_value(ary)
            }
            RowRecord::Map(map) => {
                let hash = handle.hash_new_capa(map.len());

                let mut values: [Value; 128] = [handle.qnil().as_value(); 128];
                let mut i = 0;

                for chunk in &map.into_iter().chunks(128) {
                    for (k, v) in chunk {
                        values[i] = handle.into_value(k);
                        values[i + 1] = handle.into_value(v);
                        i += 2;
                    }
                    hash.bulk_insert(&values[..i]).unwrap();

                    // Zero out used values
                    values[..i].fill(handle.qnil().as_value());
                    i = 0;
                }

                hash.into_value_with(handle)
            }
        }
    }
}

impl<S: BuildHasher + Default> IntoValue for ColumnRecord<S> {
    fn into_value_with(self, handle: &Ruby) -> Value {
        match self {
            ColumnRecord::Vec(vec) => {
                let ary = handle.ary_new_capa(vec.len());
                vec.into_iter()
                    .try_for_each(|v| {
                        let nested_ary = handle.ary_new_capa(v.len());
                        v.into_iter().try_for_each(|v| nested_ary.push(v)).unwrap();
                        ary.push(nested_ary.into_value_with(handle))
                    })
                    .unwrap();
                ary.into_value_with(handle)
            }
            ColumnRecord::Map(map) => {
                let hash = handle.hash_new_capa(map.len());

                let mut values: [Value; 128] = [handle.qnil().as_value(); 128];
                let mut i = 0;

                for chunk in &map.into_iter().chunks(128) {
                    for (k, v) in chunk {
                        values[i] = handle.into_value(k);
                        let ary = handle.ary_new_capa(v.len());
                        v.into_iter().try_for_each(|v| ary.push(v)).unwrap();
                        values[i + 1] = handle.into_value(ary);
                        i += 2;
                    }
                    hash.bulk_insert(&values[..i]).unwrap();

                    // Zero out used values
                    values[..i].fill(handle.qnil().as_value());
                    i = 0;
                }

                hash.into_value_with(handle)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct CowValue<'a>(pub Cow<'a, str>);

impl<'a> IntoValue for CowValue<'a> {
    fn into_value_with(self, handle: &Ruby) -> Value {
        self.0.into_value_with(handle)
    }
}

#[derive(Debug)]
pub struct ParquetField(pub Field);

impl IntoValue for ParquetField {
    fn into_value_with(self, handle: &Ruby) -> Value {
        match self.0 {
            Field::Byte(b) => b.into_value_with(handle),
            Field::Bool(b) => b.into_value_with(handle),
            Field::Short(s) => s.into_value_with(handle),
            Field::Int(i) => i.into_value_with(handle),
            Field::Long(l) => l.into_value_with(handle),
            Field::UByte(ub) => ub.into_value_with(handle),
            Field::UShort(us) => us.into_value_with(handle),
            Field::UInt(ui) => ui.into_value_with(handle),
            Field::ULong(ul) => ul.into_value_with(handle),
            Field::Float16(f) => f32::from(f).into_value_with(handle),
            Field::Float(f) => f.into_value_with(handle),
            Field::Double(d) => d.into_value_with(handle),
            Field::Str(s) => s.into_value_with(handle),
            Field::Bytes(b) => handle.str_from_slice(b.data()).as_value(),
            Field::Date(d) => {
                let ts = jiff::Timestamp::from_second((d as i64) * 86400).unwrap();
                let formatted = ts.strftime("%Y-%m-%d").to_string();
                formatted.into_value_with(handle)
            }
            Field::TimestampMillis(ts) => {
                let ts = jiff::Timestamp::from_millisecond(ts).unwrap();
                ts.to_string().into_value_with(handle)
            }
            Field::TimestampMicros(ts) => {
                let ts = jiff::Timestamp::from_microsecond(ts).unwrap();
                ts.to_string().into_value_with(handle)
            }
            Field::ListInternal(list) => {
                let elements = list.elements();
                let ary = handle.ary_new_capa(elements.len());
                elements
                    .iter()
                    .try_for_each(|e| ary.push(ParquetField(e.clone()).into_value_with(handle)))
                    .unwrap();
                ary.into_value_with(handle)
            }
            Field::MapInternal(map) => {
                let entries = map.entries();
                let hash = handle.hash_new_capa(entries.len());
                entries
                    .iter()
                    .try_for_each(|(k, v)| {
                        hash.aset(
                            ParquetField(k.clone()).into_value_with(handle),
                            ParquetField(v.clone()).into_value_with(handle),
                        )
                    })
                    .unwrap();
                hash.into_value_with(handle)
            }
            Field::Null => handle.qnil().as_value(),
            Field::Group(row) => {
                let hash = handle.hash_new();
                row.get_column_iter()
                    .try_for_each(|(k, v)| {
                        hash.aset(
                            k.clone().into_value_with(handle),
                            ParquetField(v.clone()).into_value_with(handle),
                        )
                    })
                    .unwrap();
                hash.into_value_with(handle)
            }
            e => panic!("Unsupported field type: {:?}", e),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
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

impl TryFrom<Arc<dyn Array>> for ParquetValueVec {
    type Error = String;

    fn try_from(column: Arc<dyn Array>) -> Result<Self, Self::Error> {
        ParquetValueVec::try_from(&*column)
    }
}

impl TryFrom<&dyn Array> for ParquetValueVec {
    type Error = String;

    fn try_from(column: &dyn Array) -> Result<Self, Self::Error> {
        let tmp_vec = match column.data_type() {
            DataType::Int8 => downcast_array::<Int8Array>(column)
                .values()
                .iter()
                .map(|x| ParquetValue::Int8(*x))
                .collect(),
            DataType::Int16 => downcast_array::<Int16Array>(column)
                .values()
                .iter()
                .map(|x| ParquetValue::Int16(*x))
                .collect(),
            DataType::Int32 => downcast_array::<Int32Array>(column)
                .values()
                .iter()
                .map(|x| ParquetValue::Int32(*x))
                .collect(),
            DataType::Int64 => downcast_array::<Int64Array>(column)
                .values()
                .iter()
                .map(|x| ParquetValue::Int64(*x))
                .collect(),
            DataType::UInt8 => downcast_array::<UInt8Array>(column)
                .values()
                .iter()
                .map(|x| ParquetValue::UInt8(*x))
                .collect(),
            DataType::UInt16 => downcast_array::<UInt16Array>(column)
                .values()
                .iter()
                .map(|x| ParquetValue::UInt16(*x))
                .collect(),
            DataType::UInt32 => downcast_array::<UInt32Array>(column)
                .values()
                .iter()
                .map(|x| ParquetValue::UInt32(*x))
                .collect(),
            DataType::UInt64 => downcast_array::<UInt64Array>(column)
                .values()
                .iter()
                .map(|x| ParquetValue::UInt64(*x))
                .collect(),
            DataType::Float16 => downcast_array::<Float16Array>(column)
                .values()
                .iter()
                .map(|x| ParquetValue::Float16(f32::from(*x)))
                .collect(),
            DataType::Float32 => downcast_array::<Float32Array>(column)
                .values()
                .iter()
                .map(|x| ParquetValue::Float32(*x))
                .collect(),
            DataType::Float64 => downcast_array::<Float64Array>(column)
                .values()
                .iter()
                .map(|x| ParquetValue::Float64(*x))
                .collect(),
            DataType::Boolean => downcast_array::<BooleanArray>(column)
                .values()
                .iter()
                .map(|x| ParquetValue::Boolean(x))
                .collect(),
            DataType::Utf8 => downcast_array::<StringArray>(column)
                .iter()
                .map(|x| ParquetValue::String(x.unwrap_or_default().to_string()))
                .collect(),
            DataType::Binary => downcast_array::<BinaryArray>(column)
                .iter()
                .map(|x| ParquetValue::Bytes(x.unwrap_or_default().to_vec()))
                .collect(),
            DataType::Date32 => downcast_array::<Date32Array>(column)
                .values()
                .iter()
                .map(|x| ParquetValue::Date32(*x))
                .collect(),
            DataType::Date64 => downcast_array::<Date64Array>(column)
                .values()
                .iter()
                .map(|x| ParquetValue::Date64(*x))
                .collect(),
            DataType::Timestamp(TimeUnit::Second, tz) => {
                downcast_array::<TimestampSecondArray>(column)
                    .values()
                    .iter()
                    .map(|x| ParquetValue::TimestampSecond(*x, tz.clone()))
                    .collect()
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                downcast_array::<TimestampMillisecondArray>(column)
                    .values()
                    .iter()
                    .map(|x| ParquetValue::TimestampMillis(*x, tz.clone()))
                    .collect()
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                downcast_array::<TimestampMicrosecondArray>(column)
                    .values()
                    .iter()
                    .map(|x| ParquetValue::TimestampMicros(*x, tz.clone()))
                    .collect()
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                downcast_array::<TimestampNanosecondArray>(column)
                    .values()
                    .iter()
                    .map(|x| ParquetValue::TimestampNanos(*x, tz.clone()))
                    .collect()
            }
            DataType::List(_field) => {
                let list_array = downcast_array::<ListArray>(column);
                list_array
                    .iter()
                    .map(|x| match x {
                        Some(values) => match ParquetValueVec::try_from(values) {
                            Ok(vec) => ParquetValue::List(vec.into_inner()),
                            Err(e) => {
                                panic!("Error converting list array to ParquetValueVec: {}", e)
                            }
                        },
                        None => ParquetValue::Null,
                    })
                    .collect()
            }
            DataType::Null => vec![ParquetValue::Null],
            _ => {
                return Err(format!("Unsupported data type: {:?}", column.data_type()));
            }
        };
        Ok(ParquetValueVec(tmp_vec))
    }
}

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

impl IntoValue for ParquetValue {
    fn into_value_with(self, handle: &Ruby) -> Value {
        match self {
            ParquetValue::Int8(i) => i.into_value_with(handle),
            ParquetValue::Int16(i) => i.into_value_with(handle),
            ParquetValue::Int32(i) => i.into_value_with(handle),
            ParquetValue::Int64(i) => i.into_value_with(handle),
            ParquetValue::UInt8(i) => i.into_value_with(handle),
            ParquetValue::UInt16(i) => i.into_value_with(handle),
            ParquetValue::UInt32(i) => i.into_value_with(handle),
            ParquetValue::UInt64(i) => i.into_value_with(handle),
            ParquetValue::Float16(f) => f.into_value_with(handle),
            ParquetValue::Float32(f) => f.into_value_with(handle),
            ParquetValue::Float64(f) => f.into_value_with(handle),
            ParquetValue::Boolean(b) => b.into_value_with(handle),
            ParquetValue::String(s) => s.into_value_with(handle),
            ParquetValue::Bytes(b) => b.into_value_with(handle),
            ParquetValue::Date32(d) => {
                let ts = jiff::Timestamp::from_second((d as i64) * 86400).unwrap();
                let formatted = ts.strftime("%Y-%m-%d").to_string();
                formatted.into_value_with(handle)
            }
            ParquetValue::Date64(d) => {
                let ts = jiff::Timestamp::from_second((d as i64) * 86400).unwrap();
                let formatted = ts.strftime("%Y-%m-%d").to_string();
                formatted.into_value_with(handle)
            }
            ParquetValue::TimestampSecond(ts, tz) => {
                let ts = parse_zoned_timestamp(&ParquetValue::TimestampSecond(ts, tz));
                ts.to_string().into_value_with(handle)
            }
            ParquetValue::TimestampMillis(ts, tz) => {
                let ts = parse_zoned_timestamp(&ParquetValue::TimestampMillis(ts, tz));
                ts.to_string().into_value_with(handle)
            }
            ParquetValue::TimestampMicros(ts, tz) => {
                let ts = parse_zoned_timestamp(&ParquetValue::TimestampMicros(ts, tz));
                ts.to_string().into_value_with(handle)
            }
            ParquetValue::TimestampNanos(ts, tz) => {
                let ts = parse_zoned_timestamp(&ParquetValue::TimestampNanos(ts, tz));
                ts.to_string().into_value_with(handle)
            }
            ParquetValue::List(l) => {
                let ary = handle.ary_new_capa(l.len());
                l.into_iter()
                    .try_for_each(|v| ary.push(v.into_value_with(handle)))
                    .unwrap();
                ary.into_value_with(handle)
            }
            ParquetValue::Map(m) => {
                let hash = handle.hash_new_capa(m.len());
                m.into_iter()
                    .try_for_each(|(k, v)| {
                        hash.aset(k.into_value_with(handle), v.into_value_with(handle))
                    })
                    .unwrap();
                hash.into_value_with(handle)
            }
            ParquetValue::Null => handle.qnil().as_value(),
        }
    }
}

fn parse_zoned_timestamp(value: &ParquetValue) -> jiff::Timestamp {
    let (ts, tz) = match value {
        ParquetValue::TimestampSecond(ts, tz) => (jiff::Timestamp::from_second(*ts).unwrap(), tz),
        ParquetValue::TimestampMillis(ts, tz) => {
            (jiff::Timestamp::from_millisecond(*ts).unwrap(), tz)
        }
        ParquetValue::TimestampMicros(ts, tz) => {
            (jiff::Timestamp::from_microsecond(*ts).unwrap(), tz)
        }
        ParquetValue::TimestampNanos(ts, tz) => {
            (jiff::Timestamp::from_nanosecond(*ts as i128).unwrap(), tz)
        }
        _ => panic!("Invalid timestamp value"),
    };

    // If timezone is provided, convert to zoned timestamp
    if let Some(tz) = tz {
        // Handle both fixed offset timezones like "+09:00" and IANA timezones like "America/New_York"
        ts.intz(&tz).unwrap().timestamp()
    } else {
        // No timezone provided - treat as UTC
        ts
    }
}
