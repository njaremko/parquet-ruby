use std::{borrow::Cow, collections::HashMap, hash::BuildHasher, sync::Arc};

use arrow_array::cast::downcast_array;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float16Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, NullArray, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, TimeUnit};
use itertools::Itertools;
use magnus::TryConvert;
use magnus::{value::ReprValue, Error as MagnusError, IntoValue, Ruby, Value};
use parquet::data_type::Decimal;
use parquet::record::Field;
use type_conversion::{
    convert_to_binary, convert_to_boolean, convert_to_date32, convert_to_timestamp_micros,
    convert_to_timestamp_millis, NumericConverter,
};

use crate::header_cache::StringCacheKey;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ParserResultType {
    Hash,
    Array,
}

impl ParserResultType {
    pub fn iter() -> impl Iterator<Item = Self> {
        [Self::Hash, Self::Array].into_iter()
    }
}

impl TryFrom<&str> for ParserResultType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "hash" => Ok(ParserResultType::Hash),
            "array" => Ok(ParserResultType::Array),
            _ => Err(format!("Invalid parser result type: {}", value)),
        }
    }
}

impl TryFrom<String> for ParserResultType {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl std::fmt::Display for ParserResultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParserResultType::Hash => write!(f, "hash"),
            ParserResultType::Array => write!(f, "array"),
        }
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

                for chunk in &map.into_iter().chunks(64) {
                    // Reduced to 64 to ensure space for pairs
                    for (k, v) in chunk {
                        if i + 1 >= values.len() {
                            // Bulk insert current batch if array is full
                            hash.bulk_insert(&values[..i]).unwrap();
                            values[..i].fill(handle.qnil().as_value());
                            i = 0;
                        }
                        values[i] = handle.into_value(k);
                        values[i + 1] = handle.into_value(v);
                        i += 2;
                    }
                    // Insert any remaining pairs
                    if i > 0 {
                        hash.bulk_insert(&values[..i]).unwrap();
                        values[..i].fill(handle.qnil().as_value());
                        i = 0;
                    }
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

                for chunk in &map.into_iter().chunks(64) {
                    // Reduced to 64 to ensure space for pairs
                    for (k, v) in chunk {
                        if i + 1 >= values.len() {
                            // Bulk insert current batch if array is full
                            hash.bulk_insert(&values[..i]).unwrap();
                            values[..i].fill(handle.qnil().as_value());
                            i = 0;
                        }
                        values[i] = handle.into_value(k);
                        let ary = handle.ary_new_capa(v.len());
                        v.into_iter().try_for_each(|v| ary.push(v)).unwrap();
                        values[i + 1] = handle.into_value(ary);
                        i += 2;
                    }
                    // Insert any remaining pairs
                    if i > 0 {
                        hash.bulk_insert(&values[..i]).unwrap();
                        values[..i].fill(handle.qnil().as_value());
                        i = 0;
                    }
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
            Field::Null => handle.qnil().as_value(),
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
            Field::Byte(b) => b.into_value_with(handle),
            Field::Bytes(b) => handle.str_from_slice(b.data()).as_value(),
            Field::Date(d) => {
                let ts = jiff::Timestamp::from_second((d as i64) * 86400).unwrap();
                let formatted = ts.strftime("%Y-%m-%d").to_string();
                formatted.into_value_with(handle)
            }
            Field::TimestampMillis(ts) => {
                let ts = jiff::Timestamp::from_millisecond(ts).unwrap();
                let time_class = handle.class_time();
                time_class
                    .funcall::<_, _, Value>("parse", (ts.to_string(),))
                    .unwrap()
                    .into_value_with(handle)
            }
            Field::TimestampMicros(ts) => {
                let ts = jiff::Timestamp::from_microsecond(ts).unwrap();
                let time_class = handle.class_time();
                time_class
                    .funcall::<_, _, Value>("parse", (ts.to_string(),))
                    .unwrap()
                    .into_value_with(handle)
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
            Field::Decimal(d) => {
                let value = match d {
                    Decimal::Int32 { value, scale, .. } => {
                        let unscaled = i32::from_be_bytes(value);
                        format!("{}e-{}", unscaled, scale)
                    }
                    Decimal::Int64 { value, scale, .. } => {
                        let unscaled = i64::from_be_bytes(value);
                        format!("{}e-{}", unscaled, scale)
                    }
                    Decimal::Bytes { value, scale, .. } => {
                        // Convert bytes to string representation of unscaled value
                        let unscaled = String::from_utf8_lossy(value.data());
                        format!("{}e-{}", unscaled, scale)
                    }
                };
                handle.eval(&format!("BigDecimal(\"{value}\")")).unwrap()
            }
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
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
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

// Add macro for handling numeric array conversions
macro_rules! impl_numeric_array_conversion {
    ($column:expr, $array_type:ty, $variant:ident) => {{
        let array = downcast_array::<$array_type>($column);
        if array.is_nullable() {
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
        }
    }};
}

// Add macro for handling boolean array conversions
macro_rules! impl_boolean_array_conversion {
    ($column:expr, $array_type:ty, $variant:ident) => {{
        let array = downcast_array::<$array_type>($column);
        if array.is_nullable() {
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
        }
    }};
}

// Add macro for handling timestamp array conversions
macro_rules! impl_timestamp_array_conversion {
    ($column:expr, $array_type:ty, $variant:ident, $tz:expr) => {{
        let array = downcast_array::<$array_type>($column);
        if array.is_nullable() {
            array
                .values()
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    if array.is_null(i) {
                        ParquetValue::Null
                    } else {
                        ParquetValue::$variant(*x, $tz.clone())
                    }
                })
                .collect()
        } else {
            array
                .values()
                .iter()
                .map(|x| ParquetValue::$variant(*x, $tz.clone()))
                .collect()
        }
    }};
}

impl TryFrom<&dyn Array> for ParquetValueVec {
    type Error = String;

    fn try_from(column: &dyn Array) -> Result<Self, Self::Error> {
        let tmp_vec = match column.data_type() {
            DataType::Boolean => impl_boolean_array_conversion!(column, BooleanArray, Boolean),
            DataType::Int8 => impl_numeric_array_conversion!(column, Int8Array, Int8),
            DataType::Int16 => impl_numeric_array_conversion!(column, Int16Array, Int16),
            DataType::Int32 => impl_numeric_array_conversion!(column, Int32Array, Int32),
            DataType::Int64 => impl_numeric_array_conversion!(column, Int64Array, Int64),
            DataType::UInt8 => impl_numeric_array_conversion!(column, UInt8Array, UInt8),
            DataType::UInt16 => impl_numeric_array_conversion!(column, UInt16Array, UInt16),
            DataType::UInt32 => impl_numeric_array_conversion!(column, UInt32Array, UInt32),
            DataType::UInt64 => impl_numeric_array_conversion!(column, UInt64Array, UInt64),
            DataType::Float32 => impl_numeric_array_conversion!(column, Float32Array, Float32),
            DataType::Float64 => impl_numeric_array_conversion!(column, Float64Array, Float64),
            DataType::Date32 => impl_numeric_array_conversion!(column, Date32Array, Date32),
            DataType::Date64 => impl_numeric_array_conversion!(column, Date64Array, Date64),
            DataType::Timestamp(TimeUnit::Second, tz) => {
                impl_timestamp_array_conversion!(column, TimestampSecondArray, TimestampSecond, tz)
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                impl_timestamp_array_conversion!(
                    column,
                    TimestampMillisecondArray,
                    TimestampMillis,
                    tz
                )
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                impl_timestamp_array_conversion!(
                    column,
                    TimestampMicrosecondArray,
                    TimestampMicros,
                    tz
                )
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                impl_timestamp_array_conversion!(
                    column,
                    TimestampNanosecondArray,
                    TimestampNanos,
                    tz
                )
            }
            // Because f16 is unstable in Rust, we convert it to f32
            DataType::Float16 => {
                let array = downcast_array::<Float16Array>(column);
                if array.is_nullable() {
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
                        .collect()
                } else {
                    array
                        .values()
                        .iter()
                        .map(|x| ParquetValue::Float16(f32::from(*x)))
                        .collect()
                }
            }
            DataType::Utf8 => {
                let array = downcast_array::<StringArray>(column);
                array
                    .iter()
                    .map(|opt_x| match opt_x {
                        Some(x) => ParquetValue::String(x.to_string()),
                        None => ParquetValue::Null,
                    })
                    .collect()
            }
            DataType::Binary => {
                let array = downcast_array::<BinaryArray>(column);
                array
                    .iter()
                    .map(|opt_x| match opt_x {
                        Some(x) => ParquetValue::Bytes(x.to_vec()),
                        None => ParquetValue::Null,
                    })
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
            DataType::Struct(_) => {
                let struct_array = downcast_array::<StructArray>(column);
                let mut values = Vec::with_capacity(struct_array.len());
                for i in 0..struct_array.len() {
                    if struct_array.is_null(i) {
                        values.push(ParquetValue::Null);
                        continue;
                    }

                    let mut map = std::collections::HashMap::new();
                    for (field_idx, field) in struct_array.fields().iter().enumerate() {
                        let column = struct_array.column(field_idx);
                        let field_values = match ParquetValueVec::try_from(column.slice(i, 1)) {
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
                values
            }
            DataType::Null => {
                let x = downcast_array::<NullArray>(column);
                vec![ParquetValue::Null; x.len()]
            }
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

#[derive(Debug, Clone)]
pub struct ListField {
    pub item_type: ParquetSchemaType,
}

#[derive(Debug, Clone)]
pub struct MapField {
    pub key_type: ParquetSchemaType,
    pub value_type: ParquetSchemaType,
}

#[derive(Debug, Clone)]
pub enum ParquetSchemaType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float,
    Double,
    String,
    Binary,
    Boolean,
    Date32,
    TimestampMillis,
    TimestampMicros,
    List(Box<ListField>),
    Map(Box<MapField>),
}

impl ParquetValue {
    pub fn from_value(value: Value, type_: &ParquetSchemaType) -> Result<Self, MagnusError> {
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
                let v = String::try_convert(value)?;
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
                let v = convert_to_date32(value)?;
                Ok(ParquetValue::Date32(v))
            }
            ParquetSchemaType::TimestampMillis => {
                let v = convert_to_timestamp_millis(value)?;
                Ok(ParquetValue::TimestampMillis(v, None))
            }
            ParquetSchemaType::TimestampMicros => {
                let v = convert_to_timestamp_micros(value)?;
                Ok(ParquetValue::TimestampMicros(v, None))
            }
            ParquetSchemaType::List(_) | ParquetSchemaType::Map(_) => Err(MagnusError::new(
                magnus::exception::type_error(),
                "Nested lists and maps are not supported",
            )),
        }
    }
}

macro_rules! impl_timestamp_conversion {
    ($value:expr, $unit:ident, $handle:expr) => {{
        match $value {
            ParquetValue::$unit(ts, tz) => {
                let ts = parse_zoned_timestamp(&ParquetValue::$unit(ts, tz));
                let time_class = $handle.class_time();
                time_class
                    .funcall::<_, _, Value>("parse", (ts.to_string(),))
                    .unwrap()
                    .into_value_with($handle)
            }
            _ => panic!("Invalid timestamp type"),
        }
    }};
}

macro_rules! impl_date_conversion {
    ($value:expr, $handle:expr) => {{
        let ts = jiff::Timestamp::from_second(($value as i64) * 86400).unwrap();
        let formatted = ts.strftime("%Y-%m-%d").to_string();
        formatted.into_value_with($handle)
    }};
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
        // Handle fixed offset timezones like "+09:00" first
        if tz.starts_with('+') || tz.starts_with('-') {
            // Parse the offset string into hours and minutes
            let (hours, minutes) = if tz.len() >= 5 && tz.contains(':') {
                // Format: "+09:00" or "-09:00"
                let h = tz[1..3].parse::<i32>().unwrap_or(0);
                let m = tz[4..6].parse::<i32>().unwrap_or(0);
                (h, m)
            } else if tz.len() >= 3 {
                // Format: "+09" or "-09"
                let h = tz[1..3].parse::<i32>().unwrap_or(0);
                (h, 0)
            } else {
                (0, 0)
            };

            // Apply sign
            let total_minutes = if tz.starts_with('-') {
                -(hours * 60 + minutes)
            } else {
                hours * 60 + minutes
            };

            // Create fixed timezone
            let tz = jiff::tz::TimeZone::fixed(jiff::tz::offset((total_minutes / 60) as i8));
            ts.to_zoned(tz).timestamp()
        } else {
            // Try IANA timezone
            match ts.intz(&tz) {
                Ok(zoned) => zoned.timestamp(),
                Err(_) => ts, // Fall back to UTC if timezone is invalid
            }
        }
    } else {
        // No timezone provided - treat as UTC
        ts
    }
}

pub mod type_conversion {
    use std::str::FromStr;

    use super::*;
    use arrow_array::builder::*;
    use jiff::tz::{Offset, TimeZone};
    use magnus::{Error as MagnusError, RArray, TryConvert};

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

    pub fn convert_to_date32(value: Value) -> Result<i32, MagnusError> {
        let ruby = unsafe { Ruby::get_unchecked() };
        if value.is_kind_of(ruby.class_string()) {
            let s = String::try_convert(value)?;
            // Parse string into Timestamp using jiff
            let date: jiff::civil::Date = s.parse().map_err(|e| {
                MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Failed to parse '{}' as date32: {}", s, e),
                )
            })?;

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

    pub fn convert_to_timestamp_millis(value: Value) -> Result<i64, MagnusError> {
        let ruby = unsafe { Ruby::get_unchecked() };
        if value.is_kind_of(ruby.class_string()) {
            let s = String::try_convert(value)?;
            // Parse string into Timestamp using jiff
            let timestamp: jiff::Timestamp = s.parse().map_err(|e| {
                MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Failed to parse '{}' as timestamp_millis: {}", s, e),
                )
            })?;
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

    pub fn convert_to_timestamp_micros(value: Value) -> Result<i64, MagnusError> {
        let ruby = unsafe { Ruby::get_unchecked() };
        if value.is_kind_of(ruby.class_string()) {
            let s = String::try_convert(value)?;
            // Parse string into Timestamp using jiff
            let timestamp: jiff::Timestamp = s.parse().map_err(|e| {
                MagnusError::new(
                    magnus::exception::type_error(),
                    format!("Failed to parse '{}' as timestamp_micros: {}", s, e),
                )
            })?;
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
                        let v = convert_to_date32(item_value)?;
                        ParquetValue::Date32(v)
                    }
                    ParquetSchemaType::TimestampMillis => {
                        let v = convert_to_timestamp_millis(item_value)?;
                        ParquetValue::TimestampMillis(v, None)
                    }
                    ParquetSchemaType::TimestampMicros => {
                        let v = convert_to_timestamp_micros(item_value)?;
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
                        let v = convert_to_date32(value)?;
                        ParquetValue::Date32(v)
                    }
                    ParquetSchemaType::TimestampMillis => {
                        let v = convert_to_timestamp_millis(value)?;
                        ParquetValue::TimestampMillis(v, None)
                    }
                    ParquetSchemaType::TimestampMicros => {
                        let v = convert_to_timestamp_micros(value)?;
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

    pub fn convert_parquet_values_to_arrow(
        values: Vec<ParquetValue>,
        type_: &ParquetSchemaType,
    ) -> Result<Arc<dyn Array>, MagnusError> {
        match type_ {
            ParquetSchemaType::Int8 => {
                let mut builder = Int8Builder::with_capacity(values.len());
                for value in values {
                    match value {
                        ParquetValue::Int8(v) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected Int8, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::Int16 => {
                let mut builder = Int16Builder::with_capacity(values.len());
                for value in values {
                    match value {
                        ParquetValue::Int16(v) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected Int16, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::Int32 => {
                let mut builder = Int32Builder::with_capacity(values.len());
                for value in values {
                    match value {
                        ParquetValue::Int32(v) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected Int32, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::Int64 => {
                let mut builder = Int64Builder::with_capacity(values.len());
                for value in values {
                    match value {
                        ParquetValue::Int64(v) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected Int64, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::UInt8 => {
                let mut builder = UInt8Builder::with_capacity(values.len());
                for value in values {
                    match value {
                        ParquetValue::UInt8(v) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected UInt8, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::UInt16 => {
                let mut builder = UInt16Builder::with_capacity(values.len());
                for value in values {
                    match value {
                        ParquetValue::UInt16(v) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected UInt16, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::UInt32 => {
                let mut builder = UInt32Builder::with_capacity(values.len());
                for value in values {
                    match value {
                        ParquetValue::UInt32(v) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected UInt32, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::UInt64 => {
                let mut builder = UInt64Builder::with_capacity(values.len());
                for value in values {
                    match value {
                        ParquetValue::UInt64(v) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected UInt64, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::Float => {
                let mut builder = Float32Builder::with_capacity(values.len());
                for value in values {
                    match value {
                        ParquetValue::Float32(v) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected Float32, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::Double => {
                let mut builder = Float64Builder::with_capacity(values.len());
                for value in values {
                    match value {
                        ParquetValue::Float64(v) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected Float64, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::String => {
                let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 32);
                for value in values {
                    match value {
                        ParquetValue::String(v) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected String, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::Binary => {
                let mut builder = BinaryBuilder::with_capacity(values.len(), values.len() * 32);
                for value in values {
                    match value {
                        ParquetValue::Bytes(v) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected Binary, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(values.len());
                for value in values {
                    match value {
                        ParquetValue::Boolean(v) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected Boolean, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::Date32 => {
                let mut builder = Date32Builder::with_capacity(values.len());
                for value in values {
                    match value {
                        ParquetValue::Date32(v) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected Date32, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::TimestampMillis => {
                let mut builder = TimestampMillisecondBuilder::with_capacity(values.len());
                for value in values {
                    match value {
                        ParquetValue::TimestampMillis(v, _) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected TimestampMillis, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::TimestampMicros => {
                let mut builder = TimestampMicrosecondBuilder::with_capacity(values.len());
                for value in values {
                    match value {
                        ParquetValue::TimestampMicros(v, _) => builder.append_value(v),
                        ParquetValue::Null => builder.append_null(),
                        _ => {
                            return Err(MagnusError::new(
                                magnus::exception::type_error(),
                                format!("Expected TimestampMicros, got {:?}", value),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ParquetSchemaType::List(list_field) => {
                let value_builder = match list_field.item_type {
                    ParquetSchemaType::Int8 => {
                        Box::new(Int8Builder::new()) as Box<dyn ArrayBuilder>
                    }
                    ParquetSchemaType::Int16 => {
                        Box::new(Int16Builder::new()) as Box<dyn ArrayBuilder>
                    }
                    ParquetSchemaType::Int32 => {
                        Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>
                    }
                    ParquetSchemaType::Int64 => {
                        Box::new(Int64Builder::new()) as Box<dyn ArrayBuilder>
                    }
                    ParquetSchemaType::UInt8 => {
                        Box::new(UInt8Builder::new()) as Box<dyn ArrayBuilder>
                    }
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
                            list_builder.append(true);
                            for item in items {
                                match (&list_field.item_type, &item) {
                                    (ParquetSchemaType::Int8, ParquetValue::Int8(v)) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<Int8Builder>()
                                            .unwrap()
                                            .append_value(*v);
                                    }
                                    (ParquetSchemaType::Int16, ParquetValue::Int16(v)) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<Int16Builder>()
                                            .unwrap()
                                            .append_value(*v);
                                    }
                                    (ParquetSchemaType::Int32, ParquetValue::Int32(v)) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<Int32Builder>()
                                            .unwrap()
                                            .append_value(*v);
                                    }
                                    (ParquetSchemaType::Int64, ParquetValue::Int64(v)) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<Int64Builder>()
                                            .unwrap()
                                            .append_value(*v);
                                    }
                                    (ParquetSchemaType::UInt8, ParquetValue::UInt8(v)) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<UInt8Builder>()
                                            .unwrap()
                                            .append_value(*v);
                                    }
                                    (ParquetSchemaType::UInt16, ParquetValue::UInt16(v)) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<UInt16Builder>()
                                            .unwrap()
                                            .append_value(*v);
                                    }
                                    (ParquetSchemaType::UInt32, ParquetValue::UInt32(v)) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<UInt32Builder>()
                                            .unwrap()
                                            .append_value(*v);
                                    }
                                    (ParquetSchemaType::UInt64, ParquetValue::UInt64(v)) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<UInt64Builder>()
                                            .unwrap()
                                            .append_value(*v);
                                    }
                                    (ParquetSchemaType::Float, ParquetValue::Float32(v)) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<Float32Builder>()
                                            .unwrap()
                                            .append_value(*v);
                                    }
                                    (ParquetSchemaType::Double, ParquetValue::Float64(v)) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<Float64Builder>()
                                            .unwrap()
                                            .append_value(*v);
                                    }
                                    (ParquetSchemaType::String, ParquetValue::String(v)) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<StringBuilder>()
                                            .unwrap()
                                            .append_value(v);
                                    }
                                    (ParquetSchemaType::Binary, ParquetValue::Bytes(v)) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<BinaryBuilder>()
                                            .unwrap()
                                            .append_value(v);
                                    }
                                    (ParquetSchemaType::Boolean, ParquetValue::Boolean(v)) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<BooleanBuilder>()
                                            .unwrap()
                                            .append_value(*v);
                                    }
                                    (ParquetSchemaType::Date32, ParquetValue::Date32(v)) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<Date32Builder>()
                                            .unwrap()
                                            .append_value(*v);
                                    }
                                    (
                                        ParquetSchemaType::TimestampMillis,
                                        ParquetValue::TimestampMillis(v, _),
                                    ) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<TimestampMillisecondBuilder>()
                                            .unwrap()
                                            .append_value(*v);
                                    }
                                    (
                                        ParquetSchemaType::TimestampMicros,
                                        ParquetValue::TimestampMicros(v, _),
                                    ) => {
                                        list_builder
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<TimestampMicrosecondBuilder>()
                                            .unwrap()
                                            .append_value(*v);
                                    }
                                    (_, ParquetValue::Null) => {
                                        list_builder.append_null();
                                    }
                                    _ => {
                                        return Err(MagnusError::new(
                                            magnus::exception::type_error(),
                                            format!(
                                                "Type mismatch in list: expected {:?}, got {:?}",
                                                list_field.item_type, item
                                            ),
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
            ParquetSchemaType::Map(map_field) => {
                unimplemented!("Writing structs is not yet supported")
            }
        }
    }

    pub fn convert_ruby_array_to_arrow(
        values: RArray,
        type_: &ParquetSchemaType,
    ) -> Result<Arc<dyn Array>, MagnusError> {
        match type_ {
            ParquetSchemaType::Int8 => {
                let values: Vec<i8> = values
                    .into_iter()
                    .map(NumericConverter::<i8>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Int8Array::from_iter_values(values)))
            }
            ParquetSchemaType::Int16 => {
                let values: Vec<i16> = values
                    .into_iter()
                    .map(NumericConverter::<i16>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Int16Array::from_iter_values(values)))
            }
            ParquetSchemaType::Int32 => {
                let values: Vec<i32> = values
                    .into_iter()
                    .map(NumericConverter::<i32>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Int32Array::from_iter_values(values)))
            }
            ParquetSchemaType::Int64 => {
                let values: Vec<i64> = values
                    .into_iter()
                    .map(NumericConverter::<i64>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Int64Array::from_iter_values(values)))
            }
            ParquetSchemaType::UInt8 => {
                let values: Vec<u8> = values
                    .into_iter()
                    .map(NumericConverter::<u8>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(UInt8Array::from_iter_values(values)))
            }
            ParquetSchemaType::UInt16 => {
                let values: Vec<u16> = values
                    .into_iter()
                    .map(NumericConverter::<u16>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(UInt16Array::from_iter_values(values)))
            }
            ParquetSchemaType::UInt32 => {
                let values: Vec<u32> = values
                    .into_iter()
                    .map(NumericConverter::<u32>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(UInt32Array::from_iter_values(values)))
            }
            ParquetSchemaType::UInt64 => {
                let values: Vec<u64> = values
                    .into_iter()
                    .map(NumericConverter::<u64>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(UInt64Array::from_iter_values(values)))
            }
            ParquetSchemaType::Float => {
                let values: Vec<f32> = values
                    .into_iter()
                    .map(NumericConverter::<f32>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Float32Array::from_iter_values(values)))
            }
            ParquetSchemaType::Double => {
                let values: Vec<f64> = values
                    .into_iter()
                    .map(NumericConverter::<f64>::convert_with_string_fallback)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Float64Array::from_iter_values(values)))
            }
            ParquetSchemaType::String => {
                let values: Vec<String> = values
                    .into_iter()
                    .map(String::try_convert)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(StringArray::from_iter_values(values)))
            }
            ParquetSchemaType::Binary => {
                let values: Vec<Vec<u8>> = values
                    .into_iter()
                    .map(convert_to_binary)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(BinaryArray::from_iter_values(values)))
            }
            ParquetSchemaType::Boolean => {
                let values: Vec<bool> = values
                    .into_iter()
                    .map(convert_to_boolean)
                    .collect::<Result<_, _>>()?;
                let x: BooleanArray = values.into();
                Ok(Arc::new(x))
            }
            ParquetSchemaType::Date32 => {
                let values: Vec<i32> = values
                    .into_iter()
                    .map(convert_to_date32)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(Date32Array::from_iter_values(values)))
            }
            ParquetSchemaType::TimestampMillis => {
                let values: Vec<i64> = values
                    .into_iter()
                    .map(convert_to_timestamp_millis)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(TimestampMillisecondArray::from_iter_values(
                    values,
                )))
            }
            ParquetSchemaType::TimestampMicros => {
                let values: Vec<i64> = values
                    .into_iter()
                    .map(convert_to_timestamp_micros)
                    .collect::<Result<_, _>>()?;
                Ok(Arc::new(TimestampMicrosecondArray::from_iter_values(
                    values,
                )))
            }
            ParquetSchemaType::List(list_field) => {
                let list_array = match list_field.item_type {
                    ParquetSchemaType::Int8 => {
                        let value_builder = Int8Builder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted =
                                NumericConverter::<i8>::convert_with_string_fallback(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::Int16 => {
                        let value_builder = Int16Builder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted =
                                NumericConverter::<i16>::convert_with_string_fallback(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::Int32 => {
                        let value_builder = Int32Builder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted =
                                NumericConverter::<i32>::convert_with_string_fallback(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::Int64 => {
                        let value_builder = Int64Builder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted =
                                NumericConverter::<i64>::convert_with_string_fallback(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::UInt8 => {
                        let value_builder = UInt8Builder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted =
                                NumericConverter::<u8>::convert_with_string_fallback(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::UInt16 => {
                        let value_builder = UInt16Builder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted =
                                NumericConverter::<u16>::convert_with_string_fallback(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::UInt32 => {
                        let value_builder = UInt32Builder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted =
                                NumericConverter::<u32>::convert_with_string_fallback(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::UInt64 => {
                        let value_builder = UInt64Builder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted =
                                NumericConverter::<u64>::convert_with_string_fallback(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::Float => {
                        let value_builder = Float32Builder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted =
                                NumericConverter::<f32>::convert_with_string_fallback(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::Double => {
                        let value_builder = Float64Builder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted =
                                NumericConverter::<f64>::convert_with_string_fallback(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::String => {
                        let value_builder = StringBuilder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted = String::try_convert(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::Binary => {
                        let value_builder = BinaryBuilder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted = convert_to_binary(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::Boolean => {
                        let value_builder = BooleanBuilder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted = convert_to_boolean(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::Date32 => {
                        let value_builder = Date32Builder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted = convert_to_date32(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::TimestampMillis => {
                        let value_builder = TimestampMillisecondBuilder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted = convert_to_timestamp_millis(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::TimestampMicros => {
                        let value_builder = TimestampMicrosecondBuilder::new();
                        let mut list_builder = ListBuilder::new(value_builder);
                        for value in values {
                            let casted = convert_to_timestamp_micros(value)?;
                            list_builder.values().append_value(casted);
                        }
                        list_builder.finish()
                    }
                    ParquetSchemaType::List(_) | ParquetSchemaType::Map(_) => {
                        return Err(MagnusError::new(
                            magnus::exception::type_error(),
                            "Nested lists and maps are not supported",
                        ))
                    }
                };

                Ok(Arc::new(list_array))
            }
            ParquetSchemaType::Map(map_field) => {
                unimplemented!("Writing structs is not yet supported")
            }
        }
    }
}
