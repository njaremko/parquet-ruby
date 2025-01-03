use std::{borrow::Cow, collections::HashMap, hash::BuildHasher};

use itertools::Itertools;
use magnus::{value::ReprValue, IntoValue, Ruby, Value};
use parquet::record::Field;

use crate::header_cache::StringCacheKey;

#[derive(Debug)]
pub enum Record<S: BuildHasher + Default> {
    Vec(Vec<ParquetField>),
    Map(HashMap<StringCacheKey, ParquetField, S>),
}

impl<S: BuildHasher + Default> IntoValue for Record<S> {
    fn into_value_with(self, handle: &Ruby) -> Value {
        match self {
            Record::Vec(vec) => {
                let ary = handle.ary_new_capa(vec.len());
                vec.into_iter().try_for_each(|v| ary.push(v)).unwrap();
                ary.into_value_with(handle)
            }
            Record::Map(map) => {
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
            Field::Date(d) => d.into_value_with(handle),
            Field::TimestampMillis(ts) => ts.into_value_with(handle),
            Field::TimestampMicros(ts) => ts.into_value_with(handle),
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
            _ => panic!("Unsupported field type"),
        }
    }
}
