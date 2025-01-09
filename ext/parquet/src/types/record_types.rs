use itertools::Itertools;

use super::*;

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
pub struct ParquetField(pub Field);

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
