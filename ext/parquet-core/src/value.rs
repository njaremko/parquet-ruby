use bytes::Bytes;
use indexmap::IndexMap;
use num::BigInt;
use triomphe::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParquetValue {
    // Numeric types
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float16(ordered_float::OrderedFloat<f32>), // f16 converted to f32
    Float32(ordered_float::OrderedFloat<f32>),
    Float64(ordered_float::OrderedFloat<f64>),

    // Basic types
    Boolean(bool),
    String(Arc<str>),
    Bytes(Bytes),
    Uuid(Uuid),

    // Date/Time types
    Date32(i32), // Days since epoch
    Date64(i64), // Milliseconds since epoch

    // Decimal types
    Decimal128(i128, i8),   // value, scale
    Decimal256(BigInt, i8), // Using BigInt instead of arrow_buffer::i256 for pure Rust

    // Timestamp types - all store microseconds since epoch with optional timezone
    TimestampSecond(i64, Option<Arc<str>>),
    TimestampMillis(i64, Option<Arc<str>>),
    TimestampMicros(i64, Option<Arc<str>>),
    TimestampNanos(i64, Option<Arc<str>>),

    // Time types
    TimeMillis(i32), // Time of day in milliseconds since midnight
    TimeMicros(i64), // Time of day in microseconds since midnight
    TimeNanos(i64),  // Time of day in nanoseconds since midnight

    // Complex types
    List(Vec<ParquetValue>),
    Map(Vec<(ParquetValue, ParquetValue)>), // Using Vec of tuples for deterministic ordering
    Record(IndexMap<Arc<str>, ParquetValue>), // For struct/record types, preserves field order

    // Null value
    Null,
}

impl std::hash::Hash for ParquetValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            ParquetValue::Int8(i) => i.hash(state),
            ParquetValue::Int16(i) => i.hash(state),
            ParquetValue::Int32(i) => i.hash(state),
            ParquetValue::Int64(i) => i.hash(state),
            ParquetValue::UInt8(i) => i.hash(state),
            ParquetValue::UInt16(i) => i.hash(state),
            ParquetValue::UInt32(i) => i.hash(state),
            ParquetValue::UInt64(i) => i.hash(state),
            ParquetValue::Float16(f) => f.hash(state),
            ParquetValue::Float32(f) => f.hash(state),
            ParquetValue::Float64(f) => f.hash(state),
            ParquetValue::Boolean(b) => b.hash(state),
            ParquetValue::String(s) => s.hash(state),
            ParquetValue::Bytes(b) => b.hash(state),
            ParquetValue::Uuid(u) => u.hash(state),
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
            ParquetValue::TimeNanos(t) => t.hash(state),
            ParquetValue::List(l) => l.hash(state),
            ParquetValue::Map(m) => m.hash(state),
            ParquetValue::Record(r) => {
                r.len().hash(state);
                let mut entries = r.iter().collect::<Vec<_>>();
                entries.sort_by(|(left_key, _), (right_key, _)| {
                    left_key.as_ref().cmp(right_key.as_ref())
                });
                for (k, v) in entries {
                    k.hash(state);
                    v.hash(state);
                }
            }
            ParquetValue::Null => 0_i32.hash(state),
        }
    }
}

impl ParquetValue {
    /// Check if the value is null
    pub fn is_null(&self) -> bool {
        matches!(self, ParquetValue::Null)
    }

    /// Get the type name of the value
    pub fn type_name(&self) -> &'static str {
        match self {
            ParquetValue::Int8(_) => "Int8",
            ParquetValue::Int16(_) => "Int16",
            ParquetValue::Int32(_) => "Int32",
            ParquetValue::Int64(_) => "Int64",
            ParquetValue::UInt8(_) => "UInt8",
            ParquetValue::UInt16(_) => "UInt16",
            ParquetValue::UInt32(_) => "UInt32",
            ParquetValue::UInt64(_) => "UInt64",
            ParquetValue::Float16(_) => "Float16",
            ParquetValue::Float32(_) => "Float32",
            ParquetValue::Float64(_) => "Float64",
            ParquetValue::Boolean(_) => "Boolean",
            ParquetValue::String(_) => "String",
            ParquetValue::Bytes(_) => "Bytes",
            ParquetValue::Uuid(_) => "Uuid",
            ParquetValue::Date32(_) => "Date32",
            ParquetValue::Date64(_) => "Date64",
            ParquetValue::Decimal128(_, _) => "Decimal128",
            ParquetValue::Decimal256(_, _) => "Decimal256",
            ParquetValue::TimestampSecond(_, _) => "TimestampSecond",
            ParquetValue::TimestampMillis(_, _) => "TimestampMillis",
            ParquetValue::TimestampMicros(_, _) => "TimestampMicros",
            ParquetValue::TimestampNanos(_, _) => "TimestampNanos",
            ParquetValue::TimeMillis(_) => "TimeMillis",
            ParquetValue::TimeMicros(_) => "TimeMicros",
            ParquetValue::TimeNanos(_) => "TimeNanos",
            ParquetValue::List(_) => "List",
            ParquetValue::Map(_) => "Map",
            ParquetValue::Record(_) => "Record",
            ParquetValue::Null => "Null",
        }
    }

    /// Conservative retained-memory charge used by the streaming writer.
    ///
    /// Shared string/byte contents are charged to every value that can retain
    /// them. This may flush early, but it prevents sharing from making the
    /// writer's admission decision depend on ownership aliases it cannot see.
    /// The traversal is iterative so adversarially nested values do not consume
    /// the native call stack.
    pub(crate) fn retained_size_bytes(&self) -> usize {
        let mut total = 0usize;
        let mut pending = vec![self];

        while let Some(value) = pending.pop() {
            total = total.saturating_add(std::mem::size_of::<Self>());
            match value {
                Self::String(value) => {
                    total = total
                        .saturating_add(value.len())
                        .saturating_add(2 * std::mem::size_of::<usize>());
                }
                Self::Bytes(value) => {
                    total = total
                        .saturating_add(value.len())
                        .saturating_add(2 * std::mem::size_of::<usize>());
                }
                Self::Decimal256(value, _) => {
                    let byte_len = (value.bits().saturating_add(7) / 8).max(32);
                    total = total.saturating_add(usize::try_from(byte_len).unwrap_or(usize::MAX));
                }
                Self::TimestampSecond(_, timezone)
                | Self::TimestampMillis(_, timezone)
                | Self::TimestampMicros(_, timezone)
                | Self::TimestampNanos(_, timezone) => {
                    if let Some(timezone) = timezone {
                        total = total.saturating_add(timezone.len());
                    }
                }
                Self::List(values) => {
                    total = total.saturating_add(
                        values
                            .capacity()
                            .saturating_mul(std::mem::size_of::<Self>()),
                    );
                    pending.extend(values);
                }
                Self::Map(entries) => {
                    total = total.saturating_add(
                        entries
                            .capacity()
                            .saturating_mul(std::mem::size_of::<(Self, Self)>()),
                    );
                    for (key, value) in entries {
                        pending.push(key);
                        pending.push(value);
                    }
                }
                Self::Record(fields) => {
                    // IndexMap owns both a dense entry table and a hash index.
                    // Charging two dense entries per capacity is deliberately
                    // conservative across IndexMap implementation changes.
                    let entry_bytes = std::mem::size_of::<(Arc<str>, Self)>();
                    total = total.saturating_add(
                        fields
                            .capacity()
                            .saturating_mul(entry_bytes)
                            .saturating_mul(2),
                    );
                    for (name, value) in fields {
                        total = total.saturating_add(name.len());
                        pending.push(value);
                    }
                }
                Self::Int8(_)
                | Self::Int16(_)
                | Self::Int32(_)
                | Self::Int64(_)
                | Self::UInt8(_)
                | Self::UInt16(_)
                | Self::UInt32(_)
                | Self::UInt64(_)
                | Self::Float16(_)
                | Self::Float32(_)
                | Self::Float64(_)
                | Self::Boolean(_)
                | Self::Uuid(_)
                | Self::Date32(_)
                | Self::Date64(_)
                | Self::Decimal128(_, _)
                | Self::TimeMillis(_)
                | Self::TimeMicros(_)
                | Self::TimeNanos(_)
                | Self::Null => {}
            }
        }

        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ordered_float::OrderedFloat;

    #[test]
    fn test_value_creation() {
        let v = ParquetValue::Int32(42);
        assert_eq!(v, ParquetValue::Int32(42));
        assert!(!v.is_null());
        assert_eq!(v.type_name(), "Int32");
    }

    #[test]
    fn test_null_value() {
        let v = ParquetValue::Null;
        assert!(v.is_null());
        assert_eq!(v.type_name(), "Null");
    }

    #[test]
    fn test_float_equality() {
        let v1 = ParquetValue::Float32(OrderedFloat(3.5));
        let v2 = ParquetValue::Float32(OrderedFloat(3.5));
        assert_eq!(v1, v2);
    }

    #[test]
    fn test_complex_types() {
        let list = ParquetValue::List(vec![
            ParquetValue::Int32(1),
            ParquetValue::Int32(2),
            ParquetValue::Int32(3),
        ]);
        assert_eq!(list.type_name(), "List");

        let map = ParquetValue::Map(vec![(
            ParquetValue::String(Arc::from("key")),
            ParquetValue::Int32(42),
        )]);
        assert_eq!(map.type_name(), "Map");
    }

    #[test]
    fn test_hash_consistency() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(ParquetValue::Int32(42));
        set.insert(ParquetValue::String(Arc::from("hello")));

        assert!(set.contains(&ParquetValue::Int32(42)));
        assert!(set.contains(&ParquetValue::String(Arc::from("hello"))));
        assert!(!set.contains(&ParquetValue::Int32(43)));
    }
}
