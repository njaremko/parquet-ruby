//! Test utilities for parquet-core

#[cfg(test)]
pub mod test {
    use crate::{ParquetValue, PrimitiveType, Schema, SchemaBuilder, SchemaNode};
    use indexmap::IndexMap;
    use ordered_float::OrderedFloat;
    use std::sync::Arc;

    /// Create a simple schema for testing
    pub fn sample_schema() -> Schema {
        SchemaBuilder::new()
            .with_root(SchemaNode::Struct {
                name: "root".to_string(),
                nullable: false,
                fields: vec![
                    SchemaNode::Primitive {
                        name: "id".to_string(),
                        primitive_type: PrimitiveType::Int64,
                        nullable: false,
                        format: None,
                    },
                    SchemaNode::Primitive {
                        name: "name".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: true,
                        format: None,
                    },
                    SchemaNode::Primitive {
                        name: "age".to_string(),
                        primitive_type: PrimitiveType::Int32,
                        nullable: true,
                        format: None,
                    },
                    SchemaNode::Primitive {
                        name: "salary".to_string(),
                        primitive_type: PrimitiveType::Float64,
                        nullable: true,
                        format: None,
                    },
                ],
            })
            .build()
            .unwrap()
    }

    /// Create a complex schema with nested types
    pub fn complex_schema() -> Schema {
        SchemaBuilder::new()
            .with_root(SchemaNode::Struct {
                name: "root".to_string(),
                nullable: false,
                fields: vec![
                    SchemaNode::Primitive {
                        name: "id".to_string(),
                        primitive_type: PrimitiveType::Int64,
                        nullable: false,
                        format: None,
                    },
                    SchemaNode::Struct {
                        name: "person".to_string(),
                        nullable: true,
                        fields: vec![
                            SchemaNode::Primitive {
                                name: "name".to_string(),
                                primitive_type: PrimitiveType::String,
                                nullable: false,
                                format: None,
                            },
                            SchemaNode::Primitive {
                                name: "age".to_string(),
                                primitive_type: PrimitiveType::Int32,
                                nullable: true,
                                format: None,
                            },
                        ],
                    },
                    SchemaNode::List {
                        name: "scores".to_string(),
                        nullable: true,
                        item: Box::new(SchemaNode::Primitive {
                            name: "item".to_string(),
                            primitive_type: PrimitiveType::Float32,
                            nullable: false,
                            format: None,
                        }),
                    },
                ],
            })
            .build()
            .unwrap()
    }

    /// Create sample row values matching the simple schema
    pub fn sample_values() -> Vec<ParquetValue> {
        vec![
            ParquetValue::Int64(1),
            ParquetValue::String(Arc::from("Alice")),
            ParquetValue::Int32(30),
            ParquetValue::Float64(OrderedFloat(75000.0)),
        ]
    }

    /// Create multiple sample rows
    pub fn sample_rows(count: usize) -> Vec<Vec<ParquetValue>> {
        (0..count)
            .map(|i| {
                vec![
                    ParquetValue::Int64(i as i64),
                    ParquetValue::String(Arc::from(format!("Person{}", i))),
                    ParquetValue::Int32((20 + i % 50) as i32),
                    ParquetValue::Float64(OrderedFloat(50000.0 + (i as f64 * 1000.0))),
                ]
            })
            .collect()
    }

    /// Create sample values with nulls
    pub fn sample_values_with_nulls() -> Vec<ParquetValue> {
        vec![
            ParquetValue::Int64(2),
            ParquetValue::Null,
            ParquetValue::Int32(25),
            ParquetValue::Null,
        ]
    }

    /// Create complex values matching the complex schema
    pub fn complex_values() -> Vec<ParquetValue> {
        let mut person = IndexMap::new();
        person.insert(Arc::from("name"), ParquetValue::String(Arc::from("Bob")));
        person.insert(Arc::from("age"), ParquetValue::Int32(35));

        vec![
            ParquetValue::Int64(1),
            ParquetValue::Record(person),
            ParquetValue::List(vec![
                ParquetValue::Float32(OrderedFloat(90.5)),
                ParquetValue::Float32(OrderedFloat(87.3)),
                ParquetValue::Float32(OrderedFloat(92.1)),
            ]),
        ]
    }

    /// Test data for all primitive types
    pub fn all_primitive_values() -> Vec<(PrimitiveType, ParquetValue)> {
        vec![
            (PrimitiveType::Boolean, ParquetValue::Boolean(true)),
            (PrimitiveType::Int8, ParquetValue::Int8(42)),
            (PrimitiveType::Int16, ParquetValue::Int16(1000)),
            (PrimitiveType::Int32, ParquetValue::Int32(100000)),
            (PrimitiveType::Int64, ParquetValue::Int64(1000000000)),
            (PrimitiveType::UInt8, ParquetValue::UInt8(200)),
            (PrimitiveType::UInt16, ParquetValue::UInt16(50000)),
            (PrimitiveType::UInt32, ParquetValue::UInt32(3000000000)),
            (PrimitiveType::UInt64, ParquetValue::UInt64(10000000000)),
            (
                PrimitiveType::Float32,
                ParquetValue::Float32(OrderedFloat(3.75)),
            ),
            (
                PrimitiveType::Float64,
                ParquetValue::Float64(OrderedFloat(2.625)),
            ),
            (
                PrimitiveType::String,
                ParquetValue::String(Arc::from("test string")),
            ),
            (
                PrimitiveType::Binary,
                ParquetValue::Bytes(bytes::Bytes::from(vec![0x01, 0x02, 0x03])),
            ),
            (PrimitiveType::Date32, ParquetValue::Date32(18628)), // 2021-01-01
            (
                PrimitiveType::TimeMillis,
                ParquetValue::TimeMillis(43200000),
            ), // 12:00:00
            (
                PrimitiveType::TimeMicros,
                ParquetValue::TimeMicros(43200000000),
            ), // 12:00:00
            (
                PrimitiveType::TimestampMillis(None),
                ParquetValue::TimestampMillis(1609459200000, None),
            ), // 2021-01-01 00:00:00
            (
                PrimitiveType::TimestampMicros(None),
                ParquetValue::TimestampMicros(1609459200000000, None),
            ), // 2021-01-01 00:00:00
            (
                PrimitiveType::Decimal128(10, 2),
                ParquetValue::Decimal128(12345, 2),
            ), // 123.45
        ]
    }

    /// Create a temporary file path for testing
    pub fn temp_file_path() -> String {
        format!("/tmp/parquet_test_{}.parquet", uuid::Uuid::new_v4())
    }

    /// Compare two ParquetValues for equality, handling floating point comparison
    pub fn values_equal(a: &ParquetValue, b: &ParquetValue) -> bool {
        match (a, b) {
            (ParquetValue::Float32(OrderedFloat(a)), ParquetValue::Float32(OrderedFloat(b))) => {
                (a - b).abs() < f32::EPSILON
            }
            (ParquetValue::Float64(OrderedFloat(a)), ParquetValue::Float64(OrderedFloat(b))) => {
                (a - b).abs() < f64::EPSILON
            }
            (ParquetValue::List(a), ParquetValue::List(b)) => {
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(a, b)| values_equal(a, b))
            }
            (ParquetValue::Map(a), ParquetValue::Map(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .zip(b.iter())
                        .all(|((k1, v1), (k2, v2))| values_equal(k1, k2) && values_equal(v1, v2))
            }
            (ParquetValue::Record(a), ParquetValue::Record(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .all(|(k, v)| b.get(k).map_or(false, |v2| values_equal(v, v2)))
            }
            _ => a == b,
        }
    }

    /// Assert that two vectors of ParquetValues are equal
    pub fn assert_values_equal(expected: &[ParquetValue], actual: &[ParquetValue]) {
        assert_eq!(
            expected.len(),
            actual.len(),
            "Value vectors have different lengths: expected {}, got {}",
            expected.len(),
            actual.len()
        );

        for (i, (e, a)) in expected.iter().zip(actual.iter()).enumerate() {
            assert!(
                values_equal(e, a),
                "Values at index {} are not equal:\nExpected: {:?}\nActual: {:?}",
                i,
                e,
                a
            );
        }
    }
}

#[cfg(test)]
mod test_utils_tests {
    use super::test::*;

    #[test]
    fn test_sample_schema() {
        let schema = sample_schema();
        assert_eq!(schema.root.name(), "root");

        if let crate::SchemaNode::Struct { fields, .. } = &schema.root {
            assert_eq!(fields.len(), 4);
            assert_eq!(fields[0].name(), "id");
            assert_eq!(fields[1].name(), "name");
            assert_eq!(fields[2].name(), "age");
            assert_eq!(fields[3].name(), "salary");
        } else {
            panic!("Expected struct schema");
        }
    }

    #[test]
    fn test_sample_values() {
        let values = sample_values();
        assert_eq!(values.len(), 4);
        assert!(matches!(values[0], crate::ParquetValue::Int64(1)));
        assert!(matches!(&values[1], crate::ParquetValue::String(s) if s.as_ref() == "Alice"));
    }

    #[test]
    fn test_values_equal() {
        use crate::ParquetValue;
        use ordered_float::OrderedFloat;

        // Test exact equality
        assert!(values_equal(
            &ParquetValue::Int32(42),
            &ParquetValue::Int32(42)
        ));

        // Test floating point equality
        assert!(values_equal(
            &ParquetValue::Float32(OrderedFloat(1.0)),
            &ParquetValue::Float32(OrderedFloat(1.0 + f32::EPSILON / 2.0))
        ));

        // Test list equality
        assert!(values_equal(
            &ParquetValue::List(vec![ParquetValue::Int32(1), ParquetValue::Int32(2)]),
            &ParquetValue::List(vec![ParquetValue::Int32(1), ParquetValue::Int32(2)])
        ));

        // Test inequality
        assert!(!values_equal(
            &ParquetValue::Int32(42),
            &ParquetValue::Int32(43)
        ));
    }
}
