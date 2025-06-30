use bytes::Bytes;
use ordered_float::OrderedFloat;
use parquet_core::*;
use std::sync::Arc;

// =============================================================================
// Boolean Type Tests
// =============================================================================

#[test]
fn test_boolean_values() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "bool_field".to_string(),
                    primitive_type: PrimitiveType::Boolean,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "nullable_bool".to_string(),
                    primitive_type: PrimitiveType::Boolean,
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let rows = vec![
        vec![ParquetValue::Boolean(true), ParquetValue::Boolean(true)],
        vec![ParquetValue::Boolean(false), ParquetValue::Boolean(false)],
        vec![ParquetValue::Boolean(true), ParquetValue::Null],
        vec![ParquetValue::Boolean(false), ParquetValue::Null],
        // Many repeated values to test encoding efficiency
        vec![ParquetValue::Boolean(true), ParquetValue::Boolean(true)],
        vec![ParquetValue::Boolean(true), ParquetValue::Boolean(true)],
        vec![ParquetValue::Boolean(true), ParquetValue::Boolean(true)],
        vec![ParquetValue::Boolean(false), ParquetValue::Boolean(false)],
        vec![ParquetValue::Boolean(false), ParquetValue::Boolean(false)],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

    // Read back and verify
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(rows.len(), read_rows.len());
    for (expected, actual) in rows.iter().zip(read_rows.iter()) {
        assert_eq!(expected, actual);
    }
}

#[test]
fn test_boolean_in_complex_types() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::List {
                    name: "bool_list".to_string(),
                    nullable: false,
                    item: Box::new(SchemaNode::Primitive {
                        name: "item".to_string(),
                        primitive_type: PrimitiveType::Boolean,
                        nullable: false,
                        format: None,
                    }),
                },
                SchemaNode::Map {
                    name: "bool_map".to_string(),
                    nullable: false,
                    key: Box::new(SchemaNode::Primitive {
                        name: "key".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: false,
                        format: None,
                    }),
                    value: Box::new(SchemaNode::Primitive {
                        name: "value".to_string(),
                        primitive_type: PrimitiveType::Boolean,
                        nullable: true,
                        format: None,
                    }),
                },
            ],
        })
        .build()
        .unwrap();

    let rows = vec![
        vec![
            ParquetValue::List(vec![
                ParquetValue::Boolean(true),
                ParquetValue::Boolean(false),
                ParquetValue::Boolean(true),
            ]),
            ParquetValue::Map(vec![
                (
                    ParquetValue::String(Arc::from("enabled")),
                    ParquetValue::Boolean(true),
                ),
                (
                    ParquetValue::String(Arc::from("disabled")),
                    ParquetValue::Boolean(false),
                ),
                (
                    ParquetValue::String(Arc::from("unknown")),
                    ParquetValue::Null,
                ),
            ]),
        ],
        vec![ParquetValue::List(vec![]), ParquetValue::Map(vec![])],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

    // Read back and verify
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(rows.len(), read_rows.len());
    for (expected, actual) in rows.iter().zip(read_rows.iter()) {
        assert_eq!(expected, actual);
    }
}

// =============================================================================
// String Type Tests
// =============================================================================

#[test]
fn test_string_roundtrip() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "text".to_string(),
                primitive_type: PrimitiveType::String,
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap();

    let repeated_str = "Repeated".repeat(100);
    let long_str = "x".repeat(10_000);

    let test_strings = vec![
        "",                             // Empty string
        "Hello, World!",                // ASCII
        "Hello, 世界! 🦀",              // Unicode with emoji
        "Line1\nLine2\rLine3\r\nLine4", // Various line endings
        "\t\t\tTabbed\t\t\t",           // Tabs
        "Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?",
        repeated_str.as_str(), // Long repeated string
        long_str.as_str(),     // Very long string
    ];

    let rows: Vec<Vec<ParquetValue>> = test_strings
        .into_iter()
        .map(|s| vec![ParquetValue::String(Arc::from(s))])
        .collect();

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

    // Read back and verify
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(rows.len(), read_rows.len());
    for (expected, actual) in rows.iter().zip(read_rows.iter()) {
        assert_eq!(expected, actual);
    }
}

// =============================================================================
// Numeric Type Tests
// =============================================================================

#[test]
fn test_float_special_values() {
    // Test NaN, Infinity, and -Infinity handling
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "float32_val".to_string(),
                    primitive_type: PrimitiveType::Float32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "float64_val".to_string(),
                    primitive_type: PrimitiveType::Float64,
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let special_values = vec![
        // Normal values
        vec![
            ParquetValue::Float32(OrderedFloat(1.23f32)),
            ParquetValue::Float64(OrderedFloat(4.56f64)),
        ],
        // Positive infinity
        vec![
            ParquetValue::Float32(OrderedFloat(f32::INFINITY)),
            ParquetValue::Float64(OrderedFloat(f64::INFINITY)),
        ],
        // Negative infinity
        vec![
            ParquetValue::Float32(OrderedFloat(f32::NEG_INFINITY)),
            ParquetValue::Float64(OrderedFloat(f64::NEG_INFINITY)),
        ],
        // NaN values
        vec![
            ParquetValue::Float32(OrderedFloat(f32::NAN)),
            ParquetValue::Float64(OrderedFloat(f64::NAN)),
        ],
        // Zero values
        vec![
            ParquetValue::Float32(OrderedFloat(0.0f32)),
            ParquetValue::Float64(OrderedFloat(0.0f64)),
        ],
        // Negative zero
        vec![
            ParquetValue::Float32(OrderedFloat(-0.0f32)),
            ParquetValue::Float64(OrderedFloat(-0.0f64)),
        ],
        // Very small values
        vec![
            ParquetValue::Float32(OrderedFloat(f32::MIN_POSITIVE)),
            ParquetValue::Float64(OrderedFloat(f64::MIN_POSITIVE)),
        ],
        // Very large values
        vec![
            ParquetValue::Float32(OrderedFloat(f32::MAX)),
            ParquetValue::Float64(OrderedFloat(f64::MAX)),
        ],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(special_values.clone()).unwrap();
        writer.close().unwrap();
    }

    // Read back and verify
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_rows.len(), special_values.len());

    // Verify special values are preserved
    for (expected, actual) in special_values.iter().zip(read_rows.iter()) {
        for (exp_val, act_val) in expected.iter().zip(actual.iter()) {
            match (exp_val, act_val) {
                (
                    ParquetValue::Float32(OrderedFloat(e)),
                    ParquetValue::Float32(OrderedFloat(a)),
                ) => {
                    if e.is_nan() {
                        assert!(a.is_nan());
                    } else {
                        assert_eq!(e, a);
                    }
                }
                (
                    ParquetValue::Float64(OrderedFloat(e)),
                    ParquetValue::Float64(OrderedFloat(a)),
                ) => {
                    if e.is_nan() {
                        assert!(a.is_nan());
                    } else {
                        assert_eq!(e, a);
                    }
                }
                _ => panic!("Type mismatch"),
            }
        }
    }
}

// Macro to generate integer boundary tests for each type
macro_rules! test_integer_boundaries {
    ($test_name:ident, $type_name:expr, $primitive_type:expr, $rust_type:ty, $parquet_variant:ident, $test_values:expr) => {
        #[test]
        fn $test_name() {
            let schema = SchemaBuilder::new()
                .with_root(SchemaNode::Struct {
                    name: "root".to_string(),
                    nullable: false,
                    fields: vec![SchemaNode::Primitive {
                        name: $type_name.to_string(),
                        primitive_type: $primitive_type,
                        nullable: false,
                        format: None,
                    }],
                })
                .build()
                .unwrap();

            let boundary_values: Vec<Vec<ParquetValue>> = $test_values
                .into_iter()
                .map(|v| vec![ParquetValue::$parquet_variant(v)])
                .collect();

            let mut buffer = Vec::new();
            {
                let mut writer = Writer::new(&mut buffer, schema).unwrap();
                writer.write_rows(boundary_values.clone()).unwrap();
                writer.close().unwrap();
            }

            // Read back and verify
            let bytes = Bytes::from(buffer);
            let reader = Reader::new(bytes);

            let read_rows: Vec<_> = reader
                .read_rows()
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();

            assert_eq!(read_rows.len(), boundary_values.len());
            for (expected, actual) in boundary_values.iter().zip(read_rows.iter()) {
                assert_eq!(expected, actual);
            }
        }
    };
}

// Generate tests for all integer types
test_integer_boundaries!(
    test_int8_boundaries,
    "int8",
    PrimitiveType::Int8,
    i8,
    Int8,
    vec![i8::MIN, i8::MAX, 0, -1, 42, -42]
);

test_integer_boundaries!(
    test_int16_boundaries,
    "int16",
    PrimitiveType::Int16,
    i16,
    Int16,
    vec![i16::MIN, i16::MAX, 0, -1, 1000, -1000]
);

test_integer_boundaries!(
    test_int32_boundaries,
    "int32",
    PrimitiveType::Int32,
    i32,
    Int32,
    vec![i32::MIN, i32::MAX, 0, -1, 1_000_000, -1_000_000]
);

test_integer_boundaries!(
    test_int64_boundaries,
    "int64",
    PrimitiveType::Int64,
    i64,
    Int64,
    vec![
        i64::MIN,
        i64::MAX,
        0,
        -1,
        1_000_000_000_000,
        -1_000_000_000_000
    ]
);

test_integer_boundaries!(
    test_uint8_boundaries,
    "uint8",
    PrimitiveType::UInt8,
    u8,
    UInt8,
    vec![u8::MIN, u8::MAX, 0, 1, 128, 255]
);

test_integer_boundaries!(
    test_uint16_boundaries,
    "uint16",
    PrimitiveType::UInt16,
    u16,
    UInt16,
    vec![u16::MIN, u16::MAX, 0, 1, 32768, 65535]
);

test_integer_boundaries!(
    test_uint32_boundaries,
    "uint32",
    PrimitiveType::UInt32,
    u32,
    UInt32,
    vec![u32::MIN, u32::MAX, 0, 1, 2_147_483_648, 4_294_967_295]
);

test_integer_boundaries!(
    test_uint64_boundaries,
    "uint64",
    PrimitiveType::UInt64,
    u64,
    UInt64,
    vec![
        u64::MIN,
        u64::MAX,
        0,
        1,
        9_223_372_036_854_775_808,
        18_446_744_073_709_551_615
    ]
);

#[test]
fn test_mixed_numeric_types() {
    // Test writing different numeric types and reading them back
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "as_int8".to_string(),
                    primitive_type: PrimitiveType::Int8,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "as_int32".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "as_float".to_string(),
                    primitive_type: PrimitiveType::Float32,
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    // Test values that fit in all types
    let test_values = vec![
        vec![
            ParquetValue::Int8(42),
            ParquetValue::Int32(42),
            ParquetValue::Float32(OrderedFloat(42.0)),
        ],
        vec![
            ParquetValue::Int8(-50),
            ParquetValue::Int32(-50),
            ParquetValue::Float32(OrderedFloat(-50.0)),
        ],
        vec![
            ParquetValue::Int8(0),
            ParquetValue::Int32(0),
            ParquetValue::Float32(OrderedFloat(0.0)),
        ],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(test_values.clone()).unwrap();
        writer.close().unwrap();
    }

    // Read back and verify
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_rows.len(), test_values.len());

    for (expected, actual) in test_values.iter().zip(read_rows.iter()) {
        assert_eq!(expected, actual);
    }
}
