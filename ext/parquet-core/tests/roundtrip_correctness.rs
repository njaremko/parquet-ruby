use bytes::Bytes;
use num::BigInt;
use ordered_float::OrderedFloat;
use parquet_core::*;
use std::sync::Arc;

#[test]
fn test_all_primitive_types_roundtrip() {
    // Comprehensive test that all primitive types roundtrip correctly
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "bool_val".to_string(),
                    primitive_type: PrimitiveType::Boolean,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "int8_val".to_string(),
                    primitive_type: PrimitiveType::Int8,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "int16_val".to_string(),
                    primitive_type: PrimitiveType::Int16,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "int32_val".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "int64_val".to_string(),
                    primitive_type: PrimitiveType::Int64,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "uint8_val".to_string(),
                    primitive_type: PrimitiveType::UInt8,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "uint16_val".to_string(),
                    primitive_type: PrimitiveType::UInt16,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "uint32_val".to_string(),
                    primitive_type: PrimitiveType::UInt32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "uint64_val".to_string(),
                    primitive_type: PrimitiveType::UInt64,
                    nullable: false,
                    format: None,
                },
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
                SchemaNode::Primitive {
                    name: "string_val".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "binary_val".to_string(),
                    primitive_type: PrimitiveType::Binary,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "date32_val".to_string(),
                    primitive_type: PrimitiveType::Date32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "date64_val".to_string(),
                    primitive_type: PrimitiveType::Date64,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "time_millis_val".to_string(),
                    primitive_type: PrimitiveType::TimeMillis,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "time_micros_val".to_string(),
                    primitive_type: PrimitiveType::TimeMicros,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "timestamp_millis_val".to_string(),
                    primitive_type: PrimitiveType::TimestampMillis(None),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "decimal128_val".to_string(),
                    primitive_type: PrimitiveType::Decimal128(10, 2),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "decimal256_val".to_string(),
                    primitive_type: PrimitiveType::Decimal256(50, 10),
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let rows = vec![vec![
        ParquetValue::Boolean(true),
        ParquetValue::Int8(42),
        ParquetValue::Int16(1000),
        ParquetValue::Int32(100000),
        ParquetValue::Int64(1000000000),
        ParquetValue::UInt8(200),
        ParquetValue::UInt16(50000),
        ParquetValue::UInt32(3000000000),
        ParquetValue::UInt64(10000000000),
        ParquetValue::Float32(OrderedFloat(std::f32::consts::PI)),
        ParquetValue::Float64(OrderedFloat(std::f64::consts::E)),
        ParquetValue::String(Arc::from("Test string 🦀")),
        ParquetValue::Bytes(Bytes::from(vec![0xDE, 0xAD, 0xBE, 0xEF])),
        ParquetValue::Date32(19000),
        ParquetValue::Date64(1640995200000),
        ParquetValue::TimeMillis(43200000),
        ParquetValue::TimeMicros(43200000000),
        ParquetValue::TimestampMillis(1640995200000, None),
        ParquetValue::Decimal128(12345, 2),
        ParquetValue::Decimal256(
            BigInt::parse_bytes(b"1234567890123456789012345678901234567890", 10).unwrap(),
            10,
        ),
    ]];

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
fn test_empty_collections_roundtrip() {
    // Test that empty lists, maps, and strings roundtrip correctly
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "empty_string".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "empty_binary".to_string(),
                    primitive_type: PrimitiveType::Binary,
                    nullable: false,
                    format: None,
                },
                SchemaNode::List {
                    name: "empty_list".to_string(),
                    nullable: false,
                    item: Box::new(SchemaNode::Primitive {
                        name: "item".to_string(),
                        primitive_type: PrimitiveType::Int32,
                        nullable: false,
                        format: None,
                    }),
                },
                SchemaNode::Map {
                    name: "empty_map".to_string(),
                    nullable: false,
                    key: Box::new(SchemaNode::Primitive {
                        name: "key".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: false,
                        format: None,
                    }),
                    value: Box::new(SchemaNode::Primitive {
                        name: "value".to_string(),
                        primitive_type: PrimitiveType::Int32,
                        nullable: false,
                        format: None,
                    }),
                },
            ],
        })
        .build()
        .unwrap();

    let rows = vec![
        vec![
            ParquetValue::String(Arc::from("")),
            ParquetValue::Bytes(Bytes::from(vec![])),
            ParquetValue::List(vec![]),
            ParquetValue::Map(vec![]),
        ],
        vec![
            ParquetValue::String(Arc::from("not empty")),
            ParquetValue::Bytes(Bytes::from(vec![1, 2, 3])),
            ParquetValue::List(vec![ParquetValue::Int32(42)]),
            ParquetValue::Map(vec![(
                ParquetValue::String(Arc::from("key")),
                ParquetValue::Int32(100),
            )]),
        ],
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
