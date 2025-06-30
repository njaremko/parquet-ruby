use bytes::Bytes;
use num::BigInt;
use parquet_core::*;
use std::sync::Arc;

#[test]
fn test_decimal128_precision_scale_combinations() {
    // Test various precision and scale combinations for Decimal128
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "dec_5_2".to_string(),
                    primitive_type: PrimitiveType::Decimal128(5, 2),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "dec_9_2".to_string(),
                    primitive_type: PrimitiveType::Decimal128(9, 2),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "dec_18_0".to_string(),
                    primitive_type: PrimitiveType::Decimal128(18, 0),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "dec_38_0".to_string(),
                    primitive_type: PrimitiveType::Decimal128(38, 0),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "dec_38_10".to_string(),
                    primitive_type: PrimitiveType::Decimal128(38, 10),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "dec_38_38".to_string(),
                    primitive_type: PrimitiveType::Decimal128(38, 38),
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let test_cases = vec![
        // Maximum positive values for each precision
        vec![
            ParquetValue::Decimal128(99999, 2),       // 999.99 (max for 5,2)
            ParquetValue::Decimal128(999_999_999, 2), // 9999999.99
            ParquetValue::Decimal128(999999999999999999, 0), // 18 digits
            ParquetValue::Decimal128(99999999999999999999999999999999999999_i128, 0), // Max 38 digits
            ParquetValue::Decimal128(99_999_999_999_999_999_999_999_999_999_999_999_999i128, 10), // 38 digits, 10 scale
            ParquetValue::Decimal128(99999999999999999999999999999999999999_i128, 38), // 0.99999... (38 9s after decimal)
        ],
        // Maximum negative values
        vec![
            ParquetValue::Decimal128(-99999, 2), // -999.99 (min for 5,2)
            ParquetValue::Decimal128(-999_999_999, 2), // -9999999.99
            ParquetValue::Decimal128(-999999999999999999, 0), // -18 digits
            ParquetValue::Decimal128(-99999999999999999999999999999999999999_i128, 0), // Min 38 digits
            ParquetValue::Decimal128(-99_999_999_999_999_999_999_999_999_999_999_999_999i128, 10), // -38 digits, 10 scale
            ParquetValue::Decimal128(-99999999999999999999999999999999999999_i128, 38), // -0.99999...
        ],
        // Zero values
        vec![
            ParquetValue::Decimal128(0, 2), // 0.00
            ParquetValue::Decimal128(0, 2),
            ParquetValue::Decimal128(0, 0),
            ParquetValue::Decimal128(0, 0), // 0
            ParquetValue::Decimal128(0, 10),
            ParquetValue::Decimal128(0, 38), // 0.00000... (38 zeros)
        ],
        // Small positive values
        vec![
            ParquetValue::Decimal128(1, 2),  // 0.01 (smallest positive for 5,2)
            ParquetValue::Decimal128(1, 2),  // 0.01
            ParquetValue::Decimal128(1, 0),  // 1
            ParquetValue::Decimal128(1, 0),  // 1
            ParquetValue::Decimal128(1, 10), // 0.0000000001
            ParquetValue::Decimal128(1, 38), // 0.00000...01 (37 zeros then 1)
        ],
        // Maximum value for i128
        vec![
            ParquetValue::Decimal128(99999, 2),
            ParquetValue::Decimal128(999_999_999, 2),
            ParquetValue::Decimal128(i64::MAX as i128, 0),
            ParquetValue::Decimal128(i128::MAX, 0),
            ParquetValue::Decimal128(12345678901234567890123456789_i128, 10),
            ParquetValue::Decimal128(1, 38),
        ],
        // Minimum value for i128
        vec![
            ParquetValue::Decimal128(-99999, 2),
            ParquetValue::Decimal128(-999_999_999, 2),
            ParquetValue::Decimal128(i64::MIN as i128, 0),
            ParquetValue::Decimal128(i128::MIN, 0),
            ParquetValue::Decimal128(-12345678901234567890123456789_i128, 10),
            ParquetValue::Decimal128(-1, 38),
        ],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(test_cases.clone()).unwrap();
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

    assert_eq!(read_rows.len(), test_cases.len());
    for (expected, actual) in test_cases.iter().zip(read_rows.iter()) {
        assert_eq!(expected, actual);
    }
}

#[test]
fn test_decimal256_large_values() {
    // Test very large Decimal256 values
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "dec256_76_0".to_string(),
                    primitive_type: PrimitiveType::Decimal256(76, 0),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "dec256_76_38".to_string(),
                    primitive_type: PrimitiveType::Decimal256(76, 38),
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    // Test values that require Decimal256
    let large_positive = BigInt::parse_bytes(
        b"9999999999999999999999999999999999999999999999999999999999999999999999999",
        10,
    )
    .unwrap();
    let large_negative = -large_positive.clone();
    let medium_value = BigInt::parse_bytes(
        b"123456789012345678901234567890123456789012345678901234567890",
        10,
    )
    .unwrap();

    let test_cases = vec![
        vec![
            ParquetValue::Decimal256(large_positive.clone(), 0),
            ParquetValue::Decimal256(medium_value.clone(), 38),
        ],
        vec![
            ParquetValue::Decimal256(large_negative.clone(), 0),
            ParquetValue::Decimal256(-medium_value.clone(), 38),
        ],
        vec![
            ParquetValue::Decimal256(BigInt::from(0), 0),
            ParquetValue::Decimal256(BigInt::from(1), 38),
        ],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(test_cases.clone()).unwrap();
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

    assert_eq!(read_rows.len(), test_cases.len());
    for (expected, actual) in test_cases.iter().zip(read_rows.iter()) {
        assert_eq!(expected, actual);
    }
}

#[test]
fn test_decimal_null_handling() {
    // Test nullable decimal fields
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "nullable_dec128".to_string(),
                    primitive_type: PrimitiveType::Decimal128(18, 4),
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "nullable_dec256".to_string(),
                    primitive_type: PrimitiveType::Decimal256(50, 10),
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let rows = vec![
        vec![
            ParquetValue::Decimal128(123456789012345678, 4),
            ParquetValue::Decimal256(BigInt::from(987654321), 10),
        ],
        vec![
            ParquetValue::Null,
            ParquetValue::Decimal256(BigInt::from(0), 10),
        ],
        vec![
            ParquetValue::Decimal128(-999999999999999999, 4),
            ParquetValue::Null,
        ],
        vec![ParquetValue::Null, ParquetValue::Null],
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

    assert_eq!(read_rows.len(), rows.len());
    for (expected, actual) in rows.iter().zip(read_rows.iter()) {
        assert_eq!(expected, actual);
    }
}

#[test]
fn test_decimal_in_complex_types() {
    use indexmap::IndexMap;

    // Test decimals within lists, maps, and structs
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::List {
                    name: "decimal_list".to_string(),
                    nullable: false,
                    item: Box::new(SchemaNode::Primitive {
                        name: "item".to_string(),
                        primitive_type: PrimitiveType::Decimal128(10, 2),
                        nullable: false,
                        format: None,
                    }),
                },
                SchemaNode::Map {
                    name: "decimal_map".to_string(),
                    nullable: false,
                    key: Box::new(SchemaNode::Primitive {
                        name: "key".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: false,
                        format: None,
                    }),
                    value: Box::new(SchemaNode::Primitive {
                        name: "value".to_string(),
                        primitive_type: PrimitiveType::Decimal256(40, 10),
                        nullable: true,
                        format: None,
                    }),
                },
                SchemaNode::Struct {
                    name: "decimal_struct".to_string(),
                    nullable: true,
                    fields: vec![
                        SchemaNode::Primitive {
                            name: "amount".to_string(),
                            primitive_type: PrimitiveType::Decimal128(18, 2),
                            nullable: false,
                            format: None,
                        },
                        SchemaNode::Primitive {
                            name: "large_amount".to_string(),
                            primitive_type: PrimitiveType::Decimal256(50, 6),
                            nullable: true,
                            format: None,
                        },
                    ],
                },
            ],
        })
        .build()
        .unwrap();

    let rows = vec![
        vec![
            // List of decimals
            ParquetValue::List(vec![
                ParquetValue::Decimal128(100, 2),  // 1.00
                ParquetValue::Decimal128(250, 2),  // 2.50
                ParquetValue::Decimal128(-375, 2), // -3.75
            ]),
            // Map with decimal values
            ParquetValue::Map(vec![
                (
                    ParquetValue::String(Arc::from("total")),
                    ParquetValue::Decimal256(BigInt::from(1234567890123456789_i64), 10),
                ),
                (
                    ParquetValue::String(Arc::from("discount")),
                    ParquetValue::Null,
                ),
                (
                    ParquetValue::String(Arc::from("tax")),
                    ParquetValue::Decimal256(BigInt::from(98765432109876543_i64), 10),
                ),
            ]),
            // Struct with decimals
            ParquetValue::Record({
                let mut map = IndexMap::new();
                map.insert(
                    Arc::from("amount"),
                    ParquetValue::Decimal128(999999999999999999, 2),
                );
                map.insert(
                    Arc::from("large_amount"),
                    ParquetValue::Decimal256(
                        BigInt::parse_bytes(b"12345678901234567890123456789012345678901234", 10)
                            .unwrap(),
                        6,
                    ),
                );
                map
            }),
        ],
        vec![
            // Empty list
            ParquetValue::List(vec![]),
            // Empty map
            ParquetValue::Map(vec![]),
            // Null struct
            ParquetValue::Null,
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

    assert_eq!(read_rows.len(), rows.len());
    for (expected, actual) in rows.iter().zip(read_rows.iter()) {
        assert_eq!(expected, actual);
    }
}

#[test]
fn test_decimal_precision_edge_cases() {
    // Test decimal values at the edge of precision limits
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "dec_9_2".to_string(),
                    primitive_type: PrimitiveType::Decimal128(9, 2),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "dec_18_0".to_string(),
                    primitive_type: PrimitiveType::Decimal128(18, 0),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "dec_38_10".to_string(),
                    primitive_type: PrimitiveType::Decimal128(38, 10),
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let decimal_values = vec![
        // Maximum positive values for each precision
        vec![
            ParquetValue::Decimal128(999_999_999, 2), // 9999999.99
            ParquetValue::Decimal128(999999999999999999, 0), // 18 digits
            ParquetValue::Decimal128(99_999_999_999_999_999_999_999_999_999_999_999_999i128, 10), // 38 digits, 10 scale
        ],
        // Maximum negative values
        vec![
            ParquetValue::Decimal128(-999_999_999, 2), // -9999999.99
            ParquetValue::Decimal128(-999999999999999999, 0), // -18 digits
            ParquetValue::Decimal128(-99_999_999_999_999_999_999_999_999_999_999_999_999i128, 10), // -38 digits, 10 scale
        ],
        // Zero values
        vec![
            ParquetValue::Decimal128(0, 2),
            ParquetValue::Decimal128(0, 0),
            ParquetValue::Decimal128(0, 10),
        ],
        // Small values
        vec![
            ParquetValue::Decimal128(1, 2),  // 0.01
            ParquetValue::Decimal128(1, 0),  // 1
            ParquetValue::Decimal128(1, 10), // 0.0000000001
        ],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(decimal_values.clone()).unwrap();
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

    assert_eq!(read_rows.len(), decimal_values.len());

    for (expected, actual) in decimal_values.iter().zip(read_rows.iter()) {
        assert_eq!(expected, actual);
    }
}
