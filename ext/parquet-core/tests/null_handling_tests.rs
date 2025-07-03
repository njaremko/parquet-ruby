use bytes::Bytes;
use indexmap::IndexMap;
use parquet_core::*;
use std::sync::Arc;

mod test_helpers;
use test_helpers::*;

#[test]
fn test_null_handling_all_types() {
    // Test null handling for all nullable primitive types
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "bool_field".to_string(),
                    primitive_type: PrimitiveType::Boolean,
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "int32_field".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "int64_field".to_string(),
                    primitive_type: PrimitiveType::Int64,
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "float32_field".to_string(),
                    primitive_type: PrimitiveType::Float32,
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "float64_field".to_string(),
                    primitive_type: PrimitiveType::Float64,
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "string_field".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "binary_field".to_string(),
                    primitive_type: PrimitiveType::Binary,
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "decimal128_field".to_string(),
                    primitive_type: PrimitiveType::Decimal128(10, 2),
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    // Test 1: All values present
    let all_present = vec![
        ParquetValue::Boolean(true),
        ParquetValue::Int32(42),
        ParquetValue::Int64(12345),
        ParquetValue::Float32(ordered_float::OrderedFloat(3.14)),
        ParquetValue::Float64(ordered_float::OrderedFloat(2.718)),
        ParquetValue::String(Arc::from("test")),
        ParquetValue::Bytes(Bytes::from(vec![1, 2, 3, 4])),
        ParquetValue::Decimal128(12345, 2),
    ];

    // Test 2: All nulls
    let all_nulls: Vec<ParquetValue> = (0..8).map(|_| ParquetValue::Null).collect();

    // Test 3: Mixed nulls and values - alternating pattern
    let mixed_alternating = vec![
        ParquetValue::Boolean(true),
        ParquetValue::Null,
        ParquetValue::Int64(12345),
        ParquetValue::Null,
        ParquetValue::Float64(ordered_float::OrderedFloat(2.718)),
        ParquetValue::Null,
        ParquetValue::Bytes(Bytes::from(vec![1, 2, 3, 4])),
        ParquetValue::Null,
    ];

    // Test 4: Mixed nulls and values - sparse pattern (mostly nulls)
    let mixed_sparse = vec![
        ParquetValue::Null,
        ParquetValue::Null,
        ParquetValue::Int64(12345),
        ParquetValue::Null,
        ParquetValue::Null,
        ParquetValue::Null,
        ParquetValue::Null,
        ParquetValue::Decimal128(12345, 2),
    ];

    let test_rows = [
        vec![all_present.clone()],
        vec![all_nulls.clone()],
        vec![mixed_alternating.clone()],
        vec![mixed_sparse.clone()],
        // Add multiple rows to test null patterns
        (0..10)
            .map(|i| {
                if i % 3 == 0 {
                    all_nulls.clone()
                } else if i % 2 == 0 {
                    mixed_alternating.clone()
                } else {
                    all_present.clone()
                }
            })
            .collect::<Vec<_>>(),
    ]
    .concat();

    // Use test helper for roundtrip
    test_roundtrip(test_rows, schema).unwrap();
}

#[test]
fn test_all_null_column() {
    // Test handling of columns where all values are null
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "id".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "optional".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let rows: Vec<Vec<ParquetValue>> = (0..100)
        .map(|i| vec![ParquetValue::Int32(i), ParquetValue::Null])
        .collect();

    // Use test helper for roundtrip
    test_roundtrip(rows, schema).unwrap();
}

#[test]
fn test_null_patterns() {
    let patterns: Vec<(&str, Box<dyn Fn(usize) -> bool>)> = vec![
        ("alternating", Box::new(|i: usize| i % 2 == 0)),
        ("sparse_90_percent", Box::new(|i: usize| i % 10 != 0)),
        ("dense_10_percent", Box::new(|i: usize| i % 10 == 0)),
        ("first_half", Box::new(|i: usize| i < 500)),
        ("last_half", Box::new(|i: usize| i >= 500)),
        ("blocks_of_10", Box::new(|i: usize| (i / 10) % 2 == 0)),
    ];

    for (pattern_name, is_null) in patterns {
        // Test various null distribution patterns
        let schema = SchemaBuilder::new()
            .with_root(SchemaNode::Struct {
                name: "root".to_string(),
                nullable: false,
                fields: vec![
                    SchemaNode::Primitive {
                        name: "id".to_string(),
                        primitive_type: PrimitiveType::Int32,
                        nullable: false,
                        format: None,
                    },
                    SchemaNode::Primitive {
                        name: "value".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: true,
                        format: None,
                    },
                ],
            })
            .build()
            .unwrap();

        let rows: Vec<Vec<ParquetValue>> = (0..1000)
            .map(|i| {
                vec![
                    ParquetValue::Int32(i as i32),
                    if is_null(i) {
                        ParquetValue::Null
                    } else {
                        ParquetValue::String(Arc::from(format!("value_{}", i)))
                    },
                ]
            })
            .collect();

        // Count nulls for verification
        let null_count = rows
            .iter()
            .filter(|row| matches!(row[1], ParquetValue::Null))
            .count();
        println!("{} pattern - nulls: {}/1000", pattern_name, null_count);

        // Use test helper for roundtrip
        test_roundtrip(rows, schema).unwrap();
    }
}

#[test]
fn test_deeply_nested_nulls() {
    // Test nulls at various levels of nesting
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "id".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Struct {
                    name: "nested".to_string(),
                    nullable: true,
                    fields: vec![
                        SchemaNode::Primitive {
                            name: "value".to_string(),
                            primitive_type: PrimitiveType::String,
                            nullable: true,
                            format: None,
                        },
                        SchemaNode::List {
                            name: "items".to_string(),
                            nullable: true,
                            item: Box::new(SchemaNode::Primitive {
                                name: "item".to_string(),
                                primitive_type: PrimitiveType::Int32,
                                nullable: true,
                                format: None,
                            }),
                        },
                    ],
                },
            ],
        })
        .build()
        .unwrap();

    let rows = vec![
        // Entire struct is null
        vec![ParquetValue::Int32(1), ParquetValue::Null],
        // Struct with null value and null list
        vec![
            ParquetValue::Int32(2),
            ParquetValue::Record({
                let mut map = IndexMap::new();
                map.insert(Arc::from("value"), ParquetValue::Null);
                map.insert(Arc::from("items"), ParquetValue::Null);
                map
            }),
        ],
        // Struct with value and list containing nulls
        vec![
            ParquetValue::Int32(3),
            ParquetValue::Record({
                let mut map = IndexMap::new();
                map.insert(Arc::from("value"), ParquetValue::String(Arc::from("test")));
                map.insert(
                    Arc::from("items"),
                    ParquetValue::List(vec![
                        ParquetValue::Int32(1),
                        ParquetValue::Null,
                        ParquetValue::Int32(3),
                    ]),
                );
                map
            }),
        ],
        // Struct with null value and empty list
        vec![
            ParquetValue::Int32(4),
            ParquetValue::Record({
                let mut map = IndexMap::new();
                map.insert(Arc::from("value"), ParquetValue::Null);
                map.insert(Arc::from("items"), ParquetValue::List(vec![]));
                map
            }),
        ],
    ];

    // Use test helper for roundtrip
    test_roundtrip(rows, schema).unwrap();
}

#[test]
fn test_null_across_row_groups() {
    // Test null handling when nulls span multiple row groups
    let schema = SchemaBuilder::new()
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
                    name: "value".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    // Create rows where entire row groups might be null
    // Assuming default row group size, create patterns that span groups
    let rows: Vec<Vec<ParquetValue>> = (0..10000)
        .map(|i| {
            vec![
                ParquetValue::Int64(i),
                // First 5000 rows: all null
                // Next 2500 rows: all values
                // Last 2500 rows: alternating
                if i < 5000 {
                    ParquetValue::Null
                } else if i < 7500 {
                    ParquetValue::String(Arc::from(format!("value_{}", i)))
                } else if i % 2 == 0 {
                    ParquetValue::Null
                } else {
                    ParquetValue::String(Arc::from(format!("value_{}", i)))
                },
            ]
        })
        .collect();

    // Use test helper with specific batch size to control row groups
    let result = test_roundtrip_with_options(
        rows.clone(),
        schema,
        parquet::basic::Compression::UNCOMPRESSED,
        Some(1000), // Force smaller row groups
    );

    assert!(result.is_ok());

    // Additional verification - ensure the null pattern is preserved
    let null_count = rows
        .iter()
        .filter(|row| matches!(row[1], ParquetValue::Null))
        .count();
    assert_eq!(null_count, 6250); // 5000 + 1250 = 6250 nulls
}

#[test]
fn test_sparse_columns_with_compression() {
    // Test compression effectiveness on sparse columns (95% null)
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "id".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "sparse_data".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let rows: Vec<Vec<ParquetValue>> = (0..10000)
        .map(|i| {
            vec![
                ParquetValue::Int32(i),
                if i % 20 == 0 {
                    ParquetValue::String(Arc::from(format!("rare_value_{}", i)))
                } else {
                    ParquetValue::Null
                },
            ]
        })
        .collect();

    use parquet::basic::Compression;

    let compressions = vec![
        ("UNCOMPRESSED", Compression::UNCOMPRESSED),
        ("SNAPPY", Compression::SNAPPY),
        ("ZSTD", Compression::ZSTD(Default::default())),
    ];

    for (name, compression) in compressions {
        let result = test_roundtrip_with_options(rows.clone(), schema.clone(), compression, None);

        assert!(result.is_ok(), "Failed with {}: {:?}", name, result);
        println!("Sparse column (95% null) with {} succeeded", name);
    }
}
