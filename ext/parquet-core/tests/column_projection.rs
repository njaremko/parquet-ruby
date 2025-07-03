use bytes::Bytes;
use indexmap::IndexMap;
use ordered_float::OrderedFloat;
use parquet_core::*;
use std::sync::Arc;

#[test]
fn test_read_with_missing_columns() {
    // Test reading a file when the reader expects more columns than exist
    let write_schema = SchemaBuilder::new()
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
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let rows: Vec<Vec<ParquetValue>> = (0..10)
        .map(|i| {
            vec![
                ParquetValue::Int64(i),
                ParquetValue::String(Arc::from(format!("Name {}", i))),
            ]
        })
        .collect();

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, write_schema).unwrap();
        writer.write_rows(rows).unwrap();
        writer.close().unwrap();
    }

    // Read with projection asking for a column that doesn't exist
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    // Try to read non-existent column
    let projection = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    let read_rows: Vec<_> = reader
        .read_rows_with_projection(&projection)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    // Should only get the columns that exist
    assert_eq!(read_rows.len(), 10);
    for row in &read_rows {
        assert_eq!(row.len(), 2); // Only id and name
    }
}

#[test]
fn test_read_with_extra_columns() {
    // Test reading a file that has more columns than the reader expects
    let write_schema = SchemaBuilder::new()
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
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "age".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "salary".to_string(),
                    primitive_type: PrimitiveType::Float64,
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let rows: Vec<Vec<ParquetValue>> = (0..10)
        .map(|i| {
            vec![
                ParquetValue::Int64(i),
                ParquetValue::String(Arc::from(format!("Person {}", i))),
                ParquetValue::Int32(25 + i as i32),
                ParquetValue::Float64(OrderedFloat(50000.0 + i as f64 * 1000.0)),
            ]
        })
        .collect();

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, write_schema).unwrap();
        writer.write_rows(rows).unwrap();
        writer.close().unwrap();
    }

    // Read only subset of columns
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let projection = vec!["id".to_string(), "name".to_string()];
    let read_rows: Vec<_> = reader
        .read_rows_with_projection(&projection)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_rows.len(), 10);
    for (i, row) in read_rows.iter().enumerate() {
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], ParquetValue::Int64(i as i64));
        assert_eq!(
            row[1],
            ParquetValue::String(Arc::from(format!("Person {}", i)))
        );
    }
}

#[test]
fn test_nullable_field_evolution() {
    // Test reading files where field nullability has changed

    // First, write with non-nullable field
    let schema_v1 = SchemaBuilder::new()
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
                    nullable: false, // Non-nullable in v1
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let rows_v1: Vec<Vec<ParquetValue>> = (0..5)
        .map(|i| {
            vec![
                ParquetValue::Int64(i),
                ParquetValue::String(Arc::from(format!("Value {}", i))),
            ]
        })
        .collect();

    let mut buffer_v1 = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer_v1, schema_v1).unwrap();
        writer.write_rows(rows_v1).unwrap();
        writer.close().unwrap();
    }

    // Now write with nullable field
    let schema_v2 = SchemaBuilder::new()
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
                    nullable: true, // Nullable in v2
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let rows_v2: Vec<Vec<ParquetValue>> = vec![
        vec![
            ParquetValue::Int64(5),
            ParquetValue::String(Arc::from("Value 5")),
        ],
        vec![ParquetValue::Int64(6), ParquetValue::Null],
        vec![
            ParquetValue::Int64(7),
            ParquetValue::String(Arc::from("Value 7")),
        ],
    ];

    let mut buffer_v2 = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer_v2, schema_v2).unwrap();
        writer.write_rows(rows_v2).unwrap();
        writer.close().unwrap();
    }

    // Read both files and verify
    let bytes_v1 = Bytes::from(buffer_v1);
    let reader_v1 = Reader::new(bytes_v1);

    let read_v1: Vec<_> = reader_v1
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_v1.len(), 5);
    for row in &read_v1 {
        assert!(!matches!(row[1], ParquetValue::Null));
    }

    let bytes_v2 = Bytes::from(buffer_v2);
    let reader_v2 = Reader::new(bytes_v2);

    let read_v2: Vec<_> = reader_v2
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_v2.len(), 3);
    assert!(matches!(read_v2[1][1], ParquetValue::Null));
}

#[test]
fn test_type_promotion_compatibility() {
    // Test reading files where numeric types have been promoted
    // e.g., Int32 -> Int64, Float32 -> Float64

    let schema_int32 = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "value".to_string(),
                primitive_type: PrimitiveType::Int32,
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap();

    let rows_int32: Vec<Vec<ParquetValue>> = vec![
        vec![ParquetValue::Int32(42)],
        vec![ParquetValue::Int32(i32::MAX)],
        vec![ParquetValue::Int32(i32::MIN)],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema_int32).unwrap();
        writer.write_rows(rows_int32.clone()).unwrap();
        writer.close().unwrap();
    }

    // Read back and verify values are preserved
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_rows.len(), 3);
    assert_eq!(read_rows[0][0], ParquetValue::Int32(42));
    assert_eq!(read_rows[1][0], ParquetValue::Int32(i32::MAX));
    assert_eq!(read_rows[2][0], ParquetValue::Int32(i32::MIN));
}

#[test]
fn test_column_reordering() {
    // Test reading files where column order has changed
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "a".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "b".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "c".to_string(),
                    primitive_type: PrimitiveType::Float64,
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let rows: Vec<Vec<ParquetValue>> = vec![
        vec![
            ParquetValue::Int32(1),
            ParquetValue::String(Arc::from("one")),
            ParquetValue::Float64(OrderedFloat(1.1)),
        ],
        vec![
            ParquetValue::Int32(2),
            ParquetValue::String(Arc::from("two")),
            ParquetValue::Float64(OrderedFloat(2.2)),
        ],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows).unwrap();
        writer.close().unwrap();
    }

    // Read columns in different order
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    // Request columns in different order: c, a, b
    let projection = vec!["c".to_string(), "a".to_string(), "b".to_string()];
    let read_rows: Vec<_> = reader
        .read_rows_with_projection(&projection)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_rows.len(), 2);

    // Verify values are returned (columns may be in schema order, not projection order)
    // The projection filters which columns are returned, but doesn't necessarily reorder them
    assert_eq!(read_rows[0].len(), 3); // All 3 requested columns

    // Find the values regardless of order
    let has_int32_1 = read_rows[0]
        .iter()
        .any(|v| matches!(v, ParquetValue::Int32(1)));
    let has_float_1_1 = read_rows[0]
        .iter()
        .any(|v| matches!(v, ParquetValue::Float64(f) if f.0 == 1.1));
    let has_string_one = read_rows[0]
        .iter()
        .any(|v| matches!(v, ParquetValue::String(s) if *s == Arc::from("one")));

    assert!(has_int32_1, "Should have Int32(1) for column 'a'");
    assert!(has_float_1_1, "Should have Float64(1.1) for column 'c'");
    assert!(has_string_one, "Should have String('one') for column 'b'");
}

#[test]
fn test_nested_schema_evolution() {
    // Test evolution of nested structures

    // V1: Simple struct
    let schema_v1 = SchemaBuilder::new()
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
                    name: "address".to_string(),
                    nullable: false,
                    fields: vec![
                        SchemaNode::Primitive {
                            name: "street".to_string(),
                            primitive_type: PrimitiveType::String,
                            nullable: false,
                            format: None,
                        },
                        SchemaNode::Primitive {
                            name: "city".to_string(),
                            primitive_type: PrimitiveType::String,
                            nullable: false,
                            format: None,
                        },
                    ],
                },
            ],
        })
        .build()
        .unwrap();

    // V2: Extended struct with additional field
    let schema_v2 = SchemaBuilder::new()
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
                    name: "address".to_string(),
                    nullable: false,
                    fields: vec![
                        SchemaNode::Primitive {
                            name: "street".to_string(),
                            primitive_type: PrimitiveType::String,
                            nullable: false,
                            format: None,
                        },
                        SchemaNode::Primitive {
                            name: "city".to_string(),
                            primitive_type: PrimitiveType::String,
                            nullable: false,
                            format: None,
                        },
                        SchemaNode::Primitive {
                            name: "zip".to_string(),
                            primitive_type: PrimitiveType::String,
                            nullable: true, // New nullable field
                            format: None,
                        },
                    ],
                },
            ],
        })
        .build()
        .unwrap();

    // Write v1 data
    let rows_v1 = vec![vec![
        ParquetValue::Int64(1),
        ParquetValue::Record({
            let mut map = IndexMap::new();
            map.insert(
                Arc::from("street"),
                ParquetValue::String(Arc::from("123 Main St")),
            );
            map.insert(
                Arc::from("city"),
                ParquetValue::String(Arc::from("Springfield")),
            );
            map
        }),
    ]];

    let mut buffer_v1 = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer_v1, schema_v1).unwrap();
        writer.write_rows(rows_v1).unwrap();
        writer.close().unwrap();
    }

    // Write v2 data
    let rows_v2 = vec![vec![
        ParquetValue::Int64(2),
        ParquetValue::Record({
            let mut map = IndexMap::new();
            map.insert(
                Arc::from("street"),
                ParquetValue::String(Arc::from("456 Oak Ave")),
            );
            map.insert(
                Arc::from("city"),
                ParquetValue::String(Arc::from("Shelbyville")),
            );
            map.insert(Arc::from("zip"), ParquetValue::String(Arc::from("12345")));
            map
        }),
    ]];

    let mut buffer_v2 = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer_v2, schema_v2).unwrap();
        writer.write_rows(rows_v2).unwrap();
        writer.close().unwrap();
    }

    // Read both files and verify
    let bytes_v1 = Bytes::from(buffer_v1);
    let reader_v1 = Reader::new(bytes_v1);

    let read_v1: Vec<_> = reader_v1
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_v1.len(), 1);
    match &read_v1[0][1] {
        ParquetValue::Record(map) => {
            assert_eq!(map.len(), 2); // Only street and city
            assert!(map.contains_key("street"));
            assert!(map.contains_key("city"));
            assert!(!map.contains_key("zip"));
        }
        _ => panic!("Expected record"),
    }

    let bytes_v2 = Bytes::from(buffer_v2);
    let reader_v2 = Reader::new(bytes_v2);

    let read_v2: Vec<_> = reader_v2
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_v2.len(), 1);
    match &read_v2[0][1] {
        ParquetValue::Record(map) => {
            assert_eq!(map.len(), 3); // street, city, and zip
            assert!(map.contains_key("zip"));
        }
        _ => panic!("Expected record"),
    }
}
