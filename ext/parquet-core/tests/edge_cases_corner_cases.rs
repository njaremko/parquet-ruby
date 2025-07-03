use bytes::Bytes;
use parquet_core::*;
use std::sync::Arc;

#[test]
fn test_single_row_file() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "value".to_string(),
                primitive_type: PrimitiveType::String,
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap();

    let rows = vec![vec![ParquetValue::String(Arc::from("single"))]];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_rows.len(), 1);
    assert_eq!(read_rows[0], rows[0]);
}

#[test]
fn test_unicode_edge_cases() {
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

    let test_strings = vec![
        // Various Unicode edge cases
        "".to_string(),                          // Empty
        "A".to_string(),                         // ASCII
        "Ω".to_string(),                         // Greek
        "中文".to_string(),                      // Chinese
        "🦀".to_string(),                        // Emoji (4-byte UTF-8)
        "👨‍👩‍👧‍👦".to_string(),                        // Family emoji (ZWJ sequence)
        "\u{0000}".to_string(),                  // Null character
        "\u{FFFD}".to_string(),                  // Replacement character
        "A\u{0301}".to_string(),                 // Combining character (A with accent)
        "\u{200B}invisible\u{200B}".to_string(), // Zero-width space
        "🏴󠁧󠁢󠁳󠁣󠁴󠁿".to_string(),                        // Flag (tag sequence)
        "\u{1F1FA}\u{1F1F8}".to_string(),        // US flag (regional indicators)
    ];

    let rows: Vec<Vec<ParquetValue>> = test_strings
        .into_iter()
        .map(|s| vec![ParquetValue::String(s.into())])
        .collect();

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

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
fn test_decimal_precision_boundaries() {
    // Test decimal values at exact precision boundaries
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
                    name: "dec_38_0".to_string(),
                    primitive_type: PrimitiveType::Decimal128(38, 0),
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

    let rows = vec![
        vec![
            ParquetValue::Decimal128(99999, 2), // 999.99 (max for 5,2)
            ParquetValue::Decimal128(99999999999999999999999999999999999999_i128, 0), // Max 38 digits
            ParquetValue::Decimal128(99999999999999999999999999999999999999_i128, 38), // 0.99999... (38 9s after decimal)
        ],
        vec![
            ParquetValue::Decimal128(-99999, 2), // -999.99 (min for 5,2)
            ParquetValue::Decimal128(-99999999999999999999999999999999999999_i128, 0), // Min 38 digits
            ParquetValue::Decimal128(-99999999999999999999999999999999999999_i128, 38), // -0.99999...
        ],
        vec![
            ParquetValue::Decimal128(0, 2),  // 0.00
            ParquetValue::Decimal128(0, 0),  // 0
            ParquetValue::Decimal128(0, 38), // 0.00000... (38 zeros)
        ],
        vec![
            ParquetValue::Decimal128(1, 2),  // 0.01 (smallest positive for 5,2)
            ParquetValue::Decimal128(1, 0),  // 1
            ParquetValue::Decimal128(1, 38), // 0.00000...01 (37 zeros then 1)
        ],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

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
fn test_map_with_duplicate_keys() {
    // Maps can have duplicate keys in Parquet
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Map {
                name: "map_field".to_string(),
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
            }],
        })
        .build()
        .unwrap();

    let rows = vec![
        // Map with duplicate keys
        vec![ParquetValue::Map(vec![
            (
                ParquetValue::String(Arc::from("key1")),
                ParquetValue::Int32(1),
            ),
            (
                ParquetValue::String(Arc::from("key2")),
                ParquetValue::Int32(2),
            ),
            (
                ParquetValue::String(Arc::from("key1")),
                ParquetValue::Int32(3),
            ), // Duplicate key
            (
                ParquetValue::String(Arc::from("key1")),
                ParquetValue::Int32(4),
            ), // Another duplicate
        ])],
        // Empty map
        vec![ParquetValue::Map(vec![])],
        // Single entry
        vec![ParquetValue::Map(vec![(
            ParquetValue::String(Arc::from("only")),
            ParquetValue::Int32(42),
        )])],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_rows.len(), rows.len());

    // Check that all entries including duplicates are preserved
    match &read_rows[0][0] {
        ParquetValue::Map(entries) => {
            assert_eq!(entries.len(), 4); // All 4 entries including duplicates
        }
        _ => panic!("Expected map"),
    }
}

#[test]
fn test_list_of_empty_lists() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::List {
                name: "nested_lists".to_string(),
                nullable: false,
                item: Box::new(SchemaNode::List {
                    name: "inner_list".to_string(),
                    nullable: false,
                    item: Box::new(SchemaNode::Primitive {
                        name: "value".to_string(),
                        primitive_type: PrimitiveType::Int32,
                        nullable: false,
                        format: None,
                    }),
                }),
            }],
        })
        .build()
        .unwrap();

    let rows = vec![
        // List containing empty lists
        vec![ParquetValue::List(vec![
            ParquetValue::List(vec![]),
            ParquetValue::List(vec![]),
            ParquetValue::List(vec![]),
        ])],
        // List with mix of empty and non-empty
        vec![ParquetValue::List(vec![
            ParquetValue::List(vec![ParquetValue::Int32(1), ParquetValue::Int32(2)]),
            ParquetValue::List(vec![]),
            ParquetValue::List(vec![ParquetValue::Int32(3)]),
        ])],
        // Empty outer list
        vec![ParquetValue::List(vec![])],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

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
