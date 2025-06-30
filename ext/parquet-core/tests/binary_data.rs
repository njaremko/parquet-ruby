use bytes::Bytes;
use indexmap::IndexMap;
use parquet_core::*;
use std::sync::Arc;

#[test]
fn test_binary_data_basic() {
    // Test basic binary data handling
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
                    name: "data".to_string(),
                    primitive_type: PrimitiveType::Binary,
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let test_data = vec![
        // Empty binary data
        vec![
            ParquetValue::Int32(1),
            ParquetValue::Bytes(Bytes::from(vec![])),
        ],
        // Small binary data
        vec![
            ParquetValue::Int32(2),
            ParquetValue::Bytes(Bytes::from(vec![0x00, 0x01, 0x02, 0x03])),
        ],
        // Bytes data with all byte values
        vec![
            ParquetValue::Int32(3),
            ParquetValue::Bytes(Bytes::from((0u8..=255u8).collect::<Vec<u8>>())),
        ],
        // Bytes data with null bytes
        vec![
            ParquetValue::Int32(4),
            ParquetValue::Bytes(Bytes::from(vec![0x00, 0x00, 0x00, 0x00])),
        ],
        // Random binary data
        vec![
            ParquetValue::Int32(5),
            ParquetValue::Bytes(Bytes::from(vec![
                0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
            ])),
        ],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(test_data.clone()).unwrap();
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

    assert_eq!(read_rows.len(), test_data.len());

    // Verify binary data is preserved exactly
    for (expected, actual) in test_data.iter().zip(read_rows.iter()) {
        assert_eq!(expected, actual);
    }
}

#[test]
fn test_large_binary_data() {
    // Test handling of large binary blobs
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "blob".to_string(),
                primitive_type: PrimitiveType::Binary,
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap();

    let sizes = vec![
        1024,        // 1 KB
        10 * 1024,   // 10 KB
        100 * 1024,  // 100 KB
        1024 * 1024, // 1 MB
    ];

    for size in sizes {
        let large_data: Bytes = (0..size).map(|i| (i % 256) as u8).collect();

        let rows = vec![vec![ParquetValue::Bytes(large_data.clone())]];

        let mut buffer = Vec::new();
        {
            let mut writer = Writer::new(&mut buffer, schema.clone()).unwrap();
            writer.write_rows(rows).unwrap();
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

        assert_eq!(read_rows.len(), 1);

        match &read_rows[0][0] {
            ParquetValue::Bytes(data) => {
                assert_eq!(data.len(), size);
                assert_eq!(data, &large_data);
            }
            _ => panic!("Expected binary value"),
        }
    }
}

#[test]
fn test_nullable_binary() {
    // Test nullable binary fields
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "optional_data".to_string(),
                primitive_type: PrimitiveType::Binary,
                nullable: true,
                format: None,
            }],
        })
        .build()
        .unwrap();

    let rows = vec![
        vec![ParquetValue::Bytes(Bytes::from(vec![1, 2, 3]))],
        vec![ParquetValue::Null],
        vec![ParquetValue::Bytes(Bytes::from(vec![]))],
        vec![ParquetValue::Null],
        vec![ParquetValue::Bytes(Bytes::from(vec![255, 254, 253]))],
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

    // Verify nulls and empty binary are handled correctly
    for (expected, actual) in rows.iter().zip(read_rows.iter()) {
        assert_eq!(expected, actual);
    }
}

#[test]
fn test_fixed_size_binary() {
    // Test fixed-size binary data (if supported)
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "uuid".to_string(),
                    primitive_type: PrimitiveType::Binary, // Ideally would be FixedBytes(16)
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "hash".to_string(),
                    primitive_type: PrimitiveType::Binary, // Ideally would be FixedBytes(32)
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let rows = vec![
        vec![
            // 16-byte UUID-like value
            ParquetValue::Bytes(Bytes::from(vec![
                0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc,
                0xde, 0xf0,
            ])),
            // 32-byte hash-like value
            ParquetValue::Bytes(Bytes::from(vec![
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb,
                0xcc, 0xdd, 0xee, 0xff,
            ])),
        ],
        vec![
            // Another UUID
            ParquetValue::Bytes(Bytes::from(vec![
                0xf0, 0xe1, 0xd2, 0xc3, 0xb4, 0xa5, 0x96, 0x87, 0x78, 0x69, 0x5a, 0x4b, 0x3c, 0x2d,
                0x1e, 0x0f,
            ])),
            // Another hash
            ParquetValue::Bytes(Bytes::from(vec![
                0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22,
                0x11, 0x00, 0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44,
                0x33, 0x22, 0x11, 0x00,
            ])),
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
fn test_binary_string_interoperability() {
    // Test that binary data doesn't get confused with strings
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "text".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "binary".to_string(),
                    primitive_type: PrimitiveType::Binary,
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let test_string = "Hello, 世界! 🦀";
    let test_bytes = test_string.as_bytes().to_vec();

    let rows = vec![
        vec![
            ParquetValue::String(Arc::from(test_string)),
            ParquetValue::Bytes(test_bytes.into()),
        ],
        vec![
            ParquetValue::String(Arc::from("Regular ASCII text")),
            ParquetValue::Bytes(Bytes::from(vec![0xff, 0xfe, 0xfd])), // Invalid UTF-8
        ],
        vec![
            ParquetValue::String(Arc::from("")),      // Empty string
            ParquetValue::Bytes(Bytes::from(vec![])), // Empty binary
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

    // Verify string and binary are kept separate
    for (expected, actual) in rows.iter().zip(read_rows.iter()) {
        assert_eq!(expected, actual);
    }
}

#[test]
fn test_binary_in_complex_types() {
    // Test binary data within lists and structs
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::List {
                    name: "binary_list".to_string(),
                    nullable: false,
                    item: Box::new(SchemaNode::Primitive {
                        name: "item".to_string(),
                        primitive_type: PrimitiveType::Binary,
                        nullable: false,
                        format: None,
                    }),
                },
                SchemaNode::Struct {
                    name: "binary_struct".to_string(),
                    nullable: false,
                    fields: vec![
                        SchemaNode::Primitive {
                            name: "data1".to_string(),
                            primitive_type: PrimitiveType::Binary,
                            nullable: false,
                            format: None,
                        },
                        SchemaNode::Primitive {
                            name: "data2".to_string(),
                            primitive_type: PrimitiveType::Binary,
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
            ParquetValue::List(vec![
                ParquetValue::Bytes(Bytes::from(vec![1, 2, 3])),
                ParquetValue::Bytes(Bytes::from(vec![4, 5, 6])),
                ParquetValue::Bytes(Bytes::from(vec![7, 8, 9])),
            ]),
            ParquetValue::Record({
                let mut map = IndexMap::new();
                map.insert(
                    Arc::from("data1"),
                    ParquetValue::Bytes(Bytes::from(vec![0xAA, 0xBB])),
                );
                map.insert(
                    Arc::from("data2"),
                    ParquetValue::Bytes(Bytes::from(vec![0xCC, 0xDD])),
                );
                map
            }),
        ],
        vec![
            ParquetValue::List(vec![ParquetValue::Bytes(Bytes::from(vec![]))]),
            ParquetValue::Record({
                let mut map = IndexMap::new();
                map.insert(
                    Arc::from("data1"),
                    ParquetValue::Bytes(Bytes::from(vec![0xFF])),
                );
                map.insert(Arc::from("data2"), ParquetValue::Null);
                map
            }),
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
