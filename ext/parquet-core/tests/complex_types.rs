use bytes::Bytes;
use indexmap::IndexMap;
use num::BigInt;
use ordered_float::OrderedFloat;
use parquet_core::*;
use std::sync::Arc;

#[test]
fn test_write_and_read_lists() {
    // Create schema with list fields
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
                SchemaNode::List {
                    name: "tags".to_string(),
                    nullable: true,
                    item: Box::new(SchemaNode::Primitive {
                        name: "tag".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: false,
                        format: None,
                    }),
                },
                SchemaNode::List {
                    name: "scores".to_string(),
                    nullable: false,
                    item: Box::new(SchemaNode::Primitive {
                        name: "score".to_string(),
                        primitive_type: PrimitiveType::Float64,
                        nullable: true,
                        format: None,
                    }),
                },
            ],
        })
        .build()
        .unwrap();

    // Create test data with various list scenarios
    let rows = vec![
        // Row with populated lists
        vec![
            ParquetValue::Int32(1),
            ParquetValue::List(vec![
                ParquetValue::String(Arc::from("rust")),
                ParquetValue::String(Arc::from("parquet")),
                ParquetValue::String(Arc::from("ffi")),
            ]),
            ParquetValue::List(vec![
                ParquetValue::Float64(OrderedFloat(95.5)),
                ParquetValue::Float64(OrderedFloat(87.3)),
                ParquetValue::Null,
            ]),
        ],
        // Row with empty lists
        vec![
            ParquetValue::Int32(2),
            ParquetValue::List(vec![]),
            ParquetValue::List(vec![]),
        ],
        // Row with null list
        vec![
            ParquetValue::Int32(3),
            ParquetValue::Null,
            ParquetValue::List(vec![ParquetValue::Float64(OrderedFloat(100.0))]),
        ],
        // Row with single-element lists
        vec![
            ParquetValue::Int32(4),
            ParquetValue::List(vec![ParquetValue::String(Arc::from("single"))]),
            ParquetValue::List(vec![ParquetValue::Float64(OrderedFloat(42.0))]),
        ],
    ];

    // Write to buffer
    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema.clone()).unwrap();
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
fn test_write_and_read_maps() {
    // Create schema with map field
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "user_id".to_string(),
                    primitive_type: PrimitiveType::Int64,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Map {
                    name: "attributes".to_string(),
                    nullable: true,
                    key: Box::new(SchemaNode::Primitive {
                        name: "key".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: false,
                        format: None,
                    }),
                    value: Box::new(SchemaNode::Primitive {
                        name: "value".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: true,
                        format: None,
                    }),
                },
            ],
        })
        .build()
        .unwrap();

    let rows = vec![
        // Row with populated map
        vec![
            ParquetValue::Int64(1001),
            ParquetValue::Map(vec![
                (
                    ParquetValue::String(Arc::from("name")),
                    ParquetValue::String(Arc::from("Alice")),
                ),
                (
                    ParquetValue::String(Arc::from("role")),
                    ParquetValue::String(Arc::from("admin")),
                ),
                (
                    ParquetValue::String(Arc::from("department")),
                    ParquetValue::Null,
                ),
            ]),
        ],
        // Row with empty map
        vec![ParquetValue::Int64(1002), ParquetValue::Map(vec![])],
        // Row with null map
        vec![ParquetValue::Int64(1003), ParquetValue::Null],
        // Row with single-entry map
        vec![
            ParquetValue::Int64(1004),
            ParquetValue::Map(vec![(
                ParquetValue::String(Arc::from("status")),
                ParquetValue::String(Arc::from("active")),
            )]),
        ],
    ];

    // Write to buffer
    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema.clone()).unwrap();
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
fn test_write_and_read_nested_structs() {
    // Create schema with nested structs
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
                    name: "address".to_string(),
                    nullable: true,
                    fields: vec![
                        SchemaNode::Primitive {
                            name: "street".to_string(),
                            primitive_type: PrimitiveType::String,
                            nullable: true, // Changed to nullable
                            format: None,
                        },
                        SchemaNode::Primitive {
                            name: "city".to_string(),
                            primitive_type: PrimitiveType::String,
                            nullable: true, // Changed to nullable
                            format: None,
                        },
                        SchemaNode::Struct {
                            name: "coordinates".to_string(),
                            nullable: true,
                            fields: vec![
                                SchemaNode::Primitive {
                                    name: "latitude".to_string(),
                                    primitive_type: PrimitiveType::Float64,
                                    nullable: true, // Changed to nullable
                                    format: None,
                                },
                                SchemaNode::Primitive {
                                    name: "longitude".to_string(),
                                    primitive_type: PrimitiveType::Float64,
                                    nullable: true, // Changed to nullable
                                    format: None,
                                },
                            ],
                        },
                    ],
                },
            ],
        })
        .build()
        .unwrap();

    let rows = vec![
        // Row with fully populated nested struct
        vec![
            ParquetValue::Int32(1),
            ParquetValue::Record({
                let mut map = IndexMap::new();
                map.insert(
                    Arc::from("street"),
                    ParquetValue::String(Arc::from("123 Main St")),
                );
                map.insert(
                    Arc::from("city"),
                    ParquetValue::String(Arc::from("Seattle")),
                );
                map.insert(
                    Arc::from("coordinates"),
                    ParquetValue::Record({
                        let mut coords = IndexMap::new();
                        coords.insert(
                            Arc::from("latitude"),
                            ParquetValue::Float64(OrderedFloat(47.6062)),
                        );
                        coords.insert(
                            Arc::from("longitude"),
                            ParquetValue::Float64(OrderedFloat(-122.3321)),
                        );
                        coords
                    }),
                );
                map
            }),
        ],
        // Row with null nested struct
        vec![ParquetValue::Int32(2), ParquetValue::Null],
        // Row with struct containing all required fields
        vec![
            ParquetValue::Int32(3),
            ParquetValue::Record({
                let mut map = IndexMap::new();
                map.insert(
                    Arc::from("street"),
                    ParquetValue::String(Arc::from("456 Oak Ave")),
                );
                map.insert(
                    Arc::from("city"),
                    ParquetValue::String(Arc::from("Portland")),
                );
                // Now we can use null since fields are nullable
                map.insert(Arc::from("coordinates"), ParquetValue::Null);
                map
            }),
        ],
    ];

    // Write to buffer
    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema.clone()).unwrap();
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

    // Check first row (fully populated)
    assert_eq!(rows[0], read_rows[0]);

    // Check second row - when we write a null struct, it reads back as null
    assert_eq!(read_rows[1][0], ParquetValue::Int32(2)); // ID should match
    match &read_rows[1][1] {
        ParquetValue::Null => {
            // Correct - null structs read back as null
        }
        _ => panic!("Expected second row address to be Null"),
    }

    // Check third row - same issue with null coordinates
    match &read_rows[2][1] {
        ParquetValue::Record(record) => {
            // Check the string fields match
            assert_eq!(
                record.get("street"),
                Some(&ParquetValue::String(Arc::from("456 Oak Ave")))
            );
            assert_eq!(
                record.get("city"),
                Some(&ParquetValue::String(Arc::from("Portland")))
            );
            // Verify coordinates is null (not a struct with null fields)
            match record.get("coordinates") {
                Some(ParquetValue::Null) => {
                    // Correct - null nested struct reads back as null
                }
                _ => panic!("Expected coordinates to be Null"),
            }
        }
        _ => panic!("Expected third row address to be a Record"),
    }
}

#[test]
fn test_complex_list_of_structs() {
    // Create schema with list of structs
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "order_id".to_string(),
                    primitive_type: PrimitiveType::Int64,
                    nullable: false,
                    format: None,
                },
                SchemaNode::List {
                    name: "items".to_string(),
                    nullable: false,
                    item: Box::new(SchemaNode::Struct {
                        name: "item".to_string(),
                        nullable: false,
                        fields: vec![
                            SchemaNode::Primitive {
                                name: "product_id".to_string(),
                                primitive_type: PrimitiveType::Int32,
                                nullable: false,
                                format: None,
                            },
                            SchemaNode::Primitive {
                                name: "quantity".to_string(),
                                primitive_type: PrimitiveType::Int32,
                                nullable: false,
                                format: None,
                            },
                            SchemaNode::Primitive {
                                name: "price".to_string(),
                                primitive_type: PrimitiveType::Decimal128(10, 2),
                                nullable: false,
                                format: None,
                            },
                        ],
                    }),
                },
            ],
        })
        .build()
        .unwrap();

    let rows = vec![
        // Order with multiple items
        vec![
            ParquetValue::Int64(100001),
            ParquetValue::List(vec![
                ParquetValue::Record({
                    let mut item = IndexMap::new();
                    item.insert(Arc::from("product_id"), ParquetValue::Int32(1));
                    item.insert(Arc::from("quantity"), ParquetValue::Int32(2));
                    item.insert(Arc::from("price"), ParquetValue::Decimal128(1999, 2));
                    item
                }),
                ParquetValue::Record({
                    let mut item = IndexMap::new();
                    item.insert(Arc::from("product_id"), ParquetValue::Int32(2));
                    item.insert(Arc::from("quantity"), ParquetValue::Int32(1));
                    item.insert(Arc::from("price"), ParquetValue::Decimal128(4995, 2));
                    item
                }),
            ]),
        ],
        // Order with single item
        vec![
            ParquetValue::Int64(100002),
            ParquetValue::List(vec![ParquetValue::Record({
                let mut item = IndexMap::new();
                item.insert(Arc::from("product_id"), ParquetValue::Int32(3));
                item.insert(Arc::from("quantity"), ParquetValue::Int32(5));
                item.insert(Arc::from("price"), ParquetValue::Decimal128(999, 2));
                item
            })]),
        ],
        // Order with no items
        vec![ParquetValue::Int64(100003), ParquetValue::List(vec![])],
    ];

    // Write to buffer
    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema.clone()).unwrap();
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
fn test_map_with_complex_values() {
    // Create schema with map containing struct values
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "session_id".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Map {
                    name: "metrics".to_string(),
                    nullable: false,
                    key: Box::new(SchemaNode::Primitive {
                        name: "metric_name".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: false,
                        format: None,
                    }),
                    value: Box::new(SchemaNode::Struct {
                        name: "metric_data".to_string(),
                        nullable: false,
                        fields: vec![
                            SchemaNode::Primitive {
                                name: "value".to_string(),
                                primitive_type: PrimitiveType::Float64,
                                nullable: false,
                                format: None,
                            },
                            SchemaNode::Primitive {
                                name: "unit".to_string(),
                                primitive_type: PrimitiveType::String,
                                nullable: false,
                                format: None,
                            },
                            SchemaNode::Primitive {
                                name: "timestamp".to_string(),
                                primitive_type: PrimitiveType::TimestampMillis(None),
                                nullable: false,
                                format: None,
                            },
                        ],
                    }),
                },
            ],
        })
        .build()
        .unwrap();

    let rows = vec![vec![
        ParquetValue::String(Arc::from("session-123")),
        ParquetValue::Map(vec![
            (
                ParquetValue::String(Arc::from("cpu_usage")),
                ParquetValue::Record({
                    let mut data = IndexMap::new();
                    data.insert(
                        Arc::from("value"),
                        ParquetValue::Float64(OrderedFloat(85.5)),
                    );
                    data.insert(
                        Arc::from("unit"),
                        ParquetValue::String(Arc::from("percent")),
                    );
                    data.insert(
                        Arc::from("timestamp"),
                        ParquetValue::TimestampMillis(1640000000000, None),
                    );
                    data
                }),
            ),
            (
                ParquetValue::String(Arc::from("memory_usage")),
                ParquetValue::Record({
                    let mut data = IndexMap::new();
                    data.insert(
                        Arc::from("value"),
                        ParquetValue::Float64(OrderedFloat(1024.0)),
                    );
                    data.insert(Arc::from("unit"), ParquetValue::String(Arc::from("MB")));
                    data.insert(
                        Arc::from("timestamp"),
                        ParquetValue::TimestampMillis(1640000001000, None),
                    );
                    data
                }),
            ),
        ]),
    ]];

    // Write to buffer
    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema.clone()).unwrap();
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
fn test_deeply_nested_structures() {
    // Create a deeply nested schema: struct -> list -> struct -> list
    // (avoiding map with struct values which is not supported)
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "doc_id".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Struct {
                    name: "content".to_string(),
                    nullable: false,
                    fields: vec![SchemaNode::List {
                        name: "sections".to_string(),
                        nullable: false,
                        item: Box::new(SchemaNode::Struct {
                            name: "section".to_string(),
                            nullable: false,
                            fields: vec![
                                SchemaNode::Primitive {
                                    name: "title".to_string(),
                                    primitive_type: PrimitiveType::String,
                                    nullable: false,
                                    format: None,
                                },
                                SchemaNode::List {
                                    name: "paragraphs".to_string(),
                                    nullable: false,
                                    item: Box::new(SchemaNode::Struct {
                                        name: "paragraph".to_string(),
                                        nullable: false,
                                        fields: vec![
                                            SchemaNode::Primitive {
                                                name: "text".to_string(),
                                                primitive_type: PrimitiveType::String,
                                                nullable: false,
                                                format: None,
                                            },
                                            SchemaNode::Primitive {
                                                name: "score".to_string(),
                                                primitive_type: PrimitiveType::Float32,
                                                nullable: true,
                                                format: None,
                                            },
                                        ],
                                    }),
                                },
                            ],
                        }),
                    }],
                },
            ],
        })
        .build()
        .unwrap();

    let rows = vec![vec![
        ParquetValue::String(Arc::from("doc-001")),
        ParquetValue::Record({
            let mut content = IndexMap::new();
            content.insert(
                Arc::from("sections"),
                ParquetValue::List(vec![
                    ParquetValue::Record({
                        let mut section = IndexMap::new();
                        section.insert(
                            Arc::from("title"),
                            ParquetValue::String(Arc::from("Introduction")),
                        );
                        section.insert(
                            Arc::from("paragraphs"),
                            ParquetValue::List(vec![
                                ParquetValue::Record({
                                    let mut para = IndexMap::new();
                                    para.insert(
                                        Arc::from("text"),
                                        ParquetValue::String(Arc::from(
                                            "Welcome to this document.",
                                        )),
                                    );
                                    para.insert(
                                        Arc::from("score"),
                                        ParquetValue::Float32(OrderedFloat(0.95)),
                                    );
                                    para
                                }),
                                ParquetValue::Record({
                                    let mut para = IndexMap::new();
                                    para.insert(
                                        Arc::from("text"),
                                        ParquetValue::String(Arc::from(
                                            "This is the second paragraph.",
                                        )),
                                    );
                                    para.insert(Arc::from("score"), ParquetValue::Null);
                                    para
                                }),
                            ]),
                        );
                        section
                    }),
                    ParquetValue::Record({
                        let mut section = IndexMap::new();
                        section.insert(
                            Arc::from("title"),
                            ParquetValue::String(Arc::from("Conclusion")),
                        );
                        section.insert(
                            Arc::from("paragraphs"),
                            ParquetValue::List(vec![ParquetValue::Record({
                                let mut para = IndexMap::new();
                                para.insert(
                                    Arc::from("text"),
                                    ParquetValue::String(Arc::from("In summary...")),
                                );
                                para.insert(
                                    Arc::from("score"),
                                    ParquetValue::Float32(OrderedFloat(0.88)),
                                );
                                para
                            })]),
                        );
                        section
                    }),
                ]),
            );
            content
        }),
    ]];

    // Write to buffer
    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema.clone()).unwrap();
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
fn test_decimal256_complex_type() {
    // Test Decimal256 within complex structures
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
                SchemaNode::List {
                    name: "large_values".to_string(),
                    nullable: false,
                    item: Box::new(SchemaNode::Primitive {
                        name: "value".to_string(),
                        primitive_type: PrimitiveType::Decimal256(50, 10),
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
            ParquetValue::Int32(1),
            ParquetValue::List(vec![
                ParquetValue::Decimal256(
                    BigInt::parse_bytes(b"123456789012345678901234567890", 10).unwrap(),
                    10,
                ),
                ParquetValue::Decimal256(
                    BigInt::parse_bytes(b"-987654321098765432109876543210", 10).unwrap(),
                    10,
                ),
                ParquetValue::Null,
            ]),
        ],
        vec![
            ParquetValue::Int32(2),
            ParquetValue::List(vec![ParquetValue::Decimal256(BigInt::from(0), 10)]),
        ],
    ];

    // Write to buffer
    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema.clone()).unwrap();
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
