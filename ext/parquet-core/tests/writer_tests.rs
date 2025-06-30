use bytes::Bytes;
use ordered_float::OrderedFloat;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet_core::*;
use std::sync::Arc;

mod test_helpers;
use test_helpers::*;

// =============================================================================
// Basic Writer Functionality Tests
// =============================================================================

#[test]
fn test_writer_basic_functionality() {
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
                    name: "name".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let rows = vec![
        vec![ParquetValue::Int32(1), ParquetValue::String("Alice".into())],
        vec![
            ParquetValue::Int32(2),
            ParquetValue::Null, // nullable field
        ],
    ];

    test_roundtrip(rows, schema).unwrap();
}

// =============================================================================
// Batch Size Configuration Tests
// =============================================================================

#[test]
fn test_writer_fixed_batch_sizes() {
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
                    name: "data".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    // Test different batch sizes
    let batch_sizes = vec![10, 100, 1000, 5000];

    for batch_size in batch_sizes {
        // Generate test data
        let rows: Vec<Vec<ParquetValue>> = (0..10000)
            .map(|i| {
                vec![
                    ParquetValue::Int64(i),
                    ParquetValue::String(Arc::from(format!("Row {}", i))),
                ]
            })
            .collect();

        // Use test_roundtrip_with_options for batch size testing
        let result = test_roundtrip_with_options(
            rows,
            schema.clone(),
            Compression::UNCOMPRESSED,
            Some(batch_size),
        );

        assert!(
            result.is_ok(),
            "Batch size {} failed: {:?}",
            batch_size,
            result
        );
    }
}

#[test]
fn test_writer_adaptive_batch_sizing() {
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
                    name: "variable_string".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let mut buffer = Vec::new();
    {
        // Don't set a fixed batch size - let it adapt
        let mut writer = WriterBuilder::new()
            .with_sample_size(50)
            .build(&mut buffer, schema)
            .unwrap();

        // Write rows with varying sizes
        for i in 0..1000 {
            let string_size = if i % 100 == 0 {
                10000 // Large string every 100 rows
            } else {
                100 // Normal string
            };

            let row = vec![
                ParquetValue::Int32(i),
                ParquetValue::String(Arc::from("x".repeat(string_size))),
            ];

            writer.write_row(row).unwrap();
        }

        writer.close().unwrap();
    }

    // Verify all data was written
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_rows.len(), 1000);

    // Verify variable string sizes
    for (i, row) in read_rows.iter().enumerate() {
        match &row[1] {
            ParquetValue::String(s) => {
                let expected_len = if i % 100 == 0 { 10000 } else { 100 };
                assert_eq!(s.len(), expected_len, "Wrong string length at row {}", i);
            }
            _ => panic!("Expected string value"),
        }
    }
}

// =============================================================================
// Memory Management Tests
// =============================================================================

#[test]
fn test_memory_threshold_configuration() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "large_string".to_string(),
                primitive_type: PrimitiveType::String,
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap();

    // Test with different memory thresholds
    let thresholds = vec![
        1024 * 1024,      // 1MB
        10 * 1024 * 1024, // 10MB
        50 * 1024 * 1024, // 50MB
    ];

    for threshold in thresholds {
        let mut buffer = Vec::new();
        {
            let mut writer = WriterBuilder::new()
                .with_memory_threshold(threshold)
                .build(&mut buffer, schema.clone())
                .unwrap();

            // Write large strings that will trigger memory-based flushing
            let large_string: Arc<str> = Arc::from("x".repeat(1024)); // 1KB string
            let rows: Vec<Vec<ParquetValue>> = (0..5000)
                .map(|_| vec![ParquetValue::String(large_string.clone())])
                .collect();

            writer.write_rows(rows).unwrap();
            writer.close().unwrap();
        }

        // Verify data was written correctly
        let bytes = Bytes::from(buffer);
        let reader = Reader::new(bytes);
        let read_count = reader.read_rows().unwrap().count();
        assert_eq!(read_count, 5000);
    }
}

#[test]
fn test_writer_memory_flushing_with_binary() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "data".to_string(),
                primitive_type: PrimitiveType::Binary,
                nullable: true,
                format: None,
            }],
        })
        .build()
        .unwrap();

    // Generate test data
    let rows: Vec<Vec<ParquetValue>> = (0..100)
        .map(|i| {
            let size = if i % 10 == 0 { 500 } else { 50 };
            vec![ParquetValue::Bytes(Bytes::from(vec![i as u8; size]))]
        })
        .collect();

    // Use a custom writer with memory threshold
    let mut buffer = Vec::new();
    {
        let mut writer = WriterBuilder::new()
            .with_memory_threshold(1024) // 1KB threshold
            .with_sample_size(5)
            .build(&mut buffer, schema.clone())
            .unwrap();

        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

    // Verify data was written correctly
    let reader = Reader::new(Bytes::from(buffer));
    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_rows, rows);
}

// =============================================================================
// Advanced Configuration Tests
// =============================================================================

#[test]
fn test_writer_properties_direct() {
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

    // Test custom writer properties
    let props = WriterProperties::builder()
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(3).unwrap(),
        ))
        .set_data_page_size_limit(1024) // Small page size
        .set_dictionary_enabled(true)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .build();

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new_with_properties(&mut buffer, schema, props).unwrap();

        // Write data with repeated values to test dictionary encoding
        let rows: Vec<Vec<ParquetValue>> = (0..1000)
            .map(|i| {
                vec![ParquetValue::String(Arc::from(format!(
                    "Category_{}",
                    i % 10
                )))]
            })
            .collect();

        writer.write_rows(rows).unwrap();
        writer.close().unwrap();
    }

    // Read back and verify
    let bytes = Bytes::from(buffer);
    let mut reader = Reader::new(bytes);

    // Check metadata
    let metadata = reader.metadata().unwrap();
    assert!(metadata.num_rows() == 1000);

    // Verify data integrity
    let read_count = reader.read_rows().unwrap().count();
    assert_eq!(read_count, 1000);
}

#[test]
fn test_writer_version_compatibility() {
    let schema = SchemaBuilder::new()
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

    let rows: Vec<Vec<ParquetValue>> = (0..100).map(|i| vec![ParquetValue::Int32(i)]).collect();

    // Test different writer versions
    let versions = vec![
        parquet::file::properties::WriterVersion::PARQUET_1_0,
        parquet::file::properties::WriterVersion::PARQUET_2_0,
    ];

    for version in versions {
        let mut buffer = Vec::new();
        {
            let props = WriterProperties::builder()
                .set_writer_version(version)
                .build();

            let mut writer =
                Writer::new_with_properties(&mut buffer, schema.clone(), props).unwrap();
            writer.write_rows(rows.clone()).unwrap();
            writer.close().unwrap();
        }

        // Verify we can read both versions
        let bytes = Bytes::from(buffer);
        let reader = Reader::new(bytes);

        let read_rows: Vec<_> = reader
            .read_rows()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(read_rows.len(), 100);
    }
}

// =============================================================================
// Large Data Handling Tests
// =============================================================================

#[test]
fn test_large_string_handling() {
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
                    name: "content".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    // Create strings of various sizes
    let small = "a".repeat(100);
    let medium = "b".repeat(10_000);
    let large = "c".repeat(100_000);

    // Generate test data
    let rows: Vec<Vec<ParquetValue>> = (0..30)
        .map(|i| {
            let content = match i % 3 {
                0 => ParquetValue::String(small.clone().into()),
                1 => ParquetValue::String(medium.clone().into()),
                2 => ParquetValue::String(large.clone().into()),
                _ => unreachable!(),
            };

            vec![ParquetValue::Int32(i), content]
        })
        .collect();

    // Use custom writer with memory threshold
    let mut buffer = Vec::new();
    {
        let mut writer = WriterBuilder::new()
            .with_memory_threshold(1024 * 1024) // 1MB
            .build(&mut buffer, schema.clone())
            .unwrap();

        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

    // Verify all data was written
    let reader = Reader::new(Bytes::from(buffer));
    let read_rows: Vec<_> = reader.read_rows().unwrap().collect::<Result<_>>().unwrap();
    assert_eq!(read_rows, rows);
}

#[test]
fn test_complex_nested_data_memory() {
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
                    name: "items".to_string(),
                    nullable: true,
                    item: Box::new(SchemaNode::Struct {
                        name: "item".to_string(),
                        nullable: false,
                        fields: vec![
                            SchemaNode::Primitive {
                                name: "key".to_string(),
                                primitive_type: PrimitiveType::String,
                                nullable: false,
                                format: None,
                            },
                            SchemaNode::Primitive {
                                name: "value".to_string(),
                                primitive_type: PrimitiveType::Float64,
                                nullable: true,
                                format: None,
                            },
                        ],
                    }),
                },
            ],
        })
        .build()
        .unwrap();

    // Generate test data
    let rows: Vec<Vec<ParquetValue>> = (0..100)
        .map(|i| {
            let num_items = (i % 10 + 1) as usize;
            let mut items = Vec::new();

            for j in 0..num_items {
                items.push(ParquetValue::Record(indexmap::indexmap! {
                    "key".into() => ParquetValue::String(format!("key_{}_{}", i, j).into()),
                    "value".into() => if j % 2 == 0 {
                        ParquetValue::Float64(OrderedFloat(j as f64 * 1.5))
                    } else {
                        ParquetValue::Null
                    },
                }));
            }

            vec![ParquetValue::Int32(i), ParquetValue::List(items)]
        })
        .collect();

    // Use custom writer with memory threshold
    let mut buffer = Vec::new();
    {
        let mut writer = WriterBuilder::new()
            .with_memory_threshold(500 * 1024) // 500KB
            .build(&mut buffer, schema.clone())
            .unwrap();

        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

    // Read back and verify
    let reader = Reader::new(Bytes::from(buffer));
    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_rows, rows);
}
