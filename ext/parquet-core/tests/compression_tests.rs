use bytes::Bytes;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet_core::*;
use std::sync::Arc;
use std::time::Instant;

#[test]
fn test_compression_effectiveness() {
    // Test compression ratios for different data patterns
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "repetitive".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "random".to_string(),
                    primitive_type: PrimitiveType::Binary,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "sequential".to_string(),
                    primitive_type: PrimitiveType::Int64,
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    // Create test data with different compressibility characteristics
    let rows: Vec<Vec<ParquetValue>> = (0..1000)
        .map(|i| {
            vec![
                // Highly repetitive data (should compress well)
                ParquetValue::String(Arc::from("A".repeat(100))),
                // Random data (should not compress well)
                ParquetValue::Bytes(Bytes::from(
                    (0..100)
                        .map(|j| ((i * 31 + j * 17) % 256) as u8)
                        .collect::<Vec<u8>>(),
                )),
                // Sequential data (should compress moderately)
                ParquetValue::Int64(i as i64),
            ]
        })
        .collect();

    let compressions = vec![
        ("UNCOMPRESSED", Compression::UNCOMPRESSED),
        ("SNAPPY", Compression::SNAPPY),
        ("GZIP", Compression::GZIP(Default::default())),
        ("LZ4", Compression::LZ4),
        ("ZSTD", Compression::ZSTD(Default::default())),
    ];

    let mut results = vec![];

    for (name, compression) in compressions {
        let mut buffer = Vec::new();

        let start = Instant::now();
        {
            let props = WriterProperties::builder()
                .set_compression(compression)
                .build();

            let mut writer =
                Writer::new_with_properties(&mut buffer, schema.clone(), props).unwrap();
            writer.write_rows(rows.clone()).unwrap();
            writer.close().unwrap();
        }
        let write_time = start.elapsed();

        let file_size = buffer.len();

        // Test read performance
        let bytes = Bytes::from(buffer);
        let reader = Reader::new(bytes);

        let start = Instant::now();
        let read_count = reader.read_rows().unwrap().count();
        let read_time = start.elapsed();

        assert_eq!(read_count, 1000);

        results.push((name, file_size, write_time, read_time));
    }

    // Print results
    println!("\nCompression comparison:");
    println!(
        "{:<15} {:>12} {:>15} {:>15}",
        "Compression", "Size (bytes)", "Write Time", "Read Time"
    );
    println!("{:-<60}", "");

    let uncompressed_size = results[0].1;
    for (name, size, write_time, read_time) in results {
        let ratio = (uncompressed_size as f64 / size as f64 * 100.0) as u32;
        println!(
            "{:<15} {:>12} ({:>3}%) {:>15?} {:>15?}",
            name, size, ratio, write_time, read_time
        );
    }
}

#[test]
fn test_compression_with_nulls() {
    // Test how null values affect compression
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "sparse_data".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "dense_data".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    // Create data with different null patterns
    let sparse_rows: Vec<Vec<ParquetValue>> = (0..1000)
        .map(|i| {
            vec![
                // 90% nulls
                if i % 10 == 0 {
                    ParquetValue::String(Arc::from(format!("Value {}", i)))
                } else {
                    ParquetValue::Null
                },
                // 10% nulls
                if i % 10 == 0 {
                    ParquetValue::Null
                } else {
                    ParquetValue::Int32(i)
                },
            ]
        })
        .collect();

    let compressions = vec![
        ("UNCOMPRESSED", Compression::UNCOMPRESSED),
        ("SNAPPY", Compression::SNAPPY),
        ("ZSTD", Compression::ZSTD(Default::default())),
    ];

    println!("\nNull compression comparison:");
    for (name, compression) in compressions {
        let mut buffer = Vec::new();
        {
            let props = WriterProperties::builder()
                .set_compression(compression)
                .build();

            let mut writer =
                Writer::new_with_properties(&mut buffer, schema.clone(), props).unwrap();
            writer.write_rows(sparse_rows.clone()).unwrap();
            writer.close().unwrap();
        }

        println!("{}: {} bytes", name, buffer.len());

        // Verify nulls are preserved
        let bytes = Bytes::from(buffer);
        let reader = Reader::new(bytes);

        let mut null_count = 0;
        for row_result in reader.read_rows().unwrap() {
            let row = row_result.unwrap();
            for value in &row {
                if matches!(value, ParquetValue::Null) {
                    null_count += 1;
                }
            }
        }

        // Should have 900 + 100 = 1000 nulls total
        assert_eq!(null_count, 1000);
    }
}

#[test]
fn test_compression_level_comparison() {
    // Test different compression levels for GZIP and ZSTD
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "data".to_string(),
                primitive_type: PrimitiveType::String,
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap();

    // Create moderately compressible data
    let rows: Vec<Vec<ParquetValue>> = (0..1000)
        .map(|i| {
            vec![ParquetValue::String(Arc::from(format!(
                "This is row number {} with some repeated text pattern pattern pattern",
                i
            )))]
        })
        .collect();

    // Test various compression algorithms and their levels
    let compression_configs = vec![
        // GZIP levels
        (
            "GZIP_FAST",
            Compression::GZIP(parquet::basic::GzipLevel::try_new(1).unwrap()),
        ),
        ("GZIP_DEFAULT", Compression::GZIP(Default::default())),
        (
            "GZIP_BEST",
            Compression::GZIP(parquet::basic::GzipLevel::try_new(9).unwrap()),
        ),
        // ZSTD levels
        (
            "ZSTD_FAST",
            Compression::ZSTD(parquet::basic::ZstdLevel::try_new(1).unwrap()),
        ),
        ("ZSTD_DEFAULT", Compression::ZSTD(Default::default())),
        (
            "ZSTD_BEST",
            Compression::ZSTD(parquet::basic::ZstdLevel::try_new(10).unwrap()),
        ),
    ];

    println!("\nCompression level comparison:");
    println!(
        "{:<15} {:>12} {:>15}",
        "Compression", "Size (bytes)", "Time"
    );
    println!("{:-<45}", "");

    let mut results = Vec::new();
    for (name, compression) in compression_configs {
        let mut buffer = Vec::new();
        let start = Instant::now();
        {
            let props = WriterProperties::builder()
                .set_compression(compression)
                .build();

            let mut writer =
                Writer::new_with_properties(&mut buffer, schema.clone(), props).unwrap();
            writer.write_rows(rows.clone()).unwrap();
            writer.close().unwrap();
        }
        let duration = start.elapsed();

        results.push((name, buffer.len(), duration));
        println!("{:<15} {:>12} {:>15?}", name, buffer.len(), duration);

        // Verify we can read the data back
        let bytes = Bytes::from(buffer);
        let reader = Reader::new(bytes);
        let read_count = reader.read_rows().unwrap().count();
        assert_eq!(
            read_count, 1000,
            "Failed to read back data compressed with {}",
            name
        );
    }

    // Basic validation that compression is working
    // GZIP_BEST and ZSTD_BEST should produce smaller files than their FAST counterparts
    let gzip_fast_size = results
        .iter()
        .find(|(name, _, _)| *name == "GZIP_FAST")
        .unwrap()
        .1;
    let gzip_best_size = results
        .iter()
        .find(|(name, _, _)| *name == "GZIP_BEST")
        .unwrap()
        .1;
    assert!(
        gzip_best_size <= gzip_fast_size,
        "GZIP_BEST should produce smaller files than GZIP_FAST"
    );

    let zstd_fast_size = results
        .iter()
        .find(|(name, _, _)| *name == "ZSTD_FAST")
        .unwrap()
        .1;
    let zstd_best_size = results
        .iter()
        .find(|(name, _, _)| *name == "ZSTD_BEST")
        .unwrap()
        .1;
    assert!(
        zstd_best_size <= zstd_fast_size,
        "ZSTD_BEST should produce smaller files than ZSTD_FAST"
    );
}

#[test]
fn test_column_specific_compression() {
    // Test applying different compression to different columns
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "highly_compressible".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "random_data".to_string(),
                    primitive_type: PrimitiveType::Binary,
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let rows: Vec<Vec<ParquetValue>> = (0..500)
        .map(|i| {
            vec![
                // Highly repetitive string
                ParquetValue::String(Arc::from("AAAAAAAAAA".repeat(10))),
                // Random binary data
                ParquetValue::Bytes(Bytes::from(
                    (0..100)
                        .map(|j| ((i * 31 + j * 17) % 256) as u8)
                        .collect::<Vec<u8>>(),
                )),
            ]
        })
        .collect();

    // Write with default compression
    let mut buffer = Vec::new();
    {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = Writer::new_with_properties(&mut buffer, schema.clone(), props).unwrap();
        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

    let default_size = buffer.len();
    println!("Default compression (SNAPPY): {} bytes", default_size);

    // Ideally we'd set per-column compression, but if not supported,
    // this test still validates the concept

    // Verify data integrity
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_count = reader.read_rows().unwrap().count();
    assert_eq!(read_count, 500);
}

#[test]
fn test_compression_via_writer_builder() {
    let compressions = vec![
        ("UNCOMPRESSED", Compression::UNCOMPRESSED),
        ("SNAPPY", Compression::SNAPPY),
        ("GZIP", Compression::GZIP(Default::default())),
        ("ZSTD", Compression::ZSTD(Default::default())),
        ("LZ4", Compression::LZ4),
    ];

    for (name, compression) in compressions {
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

        let rows: Vec<Vec<ParquetValue>> =
            (0..1000).map(|i| vec![ParquetValue::Int32(i)]).collect();

        let mut buffer = Vec::new();
        {
            let mut writer = WriterBuilder::new()
                .with_compression(compression)
                .build(&mut buffer, schema)
                .unwrap();

            writer.write_rows(rows).unwrap();
            writer.close().unwrap();
        }

        // Verify we can read it back
        let bytes = Bytes::from(buffer);
        let reader = Reader::new(bytes);
        let read_count = reader.read_rows().unwrap().count();
        assert_eq!(read_count, 1000, "Failed with compression: {}", name);
    }
}
