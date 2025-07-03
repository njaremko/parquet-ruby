use bytes::Bytes;
use parquet_core::*;
use std::time::Instant;

#[test]
fn test_iterator_early_termination() {
    // Test that dropping an iterator early doesn't cause issues
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

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows).unwrap();
        writer.close().unwrap();
    }

    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes.clone());

    // Only read first 10 rows then drop iterator
    let mut count = 0;
    for row_result in reader.read_rows().unwrap() {
        let _row = row_result.unwrap();
        count += 1;
        if count >= 10 {
            break;
        }
    }

    assert_eq!(count, 10);

    // Ensure we can create a new iterator after dropping the previous one
    let reader2 = Reader::new(bytes);
    let all_rows: Vec<_> = reader2
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(all_rows.len(), 100);
}

#[test]
fn test_performance_different_batch_sizes() {
    // Test performance characteristics with different batch sizes
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "value".to_string(),
                primitive_type: PrimitiveType::Int64,
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap();

    let total_rows = 10000;
    let batch_sizes = vec![1, 10, 100, 1000, 10000];

    for batch_size in batch_sizes {
        let mut buffer = Vec::new();

        let write_start = Instant::now();
        {
            let mut writer = Writer::new(&mut buffer, schema.clone()).unwrap();

            for batch_start in (0..total_rows).step_by(batch_size) {
                let batch_end = (batch_start + batch_size).min(total_rows);
                let rows: Vec<Vec<ParquetValue>> = (batch_start..batch_end)
                    .map(|i| vec![ParquetValue::Int64(i as i64)])
                    .collect();

                writer.write_rows(rows).unwrap();
            }

            writer.close().unwrap();
        }
        let write_duration = write_start.elapsed();

        // Read back
        let bytes = Bytes::from(buffer);
        let reader = Reader::new(bytes);

        let read_start = Instant::now();
        let count = reader.read_rows().unwrap().count();
        let read_duration = read_start.elapsed();

        assert_eq!(count, total_rows);

        println!(
            "Batch size {}: Write {:?}, Read {:?}",
            batch_size, write_duration, read_duration
        );
    }
}

#[test]
fn test_string_interning_efficiency() {
    // Test efficiency when writing many repeated strings
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "category".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "value".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let categories = ["A", "B", "C", "D", "E"];
    let rows: Vec<Vec<ParquetValue>> = (0..10000)
        .map(|i| {
            vec![
                ParquetValue::String(categories[i % categories.len()].into()),
                ParquetValue::Int32(i as i32),
            ]
        })
        .collect();

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows).unwrap();
        writer.close().unwrap();
    }

    // The file should be efficiently encoded due to repeated strings
    let file_size = buffer.len();
    println!("File size with repeated strings: {} bytes", file_size);

    // Verify we can read it back correctly
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_rows.len(), 10000);

    // Verify the pattern
    for (i, row) in read_rows.iter().enumerate() {
        match &row[0] {
            ParquetValue::String(s) => {
                assert_eq!(*s, categories[i % categories.len()].into());
            }
            _ => panic!("Expected string value"),
        }
    }
}
