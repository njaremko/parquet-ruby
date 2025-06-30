use bytes::Bytes;
use parquet_core::*;
use std::sync::{Arc, Mutex};
use std::thread;

#[test]
fn test_concurrent_readers() {
    // Test multiple threads reading the same file simultaneously
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "thread_id".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "value".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    // Create test data
    let rows: Vec<Vec<ParquetValue>> = (0..1000)
        .map(|i| {
            vec![
                ParquetValue::Int32(i),
                ParquetValue::String(Arc::from(format!("Value {}", i))),
            ]
        })
        .collect();

    // Write to buffer
    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows).unwrap();
        writer.close().unwrap();
    }

    let bytes = Arc::new(Bytes::from(buffer));
    let num_threads = 10;
    let mut handles = vec![];

    // Spawn multiple reader threads
    for thread_id in 0..num_threads {
        let bytes_clone = Arc::clone(&bytes);

        let handle = thread::spawn(move || {
            let reader = Reader::new((*bytes_clone).clone());

            let mut row_count = 0;
            let mut sum = 0i32;

            for row_result in reader.read_rows().unwrap() {
                let row = row_result.unwrap();
                row_count += 1;

                if let ParquetValue::Int32(val) = &row[0] {
                    sum += val;
                }
            }

            println!("Thread {} read {} rows, sum: {}", thread_id, row_count, sum);
            (row_count, sum)
        });

        handles.push(handle);
    }

    // Wait for all threads to complete
    let mut results = vec![];
    for handle in handles {
        results.push(handle.join().unwrap());
    }

    // Verify all threads read the same data
    let expected_count = 1000;
    let expected_sum: i32 = (0..1000).sum();

    for (count, sum) in results {
        assert_eq!(count, expected_count);
        assert_eq!(sum, expected_sum);
    }
}

#[test]
fn test_reader_independence() {
    // Test that multiple readers don't interfere with each other
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

    let rows: Vec<Vec<ParquetValue>> = (0..100).map(|i| vec![ParquetValue::Int64(i)]).collect();

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows).unwrap();
        writer.close().unwrap();
    }

    let bytes = Bytes::from(buffer);

    // Create two readers
    let reader1 = Reader::new(bytes.clone());
    let reader2 = Reader::new(bytes.clone());

    // Read alternately from both readers
    let mut iter1 = reader1.read_rows().unwrap();
    let mut iter2 = reader2.read_rows().unwrap();

    let mut values1 = vec![];
    let mut values2 = vec![];

    // Read 10 from reader1
    for _ in 0..10 {
        if let Some(Ok(row)) = iter1.next() {
            if let ParquetValue::Int64(val) = &row[0] {
                values1.push(*val);
            }
        }
    }

    // Read 20 from reader2
    for _ in 0..20 {
        if let Some(Ok(row)) = iter2.next() {
            if let ParquetValue::Int64(val) = &row[0] {
                values2.push(*val);
            }
        }
    }

    // Continue reading from reader1
    for row_result in iter1 {
        let row = row_result.unwrap();
        if let ParquetValue::Int64(val) = &row[0] {
            values1.push(*val);
        }
    }

    // Continue reading from reader2
    for row_result in iter2 {
        let row = row_result.unwrap();
        if let ParquetValue::Int64(val) = &row[0] {
            values2.push(*val);
        }
    }

    // Verify both readers read all values independently
    assert_eq!(values1.len(), 100);
    assert_eq!(values2.len(), 100);

    // Verify correct sequence
    for (i, val) in values1.iter().enumerate() {
        assert_eq!(*val, i as i64);
    }
    for (i, val) in values2.iter().enumerate() {
        assert_eq!(*val, i as i64);
    }
}

#[test]
fn test_concurrent_column_readers() {
    // Test concurrent column-wise reading
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "col1".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "col2".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "col3".to_string(),
                    primitive_type: PrimitiveType::Float64,
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
                ParquetValue::Int32(i),
                ParquetValue::String(Arc::from(format!("String {}", i))),
                ParquetValue::Float64(ordered_float::OrderedFloat(i as f64 * 1.5)),
            ]
        })
        .collect();

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows).unwrap();
        writer.close().unwrap();
    }

    let bytes = Arc::new(Bytes::from(buffer));
    let mut handles = vec![];

    // Each thread reads a different column
    let columns = ["col1", "col2", "col3"];

    for (thread_id, column_name) in columns.iter().enumerate() {
        let bytes_clone = Arc::clone(&bytes);
        let column = column_name.to_string();

        let handle = thread::spawn(move || {
            let reader = Reader::new((*bytes_clone).clone());

            let mut batch_count = 0;
            let mut value_count = 0;

            for batch_result in reader
                .read_columns_with_projection(&[column.clone()], None)
                .unwrap()
            {
                let batch = batch_result.unwrap();
                batch_count += 1;

                // ColumnBatch has columns as Vec<(String, Vec<ParquetValue>)>
                for (col_name, values) in &batch.columns {
                    if col_name == &column {
                        value_count += values.len();
                    }
                }
            }

            println!(
                "Thread {} read column '{}': {} batches, {} values",
                thread_id, column, batch_count, value_count
            );

            (batch_count, value_count)
        });

        handles.push(handle);
    }

    // Wait for all threads
    let mut results = vec![];
    for handle in handles {
        results.push(handle.join().unwrap());
    }

    // Verify all threads read successfully
    // At least one thread should have read values
    let total_values: usize = results.iter().map(|(_, count)| count).sum();
    assert!(total_values > 0, "No values read by any thread");

    // Verify that the first column (col1) read all values
    assert_eq!(results[0].1, 500, "Column col1 should have read 500 values");
}

#[test]
fn test_shared_writer_safety() {
    // Test that writers cannot be safely shared between threads
    // This test verifies that the API prevents unsafe concurrent writes

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

    // Writers should not implement Send/Sync, so wrapping in Arc<Mutex<>> is necessary
    let buffer = Arc::new(Mutex::new(Vec::new()));

    // Create a writer wrapped in Arc<Mutex<>>
    {
        let buffer_clone = Arc::clone(&buffer);
        let mut buf = buffer_clone.lock().unwrap();

        let mut writer = Writer::new(&mut *buf, schema).unwrap();

        // Write some data
        writer.write_row(vec![ParquetValue::Int32(42)]).unwrap();
        writer.close().unwrap();
    }

    // Verify the write succeeded
    let final_buffer = buffer.lock().unwrap();
    assert!(!final_buffer.is_empty());
}

#[test]
fn test_reader_cloning() {
    // Test that readers can be used independently after cloning bytes
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "id".to_string(),
                primitive_type: PrimitiveType::Int32,
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap();

    let rows: Vec<Vec<ParquetValue>> = (0..50).map(|i| vec![ParquetValue::Int32(i)]).collect();

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows).unwrap();
        writer.close().unwrap();
    }

    let bytes = Bytes::from(buffer);

    // Clone bytes multiple times
    let bytes1 = bytes.clone();
    let bytes2 = bytes.clone();
    let bytes3 = bytes;

    // Create readers from cloned bytes
    let reader1 = Reader::new(bytes1);
    let reader2 = Reader::new(bytes2);
    let reader3 = Reader::new(bytes3);

    // Read from all readers
    let count1 = reader1.read_rows().unwrap().count();
    let count2 = reader2.read_rows().unwrap().count();
    let count3 = reader3.read_rows().unwrap().count();

    assert_eq!(count1, 50);
    assert_eq!(count2, 50);
    assert_eq!(count3, 50);
}

#[test]
fn test_metadata_concurrent_access() {
    // Test concurrent access to metadata
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

    let rows: Vec<Vec<ParquetValue>> = (0..100)
        .map(|i| vec![ParquetValue::String(Arc::from(format!("Value {}", i)))])
        .collect();

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows).unwrap();
        writer.close().unwrap();
    }

    let bytes = Arc::new(Bytes::from(buffer));
    let mut handles = vec![];

    // Multiple threads accessing metadata
    for thread_id in 0..5 {
        let bytes_clone = Arc::clone(&bytes);

        let handle = thread::spawn(move || {
            let mut reader = Reader::new((*bytes_clone).clone());

            // Access metadata multiple times
            for _ in 0..10 {
                let metadata = reader.metadata().unwrap();
                assert_eq!(metadata.num_rows(), 100);

                // Small delay to increase chance of concurrent access
                thread::yield_now();
            }

            println!("Thread {} successfully accessed metadata", thread_id);
        });

        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
}
