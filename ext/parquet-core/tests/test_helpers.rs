use bytes::Bytes;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet_core::*;
use std::sync::Arc;

/// Create a test schema with common field types
pub fn create_test_schema() -> Schema {
    SchemaBuilder::new()
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
                SchemaNode::Primitive {
                    name: "value".to_string(),
                    primitive_type: PrimitiveType::Float64,
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "active".to_string(),
                    primitive_type: PrimitiveType::Boolean,
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap()
}

/// Generate test rows with sequential data
pub fn generate_test_rows(count: usize) -> Vec<Vec<ParquetValue>> {
    (0..count)
        .map(|i| {
            vec![
                ParquetValue::Int32(i as i32),
                ParquetValue::String(Arc::from(format!("name_{}", i))),
                ParquetValue::Float64(ordered_float::OrderedFloat(i as f64 * 1.5)),
                ParquetValue::Boolean(i % 2 == 0),
            ]
        })
        .collect()
}

/// Perform a roundtrip test and verify data integrity
pub fn test_roundtrip(
    rows: Vec<Vec<ParquetValue>>,
    schema: Schema,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    test_roundtrip_with_options(rows, schema, Compression::UNCOMPRESSED, None)
}

/// Perform a roundtrip test with custom writer options
pub fn test_roundtrip_with_options(
    rows: Vec<Vec<ParquetValue>>,
    schema: Schema,
    compression: Compression,
    batch_size: Option<usize>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    use tempfile::NamedTempFile;

    let temp_file = NamedTempFile::new()?;
    let file_path = temp_file.path().to_str().unwrap();

    // Write
    let mut buffer = Vec::new();
    {
        let mut builder = WriterBuilder::new();

        if let Some(size) = batch_size {
            builder = builder.with_batch_size(size);
        }

        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();

        let mut writer = if batch_size.is_some() {
            builder.build(&mut buffer, schema.clone())?
        } else {
            Writer::new_with_properties(&mut buffer, schema.clone(), props)?
        };

        writer.write_rows(rows.clone())?;
        writer.close()?;
    }

    // Write to file for persistence
    std::fs::write(file_path, &buffer)?;

    // Read back
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_rows: Vec<Vec<ParquetValue>> = reader.read_rows()?.collect::<Result<Vec<_>>>()?;

    // Verify
    assert_eq!(rows.len(), read_rows.len(), "Row count mismatch");

    for (i, (original, read)) in rows.iter().zip(read_rows.iter()).enumerate() {
        assert_eq!(original, read, "Row {} mismatch", i);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_helpers_work() {
        let schema = create_test_schema();
        let rows = generate_test_rows(10);
        assert_eq!(rows.len(), 10);

        test_roundtrip(rows, schema).unwrap();
    }
}
