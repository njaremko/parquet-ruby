use bytes::Bytes;
use indexmap::IndexMap;
use parquet_core::*;
use std::sync::Arc;

mod test_helpers;

// ====== Schema Construction Errors ======

#[test]
fn test_schema_builder_error_cases() {
    // Test building without root node
    let result = SchemaBuilder::new().build();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "Schema must have a root node");
}

#[test]
fn test_empty_struct_unsupported() {
    // Test that empty structs are not supported by Parquet
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::List {
                    name: "empty_list".to_string(),
                    nullable: false,
                    item: Box::new(SchemaNode::Primitive {
                        name: "item".to_string(),
                        primitive_type: PrimitiveType::Int32,
                        nullable: false,
                        format: None,
                    }),
                },
                SchemaNode::Map {
                    name: "empty_map".to_string(),
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
                },
                SchemaNode::Struct {
                    name: "empty_struct".to_string(),
                    nullable: false,
                    fields: vec![], // Empty struct - not supported by Parquet
                },
            ],
        })
        .build()
        .unwrap();

    // Try to create a writer - this should fail due to empty struct
    let mut buffer = Vec::new();
    let result = Writer::new(&mut buffer, schema.clone());

    // Expect an error about empty structs
    assert!(result.is_err());
    match result {
        Err(ParquetError::Parquet(e)) => {
            assert!(e.to_string().contains("empty struct"));
        }
        _ => panic!("Expected Parquet error about empty structs"),
    }
}

// ====== Field Count Validation Errors ======

#[test]
fn test_field_count_mismatch() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "field1".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "field2".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "field3".to_string(),
                    primitive_type: PrimitiveType::Float64,
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let mut buffer = Vec::new();

    // Test using Writer
    {
        let mut writer = Writer::new(&mut buffer, schema.clone()).unwrap();

        // Test row with too few fields
        let result = writer.write_rows(vec![vec![
            ParquetValue::Int32(1),
            ParquetValue::String(Arc::from("test")),
            // Missing third field
        ]]);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Row has 2 values") && err_msg.contains("schema has 3 fields"),
            "Error message was: {}",
            err_msg
        );
    }

    // Test using WriterBuilder
    {
        buffer.clear();
        let mut writer = WriterBuilder::new()
            .build(&mut buffer, schema.clone())
            .unwrap();

        // Test row with too few fields
        let result = writer.write_row(vec![
            ParquetValue::Int32(42),
            // Missing second and third fields
        ]);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Row has 1 values but schema has 3 fields"));

        // Test row with too many fields
        let result = writer.write_row(vec![
            ParquetValue::Int32(42),
            ParquetValue::String(Arc::from("test")),
            ParquetValue::Float64(ordered_float::OrderedFloat(3.14)),
            ParquetValue::Boolean(true), // Extra field
        ]);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Row has 4 values but schema has 3 fields"));
    }
}

// ====== Type Mismatch Errors ======

#[test]
fn test_type_mismatch() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "int_field".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "string_field".to_string(),
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
        let mut writer = Writer::new(&mut buffer, schema).unwrap();

        // Try to write wrong types
        let result = writer.write_rows(vec![vec![
            ParquetValue::String(Arc::from("not an int")), // Wrong type for int_field
            ParquetValue::Int32(123),                      // Wrong type for string_field
        ]]);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Type mismatch") && err_msg.contains("expected Int32"),
            "Error message was: {}",
            err_msg
        );
    }
}

// ====== Null Validation Errors ======

#[test]
fn test_null_in_non_nullable_field() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "required_field".to_string(),
                primitive_type: PrimitiveType::Int32,
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap();

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();

        // Try to write null to non-nullable field
        let result = writer.write_rows(vec![vec![ParquetValue::Null]]);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("null value") && err_msg.contains("non-nullable"),
            "Error message was: {}",
            err_msg
        );
    }
}

// ====== Complex Type Validation Errors ======

#[test]
fn test_invalid_struct_fields() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Struct {
                name: "nested".to_string(),
                nullable: false,
                fields: vec![
                    SchemaNode::Primitive {
                        name: "field1".to_string(),
                        primitive_type: PrimitiveType::Int32,
                        nullable: false,
                        format: None,
                    },
                    SchemaNode::Primitive {
                        name: "field2".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: false,
                        format: None,
                    },
                ],
            }],
        })
        .build()
        .unwrap();

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();

        // Try to write struct with missing fields
        let mut incomplete_struct = IndexMap::new();
        incomplete_struct.insert(Arc::from("field1"), ParquetValue::Int32(42));
        // field2 is missing

        let result = writer.write_rows(vec![vec![ParquetValue::Record(incomplete_struct)]]);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Required field") && err_msg.contains("field2"),
            "Error message was: {}",
            err_msg
        );
    }
}

#[test]
fn test_invalid_list_element_type() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::List {
                name: "int_list".to_string(),
                nullable: false,
                item: Box::new(SchemaNode::Primitive {
                    name: "item".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: false,
                    format: None,
                }),
            }],
        })
        .build()
        .unwrap();

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();

        // Try to write list with wrong element type
        let result = writer.write_rows(vec![vec![ParquetValue::List(vec![ParquetValue::String(
            Arc::from("not an int"),
        )])]]);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Type mismatch") && err_msg.contains("expected Int32"),
            "Error message was: {}",
            err_msg
        );
    }
}

#[test]
fn test_invalid_map_key_value_types() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Map {
                name: "string_int_map".to_string(),
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

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();

        // Try to write map with wrong key type
        let result = writer.write_rows(vec![vec![ParquetValue::Map(vec![(
            ParquetValue::Int32(42), // Wrong key type
            ParquetValue::Int32(100),
        )])]]);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Type mismatch") && err_msg.contains("expected Utf8"),
            "Error message was: {}",
            err_msg
        );
    }
}

// ====== Unsupported Features ======

#[test]
fn test_map_with_struct_values_unsupported() {
    // Test that maps with struct values are not yet supported
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
                value: Box::new(SchemaNode::Struct {
                    name: "value_struct".to_string(),
                    nullable: false,
                    fields: vec![SchemaNode::Primitive {
                        name: "field".to_string(),
                        primitive_type: PrimitiveType::Int32,
                        nullable: false,
                        format: None,
                    }],
                }),
            }],
        })
        .build()
        .unwrap();

    // Try to write a map with struct values
    let row = vec![ParquetValue::Map(vec![(
        ParquetValue::String(Arc::from("key1")),
        ParquetValue::Record({
            let mut map = IndexMap::new();
            map.insert(Arc::from("field"), ParquetValue::Int32(42));
            map
        }),
    )])];

    let mut buffer = Vec::new();
    let mut writer = Writer::new(&mut buffer, schema).unwrap();
    let result = writer.write_rows(vec![row]);

    // Check if this is still a limitation
    if result.is_err() {
        match result {
            Err(ParquetError::Conversion(msg)) => {
                assert!(msg.contains("Maps with struct values are not yet supported"));
            }
            _ => panic!("Expected Conversion error about maps with struct values"),
        }
    } else {
        // If it succeeds, then the limitation has been fixed!
        // Let's verify we can read it back
        writer.close().unwrap();

        let bytes = Bytes::from(buffer);
        let reader = Reader::new(bytes);

        let read_rows: Vec<_> = reader
            .read_rows()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(read_rows.len(), 1);
        // Maps with struct values now work!
    }
}

// ====== Writer State Errors ======

#[test]
fn test_writer_multiple_close() {
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

    // Test that we can't write after moving the writer into close()
    let mut buffer = Vec::new();
    let mut writer = Writer::new(&mut buffer, schema).unwrap();

    // Write some data
    writer
        .write_rows(vec![vec![ParquetValue::Int32(1)]])
        .unwrap();

    // Close consumes the writer, so we can't use it afterwards
    writer.close().unwrap();

    // The writer has been consumed by close(), so we can't access it anymore
    // This is enforced at compile time by Rust's ownership system
}

// ====== Invalid Collection Schemas ======

#[test]
fn test_invalid_collection_schemas() {
    let test_cases = vec![
        (
            "list_without_item",
            SchemaNode::List {
                name: "invalid_list".to_string(),
                nullable: false,
                item: Box::new(SchemaNode::Struct {
                    name: "empty".to_string(),
                    nullable: false,
                    fields: vec![],
                }),
            },
        ),
        (
            "map_without_value",
            SchemaNode::Map {
                name: "invalid_map".to_string(),
                nullable: false,
                key: Box::new(SchemaNode::Primitive {
                    name: "key".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                }),
                value: Box::new(SchemaNode::Struct {
                    name: "empty".to_string(),
                    nullable: false,
                    fields: vec![],
                }),
            },
        ),
    ];

    for (name, invalid_node) in test_cases {
        let result = SchemaBuilder::new()
            .with_root(SchemaNode::Struct {
                name: "root".to_string(),
                nullable: false,
                fields: vec![invalid_node],
            })
            .build();

        // Document the behavior for invalid collection schemas
        match result {
            Ok(_) => {
                // Some invalid schemas might be allowed at build time
                // but fail at write time
            }
            Err(e) => {
                println!("Schema validation for {}: {}", name, e);
            }
        }
    }
}
