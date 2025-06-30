use bytes::Bytes;
use parquet_core::*;

mod test_helpers;

// ====== Schema Builder Tests ======

#[test]
fn test_schema_builder_error_cases() {
    // Test building without root node
    let result = SchemaBuilder::new().build();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "Schema must have a root node");
}

#[test]
fn test_schema_builder_default() {
    let builder1 = SchemaBuilder::new();
    let builder2 = SchemaBuilder::default();

    // Both should fail with the same error when building without a root
    let result1 = builder1.build();
    let result2 = builder2.build();

    assert!(result1.is_err());
    assert!(result2.is_err());
    assert_eq!(result1.unwrap_err(), result2.unwrap_err());
}

#[test]
fn test_schema_equality() {
    let schema1 = SchemaBuilder::new()
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
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let schema2 = SchemaBuilder::new()
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
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    assert_eq!(schema1, schema2);
}

#[test]
fn test_schema_inequality() {
    let schema1 = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "id".to_string(),
                primitive_type: PrimitiveType::Int64,
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap();

    let schema2 = SchemaBuilder::new()
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

    assert_ne!(schema1, schema2);
}

// ====== Complex Schema Construction Tests ======

#[test]
fn test_deeply_nested_schema_construction() {
    let inner_struct = SchemaNode::Struct {
        name: "inner".to_string(),
        nullable: true,
        fields: vec![SchemaNode::Primitive {
            name: "value".to_string(),
            primitive_type: PrimitiveType::String,
            nullable: false,
            format: None,
        }],
    };

    let list_of_structs = SchemaNode::List {
        name: "list".to_string(),
        nullable: false,
        item: Box::new(inner_struct),
    };

    let map_with_complex_value = SchemaNode::Map {
        name: "map".to_string(),
        nullable: true,
        key: Box::new(SchemaNode::Primitive {
            name: "key".to_string(),
            primitive_type: PrimitiveType::String,
            nullable: false,
            format: None,
        }),
        value: Box::new(list_of_structs),
    };

    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![map_with_complex_value],
        })
        .build()
        .unwrap();

    assert_eq!(schema.root.name(), "root");
}

#[test]
fn test_complex_schema_with_all_node_types() {
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
                SchemaNode::Map {
                    name: "metadata".to_string(),
                    nullable: false,
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
                SchemaNode::Struct {
                    name: "nested".to_string(),
                    nullable: true,
                    fields: vec![
                        SchemaNode::Primitive {
                            name: "field1".to_string(),
                            primitive_type: PrimitiveType::Float64,
                            nullable: false,
                            format: None,
                        },
                        SchemaNode::Primitive {
                            name: "field2".to_string(),
                            primitive_type: PrimitiveType::Boolean,
                            nullable: true,
                            format: None,
                        },
                    ],
                },
            ],
        })
        .build()
        .unwrap();

    assert_eq!(schema.root.name(), "root");
    assert!(!schema.root.is_nullable());
}

// ====== Primitive Type Tests ======

#[test]
fn test_primitive_type_names_and_format_requirements() {
    // Test type_name() for all types
    assert_eq!(PrimitiveType::Int8.type_name(), "Int8");
    assert_eq!(PrimitiveType::Int16.type_name(), "Int16");
    assert_eq!(PrimitiveType::Int32.type_name(), "Int32");
    assert_eq!(PrimitiveType::Int64.type_name(), "Int64");
    assert_eq!(PrimitiveType::UInt8.type_name(), "UInt8");
    assert_eq!(PrimitiveType::UInt16.type_name(), "UInt16");
    assert_eq!(PrimitiveType::UInt32.type_name(), "UInt32");
    assert_eq!(PrimitiveType::UInt64.type_name(), "UInt64");
    assert_eq!(PrimitiveType::Float32.type_name(), "Float32");
    assert_eq!(PrimitiveType::Float64.type_name(), "Float64");
    assert_eq!(PrimitiveType::Decimal128(10, 2).type_name(), "Decimal128");
    assert_eq!(PrimitiveType::Decimal256(20, 4).type_name(), "Decimal256");
    assert_eq!(PrimitiveType::Boolean.type_name(), "Boolean");
    assert_eq!(PrimitiveType::String.type_name(), "String");
    assert_eq!(PrimitiveType::Binary.type_name(), "Binary");
    assert_eq!(PrimitiveType::Date32.type_name(), "Date32");
    assert_eq!(PrimitiveType::Date64.type_name(), "Date64");
    assert_eq!(
        PrimitiveType::TimestampSecond(None).type_name(),
        "TimestampSecond"
    );
    assert_eq!(
        PrimitiveType::TimestampMillis(None).type_name(),
        "TimestampMillis"
    );
    assert_eq!(
        PrimitiveType::TimestampMicros(None).type_name(),
        "TimestampMicros"
    );
    assert_eq!(
        PrimitiveType::TimestampNanos(None).type_name(),
        "TimestampNanos"
    );
    assert_eq!(PrimitiveType::TimeMillis.type_name(), "TimeMillis");
    assert_eq!(PrimitiveType::TimeMicros.type_name(), "TimeMicros");
    assert_eq!(
        PrimitiveType::FixedLenByteArray(16).type_name(),
        "FixedLenByteArray"
    );

    // Test requires_format()
    assert!(!PrimitiveType::Int32.requires_format());
    assert!(!PrimitiveType::String.requires_format());
    assert!(!PrimitiveType::Binary.requires_format());
    assert!(!PrimitiveType::Decimal128(10, 2).requires_format());
    assert!(!PrimitiveType::FixedLenByteArray(16).requires_format());

    assert!(PrimitiveType::Date32.requires_format());
    assert!(PrimitiveType::Date64.requires_format());
    assert!(PrimitiveType::TimestampSecond(None).requires_format());
    assert!(PrimitiveType::TimestampMillis(None).requires_format());
    assert!(PrimitiveType::TimestampMicros(None).requires_format());
    assert!(PrimitiveType::TimestampNanos(None).requires_format());
    assert!(PrimitiveType::TimeMillis.requires_format());
    assert!(PrimitiveType::TimeMicros.requires_format());
}

#[test]
fn test_repetition_from_nullability() {
    let nullable_node = SchemaNode::Primitive {
        name: "nullable".to_string(),
        primitive_type: PrimitiveType::String,
        nullable: true,
        format: None,
    };
    assert_eq!(nullable_node.repetition(), Repetition::Optional);

    let required_node = SchemaNode::Primitive {
        name: "required".to_string(),
        primitive_type: PrimitiveType::String,
        nullable: false,
        format: None,
    };
    assert_eq!(required_node.repetition(), Repetition::Required);

    let nullable_struct = SchemaNode::Struct {
        name: "struct".to_string(),
        nullable: true,
        fields: vec![],
    };
    assert_eq!(nullable_struct.repetition(), Repetition::Optional);

    let nullable_list = SchemaNode::List {
        name: "list".to_string(),
        nullable: true,
        item: Box::new(required_node.clone()),
    };
    assert_eq!(nullable_list.repetition(), Repetition::Optional);

    let nullable_map = SchemaNode::Map {
        name: "map".to_string(),
        nullable: true,
        key: Box::new(required_node.clone()),
        value: Box::new(nullable_node.clone()),
    };
    assert_eq!(nullable_map.repetition(), Repetition::Optional);
}

// ====== Empty File Handling Test ======

#[test]
fn test_empty_file_handling() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "id".to_string(),
                primitive_type: PrimitiveType::Int64,
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap();

    let mut buffer = Vec::new();
    {
        let writer = Writer::new(&mut buffer, schema).unwrap();
        // Close without writing any rows
        writer.close().unwrap();
    }

    // Try to read empty file
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(rows.len(), 0);
}

// ====== All Primitive Types Test ======
// Using the comprehensive version from schema_builder_tests.rs which includes all types

#[test]
fn test_all_primitive_types_in_schema() {
    let fields = vec![
        SchemaNode::Primitive {
            name: "int8".to_string(),
            primitive_type: PrimitiveType::Int8,
            nullable: false,
            format: None,
        },
        SchemaNode::Primitive {
            name: "int16".to_string(),
            primitive_type: PrimitiveType::Int16,
            nullable: false,
            format: None,
        },
        SchemaNode::Primitive {
            name: "int32".to_string(),
            primitive_type: PrimitiveType::Int32,
            nullable: false,
            format: None,
        },
        SchemaNode::Primitive {
            name: "int64".to_string(),
            primitive_type: PrimitiveType::Int64,
            nullable: false,
            format: None,
        },
        SchemaNode::Primitive {
            name: "uint8".to_string(),
            primitive_type: PrimitiveType::UInt8,
            nullable: false,
            format: None,
        },
        SchemaNode::Primitive {
            name: "uint16".to_string(),
            primitive_type: PrimitiveType::UInt16,
            nullable: false,
            format: None,
        },
        SchemaNode::Primitive {
            name: "uint32".to_string(),
            primitive_type: PrimitiveType::UInt32,
            nullable: false,
            format: None,
        },
        SchemaNode::Primitive {
            name: "uint64".to_string(),
            primitive_type: PrimitiveType::UInt64,
            nullable: false,
            format: None,
        },
        SchemaNode::Primitive {
            name: "float32".to_string(),
            primitive_type: PrimitiveType::Float32,
            nullable: false,
            format: None,
        },
        SchemaNode::Primitive {
            name: "float64".to_string(),
            primitive_type: PrimitiveType::Float64,
            nullable: false,
            format: None,
        },
        SchemaNode::Primitive {
            name: "decimal128".to_string(),
            primitive_type: PrimitiveType::Decimal128(38, 10),
            nullable: false,
            format: None,
        },
        SchemaNode::Primitive {
            name: "decimal256".to_string(),
            primitive_type: PrimitiveType::Decimal256(76, 20),
            nullable: false,
            format: None,
        },
        SchemaNode::Primitive {
            name: "boolean".to_string(),
            primitive_type: PrimitiveType::Boolean,
            nullable: false,
            format: None,
        },
        SchemaNode::Primitive {
            name: "string".to_string(),
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
        SchemaNode::Primitive {
            name: "date32".to_string(),
            primitive_type: PrimitiveType::Date32,
            nullable: false,
            format: Some("date".to_string()),
        },
        SchemaNode::Primitive {
            name: "date64".to_string(),
            primitive_type: PrimitiveType::Date64,
            nullable: false,
            format: Some("date".to_string()),
        },
        SchemaNode::Primitive {
            name: "timestamp_second".to_string(),
            primitive_type: PrimitiveType::TimestampSecond(None),
            nullable: false,
            format: Some("timestamp".to_string()),
        },
        SchemaNode::Primitive {
            name: "timestamp_millis".to_string(),
            primitive_type: PrimitiveType::TimestampMillis(None),
            nullable: false,
            format: Some("timestamp".to_string()),
        },
        SchemaNode::Primitive {
            name: "timestamp_micros".to_string(),
            primitive_type: PrimitiveType::TimestampMicros(None),
            nullable: false,
            format: Some("timestamp".to_string()),
        },
        SchemaNode::Primitive {
            name: "timestamp_nanos".to_string(),
            primitive_type: PrimitiveType::TimestampNanos(None),
            nullable: false,
            format: Some("timestamp".to_string()),
        },
        SchemaNode::Primitive {
            name: "time_millis".to_string(),
            primitive_type: PrimitiveType::TimeMillis,
            nullable: false,
            format: Some("time".to_string()),
        },
        SchemaNode::Primitive {
            name: "time_micros".to_string(),
            primitive_type: PrimitiveType::TimeMicros,
            nullable: false,
            format: Some("time".to_string()),
        },
        SchemaNode::Primitive {
            name: "fixed_len_byte_array".to_string(),
            primitive_type: PrimitiveType::FixedLenByteArray(16),
            nullable: false,
            format: None,
        },
    ];

    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields,
        })
        .build()
        .unwrap();

    assert_eq!(schema.root.name(), "root");

    // Verify we can create a writer with this schema
    let mut buffer = Vec::new();
    let writer_result = Writer::new(&mut buffer, schema);
    assert!(
        writer_result.is_ok(),
        "Should be able to create writer with all primitive types"
    );
}
