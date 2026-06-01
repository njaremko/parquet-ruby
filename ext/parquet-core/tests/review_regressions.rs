use arrow_schema::{DataType, Field};
use bytes::Bytes;
use indexmap::IndexMap;
use num::BigInt;
use parquet_core::arrow_conversion::parquet_values_to_arrow_array;
use parquet_core::traits::SchemaInspector;
use parquet_core::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use triomphe::Arc;

fn hash_value(value: &ParquetValue) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn single_field_schema(field: SchemaNode) -> Schema {
    SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![field],
        })
        .build()
        .unwrap()
}

#[test]
fn equal_records_have_equal_hashes_independent_of_insertion_order() {
    let mut left = IndexMap::new();
    left.insert(Arc::from("id"), ParquetValue::Int64(1));
    left.insert(Arc::from("name"), ParquetValue::String(Arc::from("Ada")));

    let mut right = IndexMap::new();
    right.insert(Arc::from("name"), ParquetValue::String(Arc::from("Ada")));
    right.insert(Arc::from("id"), ParquetValue::Int64(1));

    let records = (ParquetValue::Record(left), ParquetValue::Record(right));

    assert_eq!(records.0, records.1);
    assert_eq!(hash_value(&records.0), hash_value(&records.1));
}

#[test]
fn equal_nested_records_have_equal_hashes_independent_of_insertion_order() {
    // A nested record whose inner record is built in a different field order
    // must still be equal and hash equally at every depth.
    fn inner(order_swapped: bool) -> ParquetValue {
        let mut map = IndexMap::new();
        if order_swapped {
            map.insert(Arc::from("city"), ParquetValue::String(Arc::from("Paris")));
            map.insert(Arc::from("zip"), ParquetValue::Int64(75001));
        } else {
            map.insert(Arc::from("zip"), ParquetValue::Int64(75001));
            map.insert(Arc::from("city"), ParquetValue::String(Arc::from("Paris")));
        }
        ParquetValue::Record(map)
    }

    let mut left = IndexMap::new();
    left.insert(Arc::from("id"), ParquetValue::Int64(1));
    left.insert(Arc::from("address"), inner(false));

    let mut right = IndexMap::new();
    right.insert(Arc::from("address"), inner(true));
    right.insert(Arc::from("id"), ParquetValue::Int64(1));

    let left = ParquetValue::Record(left);
    let right = ParquetValue::Record(right);

    assert_eq!(left, right);
    assert_eq!(hash_value(&left), hash_value(&right));
}

#[test]
fn writer_rejects_null_list_items_when_item_schema_is_not_nullable() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::List {
                name: "values".to_string(),
                nullable: false,
                item: Box::new(SchemaNode::Primitive {
                    name: "item".to_string(),
                    primitive_type: PrimitiveType::Int64,
                    nullable: false,
                    format: None,
                }),
            }],
        })
        .build()
        .unwrap();

    let mut buffer = Vec::new();
    let mut writer = Writer::new(&mut buffer, schema).unwrap();
    let error = writer
        .write_rows(vec![vec![ParquetValue::List(vec![ParquetValue::Null])]])
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "Schema error: Found null value for non-nullable field at row[0][0]"
    );
}

#[test]
fn writer_rejects_null_map_values_when_value_schema_is_not_nullable() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Map {
                name: "lookup".to_string(),
                nullable: false,
                key: Box::new(SchemaNode::Primitive {
                    name: "key".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                }),
                value: Box::new(SchemaNode::Primitive {
                    name: "value".to_string(),
                    primitive_type: PrimitiveType::Int64,
                    nullable: false,
                    format: None,
                }),
            }],
        })
        .build()
        .unwrap();

    let mut buffer = Vec::new();
    let mut writer = Writer::new(&mut buffer, schema).unwrap();
    let error = writer
        .write_rows(vec![vec![ParquetValue::Map(vec![(
            ParquetValue::String(Arc::from("a")),
            ParquetValue::Null,
        )])]])
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "Schema error: Found null value for non-nullable field at row[0].value[0]"
    );
}

fn nested_projection_schema() -> Schema {
    SchemaBuilder::new()
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
                SchemaNode::Struct {
                    name: "profile".to_string(),
                    nullable: false,
                    fields: vec![SchemaNode::Primitive {
                        name: "name".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: false,
                        format: None,
                    }],
                },
            ],
        })
        .build()
        .unwrap()
}

fn profile(name: &str) -> ParquetValue {
    let mut fields = IndexMap::new();
    fields.insert(Arc::from("name"), ParquetValue::String(Arc::from(name)));
    ParquetValue::Record(fields)
}

fn nested_projection_file() -> Vec<u8> {
    let rows = vec![
        vec![ParquetValue::Int64(1), profile("Ada")],
        vec![ParquetValue::Int64(2), profile("Grace")],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, nested_projection_schema()).unwrap();
        writer.write_rows(rows).unwrap();
        writer.close().unwrap();
    }
    buffer
}

#[test]
fn row_projection_decodes_nested_field_with_matching_parquet_field() {
    let reader = Reader::new(Bytes::from(nested_projection_file()));
    let rows = reader
        .read_rows_with_projection(&["profile".to_string()])
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(rows, vec![vec![profile("Ada")], vec![profile("Grace")]]);
}

#[test]
fn column_projection_decodes_nested_field_with_matching_parquet_field() {
    let reader = Reader::new(Bytes::from(nested_projection_file()));
    let batches = reader
        .read_columns_with_projection(&["profile".to_string()], None)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let columns = batches
        .into_iter()
        .map(|batch| batch.columns)
        .collect::<Vec<_>>();

    assert_eq!(
        columns,
        vec![vec![(
            "profile".to_string(),
            vec![profile("Ada"), profile("Grace")]
        )]]
    );
}

#[test]
fn writer_rejects_decimal128_scale_that_disagrees_with_schema() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "amount".to_string(),
                primitive_type: PrimitiveType::Decimal128(10, 2),
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap();

    let mut buffer = Vec::new();
    let mut writer = Writer::new(&mut buffer, schema).unwrap();
    let error = writer
        .write_rows(vec![vec![ParquetValue::Decimal128(12345, 4)]])
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "Schema error: Decimal scale mismatch at row[0]: schema scale 2, value scale 4"
    );
}

#[test]
fn writer_rejects_decimal128_precision_overflow() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "amount".to_string(),
                primitive_type: PrimitiveType::Decimal128(5, 2),
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap();

    let mut buffer = Vec::new();
    let mut writer = Writer::new(&mut buffer, schema).unwrap();
    let error = writer
        .write_rows(vec![vec![ParquetValue::Decimal128(100000, 2)]])
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "Schema error: Decimal precision overflow at row[0]: schema precision 5, value has 6 digits"
    );
}

#[test]
fn write_row_rejects_wrong_length_fixed_size_binary_without_poisoning() {
    let schema = single_field_schema(SchemaNode::Primitive {
        name: "payload".to_string(),
        primitive_type: PrimitiveType::FixedLenByteArray(2),
        nullable: false,
        format: None,
    });

    let mut buffer = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_batch_size(2)
        .build(&mut buffer, schema)
        .unwrap();

    // The wrong-length value is rejected at write_row, before it can be buffered,
    // so it never poisons a later flush.
    let error = writer
        .write_row(vec![ParquetValue::Bytes(Bytes::from_static(b"x"))])
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Schema error: Fixed size binary expected 2 bytes, got 1 at row[0]"
    );

    // The writer is still usable: a valid row writes and closes cleanly.
    writer
        .write_row(vec![ParquetValue::Bytes(Bytes::from_static(b"ab"))])
        .unwrap();
    writer.close().unwrap();
}

#[test]
fn column_write_rejects_required_null_with_schema_error() {
    let schema = single_field_schema(SchemaNode::Primitive {
        name: "id".to_string(),
        primitive_type: PrimitiveType::Int64,
        nullable: false,
        format: None,
    });

    let mut buffer = Vec::new();
    let mut writer = Writer::new(&mut buffer, schema).unwrap();
    let error = writer
        .write_columns(vec![("id".to_string(), vec![ParquetValue::Null])])
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "Schema error: Found null value for non-nullable field at column 'id'[0]"
    );
}

#[test]
fn column_write_rejects_missing_required_struct_field_with_schema_error() {
    let schema = single_field_schema(SchemaNode::Struct {
        name: "profile".to_string(),
        nullable: false,
        fields: vec![SchemaNode::Primitive {
            name: "name".to_string(),
            primitive_type: PrimitiveType::String,
            nullable: false,
            format: None,
        }],
    });

    let mut buffer = Vec::new();
    let mut writer = Writer::new(&mut buffer, schema).unwrap();
    let error = writer
        .write_columns(vec![(
            "profile".to_string(),
            vec![ParquetValue::Record(IndexMap::new())],
        )])
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "Schema error: Required field 'name' is missing in struct at column 'profile'[0]"
    );
}

#[test]
fn column_write_rejects_decimal128_scale_with_schema_error() {
    let schema = single_field_schema(SchemaNode::Primitive {
        name: "amount".to_string(),
        primitive_type: PrimitiveType::Decimal128(10, 2),
        nullable: false,
        format: None,
    });

    let mut buffer = Vec::new();
    let mut writer = Writer::new(&mut buffer, schema).unwrap();
    let error = writer
        .write_columns(vec![(
            "amount".to_string(),
            vec![ParquetValue::Decimal128(12345, 4)],
        )])
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "Schema error: Decimal scale mismatch at column 'amount'[0]: schema scale 2, value scale 4"
    );
}

#[test]
fn arrow_decimal_conversion_rejects_scale_that_disagrees_with_array_type() {
    let field = Field::new("amount", DataType::Decimal128(10, 2), false);
    let values = vec![ParquetValue::Decimal128(12345, 4)];
    let error = parquet_values_to_arrow_array(&values, &field).unwrap_err();

    assert_eq!(
        error.to_string(),
        "Conversion error: Decimal scale mismatch at value[0]: array scale 2, value scale 4"
    );
}

#[test]
fn writer_rejects_decimal256_precision_overflow() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "amount".to_string(),
                primitive_type: PrimitiveType::Decimal256(5, 2),
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap();

    let mut buffer = Vec::new();
    let mut writer = Writer::new(&mut buffer, schema).unwrap();
    let error = writer
        .write_rows(vec![vec![ParquetValue::Decimal256(
            BigInt::from(100000),
            2,
        )]])
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "Schema error: Decimal precision overflow at row[0]: schema precision 5, value has 6 digits"
    );
}

#[test]
fn timestamp_array_uses_field_timezone_instead_of_value_timezone() {
    let field = Field::new(
        "created_at",
        DataType::Timestamp(
            arrow_schema::TimeUnit::Millisecond,
            Some(std::sync::Arc::from("UTC")),
        ),
        false,
    );

    let values = vec![ParquetValue::TimestampMillis(0, Some(Arc::from("+09:00")))];
    let array = parquet_values_to_arrow_array(&values, &field).unwrap();

    assert_eq!(
        array.data_type(),
        &DataType::Timestamp(
            arrow_schema::TimeUnit::Millisecond,
            Some(std::sync::Arc::from("UTC")),
        )
    );
}

#[test]
fn timestamp_array_without_field_timezone_ignores_value_timezone() {
    let field = Field::new(
        "created_at",
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
        false,
    );

    let values = vec![ParquetValue::TimestampMillis(0, Some(Arc::from("+09:00")))];
    let array = parquet_values_to_arrow_array(&values, &field).unwrap();

    assert_eq!(
        array.data_type(),
        &DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
    );
}

#[test]
fn schema_builder_rejects_empty_nested_structs() {
    let error = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Struct {
                name: "empty".to_string(),
                nullable: false,
                fields: vec![],
            }],
        })
        .build()
        .unwrap_err();

    assert_eq!(
        error,
        "Struct field 'root.empty' must contain at least one field"
    );
}

#[test]
fn schema_builder_rejects_nullable_map_keys() {
    let error = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Map {
                name: "lookup".to_string(),
                nullable: false,
                key: Box::new(SchemaNode::Primitive {
                    name: "key".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: true,
                    format: None,
                }),
                value: Box::new(SchemaNode::Primitive {
                    name: "value".to_string(),
                    primitive_type: PrimitiveType::Int64,
                    nullable: false,
                    format: None,
                }),
            }],
        })
        .build()
        .unwrap_err();

    assert_eq!(error, "Map key field 'root.lookup.key' must be required");
}

#[test]
fn schema_builder_rejects_invalid_fixed_size_binary_lengths() {
    let error = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "payload".to_string(),
                primitive_type: PrimitiveType::FixedLenByteArray(0),
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap_err();

    assert_eq!(
        error,
        "FixedLenByteArray field 'root.payload' must have a positive length"
    );
}

#[test]
fn schema_builder_rejects_invalid_decimal_definitions() {
    let scale_error = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "amount".to_string(),
                primitive_type: PrimitiveType::Decimal128(4, 5),
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap_err();

    assert_eq!(
        scale_error,
        "Decimal128 field 'root.amount' scale 5 cannot exceed precision 4"
    );

    let precision_error = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "amount".to_string(),
                primitive_type: PrimitiveType::Decimal256(77, 0),
                nullable: false,
                format: None,
            }],
        })
        .build()
        .unwrap_err();

    assert_eq!(
        precision_error,
        "Decimal256 field 'root.amount' precision 77 exceeds maximum precision 76"
    );
}

#[test]
fn schema_builder_rejects_uuid_format_on_non_uuid_storage() {
    let error = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![SchemaNode::Primitive {
                name: "id".to_string(),
                primitive_type: PrimitiveType::FixedLenByteArray(15),
                nullable: false,
                format: Some("uuid".to_string()),
            }],
        })
        .build()
        .unwrap_err();

    assert_eq!(error, "UUID field 'root.id' must use FixedLenByteArray(16)");
}

#[test]
fn time_nanos_requires_format_metadata() {
    assert!(PrimitiveType::TimeNanos.requires_format());
}

#[test]
fn error_context_preserves_error_category() {
    let error = Err::<(), _>(ParquetError::invalid_argument("bad input"))
        .context("During file read")
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "Invalid argument: During file read: bad input"
    );
    assert!(matches!(error, ParquetError::InvalidArgument(_)));
}

#[test]
fn projected_rows_return_requested_columns_in_schema_order() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "a".to_string(),
                    primitive_type: PrimitiveType::Int64,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "b".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "c".to_string(),
                    primitive_type: PrimitiveType::Boolean,
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
        writer
            .write_row(vec![
                ParquetValue::Int64(1),
                ParquetValue::String(Arc::from("one")),
                ParquetValue::Boolean(true),
            ])
            .unwrap();
        writer.close().unwrap();
    }

    let rows = Reader::new(Bytes::from(buffer))
        .read_rows_with_projection(&["c".to_string(), "a".to_string()])
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(
        rows,
        vec![vec![ParquetValue::Int64(1), ParquetValue::Boolean(true)]]
    );
}

#[test]
fn all_schema_inspector_paths_resolve_back_to_fields() {
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
                SchemaNode::Struct {
                    name: "address".to_string(),
                    nullable: true,
                    fields: vec![SchemaNode::Primitive {
                        name: "city".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: true,
                        format: None,
                    }],
                },
                SchemaNode::List {
                    name: "tags".to_string(),
                    nullable: true,
                    item: Box::new(SchemaNode::Primitive {
                        name: "element".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: false,
                        format: None,
                    }),
                },
                SchemaNode::Map {
                    name: "attributes".to_string(),
                    nullable: true,
                    key: Box::new(SchemaNode::Primitive {
                        name: "attribute_key".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: false,
                        format: None,
                    }),
                    value: Box::new(SchemaNode::Primitive {
                        name: "attribute_value".to_string(),
                        primitive_type: PrimitiveType::Int64,
                        nullable: true,
                        format: None,
                    }),
                },
            ],
        })
        .build()
        .unwrap();

    let resolved = schema
        .all_field_paths()
        .into_iter()
        .map(|path| {
            let field_name = schema
                .get_field_by_path(&path)
                .map(|field| field.name().to_string());
            (path, field_name)
        })
        .collect::<Vec<_>>();

    assert_eq!(
        resolved,
        vec![
            ("root".to_string(), Some("root".to_string())),
            ("root.id".to_string(), Some("id".to_string())),
            ("root.address".to_string(), Some("address".to_string())),
            ("root.address.city".to_string(), Some("city".to_string())),
            ("root.tags".to_string(), Some("tags".to_string())),
            ("root.tags.element".to_string(), Some("element".to_string())),
            (
                "root.attributes".to_string(),
                Some("attributes".to_string())
            ),
            (
                "root.attributes.attribute_key".to_string(),
                Some("attribute_key".to_string())
            ),
            (
                "root.attributes.attribute_value".to_string(),
                Some("attribute_value".to_string())
            ),
        ]
    );
    assert_eq!(
        schema
            .get_field_by_path("address.city")
            .map(SchemaNode::name),
        Some("city")
    );
    assert_eq!(
        schema
            .get_field_by_path("tags.element")
            .map(SchemaNode::name),
        Some("element")
    );
    assert_eq!(
        schema
            .get_field_by_path("attributes.attribute_value")
            .map(SchemaNode::name),
        Some("attribute_value")
    );
}
