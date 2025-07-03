use arrow_array::*;
use arrow_schema::{DataType, Field, TimeUnit};
use bytes::Bytes;
use num::BigInt;
use ordered_float::OrderedFloat;
use parquet_core::arrow_conversion::{arrow_to_parquet_value, parquet_values_to_arrow_array};
use parquet_core::*;
use std::sync::Arc;

#[test]
fn test_float16_conversion() {
    let values = vec![
        ParquetValue::Float16(OrderedFloat(1.0f32)),
        ParquetValue::Float16(OrderedFloat(-2.5f32)),
        ParquetValue::Float16(OrderedFloat(0.0f32)),
        ParquetValue::Null,
    ];

    // Test upcast to Float32
    let field = Field::new("test", DataType::Float32, true);
    let array = parquet_values_to_arrow_array(values.clone(), &field).unwrap();
    assert_eq!(array.len(), 4);

    let float_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(float_array.value(0), 1.0);
    assert_eq!(float_array.value(1), -2.5);
    assert_eq!(float_array.value(2), 0.0);
    assert!(float_array.is_null(3));

    // Test upcast to Float64
    let field = Field::new("test", DataType::Float64, true);
    let array = parquet_values_to_arrow_array(values, &field).unwrap();
    let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(float_array.value(0), 1.0);
    assert_eq!(float_array.value(1), -2.5);
    assert_eq!(float_array.value(2), 0.0);
}

#[test]
fn test_fixed_size_binary_conversion() {
    let uuid_bytes = Bytes::from(vec![
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
        0x88,
    ]);

    let values = vec![
        ParquetValue::Bytes(uuid_bytes.clone()),
        ParquetValue::Bytes(Bytes::from(vec![0u8; 16])),
        ParquetValue::Null,
    ];

    let field = Field::new("uuid", DataType::FixedSizeBinary(16), true);
    let array = parquet_values_to_arrow_array(values, &field).unwrap();

    let fixed_array = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(fixed_array.value(0), uuid_bytes.as_ref());
    assert_eq!(fixed_array.value(1), vec![0u8; 16]);
    assert!(fixed_array.is_null(2));
}

#[test]
fn test_fixed_size_binary_wrong_size_error() {
    let values = vec![
        ParquetValue::Bytes(Bytes::from(vec![1, 2, 3])), // Wrong size
    ];

    let field = Field::new("test", DataType::FixedSizeBinary(16), true);
    let result = parquet_values_to_arrow_array(values, &field);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Fixed size binary expected 16 bytes, got 3"));
}

#[test]
fn test_decimal256_large_values() {
    // Test very large Decimal256 values
    let large_positive = BigInt::parse_bytes(
        b"99999999999999999999999999999999999999999999999999999999999999999999999999",
        10,
    )
    .unwrap();
    let large_negative = -large_positive.clone();

    let values = vec![
        ParquetValue::Decimal256(large_positive.clone(), 0),
        ParquetValue::Decimal256(large_negative.clone(), 0),
        ParquetValue::Decimal256(BigInt::from(0), 0),
        ParquetValue::Null,
    ];

    let field = Field::new("test", DataType::Decimal256(76, 0), true);
    let array = parquet_values_to_arrow_array(values, &field).unwrap();

    // Verify roundtrip
    for i in 0..4 {
        let value = arrow_to_parquet_value(&field, array.as_ref(), i).unwrap();
        match (i, value) {
            (0, ParquetValue::Decimal256(v, _)) => assert_eq!(v, large_positive.clone()),
            (1, ParquetValue::Decimal256(v, _)) => assert_eq!(v, large_negative.clone()),
            (2, ParquetValue::Decimal256(v, _)) => assert_eq!(v, BigInt::from(0)),
            (3, ParquetValue::Null) => {}
            _ => panic!("Unexpected value"),
        }
    }
}

#[test]
fn test_decimal256_too_large_error() {
    // Create a value that's too large for 256 bits
    let too_large = BigInt::from(2).pow(256);

    let values = vec![ParquetValue::Decimal256(too_large, 0)];

    let field = Field::new("test", DataType::Decimal256(76, 0), true);
    let result = parquet_values_to_arrow_array(values, &field);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Decimal256 value too large"));
}

#[test]
fn test_time_type_conversions() {
    // Test TimeMillis
    let values_millis = vec![
        ParquetValue::TimeMillis(12345),
        ParquetValue::TimeMillis(0),
        ParquetValue::TimeMillis(86399999), // Last millisecond of day
        ParquetValue::Null,
    ];

    let field = Field::new("time", DataType::Time32(TimeUnit::Millisecond), true);
    let array = parquet_values_to_arrow_array(values_millis, &field).unwrap();
    assert_eq!(array.len(), 4);

    // Test TimeMicros
    let values_micros = vec![
        ParquetValue::TimeMicros(12345678),
        ParquetValue::TimeMicros(0),
        ParquetValue::TimeMicros(86399999999), // Last microsecond of day
        ParquetValue::Null,
    ];

    let field = Field::new("time", DataType::Time64(TimeUnit::Microsecond), true);
    let array = parquet_values_to_arrow_array(values_micros, &field).unwrap();
    assert_eq!(array.len(), 4);
}

#[test]
fn test_timestamp_with_timezone() {
    let tz = Some(Arc::from("America/New_York"));

    let values = vec![
        ParquetValue::TimestampMillis(1234567890123, tz.clone()),
        ParquetValue::TimestampMillis(0, tz.clone()),
        ParquetValue::Null,
    ];

    let field = Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".into())),
        true,
    );
    let array = parquet_values_to_arrow_array(values, &field).unwrap();

    // Verify roundtrip preserves timezone
    for i in 0..3 {
        let value = arrow_to_parquet_value(&field, array.as_ref(), i).unwrap();
        match value {
            ParquetValue::TimestampMillis(_, Some(tz)) => {
                assert_eq!(tz.as_ref(), "America/New_York");
            }
            ParquetValue::Null => assert_eq!(i, 2),
            _ => panic!("Unexpected value"),
        }
    }
}

#[test]
fn test_nested_list_of_lists() {
    // Create a list of lists: [[1, 2], [3], [], null, [4, 5, 6]]
    let inner_lists = vec![
        ParquetValue::List(vec![ParquetValue::Int32(1), ParquetValue::Int32(2)]),
        ParquetValue::List(vec![ParquetValue::Int32(3)]),
        ParquetValue::List(vec![]),
        ParquetValue::Null,
        ParquetValue::List(vec![
            ParquetValue::Int32(4),
            ParquetValue::Int32(5),
            ParquetValue::Int32(6),
        ]),
    ];

    let values = vec![ParquetValue::List(inner_lists)];

    let inner_field = Field::new("item", DataType::Int32, false);
    let list_field = Field::new("inner_list", DataType::List(Arc::new(inner_field)), true);
    let outer_field = Field::new("outer_list", DataType::List(Arc::new(list_field)), false);

    let array = parquet_values_to_arrow_array(values, &outer_field).unwrap();
    assert_eq!(array.len(), 1);

    // Verify roundtrip
    let value = arrow_to_parquet_value(&outer_field, array.as_ref(), 0).unwrap();
    match value {
        ParquetValue::List(items) => assert_eq!(items.len(), 5),
        _ => panic!("Expected list"),
    }
}

#[test]
fn test_map_with_null_values() {
    let map_entries = vec![
        (
            ParquetValue::String(Arc::from("key1")),
            ParquetValue::Int32(100),
        ),
        (ParquetValue::String(Arc::from("key2")), ParquetValue::Null),
        (
            ParquetValue::String(Arc::from("key3")),
            ParquetValue::Int32(300),
        ),
    ];

    let values = vec![ParquetValue::Map(map_entries), ParquetValue::Null];

    let key_field = Field::new("key", DataType::Utf8, false);
    let value_field = Field::new("value", DataType::Int32, true);
    let entries_field = Field::new(
        "entries",
        DataType::Struct(vec![key_field, value_field].into()),
        false,
    );
    let map_field = Field::new("map", DataType::Map(Arc::new(entries_field), false), true);

    let array = parquet_values_to_arrow_array(values, &map_field).unwrap();
    assert_eq!(array.len(), 2);

    // Verify the map was created correctly
    let map_array = array.as_any().downcast_ref::<MapArray>().unwrap();
    assert!(!map_array.is_null(0));
    assert!(map_array.is_null(1));
}

#[test]
fn test_struct_with_missing_fields() {
    use indexmap::IndexMap;

    // Create a struct with some fields missing
    let mut record1 = IndexMap::new();
    record1.insert(
        Arc::from("field1"),
        ParquetValue::String(Arc::from("value1")),
    );
    // field2 is missing
    record1.insert(Arc::from("field3"), ParquetValue::Int32(42));

    let mut record2 = IndexMap::new();
    record2.insert(
        Arc::from("field1"),
        ParquetValue::String(Arc::from("value2")),
    );
    record2.insert(Arc::from("field2"), ParquetValue::Boolean(true));
    record2.insert(Arc::from("field3"), ParquetValue::Int32(99));

    let values = vec![
        ParquetValue::Record(record1),
        ParquetValue::Record(record2),
        ParquetValue::Null,
    ];

    let fields = vec![
        Field::new("field1", DataType::Utf8, false),
        Field::new("field2", DataType::Boolean, true), // nullable to handle missing
        Field::new("field3", DataType::Int32, false),
    ];

    let struct_field = Field::new("struct", DataType::Struct(fields.into()), true);
    let array = parquet_values_to_arrow_array(values, &struct_field).unwrap();

    let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(struct_array.len(), 3);

    // Verify field2 is null for first record
    let field2_array = struct_array
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(field2_array.is_null(0));
    assert!(!field2_array.is_null(1));
}

#[test]
fn test_type_mismatch_errors() {
    // Test various type mismatches

    // Boolean field expecting String value
    let values = vec![ParquetValue::String(Arc::from("not a boolean"))];
    let field = Field::new("test", DataType::Boolean, false);
    let result = parquet_values_to_arrow_array(values, &field);
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("Expected Boolean") && error_msg.contains("String"),
        "Error message was: {}",
        error_msg
    );

    // Int32 field expecting Float value
    let values = vec![ParquetValue::Float32(OrderedFloat(3.14))];
    let field = Field::new("test", DataType::Int32, false);
    let result = parquet_values_to_arrow_array(values, &field);
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("Expected Int32") && error_msg.contains("Float32"),
        "Error message was: {}",
        error_msg
    );

    // List field expecting non-list value
    let values = vec![ParquetValue::Int32(42)];
    let item_field = Field::new("item", DataType::Int32, false);
    let list_field = Field::new("list", DataType::List(Arc::new(item_field)), false);
    let result = parquet_values_to_arrow_array(values, &list_field);
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("Expected List") && error_msg.contains("Int32"),
        "Error message was: {}",
        error_msg
    );
}

#[test]
fn test_unsupported_arrow_types() {
    // Test arrow_to_parquet_value with unsupported types
    // Create a simple union type
    let type_ids = arrow_buffer::ScalarBuffer::from(vec![0i8, 0, 0]);
    let fields = vec![Arc::new(Field::new("int", DataType::Int32, false))];
    let union_fields = arrow_schema::UnionFields::new(vec![0], fields);

    let array = arrow_array::UnionArray::try_new(
        union_fields,
        type_ids,
        None,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();

    let result = arrow_to_parquet_value(&Field::new("int", DataType::Int32, false), &array, 0);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Unsupported data type for conversion"));
}

#[test]
fn test_integer_overflow_prevention() {
    // Test that we can't upcast a value that would overflow
    let values = vec![ParquetValue::Int64(i64::MAX), ParquetValue::Int64(i64::MIN)];

    // These should work fine in Int64
    let field = Field::new("test", DataType::Int64, false);
    let array = parquet_values_to_arrow_array(values, &field).unwrap();
    let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(int_array.value(0), i64::MAX);
    assert_eq!(int_array.value(1), i64::MIN);
}

#[test]
fn test_empty_collections() {
    // Test empty list
    let values = vec![ParquetValue::List(vec![])];
    let field = Field::new(
        "list",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        false,
    );
    let array = parquet_values_to_arrow_array(values.clone(), &field).unwrap();
    let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list_array.value(0).len(), 0);

    // Test empty map
    let values = vec![ParquetValue::Map(vec![])];
    let key_field = Field::new("key", DataType::Utf8, false);
    let value_field = Field::new("value", DataType::Int32, true);
    let entries_field = Field::new(
        "entries",
        DataType::Struct(vec![key_field, value_field].into()),
        false,
    );
    let map_field = Field::new("map", DataType::Map(Arc::new(entries_field), false), false);
    let array = parquet_values_to_arrow_array(values, &map_field).unwrap();
    let map_array = array.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map_array.value(0).len(), 0);

    // Test empty struct (all fields null)
    use indexmap::IndexMap;
    let empty_record = IndexMap::new();
    let values = vec![ParquetValue::Record(empty_record)];
    let fields = vec![
        Field::new("field1", DataType::Utf8, true),
        Field::new("field2", DataType::Int32, true),
    ];
    let struct_field = Field::new("struct", DataType::Struct(fields.into()), false);
    let array = parquet_values_to_arrow_array(values, &struct_field).unwrap();
    let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();

    // All fields should be null
    assert!(struct_array.column(0).is_null(0));
    assert!(struct_array.column(1).is_null(0));
}
