use bytes::Bytes;
use parquet_core::*;
use std::sync::Arc;

mod test_helpers;
use test_helpers::*;

#[test]
fn test_date_types() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "date32".to_string(),
                    primitive_type: PrimitiveType::Date32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "date64".to_string(),
                    primitive_type: PrimitiveType::Date64,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "date32_nullable".to_string(),
                    primitive_type: PrimitiveType::Date32,
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let epoch_date32 = 0; // 1970-01-01
    let epoch_date64 = 0; // 1970-01-01
    let today_date32 = 19000; // ~2022
    let today_date64 = 19000 * 86400 * 1000; // Same day in milliseconds

    let rows = vec![
        vec![
            ParquetValue::Date32(epoch_date32),
            ParquetValue::Date64(epoch_date64),
            ParquetValue::Date32(epoch_date32),
        ],
        vec![
            ParquetValue::Date32(today_date32),
            ParquetValue::Date64(today_date64),
            ParquetValue::Date32(today_date32),
        ],
        vec![
            ParquetValue::Date32(-365),                // One year before epoch
            ParquetValue::Date64(-365 * 86400 * 1000), // Same in milliseconds
            ParquetValue::Null,
        ],
    ];

    // Use test helper for roundtrip
    test_roundtrip(rows, schema).unwrap();
}

#[test]
fn test_timestamp_types() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "ts_millis".to_string(),
                    primitive_type: PrimitiveType::TimestampMillis(None),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "ts_micros".to_string(),
                    primitive_type: PrimitiveType::TimestampMicros(None),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "ts_millis_tz".to_string(),
                    primitive_type: PrimitiveType::TimestampMillis(Some(Arc::from(
                        "America/New_York",
                    ))),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "ts_micros_tz".to_string(),
                    primitive_type: PrimitiveType::TimestampMicros(Some(Arc::from(
                        "America/New_York",
                    ))),
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    // Test various timestamp values
    let epoch_millis = 0;
    let epoch_micros = 0;
    let now_millis = 1_700_000_000_000; // Approximate timestamp for 2023
    let now_micros = now_millis * 1000;
    let tz = Some(Arc::from("America/New_York"));

    let rows = vec![
        vec![
            ParquetValue::TimestampMillis(epoch_millis, None),
            ParquetValue::TimestampMicros(epoch_micros, None),
            ParquetValue::TimestampMillis(epoch_millis, tz.clone()),
            ParquetValue::TimestampMicros(epoch_micros, tz.clone()),
        ],
        vec![
            ParquetValue::TimestampMillis(now_millis, None),
            ParquetValue::TimestampMicros(now_micros, None),
            ParquetValue::TimestampMillis(now_millis, tz.clone()),
            ParquetValue::TimestampMicros(now_micros, tz.clone()),
        ],
        vec![
            ParquetValue::TimestampMillis(-86400000, None), // One day before epoch
            ParquetValue::TimestampMicros(-86400000000, None),
            ParquetValue::TimestampMillis(-86400000, Some(Arc::from("UTC"))),
            ParquetValue::TimestampMicros(-86400000000, Some(Arc::from("UTC"))),
        ],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
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

    assert_eq!(read_rows.len(), rows.len());

    // Verify the timestamps match, accounting for the fact that field timezone overrides value timezone
    for (row_idx, (expected_row, actual_row)) in rows.iter().zip(read_rows.iter()).enumerate() {
        assert_eq!(expected_row.len(), actual_row.len());
        for (col_idx, (expected_val, actual_val)) in
            expected_row.iter().zip(actual_row.iter()).enumerate()
        {
            match (expected_val, actual_val) {
                (
                    ParquetValue::TimestampMillis(e_ts, e_tz),
                    ParquetValue::TimestampMillis(a_ts, a_tz),
                ) => {
                    assert_eq!(
                        e_ts, a_ts,
                        "Timestamp value mismatch at row {}, col {}",
                        row_idx, col_idx
                    );
                    // For columns with timezone in schema (col 2 and 3), the schema timezone wins
                    if col_idx >= 2 {
                        assert_eq!(
                            a_tz.as_deref(),
                            Some("UTC"),
                            "Timezone mismatch at row {}, col {}",
                            row_idx,
                            col_idx
                        );
                    } else {
                        assert_eq!(
                            e_tz, a_tz,
                            "Timezone mismatch at row {}, col {}",
                            row_idx, col_idx
                        );
                    }
                }
                (
                    ParquetValue::TimestampMicros(e_ts, e_tz),
                    ParquetValue::TimestampMicros(a_ts, a_tz),
                ) => {
                    assert_eq!(
                        e_ts, a_ts,
                        "Timestamp value mismatch at row {}, col {}",
                        row_idx, col_idx
                    );
                    // For columns with timezone in schema (col 2 and 3), the schema timezone wins
                    if col_idx >= 2 {
                        assert_eq!(
                            a_tz.as_deref(),
                            Some("UTC"),
                            "Timezone mismatch at row {}, col {}",
                            row_idx,
                            col_idx
                        );
                    } else {
                        assert_eq!(
                            e_tz, a_tz,
                            "Timezone mismatch at row {}, col {}",
                            row_idx, col_idx
                        );
                    }
                }
                _ => panic!("Unexpected value types at row {}, col {}", row_idx, col_idx),
            }
        }
    }
}

#[test]
fn test_time_types() {
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "time_millis".to_string(),
                    primitive_type: PrimitiveType::TimeMillis,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "time_micros".to_string(),
                    primitive_type: PrimitiveType::TimeMicros,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "time_millis_nullable".to_string(),
                    primitive_type: PrimitiveType::TimeMillis,
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    // Time values (milliseconds/microseconds since midnight)
    let midnight = 0;
    let noon_millis = 12 * 60 * 60 * 1000; // 12:00:00
    let noon_micros = noon_millis as i64 * 1000;
    let end_of_day_millis = 23 * 60 * 60 * 1000 + 59 * 60 * 1000 + 59 * 1000 + 999; // 23:59:59.999
    let end_of_day_micros = end_of_day_millis as i64 * 1000 + 999; // 23:59:59.999999

    let rows = vec![
        vec![
            ParquetValue::TimeMillis(midnight),
            ParquetValue::TimeMicros(midnight as i64),
            ParquetValue::TimeMillis(midnight),
        ],
        vec![
            ParquetValue::TimeMillis(noon_millis),
            ParquetValue::TimeMicros(noon_micros),
            ParquetValue::TimeMillis(noon_millis),
        ],
        vec![
            ParquetValue::TimeMillis(end_of_day_millis),
            ParquetValue::TimeMicros(end_of_day_micros),
            ParquetValue::Null,
        ],
    ];

    // Use test helper for roundtrip
    test_roundtrip(rows, schema).unwrap();
}

#[test]
fn test_temporal_types_in_collections() {
    // Test temporal types within lists and maps
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::List {
                    name: "timestamp_list".to_string(),
                    nullable: false,
                    item: Box::new(SchemaNode::Primitive {
                        name: "item".to_string(),
                        primitive_type: PrimitiveType::TimestampMillis(None),
                        nullable: false,
                        format: None,
                    }),
                },
                SchemaNode::Map {
                    name: "date_map".to_string(),
                    nullable: false,
                    key: Box::new(SchemaNode::Primitive {
                        name: "key".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: false,
                        format: None,
                    }),
                    value: Box::new(SchemaNode::Primitive {
                        name: "value".to_string(),
                        primitive_type: PrimitiveType::Date32,
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
            ParquetValue::List(vec![
                ParquetValue::TimestampMillis(1000000000000, None),
                ParquetValue::TimestampMillis(1100000000000, None),
                ParquetValue::TimestampMillis(1200000000000, None),
            ]),
            ParquetValue::Map(vec![
                (
                    ParquetValue::String(Arc::from("start_date")),
                    ParquetValue::Date32(18000),
                ),
                (
                    ParquetValue::String(Arc::from("end_date")),
                    ParquetValue::Date32(18365),
                ),
                (
                    ParquetValue::String(Arc::from("milestone")),
                    ParquetValue::Null,
                ),
            ]),
        ],
        vec![ParquetValue::List(vec![]), ParquetValue::Map(vec![])],
    ];

    // Use test helper for roundtrip
    test_roundtrip(rows, schema).unwrap();
}

#[test]
fn test_temporal_edge_cases() {
    // Comprehensive test for edge cases of all temporal types
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                // All timestamp types
                SchemaNode::Primitive {
                    name: "ts_sec".to_string(),
                    primitive_type: PrimitiveType::TimestampSecond(None),
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "ts_millis".to_string(),
                    primitive_type: PrimitiveType::TimestampMillis(None),
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "ts_micros".to_string(),
                    primitive_type: PrimitiveType::TimestampMicros(None),
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "ts_nanos".to_string(),
                    primitive_type: PrimitiveType::TimestampNanos(None),
                    nullable: true,
                    format: None,
                },
                // Date types
                SchemaNode::Primitive {
                    name: "date32".to_string(),
                    primitive_type: PrimitiveType::Date32,
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "date64".to_string(),
                    primitive_type: PrimitiveType::Date64,
                    nullable: true,
                    format: None,
                },
                // Time types
                SchemaNode::Primitive {
                    name: "time_millis".to_string(),
                    primitive_type: PrimitiveType::TimeMillis,
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "time_micros".to_string(),
                    primitive_type: PrimitiveType::TimeMicros,
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    let rows = vec![
        // Minimum values
        vec![
            ParquetValue::TimestampSecond(i64::MIN, None),
            ParquetValue::TimestampMillis(i64::MIN, None),
            ParquetValue::TimestampMicros(i64::MIN, None),
            ParquetValue::TimestampNanos(i64::MIN, None),
            ParquetValue::Date32(i32::MIN),
            ParquetValue::Date64(i64::MIN),
            ParquetValue::TimeMillis(0), // Time can't be negative
            ParquetValue::TimeMicros(0),
        ],
        // Maximum values
        vec![
            ParquetValue::TimestampSecond(i64::MAX, None),
            ParquetValue::TimestampMillis(i64::MAX, None),
            ParquetValue::TimestampMicros(i64::MAX, None),
            ParquetValue::TimestampNanos(i64::MAX, None),
            ParquetValue::Date32(i32::MAX),
            ParquetValue::Date64(i64::MAX),
            ParquetValue::TimeMillis(86399999),    // 23:59:59.999
            ParquetValue::TimeMicros(86399999999), // 23:59:59.999999
        ],
        // Zero values (Unix epoch / midnight)
        vec![
            ParquetValue::TimestampSecond(0, None),
            ParquetValue::TimestampMillis(0, None),
            ParquetValue::TimestampMicros(0, None),
            ParquetValue::TimestampNanos(0, None),
            ParquetValue::Date32(0),
            ParquetValue::Date64(0),
            ParquetValue::TimeMillis(0),
            ParquetValue::TimeMicros(0),
        ],
        // Common timestamp (2025-01-01 00:00:00 UTC)
        vec![
            ParquetValue::TimestampSecond(1735689600, None),
            ParquetValue::TimestampMillis(1735689600000, None),
            ParquetValue::TimestampMicros(1735689600000000, None),
            ParquetValue::TimestampNanos(1735689600000000000, None),
            ParquetValue::Date32(19723),         // Days since Unix epoch
            ParquetValue::Date64(1735689600000), // Milliseconds since Unix epoch
            ParquetValue::TimeMillis(0),         // Midnight
            ParquetValue::TimeMicros(0),
        ],
        // All nulls
        vec![
            ParquetValue::Null,
            ParquetValue::Null,
            ParquetValue::Null,
            ParquetValue::Null,
            ParquetValue::Null,
            ParquetValue::Null,
            ParquetValue::Null,
            ParquetValue::Null,
        ],
    ];

    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
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

    assert_eq!(read_rows.len(), rows.len());

    // Verify values match exactly
    for (i, (expected_row, actual_row)) in rows.iter().zip(read_rows.iter()).enumerate() {
        for (j, (expected, actual)) in expected_row.iter().zip(actual_row.iter()).enumerate() {
            match (expected, actual) {
                (
                    ParquetValue::TimestampSecond(e_val, _),
                    ParquetValue::TimestampSecond(a_val, _),
                ) => {
                    assert_eq!(e_val, a_val, "Row {} col {}: timestamp values differ", i, j);
                }
                (
                    ParquetValue::TimestampMillis(e_val, _),
                    ParquetValue::TimestampMillis(a_val, _),
                ) => {
                    assert_eq!(e_val, a_val, "Row {} col {}: timestamp values differ", i, j);
                }
                (
                    ParquetValue::TimestampMicros(e_val, _),
                    ParquetValue::TimestampMicros(a_val, _),
                ) => {
                    assert_eq!(e_val, a_val, "Row {} col {}: timestamp values differ", i, j);
                }
                (
                    ParquetValue::TimestampNanos(e_val, _),
                    ParquetValue::TimestampNanos(a_val, _),
                ) => {
                    assert_eq!(e_val, a_val, "Row {} col {}: timestamp values differ", i, j);
                }
                (ParquetValue::Null, ParquetValue::Null) => {} // Both null is ok
                _ => assert_eq!(expected, actual, "Row {} col {}: values differ", i, j),
            }
        }
    }
}
