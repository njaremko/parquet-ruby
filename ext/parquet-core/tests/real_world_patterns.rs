use bytes::Bytes;
use indexmap::IndexMap;
use ordered_float::OrderedFloat;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet_core::*;
use std::sync::Arc;

#[test]
fn test_event_log_pattern() {
    // Common pattern: event logs with timestamps, IDs, and JSON-like data
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "timestamp".to_string(),
                    primitive_type: PrimitiveType::TimestampMillis(None),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "event_id".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "event_type".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "user_id".to_string(),
                    primitive_type: PrimitiveType::Int64,
                    nullable: true,
                    format: None,
                },
                SchemaNode::Map {
                    name: "properties".to_string(),
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
            ],
        })
        .build()
        .unwrap();

    // Simulate a day's worth of events
    let mut rows = Vec::new();
    let event_types = ["page_view", "click", "purchase", "signup", "logout"];
    let base_timestamp = 1735689600000i64; // 2025-01-01 00:00:00

    for hour in 0..24 {
        for minute in 0..60 {
            for event_idx in 0..5 {
                let timestamp =
                    base_timestamp + (hour * 3600 + minute * 60) * 1000 + event_idx * 100;
                let event_type = event_types[(event_idx as usize) % event_types.len()];
                let event_id = format!("evt_{:016x}", timestamp + event_idx);
                let user_id = if event_type == "logout" || minute % 10 == 0 {
                    ParquetValue::Null
                } else {
                    ParquetValue::Int64(1000000 + (hour * 1000 + minute))
                };

                let mut properties = vec![
                    (
                        ParquetValue::String(Arc::from("page")),
                        ParquetValue::String(Arc::from(format!("/page_{}", minute % 10))),
                    ),
                    (
                        ParquetValue::String(Arc::from("referrer")),
                        if minute % 3 == 0 {
                            ParquetValue::Null
                        } else {
                            ParquetValue::String(Arc::from("https://search.example.com"))
                        },
                    ),
                ];

                if event_type == "purchase" {
                    properties.push((
                        ParquetValue::String(Arc::from("amount")),
                        ParquetValue::String(Arc::from(format!(
                            "{:.2}",
                            10.0 + (minute as f64) * 1.5
                        ))),
                    ));
                }

                rows.push(vec![
                    ParquetValue::TimestampMillis(timestamp, None),
                    ParquetValue::String(Arc::from(event_id)),
                    ParquetValue::String(Arc::from(event_type)),
                    user_id,
                    ParquetValue::Map(properties),
                ]);
            }
        }
    }

    // Write with appropriate settings for time-series data
    let mut buffer = Vec::new();
    {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_dictionary_enabled(true) // Good for repeated event types
            .set_max_row_group_size(100000) // ~1.4 hours of data per row group
            .build();

        let mut writer = Writer::new_with_properties(&mut buffer, schema, props).unwrap();
        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

    // Verify data integrity
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_rows.len(), 24 * 60 * 5); // 24 hours * 60 minutes * 5 events

    // Spot check some values
    assert_eq!(
        read_rows[0][2],
        ParquetValue::String(Arc::from("page_view"))
    );
    assert_eq!(read_rows[4][2], ParquetValue::String(Arc::from("logout")));
}

#[test]
fn test_analytics_fact_table() {
    // Common pattern: fact table with dimensions and metrics
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "date".to_string(),
                    primitive_type: PrimitiveType::Date32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "product_id".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "store_id".to_string(),
                    primitive_type: PrimitiveType::Int16,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "customer_segment".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: true,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "units_sold".to_string(),
                    primitive_type: PrimitiveType::Int32,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "revenue".to_string(),
                    primitive_type: PrimitiveType::Decimal128(18, 2),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "cost".to_string(),
                    primitive_type: PrimitiveType::Decimal128(18, 2),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "discount_pct".to_string(),
                    primitive_type: PrimitiveType::Float32,
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    // Generate realistic fact table data
    let segments = ["Premium", "Regular", "Budget", "Corporate"];
    let mut rows = Vec::new();

    // Simulate 30 days of data
    for day in 0..30 {
        // 100 products
        for product_id in 1..=100 {
            // 10 stores
            for store_id in 1..=10 {
                // Skip some combinations (sparse data)
                if (day + product_id + store_id) % 7 == 0 {
                    continue;
                }

                let units = (product_id * store_id + day) % 50 + 1;
                let unit_price = 10.0 + (product_id as f64) * 0.5;
                let discount = if day % 7 == 0 {
                    // Weekend discount
                    Some(OrderedFloat(0.15))
                } else if product_id % 10 == 0 {
                    // Special product discount
                    Some(OrderedFloat(0.10))
                } else {
                    None
                };

                let revenue =
                    (units as f64 * unit_price * (1.0 - discount.map(|d| d.0).unwrap_or(0.0)))
                        as i128;
                let cost = (units as f64 * unit_price * 0.6) as i128;

                let segment = if units > 30 {
                    Some(segments[0])
                } else if units > 20 {
                    Some(segments[1])
                } else if units > 10 {
                    Some(segments[2])
                } else if store_id <= 3 {
                    Some(segments[3])
                } else {
                    None
                };

                rows.push(vec![
                    ParquetValue::Date32(19000 + day), // Days since epoch
                    ParquetValue::Int32(product_id),
                    ParquetValue::Int16(store_id as i16),
                    segment
                        .map(|s| ParquetValue::String(Arc::from(s)))
                        .unwrap_or(ParquetValue::Null),
                    ParquetValue::Int32(units),
                    ParquetValue::Decimal128(revenue * 100, 2), // Convert to cents
                    ParquetValue::Decimal128(cost * 100, 2),
                    discount
                        .map(|d| ParquetValue::Float32(OrderedFloat(d.0 as f32)))
                        .unwrap_or(ParquetValue::Null),
                ]);
            }
        }
    }

    // Write with settings optimized for analytics
    let mut buffer = Vec::new();
    {
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_dictionary_enabled(true)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
            .build();

        let mut writer = Writer::new_with_properties(&mut buffer, schema, props).unwrap();
        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

    // Read and verify
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_rows.len(), rows.len());

    // Verify data patterns
    let mut total_revenue = 0i128;
    let mut total_cost = 0i128;

    for row in &read_rows {
        match &row[5] {
            ParquetValue::Decimal128(rev, 2) => total_revenue += rev,
            _ => panic!("Expected decimal revenue"),
        }
        match &row[6] {
            ParquetValue::Decimal128(cost, 2) => total_cost += cost,
            _ => panic!("Expected decimal cost"),
        }
    }

    // Profit margin should be around 40%
    let profit_margin = (total_revenue - total_cost) as f64 / total_revenue as f64;
    assert!(
        profit_margin > 0.35 && profit_margin < 0.45,
        "Unexpected profit margin: {}",
        profit_margin
    );
}

#[test]
fn test_iot_sensor_data() {
    // Common pattern: IoT sensor data with nested readings
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "device_id".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "timestamp".to_string(),
                    primitive_type: PrimitiveType::TimestampMicros(None),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Struct {
                    name: "location".to_string(),
                    nullable: true,
                    fields: vec![
                        SchemaNode::Primitive {
                            name: "latitude".to_string(),
                            primitive_type: PrimitiveType::Float64,
                            nullable: false,
                            format: None,
                        },
                        SchemaNode::Primitive {
                            name: "longitude".to_string(),
                            primitive_type: PrimitiveType::Float64,
                            nullable: false,
                            format: None,
                        },
                        SchemaNode::Primitive {
                            name: "altitude".to_string(),
                            primitive_type: PrimitiveType::Float32,
                            nullable: true,
                            format: None,
                        },
                    ],
                },
                SchemaNode::List {
                    name: "readings".to_string(),
                    nullable: false,
                    item: Box::new(SchemaNode::Struct {
                        name: "reading".to_string(),
                        nullable: false,
                        fields: vec![
                            SchemaNode::Primitive {
                                name: "sensor_type".to_string(),
                                primitive_type: PrimitiveType::String,
                                nullable: false,
                                format: None,
                            },
                            SchemaNode::Primitive {
                                name: "value".to_string(),
                                primitive_type: PrimitiveType::Float64,
                                nullable: false,
                                format: None,
                            },
                            SchemaNode::Primitive {
                                name: "unit".to_string(),
                                primitive_type: PrimitiveType::String,
                                nullable: false,
                                format: None,
                            },
                            SchemaNode::Primitive {
                                name: "quality".to_string(),
                                primitive_type: PrimitiveType::Int8,
                                nullable: true,
                                format: None,
                            },
                        ],
                    }),
                },
                SchemaNode::Primitive {
                    name: "battery_level".to_string(),
                    primitive_type: PrimitiveType::Float32,
                    nullable: true,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap();

    // Generate sensor data
    let mut rows = Vec::new();
    let base_timestamp = 1735689600000000i64; // microseconds

    // 10 devices
    for device_idx in 0..10 {
        let device_id: Arc<str> = Arc::from(format!("sensor_{:04}", device_idx));
        let base_lat = 37.7749 + (device_idx as f64) * 0.01;
        let base_lon = -122.4194 + (device_idx as f64) * 0.01;

        // 1 hour of data, reading every minute
        for minute in 0..60 {
            let timestamp = base_timestamp + (minute as i64 * 60 * 1000000);

            // Location (some devices lose GPS occasionally)
            let location = if minute % 15 == 0 && device_idx % 3 == 0 {
                // When struct is null, represent it as a record with all null fields
                ParquetValue::Null
            } else {
                ParquetValue::Record({
                    let mut map = IndexMap::new();
                    map.insert(
                        Arc::from("latitude"),
                        ParquetValue::Float64(OrderedFloat(base_lat + (minute as f64) * 0.0001)),
                    );
                    map.insert(
                        Arc::from("longitude"),
                        ParquetValue::Float64(OrderedFloat(base_lon + (minute as f64) * 0.0001)),
                    );
                    map.insert(
                        Arc::from("altitude"),
                        if device_idx < 5 {
                            ParquetValue::Float32(OrderedFloat(100.0 + (minute as f32) * 0.1))
                        } else {
                            ParquetValue::Null
                        },
                    );
                    map
                })
            };

            // Sensor readings
            let mut readings = vec![];

            // Temperature
            readings.push(ParquetValue::Record({
                let mut map = IndexMap::new();
                map.insert(
                    Arc::from("sensor_type"),
                    ParquetValue::String(Arc::from("temperature")),
                );
                map.insert(
                    Arc::from("value"),
                    ParquetValue::Float64(OrderedFloat(
                        20.0 + (minute as f64) * 0.1 + device_idx as f64,
                    )),
                );
                map.insert(
                    Arc::from("unit"),
                    ParquetValue::String(Arc::from("celsius")),
                );
                map.insert(Arc::from("quality"), ParquetValue::Int8(100));
                map
            }));

            // Humidity
            readings.push(ParquetValue::Record({
                let mut map = IndexMap::new();
                map.insert(
                    Arc::from("sensor_type"),
                    ParquetValue::String(Arc::from("humidity")),
                );
                map.insert(
                    Arc::from("value"),
                    ParquetValue::Float64(OrderedFloat(45.0 + (minute as f64) * 0.2)),
                );
                map.insert(
                    Arc::from("unit"),
                    ParquetValue::String(Arc::from("percent")),
                );
                map.insert(
                    Arc::from("quality"),
                    if minute % 10 == 0 {
                        ParquetValue::Null // Missing quality score
                    } else {
                        ParquetValue::Int8(95)
                    },
                );
                map
            }));

            // Some devices have additional sensors
            if device_idx % 2 == 0 {
                readings.push(ParquetValue::Record({
                    let mut map = IndexMap::new();
                    map.insert(
                        Arc::from("sensor_type"),
                        ParquetValue::String(Arc::from("pressure")),
                    );
                    map.insert(
                        Arc::from("value"),
                        ParquetValue::Float64(OrderedFloat(1013.25 + (minute as f64) * 0.01)),
                    );
                    map.insert(Arc::from("unit"), ParquetValue::String(Arc::from("hPa")));
                    map.insert(Arc::from("quality"), ParquetValue::Int8(90));
                    map
                }));
            }

            // Battery level decreases over time
            let battery = if minute == 0 {
                ParquetValue::Float32(OrderedFloat(100.0))
            } else {
                ParquetValue::Float32(OrderedFloat(100.0 - (minute as f32) * 0.1))
            };

            rows.push(vec![
                ParquetValue::String(device_id.clone()),
                ParquetValue::TimestampMicros(timestamp, None),
                location,
                ParquetValue::List(readings),
                battery,
            ]);
        }
    }

    // Write with settings for time-series IoT data
    let mut buffer = Vec::new();
    {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_dictionary_enabled(true)
            .build();

        let mut writer = Writer::new_with_properties(&mut buffer, schema, props).unwrap();
        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

    // Verify
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_rows.len(), 10 * 60); // 10 devices * 60 minutes

    // Check first and last readings
    match &read_rows[0][0] {
        ParquetValue::String(id) => assert_eq!(id.as_ref(), "sensor_0000"),
        _ => panic!("Expected device ID"),
    }

    match &read_rows[0][3] {
        ParquetValue::List(readings) => assert!(readings.len() >= 2),
        _ => panic!("Expected readings list"),
    }
}

#[test]
fn test_change_data_capture() {
    // Common pattern: CDC (Change Data Capture) events
    let schema = SchemaBuilder::new()
        .with_root(SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: vec![
                SchemaNode::Primitive {
                    name: "operation".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "timestamp".to_string(),
                    primitive_type: PrimitiveType::TimestampMillis(None),
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "database".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "table".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Primitive {
                    name: "primary_key".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
                SchemaNode::Map {
                    name: "before".to_string(),
                    nullable: true,
                    key: Box::new(SchemaNode::Primitive {
                        name: "column".to_string(),
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
                SchemaNode::Map {
                    name: "after".to_string(),
                    nullable: true,
                    key: Box::new(SchemaNode::Primitive {
                        name: "column".to_string(),
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
            ],
        })
        .build()
        .unwrap();

    // Generate CDC events
    let mut rows = Vec::new();
    let operations = ["INSERT", "UPDATE", "DELETE"];
    let tables = ["users", "orders", "products"];
    let base_timestamp = 1735689600000i64;

    for i in 0..1000 {
        let operation = operations[i % operations.len()];
        let table = tables[(i / 10) % tables.len()];
        let timestamp = base_timestamp + (i * 1000) as i64;
        let primary_key = format!("{}_id:{}", table, i);

        let (before, after) = match operation {
            "INSERT" => (
                ParquetValue::Null,
                ParquetValue::Map(vec![
                    (
                        ParquetValue::String(Arc::from("id")),
                        ParquetValue::String(Arc::from(i.to_string())),
                    ),
                    (
                        ParquetValue::String(Arc::from("name")),
                        ParquetValue::String(Arc::from(format!("{} {}", table, i))),
                    ),
                    (
                        ParquetValue::String(Arc::from("created_at")),
                        ParquetValue::String(Arc::from(timestamp.to_string())),
                    ),
                ]),
            ),
            "UPDATE" => (
                ParquetValue::Map(vec![
                    (
                        ParquetValue::String(Arc::from("id")),
                        ParquetValue::String(Arc::from(i.to_string())),
                    ),
                    (
                        ParquetValue::String(Arc::from("name")),
                        ParquetValue::String(Arc::from(format!("old_{} {}", table, i))),
                    ),
                    (
                        ParquetValue::String(Arc::from("updated_at")),
                        ParquetValue::String(Arc::from((timestamp - 86400000).to_string())),
                    ),
                ]),
                ParquetValue::Map(vec![
                    (
                        ParquetValue::String(Arc::from("id")),
                        ParquetValue::String(Arc::from(i.to_string())),
                    ),
                    (
                        ParquetValue::String(Arc::from("name")),
                        ParquetValue::String(Arc::from(format!("new_{} {}", table, i))),
                    ),
                    (
                        ParquetValue::String(Arc::from("updated_at")),
                        ParquetValue::String(Arc::from(timestamp.to_string())),
                    ),
                ]),
            ),
            "DELETE" => (
                ParquetValue::Map(vec![
                    (
                        ParquetValue::String(Arc::from("id")),
                        ParquetValue::String(Arc::from(i.to_string())),
                    ),
                    (
                        ParquetValue::String(Arc::from("name")),
                        ParquetValue::String(Arc::from(format!("{} {}", table, i))),
                    ),
                ]),
                ParquetValue::Null,
            ),
            _ => unreachable!(),
        };

        rows.push(vec![
            ParquetValue::String(Arc::from(operation)),
            ParquetValue::TimestampMillis(timestamp, None),
            ParquetValue::String(Arc::from("production")),
            ParquetValue::String(Arc::from(table)),
            ParquetValue::String(Arc::from(primary_key)),
            before,
            after,
        ]);
    }

    // Write
    let mut buffer = Vec::new();
    {
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer.write_rows(rows.clone()).unwrap();
        writer.close().unwrap();
    }

    // Verify
    let bytes = Bytes::from(buffer);
    let reader = Reader::new(bytes);

    let read_rows: Vec<_> = reader
        .read_rows()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(read_rows.len(), 1000);

    // Count operations
    let mut insert_count = 0;
    let mut update_count = 0;
    let mut delete_count = 0;

    for row in &read_rows {
        match &row[0] {
            ParquetValue::String(op) => match op.as_ref() {
                "INSERT" => insert_count += 1,
                "UPDATE" => update_count += 1,
                "DELETE" => delete_count += 1,
                _ => panic!("Unexpected operation"),
            },
            _ => panic!("Expected operation string"),
        }
    }

    assert!(insert_count > 300);
    assert!(update_count > 300);
    assert!(delete_count > 300);
}
