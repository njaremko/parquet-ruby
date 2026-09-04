use bytes::Bytes;
use parquet_core::*;

fn int64_schema() -> Schema {
    SchemaBuilder::new()
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
        .unwrap()
}

/// Write three row groups of 3, 5, and 2 rows by flushing between chunks.
fn three_row_group_file() -> Bytes {
    let mut buffer = Vec::new();
    let mut writer = Writer::new(&mut buffer, int64_schema()).unwrap();
    for (count, offset) in [(3, 0), (5, 3), (2, 8)] {
        let rows: Vec<Vec<ParquetValue>> = (offset..offset + count)
            .map(|i| vec![ParquetValue::Int64(i)])
            .collect();
        writer.write_rows(rows).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    Bytes::from(buffer)
}

fn row_ids(reader: Reader<Bytes>, row_groups: Option<Vec<usize>>) -> Vec<i64> {
    reader
        .read_rows_selected(None, row_groups)
        .unwrap()
        .map(|row| match &row.unwrap()[0] {
            ParquetValue::Int64(v) => *v,
            other => panic!("unexpected value: {:?}", other),
        })
        .collect()
}

#[test]
fn read_rows_selected_decodes_only_the_requested_row_group() {
    assert_eq!(row_ids(Reader::new(three_row_group_file()), Some(vec![1])), [3, 4, 5, 6, 7]);
}

#[test]
fn read_rows_selected_respects_request_order() {
    let reader = Reader::new(three_row_group_file());
    assert_eq!(row_ids(reader, Some(vec![2, 0])), [8, 9, 0, 1, 2]);
}

#[test]
fn read_rows_selected_with_projection_combines() {
    let bytes = three_row_group_file();
    let reader = Reader::new(bytes.clone());
    let rows: Vec<Vec<ParquetValue>> = reader
        .read_rows_selected(Some(&["id".to_string()]), Some(vec![1]))
        .unwrap()
        .collect::<Result<_>>()
        .unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(Reader::new(bytes).num_row_groups().unwrap(), 3);
}

#[test]
fn read_columns_selected_decodes_only_the_requested_row_group() {
    let reader = Reader::new(three_row_group_file());
    let batches: Vec<parquet_core::reader::ColumnBatch> = reader
        .read_columns_selected(None, Some(vec![0]), Some(2))
        .unwrap()
        .collect::<Result<_>>()
        .unwrap();
    let values: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch.columns.iter().flat_map(|(_, values)| {
                values.iter().map(|v| match v {
                    ParquetValue::Int64(v) => *v,
                    other => panic!("unexpected value: {:?}", other),
                })
            })
        })
        .collect();
    assert_eq!(values, [0, 1, 2]);
}
