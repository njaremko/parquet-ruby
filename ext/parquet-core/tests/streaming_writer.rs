use bytes::Bytes;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use parquet::file::properties::ReaderProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet_core::{
    ParquetValue, PrimitiveType, Reader, Schema, SchemaBuilder, SchemaNode, Writer, WriterBuilder,
};
use triomphe::Arc;

const MANY_ROW_GROUPS_ENV: &str = "PARQUET_RUBY_TEST_ROW_GROUPS";
const WRITE_ONLY_ENV: &str = "PARQUET_RUBY_STREAMING_TEST_WRITE_ONLY";

fn string_schema() -> Schema {
    SchemaBuilder::new()
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
        .unwrap()
}

fn integer_schema() -> Schema {
    SchemaBuilder::new()
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
        .unwrap()
}

fn model_schema() -> Schema {
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
                SchemaNode::Primitive {
                    name: "name".to_string(),
                    primitive_type: PrimitiveType::String,
                    nullable: false,
                    format: None,
                },
            ],
        })
        .build()
        .unwrap()
}

fn model_rows() -> Vec<Vec<ParquetValue>> {
    ["", "one", "two-two", "three three three", "four"]
        .into_iter()
        .enumerate()
        .map(|(index, name)| {
            vec![
                ParquetValue::Int64(index as i64),
                ParquetValue::String(Arc::from(name)),
            ]
        })
        .collect()
}

fn row_partitions(rows: &[Vec<ParquetValue>], cut_mask: usize) -> Vec<Vec<Vec<ParquetValue>>> {
    let mut partitions = Vec::new();
    let mut start = 0;
    for boundary in 1..rows.len() {
        if cut_mask & (1 << (boundary - 1)) != 0 {
            partitions.push(rows[start..boundary].to_vec());
            start = boundary;
        }
    }
    partitions.push(rows[start..].to_vec());
    partitions
}

fn write_partitioned_rows(
    partitions: &[Vec<Vec<ParquetValue>>],
    memory_threshold: usize,
) -> Vec<u8> {
    let mut output = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_batch_size(10)
        .with_memory_threshold(memory_threshold)
        .build(&mut output, model_schema())
        .unwrap();
    for partition in partitions {
        writer.write_rows(partition.clone()).unwrap();
    }
    writer.close().unwrap();
    output
}

fn write_partitioned_columns(
    partitions: &[Vec<Vec<ParquetValue>>],
    memory_threshold: usize,
) -> Vec<u8> {
    let mut output = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_batch_size(10)
        .with_memory_threshold(memory_threshold)
        .build(&mut output, model_schema())
        .unwrap();
    for partition in partitions {
        writer
            .write_columns(vec![
                (
                    "name".to_string(),
                    partition.iter().map(|row| row[1].clone()).collect(),
                ),
                (
                    "id".to_string(),
                    partition.iter().map(|row| row[0].clone()).collect(),
                ),
            ])
            .unwrap();
    }
    writer.close().unwrap();
    output
}

fn read_all_rows(output: Vec<u8>) -> Vec<Vec<ParquetValue>> {
    Reader::new(Bytes::from(output))
        .read_rows()
        .unwrap()
        .collect::<parquet_core::Result<Vec<_>>>()
        .unwrap()
}

fn large_value(index: usize) -> ParquetValue {
    ParquetValue::String(Arc::from(format!("{index:04}-{}", "x".repeat(4_096))))
}

fn assert_three_rows_and_row_groups(buffer: Vec<u8>, expected_row_groups: usize) {
    let bytes = Bytes::from(buffer);
    let file_reader = SerializedFileReader::new(bytes.clone()).unwrap();
    assert_eq!(expected_row_groups, file_reader.num_row_groups());
    let mut metadata_reader =
        ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Required);
    metadata_reader.try_parse(&bytes).unwrap();
    let metadata = metadata_reader.finish().unwrap();
    assert_eq!(expected_row_groups, metadata.column_index().unwrap().len());
    assert_eq!(expected_row_groups, metadata.offset_index().unwrap().len());
    for row_group_index in 0..expected_row_groups {
        assert_eq!(
            metadata
                .row_group(row_group_index)
                .column(0)
                .data_page_offset(),
            metadata.offset_index().unwrap()[row_group_index][0].page_locations()[0].offset
        );
    }

    let rows = Reader::new(bytes)
        .read_rows()
        .unwrap()
        .collect::<parquet_core::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        (0..3)
            .map(|index| vec![large_value(index)])
            .collect::<Vec<_>>(),
        rows
    );
}

#[test]
fn row_and_column_writes_preserve_every_chunk_partition_across_memory_quanta() {
    let expected = model_rows();
    let cut_mask_count = 1 << expected.len().saturating_sub(1);

    for memory_threshold in [1, 32, 8_192] {
        for cut_mask in 0..cut_mask_count {
            let partitions = row_partitions(&expected, cut_mask);
            let actual_rows = read_all_rows(write_partitioned_rows(&partitions, memory_threshold));
            let actual_columns =
                read_all_rows(write_partitioned_columns(&partitions, memory_threshold));

            assert_eq!(
                (expected.clone(), expected.clone()),
                (actual_rows, actual_columns),
                "memory threshold {memory_threshold}, cut mask {cut_mask:04b}"
            );
        }
    }
}

#[test]
fn empty_row_and_column_writes_have_the_same_complete_observation() {
    let mut row_output = Vec::new();
    let row_writer = WriterBuilder::new()
        .build(&mut row_output, model_schema())
        .unwrap();
    row_writer.close().unwrap();

    let mut column_output = Vec::new();
    let mut column_writer = WriterBuilder::new()
        .build(&mut column_output, model_schema())
        .unwrap();
    column_writer.write_columns(Vec::new()).unwrap();
    column_writer.close().unwrap();

    assert_eq!(
        (Vec::new(), Vec::new()),
        (read_all_rows(row_output), read_all_rows(column_output))
    );
}

#[test]
fn tiny_memory_quantum_does_not_create_one_row_group_per_row() {
    let mut buffer = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_batch_size(1_000)
        .with_memory_threshold(256)
        .build(&mut buffer, string_schema())
        .unwrap();

    for index in 0..3 {
        writer.write_row(vec![large_value(index)]).unwrap();
    }
    writer.close().unwrap();

    assert_three_rows_and_row_groups(buffer, 1);
}

#[test]
fn column_batch_uses_the_same_bounded_row_admission() {
    let mut buffer = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_batch_size(1_000)
        .with_memory_threshold(256)
        .build(&mut buffer, string_schema())
        .unwrap();
    writer
        .write_columns(vec![(
            "value".to_string(),
            (0..3).map(large_value).collect(),
        )])
        .unwrap();
    writer.close().unwrap();

    assert_three_rows_and_row_groups(buffer, 1);
}

#[test]
fn disk_spooled_metadata_preserves_multiple_row_group_offsets() {
    let mut buffer = Vec::new();
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .build();
    let mut writer = Writer::new_with_properties(&mut buffer, string_schema(), properties).unwrap();
    for index in 0..3 {
        writer.write_row(vec![large_value(index)]).unwrap();
    }
    writer.close().unwrap();

    assert_three_rows_and_row_groups(buffer, 3);
}

#[test]
fn custom_properties_preserve_a_small_row_group_byte_target() {
    let mut buffer = Vec::new();
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_bytes(Some(1_024))
        .build();
    let mut writer = Writer::new_with_properties(&mut buffer, string_schema(), properties).unwrap();

    for batch_index in 0..2 {
        writer
            .write_columns(vec![(
                "value".to_string(),
                (0..10)
                    .map(|index| large_value(batch_index * 10 + index))
                    .collect(),
            )])
            .unwrap();
    }
    writer.close().unwrap();

    let file_reader = SerializedFileReader::new(Bytes::from(buffer)).unwrap();
    assert_eq!(2, file_reader.num_row_groups());
}

#[test]
fn disk_spooled_metadata_preserves_bloom_filter_offsets() {
    let mut buffer = Vec::new();
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .set_bloom_filter_enabled(true)
        .build();
    let mut writer = Writer::new_with_properties(&mut buffer, string_schema(), properties).unwrap();
    for index in 0..3 {
        writer.write_row(vec![large_value(index)]).unwrap();
    }
    writer.close().unwrap();

    let file_reader = SerializedFileReader::new_with_options(
        Bytes::from(buffer),
        ReadOptionsBuilder::new()
            .with_reader_properties(
                ReaderProperties::builder()
                    .set_read_bloom_filter(true)
                    .build(),
            )
            .build(),
    )
    .unwrap();
    for index in 0..3 {
        let row_group = file_reader.get_row_group(index).unwrap();
        let bloom_filter = row_group.get_column_bloom_filter(0).unwrap();
        let value = format!("{index:04}-{}", "x".repeat(4_096));
        assert!(bloom_filter.check(value.as_str()));
    }
}

#[test]
fn disk_spooled_metadata_supports_many_row_groups() {
    let row_group_count = std::env::var(MANY_ROW_GROUPS_ENV)
        .map(|value| value.parse().unwrap())
        .unwrap_or(128usize);
    let output = tempfile::NamedTempFile::new().unwrap();
    let handle = output.reopen().unwrap();
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .build();
    let mut writer = Writer::new_with_properties(handle, integer_schema(), properties).unwrap();
    for value in 0..row_group_count {
        writer
            .write_row(vec![ParquetValue::Int64(value as i64)])
            .unwrap();
    }
    writer.close().unwrap();

    assert!(output.as_file().metadata().unwrap().len() > 0);
    if std::env::var_os(WRITE_ONLY_ENV).is_some() {
        return;
    }

    let file_reader = SerializedFileReader::new(output.reopen().unwrap()).unwrap();
    assert_eq!(row_group_count, file_reader.num_row_groups());
    let bytes = Bytes::from(std::fs::read(output.path()).unwrap());
    let values = Reader::new(bytes)
        .read_rows()
        .unwrap()
        .map(|row| row.unwrap()[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        (0..row_group_count)
            .map(|value| ParquetValue::Int64(value as i64))
            .collect::<Vec<_>>(),
        values
    );
}

#[test]
fn zero_memory_quantum_is_rejected_before_writing() {
    let mut output = Vec::new();
    let error = {
        let result = WriterBuilder::new()
            .with_memory_threshold(0)
            .build(&mut output, string_schema());
        match result {
            Ok(_) => panic!("zero memory threshold must be rejected"),
            Err(error) => error,
        }
    };
    assert_eq!(
        "Schema error: memory threshold must be greater than 0",
        error.to_string()
    );
    assert!(output.is_empty());
}
