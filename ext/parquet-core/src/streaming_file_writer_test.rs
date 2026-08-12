use super::{row_group_ordinal, StreamingFileWriter, MAX_ROW_GROUPS_PER_FILE};
use arrow::record_batch::RecordBatch;
use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use parquet::file::properties::{BloomFilterProperties, WriterProperties};
use parquet::schema::types::ColumnPath;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::NamedTempFile;

use crate::ParquetError;

#[derive(Default)]
struct ToggleWriterState {
    bytes: Mutex<Vec<u8>>,
    fail_writes: AtomicBool,
}

struct ToggleWriter {
    state: Arc<ToggleWriterState>,
}

impl Write for ToggleWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if self.state.fail_writes.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("injected output failure"));
        }
        self.state.bytes.lock().unwrap().extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.state.fail_writes.load(Ordering::SeqCst) {
            Err(std::io::Error::other("injected output failure"))
        } else {
            Ok(())
        }
    }
}

fn integer_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn integer_batch() -> RecordBatch {
    RecordBatch::try_new(
        integer_arrow_schema(),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap()
}

fn error_class(error: ParquetError) -> &'static str {
    match error {
        ParquetError::Io(_) => "io",
        ParquetError::Arrow(_) => "arrow",
        ParquetError::Parquet(_) => "parquet",
        ParquetError::Schema(_) => "schema",
        ParquetError::Conversion(_) => "conversion",
        ParquetError::InvalidArgument(_) => "invalid_argument",
        ParquetError::DataValidation(_) => "data_validation",
        ParquetError::Unsupported(_) => "unsupported",
        ParquetError::Internal(_) => "internal",
        ParquetError::Utf8(_) => "utf8",
        ParquetError::ParseInt(_) => "parse_int",
        ParquetError::ParseFloat(_) => "parse_float",
    }
}

#[test]
fn segment_properties_preserve_resolved_bloom_filter_sizes() {
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("first", DataType::Utf8, false),
        Field::new("second", DataType::Int64, false),
    ]));
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .set_bloom_filter_enabled(true)
        .set_bloom_filter_fpp(0.01)
        .build();

    let writer = StreamingFileWriter::new(Vec::new(), arrow_schema, properties, 1).unwrap();
    let actual = ["first", "second"]
        .map(|name| {
            (
                name,
                writer
                    .segment_properties
                    .bloom_filter_properties(&ColumnPath::from(name))
                    .cloned(),
            )
        })
        .to_vec();
    let expected_properties = BloomFilterProperties { fpp: 0.01, ndv: 1 };

    assert_eq!(
        vec![
            ("first", Some(expected_properties.clone())),
            ("second", Some(expected_properties)),
        ],
        actual
    );
}

#[test]
fn metadata_spool_write_failure_is_an_operating_error() {
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .build();
    let mut writer =
        StreamingFileWriter::new(Vec::new(), integer_arrow_schema(), properties, 1).unwrap();
    let writable_spool = NamedTempFile::new().unwrap();
    let (writable_file, spool_path) = writable_spool.into_parts();
    drop(writable_file);
    let read_only_file = OpenOptions::new().read(true).open(&spool_path).unwrap();
    writer.metadata_spool = NamedTempFile::from_parts(read_only_file, spool_path);

    let error = writer.write(&integer_batch()).unwrap_err();

    assert_eq!("io", error_class(error));
}

#[test]
fn footer_write_failure_is_an_operating_error_without_more_output() {
    let state = Arc::new(ToggleWriterState::default());
    let output = ToggleWriter {
        state: Arc::clone(&state),
    };
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .build();
    let mut writer =
        StreamingFileWriter::new(output, integer_arrow_schema(), properties, 1).unwrap();
    writer.write(&integer_batch()).unwrap();
    let bytes_before_failure = state.bytes.lock().unwrap().len();
    state.fail_writes.store(true, Ordering::SeqCst);

    let error = writer.close().unwrap_err();
    let bytes_after_failure = state.bytes.lock().unwrap().len();

    assert_eq!(
        ("io", bytes_before_failure),
        (error_class(error), bytes_after_failure)
    );
}

#[test]
fn row_group_limit_is_checked_before_opening_another_segment() {
    assert_eq!(
        i16::MAX,
        row_group_ordinal(MAX_ROW_GROUPS_PER_FILE - 1).unwrap()
    );
    assert_eq!(
        format!(
            "Invalid argument: Parquet does not support more than \
             {MAX_ROW_GROUPS_PER_FILE} row groups per file"
        ),
        row_group_ordinal(MAX_ROW_GROUPS_PER_FILE)
            .unwrap_err()
            .to_string()
    );
}
