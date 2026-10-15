//! parquet-rs and arrow-rs panic on some damaged files instead of returning an
//! error. Every read reports such a panic as `ParquetError::ReadPanic`, so that
//! it never unwinds into the caller. These tests simulate the panic with a
//! chunk reader that panics part way through the file, since which damage makes
//! parquet-rs panic changes from one release to the next.

use bytes::Bytes;
use parquet::file::reader::{ChunkReader, Length};
use parquet_core::reader::read_metadata;
use parquet_core::*;

/// Serves a valid file, but panics when asked for any byte before `panic_below`.
#[derive(Clone)]
struct PanickingChunkReader {
    bytes: Bytes,
    panic_below: u64,
}

impl PanickingChunkReader {
    /// Panics on every read, so that opening the file panics.
    fn everywhere() -> Self {
        let bytes = valid_file();
        let panic_below = bytes.len() as u64;
        Self { bytes, panic_below }
    }

    /// Serves the footer, so that opening the file succeeds, and panics when a
    /// page is read.
    fn on_pages() -> Self {
        let bytes = valid_file();
        let footer_length =
            u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap());
        let panic_below = bytes.len() as u64 - 8 - u64::from(footer_length);
        Self { bytes, panic_below }
    }

    fn check(&self, start: u64) {
        if start < self.panic_below {
            panic!("simulated panic reading offset {start}");
        }
    }
}

impl Length for PanickingChunkReader {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
}

impl ChunkReader for PanickingChunkReader {
    type T = <Bytes as ChunkReader>::T;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        self.check(start);
        self.bytes.get_read(start)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        self.check(start);
        self.bytes.get_bytes(start, length)
    }
}

fn valid_file() -> Bytes {
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
        let mut writer = Writer::new(&mut buffer, schema).unwrap();
        writer
            .write_rows((1..=3).map(|id| vec![ParquetValue::Int64(id)]).collect())
            .unwrap();
        writer.close().unwrap();
    }
    Bytes::from(buffer)
}

/// Returns the panic message that `result` reports, failing unless it reports
/// a panic.
fn read_panic_message<T>(result: Result<T>) -> String {
    match result {
        Err(ParquetError::ReadPanic(message)) => message,
        Err(error) => panic!("expected a read panic, got: {error}"),
        Ok(_) => panic!("expected a read panic, got a value"),
    }
}

#[test]
fn every_open_reports_a_panic_as_an_error() {
    let source = PanickingChunkReader::everywhere();
    let columns = ["id".to_string()];
    let messages = [
        read_panic_message(read_metadata(&source)),
        read_panic_message(Reader::new(source.clone()).metadata()),
        read_panic_message(Reader::new(source.clone()).read_rows()),
        read_panic_message(Reader::new(source.clone()).read_rows_with_projection(&columns)),
        read_panic_message(Reader::new(source.clone()).read_columns(None)),
        read_panic_message(
            Reader::new(source.clone()).read_columns_with_projection(&columns, None),
        ),
    ];

    for message in messages {
        assert!(
            message.starts_with("simulated panic reading offset"),
            "{message}"
        );
    }
}

#[test]
fn row_iteration_reports_a_panic_as_an_error_and_then_ends() {
    let mut rows = Reader::new(PanickingChunkReader::on_pages())
        .read_rows()
        .unwrap();

    let message = read_panic_message(rows.next().expect("an error for the panic"));

    assert!(
        message.starts_with("simulated panic reading offset"),
        "{message}"
    );
    assert!(rows.next().is_none());
}

#[test]
fn column_iteration_reports_a_panic_as_an_error_and_then_ends() {
    let mut batches = Reader::new(PanickingChunkReader::on_pages())
        .read_columns(None)
        .unwrap();

    let message = read_panic_message(batches.next().expect("an error for the panic"));

    assert!(
        message.starts_with("simulated panic reading offset"),
        "{message}"
    );
    assert!(batches.next().is_none());
}

#[test]
fn a_read_panic_says_the_file_could_not_be_read_and_keeps_the_panic_message() {
    let formatted = ParquetError::read_panic(Box::new(format!("offset {} out of bounds", 7)));
    let literal =
        ParquetError::read_panic(Box::new("column start and length should not be negative"));
    let opaque = ParquetError::read_panic(Box::new(42));

    assert_eq!(
        formatted.to_string(),
        "Failed to read Parquet file (reader panicked): offset 7 out of bounds"
    );
    assert_eq!(
        literal.to_string(),
        "Failed to read Parquet file (reader panicked): column start and length should not be negative"
    );
    assert_eq!(
        opaque.to_string(),
        "Failed to read Parquet file (reader panicked): the panic carried no message"
    );
}
