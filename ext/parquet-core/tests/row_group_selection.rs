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

/// Wraps a ChunkReader and panics if any access -- via either get_bytes or
/// the Read stream returned by get_read -- ever touches a byte outside
/// `allowed`. Exists to prove a reader path is safe against a "holes"
/// source: a sparse local file backed by partial HTTP Range fetches (e.g.
/// S3), where only the declared-necessary byte spans actually exist on
/// disk and anything else is a zeroed hole. This is exactly the shape a
/// caller doing remote row-group selection over Range requests needs to
/// trust -- and exactly what the OLD sequential each_row/each_column path
/// (row_groups: not set) does NOT provide: it decodes sequentially from
/// row group 0 and, for some column types (confirmed with timestamp
/// columns against a real multi-row-group production file), reads
/// slightly past a row group's own declared byte span, which is harmless
/// against a real, complete file but fatal against a sparse one.
#[derive(Clone)]
struct RestrictedChunkReader<R> {
    inner: R,
    allowed: std::sync::Arc<Vec<std::ops::Range<u64>>>,
}

impl<R> RestrictedChunkReader<R> {
    fn new(inner: R, allowed: Vec<std::ops::Range<u64>>) -> Self {
        Self {
            inner,
            allowed: std::sync::Arc::new(allowed),
        }
    }

    fn assert_allowed(&self, start: u64, end: u64) {
        assert!(
            self.allowed.iter().any(|r| r.start <= start && end <= r.end),
            "byte range [{start}, {end}) touched a hole -- outside every declared-necessary range {:?}",
            self.allowed
        );
    }
}

impl<R: parquet::file::reader::Length> parquet::file::reader::Length for RestrictedChunkReader<R> {
    fn len(&self) -> u64 {
        self.inner.len()
    }
}

/// A Read wrapper that enforces every byte actually consumed stays inside
/// one allowed range. Checked lazily per-read (not just at get_read's own
/// `start`) since get_read's contract hands back an open-ended stream --
/// the caller decides how far to read from it.
struct BoundedRead<T> {
    inner: T,
    pos: u64,
    allowed: std::sync::Arc<Vec<std::ops::Range<u64>>>,
}

impl<T: std::io::Read> std::io::Read for BoundedRead<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        let end = self.pos + n as u64;
        assert!(
            self.allowed.iter().any(|r| r.start <= self.pos && end <= r.end),
            "sequential read [{}, {}) touched a hole -- outside every declared-necessary range {:?}",
            self.pos, end, self.allowed
        );
        self.pos = end;
        Ok(n)
    }
}

impl<R: parquet::file::reader::ChunkReader> parquet::file::reader::ChunkReader
    for RestrictedChunkReader<R>
{
    type T = BoundedRead<R::T>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        assert!(
            self.allowed.iter().any(|r| r.contains(&start)),
            "read starting at byte {start} touched a hole -- outside every declared-necessary range {:?}",
            self.allowed
        );
        Ok(BoundedRead {
            inner: self.inner.get_read(start)?,
            pos: start,
            allowed: self.allowed.clone(),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        let end = start + length as u64;
        self.assert_allowed(start, end);
        self.inner.get_bytes(start, length)
    }
}

/// The real 8-byte trailer every Parquet file ends with: a 4-byte
/// little-endian footer length, followed by the 4-byte "PAR1" magic.
/// Computed independently of parquet_core/arrow-rs's own metadata parsing
/// so this test's "allowed" set isn't circular (built from the very
/// parsing logic it's meant to check).
fn footer_start(bytes: &bytes::Bytes) -> u64 {
    let len = bytes.len();
    let trailer = &bytes[len - 8..];
    let footer_length = u32::from_le_bytes(trailer[0..4].try_into().unwrap()) as usize;
    (len - 8 - footer_length) as u64
}

/// Every declared-necessary byte range for reading ONLY `row_group_indexes`
/// from `bytes`: each selected row group's column-chunk spans (the actual
/// column data, not the logical row range) plus the file's leading magic
/// and trailing footer -- computed via a fresh, independent
/// ParquetRecordBatchReaderBuilder, mirroring how read_rows_selected itself
/// discovers row-group structure, but kept in this test rather than
/// reused from parquet_core so a bug in the real path can't quietly
/// "agree" with a bug here.
fn allowed_ranges_for(
    bytes: &bytes::Bytes,
    row_group_indexes: &[usize],
) -> Vec<std::ops::Range<u64>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
    let metadata = builder.metadata();

    // leading "PAR1" magic, trailing footer
    let mut ranges: Vec<std::ops::Range<u64>> = vec![0..4, footer_start(bytes)..bytes.len() as u64];

    for &rg_index in row_group_indexes {
        let row_group = metadata.row_group(rg_index);
        for col_index in 0..row_group.num_columns() {
            let (start, length) = row_group.column(col_index).byte_range();
            ranges.push(start..(start + length));
        }
    }

    ranges
}

#[test]
fn read_rows_selected_never_touches_bytes_outside_the_selected_row_group() {
    let bytes = three_row_group_file();
    let allowed = allowed_ranges_for(&bytes, &[1]);

    let restricted = RestrictedChunkReader::new(bytes, allowed);
    let rows: Vec<i64> = Reader::new(restricted)
        .read_rows_selected(None, Some(vec![1]))
        .unwrap()
        .map(|row| match &row.unwrap()[0] {
            ParquetValue::Int64(v) => *v,
            other => panic!("unexpected value: {:?}", other),
        })
        .collect();

    assert_eq!(rows, [3, 4, 5, 6, 7]);
}

#[test]
fn read_rows_selected_never_touches_bytes_outside_multiple_selected_row_groups() {
    let bytes = three_row_group_file();
    let allowed = allowed_ranges_for(&bytes, &[0, 2]);

    let restricted = RestrictedChunkReader::new(bytes, allowed);
    let rows: Vec<i64> = Reader::new(restricted)
        .read_rows_selected(None, Some(vec![0, 2]))
        .unwrap()
        .map(|row| match &row.unwrap()[0] {
            ParquetValue::Int64(v) => *v,
            other => panic!("unexpected value: {:?}", other),
        })
        .collect();

    assert_eq!(rows, [0, 1, 2, 8, 9]);
}

#[test]
#[should_panic(expected = "touched a hole")]
fn sanity_check_the_harness_itself_catches_a_real_violation() {
    // Sanity check for the harness above, not a real API test: the OLD
    // sequential path (row_groups: None) reads the WHOLE file, so
    // restricting to only row group 1's own span must panic -- if it
    // didn't, the two tests above would be proving nothing.
    let bytes = three_row_group_file();
    let allowed = allowed_ranges_for(&bytes, &[1]);
    let restricted = RestrictedChunkReader::new(bytes, allowed);
    let _: Vec<i64> = Reader::new(restricted)
        .read_rows_selected(None, None)
        .unwrap()
        .map(|row| match &row.unwrap()[0] {
            ParquetValue::Int64(v) => *v,
            other => panic!("unexpected value: {:?}", other),
        })
        .collect();
}
