use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::arrow::{ArrowSchemaConverter, ArrowWriter};
use parquet::file::properties::WriterProperties;
#[expect(
    deprecated,
    reason = "parquet-rs 58 exposes its generated thrift wire types through this module"
)]
use parquet::format::{FileMetaData, OffsetIndex, RowGroup};
use parquet::thrift::TSerializable;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex, MutexGuard};
use tempfile::NamedTempFile;
use thrift::protocol::{
    TCompactInputProtocol, TCompactOutputProtocol, TFieldIdentifier, TListIdentifier,
    TOutputProtocol, TStructIdentifier, TType,
};

use crate::{ParquetError, Result};

const PARQUET_MAGIC: &[u8; 4] = b"PAR1";
const FOOTER_TRAILER_BYTES: u64 = 8;
const MAX_ROW_GROUPS_PER_FILE: usize = i16::MAX as usize + 1;

/// Parquet file assembly with disk-backed footer metadata.
///
/// parquet-rs's `SerializedFileWriter` retains every completed row group's
/// metadata until close. This owner instead encodes one row group into a bounded
/// temporary file, copies its bytes to the destination, and serializes its
/// adjusted thrift metadata immediately to a disk spool. Finalization streams
/// that spool into the footer without rebuilding the metadata list in memory.
#[expect(
    deprecated,
    reason = "the bounded writer quarantines parquet-rs 58 generated thrift metadata here"
)]
pub(crate) struct StreamingFileWriter<W: Write> {
    output: SharedOutput<W>,
    metadata_spool: NamedTempFile,
    arrow_schema: SchemaRef,
    segment_properties: WriterProperties,
    row_group_target_bytes: usize,
    row_group_row_limit: Option<usize>,
    segment: Option<OpenSegment>,
    footer_template: Option<FileMetaData>,
    row_groups_written: usize,
    rows_written: i64,
}

struct OpenSegment {
    temp_file: NamedTempFile,
    writer: ArrowWriter<File>,
    rows: usize,
}

struct CountedOutput<W> {
    inner: W,
    bytes_written: u64,
}

// Compact Thrift owns its transport while the pre-serialized row-group list is
// injected into the same footer. These aliases are never used concurrently;
// Arc<Mutex<_>> preserves the writer's Send contract while keeping one byte
// position owner across protocol and raw-copy phases.
struct SharedOutput<W>(Arc<Mutex<CountedOutput<W>>>);

impl<W> Clone for SharedOutput<W> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<W: Write> SharedOutput<W> {
    fn new(inner: W) -> Self {
        Self(Arc::new(Mutex::new(CountedOutput {
            inner,
            bytes_written: 0,
        })))
    }

    fn lock(&self) -> std::io::Result<MutexGuard<'_, CountedOutput<W>>> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("Parquet output mutex was poisoned"))
    }

    fn bytes_written(&self) -> std::io::Result<u64> {
        Ok(self.lock()?.bytes_written)
    }
}

impl<W: Write> Write for SharedOutput<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.lock()?.write(buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.lock()?.flush()
    }
}

impl<W: Write> Write for CountedOutput<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.bytes_written = self
            .bytes_written
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::other("Parquet output position overflowed u64"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<W> StreamingFileWriter<W>
where
    W: Write + Send,
{
    pub(crate) fn new(
        output: W,
        arrow_schema: SchemaRef,
        properties: WriterProperties,
        row_group_target_bytes: usize,
    ) -> Result<Self> {
        let row_group_row_limit = properties.max_row_group_row_count();
        let segment_properties = segment_properties(&arrow_schema, properties)?;
        let metadata_spool = NamedTempFile::new()?;
        let mut output = SharedOutput::new(output);
        output.write_all(PARQUET_MAGIC)?;

        Ok(Self {
            output,
            metadata_spool,
            arrow_schema,
            segment_properties,
            row_group_target_bytes,
            row_group_row_limit,
            segment: None,
            footer_template: None,
            row_groups_written: 0,
            rows_written: 0,
        })
    }

    pub(crate) fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut offset = 0;
        while offset < batch.num_rows() {
            if self
                .row_group_row_limit
                .is_some_and(|limit| self.segment.as_ref().is_some_and(|open| open.rows >= limit))
            {
                self.finish_segment()?;
            }

            let remaining = batch.num_rows() - offset;
            let rows_to_write = match (self.row_group_row_limit, self.segment.as_ref()) {
                (Some(limit), Some(open)) => remaining.min(limit - open.rows),
                (Some(limit), None) => remaining.min(limit),
                (None, _) => remaining,
            };
            let batch = batch.slice(offset, rows_to_write);
            let row_group_row_limit = self.row_group_row_limit;
            let row_group_target_bytes = self.row_group_target_bytes;
            let segment = self.open_segment()?;
            segment.writer.write(&batch)?;
            segment.rows = segment.rows.checked_add(rows_to_write).ok_or_else(|| {
                ParquetError::Internal("row-group row count overflowed".to_string())
            })?;
            offset += rows_to_write;

            let reached_row_limit = row_group_row_limit.is_some_and(|limit| segment.rows >= limit);
            let reached_byte_limit = segment.writer.in_progress_size() >= row_group_target_bytes
                || segment.writer.memory_size() >= row_group_target_bytes;
            if reached_row_limit || reached_byte_limit {
                self.finish_segment()?;
            }
        }
        Ok(())
    }

    pub(crate) fn flush_row_group(&mut self) -> Result<()> {
        self.finish_segment()
    }

    pub(crate) fn close(mut self) -> Result<()> {
        self.finish_segment()?;
        if self.footer_template.is_none() {
            self.open_segment()?;
            self.finish_segment()?;
        }
        self.write_footer()
    }

    fn open_segment(&mut self) -> Result<&mut OpenSegment> {
        if self.segment.is_none() {
            row_group_ordinal(self.row_groups_written)?;
            let temp_file = NamedTempFile::new()?;
            let handle = temp_file.reopen()?;
            let writer = ArrowWriter::try_new(
                handle,
                Arc::clone(&self.arrow_schema),
                Some(self.segment_properties.clone()),
            )?;
            self.segment = Some(OpenSegment {
                temp_file,
                writer,
                rows: 0,
            });
        }
        Ok(self.segment.as_mut().expect("segment was just created"))
    }

    #[expect(
        deprecated,
        reason = "the bounded writer must adjust parquet-rs 58 generated thrift metadata"
    )]
    fn finish_segment(&mut self) -> Result<()> {
        let Some(segment) = self.segment.take() else {
            return Ok(());
        };
        segment.writer.close()?;

        let (mut metadata, footer_start) = read_segment_footer(segment.temp_file.as_file())?;
        if self.footer_template.is_none() {
            let mut template = metadata.clone();
            template.row_groups.clear();
            template.num_rows = 0;
            self.footer_template = Some(template);
        }
        if metadata.row_groups.is_empty() {
            return Ok(());
        }

        let output_start = self.output.bytes_written()?;
        copy_segment_payload(segment.temp_file.as_file(), footer_start, &self.output)?;
        let offset_delta = i64::try_from(output_start)
            .map_err(|_| ParquetError::Internal("Parquet output offset exceeds i64".to_string()))?
            .checked_sub(PARQUET_MAGIC.len() as i64)
            .ok_or_else(|| ParquetError::Internal("invalid Parquet output offset".to_string()))?;

        for mut row_group in metadata.row_groups.drain(..) {
            let ordinal = row_group_ordinal(self.row_groups_written)?;
            rewrite_offset_indexes(
                segment.temp_file.as_file(),
                footer_start,
                &mut row_group,
                offset_delta,
                &mut self.output,
            )?;
            shift_row_group_offsets(&mut row_group, offset_delta)?;
            row_group.ordinal = Some(ordinal);
            self.rows_written = self
                .rows_written
                .checked_add(row_group.num_rows)
                .ok_or_else(|| {
                    ParquetError::InvalidArgument("file row count exceeds i64".to_string())
                })?;
            write_row_group_metadata(self.metadata_spool.as_file_mut(), &row_group)?;
            self.row_groups_written += 1;
        }
        Ok(())
    }

    #[expect(
        deprecated,
        reason = "the bounded writer must stream parquet-rs 58 generated thrift metadata"
    )]
    fn write_footer(&mut self) -> Result<()> {
        let mut template = self
            .footer_template
            .take()
            .expect("an empty segment creates the footer template");
        template.num_rows = self.rows_written;
        let footer_start = self.output.bytes_written()?;
        let mut protocol = TCompactOutputProtocol::new(self.output.clone());
        protocol
            .write_struct_begin(&TStructIdentifier::new("FileMetaData"))
            .map_err(thrift_error)?;

        write_i32_field(&mut protocol, "version", 1, template.version)?;
        write_struct_list_field(&mut protocol, "schema", 2, &template.schema)?;
        write_i64_field(&mut protocol, "num_rows", 3, template.num_rows)?;

        protocol
            .write_field_begin(&TFieldIdentifier::new("row_groups", TType::List, 4))
            .map_err(thrift_error)?;
        protocol
            .write_list_begin(&TListIdentifier::new(
                TType::Struct,
                self.row_groups_written as i32,
            ))
            .map_err(thrift_error)?;
        protocol.flush().map_err(thrift_error)?;

        self.metadata_spool.as_file_mut().seek(SeekFrom::Start(0))?;
        {
            let mut output = self.output.lock()?;
            std::io::copy(self.metadata_spool.as_file_mut(), &mut *output)?;
        }

        protocol.write_list_end().map_err(thrift_error)?;
        protocol.write_field_end().map_err(thrift_error)?;
        if let Some(values) = &template.key_value_metadata {
            write_struct_list_field(&mut protocol, "key_value_metadata", 5, values)?;
        }
        if let Some(value) = &template.created_by {
            write_string_field(&mut protocol, "created_by", 6, value)?;
        }
        if let Some(values) = &template.column_orders {
            write_struct_list_field(&mut protocol, "column_orders", 7, values)?;
        }
        if let Some(value) = &template.encryption_algorithm {
            write_struct_field(&mut protocol, "encryption_algorithm", 8, value)?;
        }
        if let Some(value) = &template.footer_signing_key_metadata {
            protocol
                .write_field_begin(&TFieldIdentifier::new(
                    "footer_signing_key_metadata",
                    TType::String,
                    9,
                ))
                .map_err(thrift_error)?;
            protocol.write_bytes(value).map_err(thrift_error)?;
            protocol.write_field_end().map_err(thrift_error)?;
        }
        protocol.write_field_stop().map_err(thrift_error)?;
        protocol.write_struct_end().map_err(thrift_error)?;
        protocol.flush().map_err(thrift_error)?;
        drop(protocol);

        let footer_bytes = self
            .output
            .bytes_written()?
            .checked_sub(footer_start)
            .ok_or_else(|| ParquetError::Internal("invalid Parquet footer length".to_string()))?;
        let footer_bytes = u32::try_from(footer_bytes).map_err(|_| {
            ParquetError::InvalidArgument("Parquet footer exceeds the u32 format limit".to_string())
        })?;
        self.output.write_all(&footer_bytes.to_le_bytes())?;
        self.output.write_all(PARQUET_MAGIC)?;
        self.output.flush()?;
        Ok(())
    }
}

fn segment_properties(
    arrow_schema: &arrow_schema::Schema,
    properties: WriterProperties,
) -> Result<WriterProperties> {
    let parquet_schema = ArrowSchemaConverter::new()
        .with_coerce_types(properties.coerce_types())
        .convert(arrow_schema)?;
    let resolved_bloom_filters = parquet_schema
        .columns()
        .iter()
        .filter_map(|column| {
            properties
                .bloom_filter_properties(column.path())
                .cloned()
                .map(|bloom_filter| (column.path().clone(), bloom_filter))
        })
        .collect::<Vec<_>>();

    let mut builder = properties
        .into_builder()
        .set_max_row_group_row_count(None)
        .set_max_row_group_bytes(None);
    for (column_path, bloom_filter) in resolved_bloom_filters {
        builder = builder
            .set_column_bloom_filter_fpp(column_path.clone(), bloom_filter.fpp)
            .set_column_bloom_filter_ndv(column_path, bloom_filter.ndv);
    }
    Ok(builder.build())
}

#[expect(
    deprecated,
    reason = "the bounded writer must read parquet-rs 58 generated thrift metadata"
)]
fn read_segment_footer(file: &File) -> Result<(FileMetaData, u64)> {
    let mut file = file.try_clone()?;
    let file_bytes = file.metadata()?.len();
    if file_bytes < PARQUET_MAGIC.len() as u64 + FOOTER_TRAILER_BYTES {
        return Err(ParquetError::Internal(
            "temporary Parquet row group is missing a footer".to_string(),
        ));
    }
    file.seek(SeekFrom::End(-(FOOTER_TRAILER_BYTES as i64)))?;
    let mut trailer = [0u8; FOOTER_TRAILER_BYTES as usize];
    file.read_exact(&mut trailer)?;
    if &trailer[4..] != PARQUET_MAGIC {
        return Err(ParquetError::Internal(
            "temporary Parquet row group has invalid magic bytes".to_string(),
        ));
    }
    let footer_bytes =
        u32::from_le_bytes(trailer[..4].try_into().expect("four-byte footer length"));
    let footer_start = file_bytes
        .checked_sub(FOOTER_TRAILER_BYTES)
        .and_then(|position| position.checked_sub(footer_bytes as u64))
        .ok_or_else(|| {
            ParquetError::Internal("invalid temporary Parquet footer length".to_string())
        })?;
    file.seek(SeekFrom::Start(footer_start))?;
    let mut footer = file.take(footer_bytes as u64);
    let mut protocol = TCompactInputProtocol::new(&mut footer);
    let metadata = FileMetaData::read_from_in_protocol(&mut protocol).map_err(thrift_error)?;
    Ok((metadata, footer_start))
}

fn copy_segment_payload<W: Write>(
    file: &File,
    footer_start: u64,
    output: &SharedOutput<W>,
) -> Result<()> {
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(PARQUET_MAGIC.len() as u64))?;
    let payload_bytes = footer_start
        .checked_sub(PARQUET_MAGIC.len() as u64)
        .ok_or_else(|| ParquetError::Internal("invalid temporary Parquet payload".to_string()))?;
    let copied = {
        let mut output = output.lock()?;
        std::io::copy(&mut file.take(payload_bytes), &mut *output)?
    };
    if copied != payload_bytes {
        return Err(ParquetError::Internal(format!(
            "temporary Parquet payload ended early: expected {payload_bytes} bytes, copied {copied}"
        )));
    }
    Ok(())
}

#[expect(
    deprecated,
    reason = "the bounded writer must adjust parquet-rs 58 generated thrift metadata"
)]
fn shift_row_group_offsets(row_group: &mut RowGroup, delta: i64) -> Result<()> {
    shift_optional_offset(&mut row_group.file_offset, delta)?;
    for column in &mut row_group.columns {
        if column.file_path.is_some() {
            return Err(ParquetError::Internal(
                "temporary Parquet column unexpectedly references another file".to_string(),
            ));
        }
        if column.file_offset != 0 {
            return Err(ParquetError::Internal(
                "temporary Parquet column metadata was written outside the footer".to_string(),
            ));
        }
        shift_optional_offset(&mut column.column_index_offset, delta)?;
        if let Some(metadata) = &mut column.meta_data {
            shift_offset(&mut metadata.data_page_offset, delta)?;
            shift_optional_offset(&mut metadata.index_page_offset, delta)?;
            shift_optional_offset(&mut metadata.dictionary_page_offset, delta)?;
            shift_optional_offset(&mut metadata.bloom_filter_offset, delta)?;
        }
    }
    Ok(())
}

#[expect(
    deprecated,
    reason = "the bounded writer must adjust parquet-rs 58 generated thrift metadata"
)]
fn rewrite_offset_indexes<W: Write>(
    segment: &File,
    footer_start: u64,
    row_group: &mut RowGroup,
    offset_delta: i64,
    output: &mut SharedOutput<W>,
) -> Result<()> {
    for column in &mut row_group.columns {
        let source = match (column.offset_index_offset, column.offset_index_length) {
            (Some(offset), Some(length)) => Some((offset, length)),
            (None, None) => None,
            _ => {
                return Err(ParquetError::Internal(
                    "temporary Parquet offset index has incomplete location metadata".to_string(),
                ));
            }
        };
        let Some((source_offset, source_length)) = source else {
            continue;
        };

        let mut offset_index =
            read_offset_index(segment, footer_start, source_offset, source_length)?;
        for page in &mut offset_index.page_locations {
            shift_offset(&mut page.offset, offset_delta)?;
        }

        let output_offset = i64::try_from(output.bytes_written()?)
            .map_err(|_| ParquetError::Internal("Parquet output offset exceeds i64".to_string()))?;
        let mut protocol = TCompactOutputProtocol::new(output.clone());
        offset_index
            .write_to_out_protocol(&mut protocol)
            .map_err(thrift_error)?;
        protocol.flush().map_err(thrift_error)?;
        drop(protocol);
        let output_end = output.bytes_written()?;
        let output_length = output_end
            .checked_sub(output_offset as u64)
            .and_then(|length| i32::try_from(length).ok())
            .ok_or_else(|| {
                ParquetError::InvalidArgument(
                    "Parquet offset index exceeds the i32 format limit".to_string(),
                )
            })?;
        column.offset_index_offset = Some(output_offset);
        column.offset_index_length = Some(output_length);
    }
    Ok(())
}

#[expect(
    deprecated,
    reason = "the bounded writer must read parquet-rs 58 generated thrift metadata"
)]
fn read_offset_index(
    segment: &File,
    footer_start: u64,
    source_offset: i64,
    source_length: i32,
) -> Result<OffsetIndex> {
    let source_offset = u64::try_from(source_offset).map_err(|_| {
        ParquetError::Internal("temporary Parquet offset index has a negative offset".to_string())
    })?;
    let source_length = u64::try_from(source_length).map_err(|_| {
        ParquetError::Internal("temporary Parquet offset index has a negative length".to_string())
    })?;
    let source_end = source_offset.checked_add(source_length).ok_or_else(|| {
        ParquetError::Internal("temporary Parquet offset index location overflowed".to_string())
    })?;
    if source_end > footer_start {
        return Err(ParquetError::Internal(
            "temporary Parquet offset index overlaps its footer".to_string(),
        ));
    }

    let mut source = segment.try_clone()?;
    source.seek(SeekFrom::Start(source_offset))?;
    let mut source = source.take(source_length);
    let mut protocol = TCompactInputProtocol::new(&mut source);
    OffsetIndex::read_from_in_protocol(&mut protocol).map_err(thrift_error)
}

fn row_group_ordinal(row_groups_written: usize) -> Result<i16> {
    i16::try_from(row_groups_written).map_err(|_| {
        ParquetError::InvalidArgument(format!(
            "Parquet does not support more than {MAX_ROW_GROUPS_PER_FILE} row groups per file"
        ))
    })
}

fn shift_optional_offset(offset: &mut Option<i64>, delta: i64) -> Result<()> {
    if let Some(offset) = offset {
        shift_offset(offset, delta)?;
    }
    Ok(())
}

fn shift_offset(offset: &mut i64, delta: i64) -> Result<()> {
    *offset = offset
        .checked_add(delta)
        .ok_or_else(|| ParquetError::Internal("Parquet file offset exceeds i64".to_string()))?;
    Ok(())
}

#[expect(
    deprecated,
    reason = "the bounded writer must spool parquet-rs 58 generated thrift metadata"
)]
fn write_row_group_metadata(file: &mut File, row_group: &RowGroup) -> Result<()> {
    let mut protocol = TCompactOutputProtocol::new(file);
    row_group
        .write_to_out_protocol(&mut protocol)
        .map_err(thrift_error)?;
    protocol.flush().map_err(thrift_error)
}

fn write_i32_field(
    protocol: &mut impl TOutputProtocol,
    name: &'static str,
    id: i16,
    value: i32,
) -> Result<()> {
    protocol
        .write_field_begin(&TFieldIdentifier::new(name, TType::I32, id))
        .map_err(thrift_error)?;
    protocol.write_i32(value).map_err(thrift_error)?;
    protocol.write_field_end().map_err(thrift_error)
}

fn write_i64_field(
    protocol: &mut impl TOutputProtocol,
    name: &'static str,
    id: i16,
    value: i64,
) -> Result<()> {
    protocol
        .write_field_begin(&TFieldIdentifier::new(name, TType::I64, id))
        .map_err(thrift_error)?;
    protocol.write_i64(value).map_err(thrift_error)?;
    protocol.write_field_end().map_err(thrift_error)
}

fn write_string_field(
    protocol: &mut impl TOutputProtocol,
    name: &'static str,
    id: i16,
    value: &str,
) -> Result<()> {
    protocol
        .write_field_begin(&TFieldIdentifier::new(name, TType::String, id))
        .map_err(thrift_error)?;
    protocol.write_string(value).map_err(thrift_error)?;
    protocol.write_field_end().map_err(thrift_error)
}

fn write_struct_field<T: TSerializable>(
    protocol: &mut impl TOutputProtocol,
    name: &'static str,
    id: i16,
    value: &T,
) -> Result<()> {
    protocol
        .write_field_begin(&TFieldIdentifier::new(name, TType::Struct, id))
        .map_err(thrift_error)?;
    value
        .write_to_out_protocol(protocol)
        .map_err(thrift_error)?;
    protocol.write_field_end().map_err(thrift_error)
}

fn write_struct_list_field<T: TSerializable>(
    protocol: &mut impl TOutputProtocol,
    name: &'static str,
    id: i16,
    values: &[T],
) -> Result<()> {
    let length = i32::try_from(values.len())
        .map_err(|_| ParquetError::Internal(format!("{name} exceeds thrift's i32 list limit")))?;
    protocol
        .write_field_begin(&TFieldIdentifier::new(name, TType::List, id))
        .map_err(thrift_error)?;
    protocol
        .write_list_begin(&TListIdentifier::new(TType::Struct, length))
        .map_err(thrift_error)?;
    for value in values {
        value
            .write_to_out_protocol(protocol)
            .map_err(thrift_error)?;
    }
    protocol.write_list_end().map_err(thrift_error)?;
    protocol.write_field_end().map_err(thrift_error)
}

fn thrift_error(error: thrift::Error) -> ParquetError {
    match error {
        thrift::Error::Transport(error) => ParquetError::Io(std::io::Error::other(format!(
            "failed to access Parquet metadata storage: {}",
            error.message
        ))),
        error => ParquetError::Internal(format!("failed to encode Parquet metadata: {error}")),
    }
}

#[cfg(test)]
#[path = "streaming_file_writer_test.rs"]
mod tests;
