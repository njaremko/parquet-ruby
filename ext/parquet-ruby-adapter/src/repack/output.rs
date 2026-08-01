//! One output Parquet file under construction.
//!
//! A Parquet file is a sequence of row groups, and a row group is a sequence of
//! encoded column chunks. `OutputFile` owns that assembly and accepts chunks
//! from two sources:
//!
//! * [`OutputFile::splice_row_group`] copies an input row group's bytes
//!   verbatim, skipping decode and re-encode entirely;
//! * [`OutputFile::write_batch`] encodes Arrow rows, used when a row group
//!   straddles an output boundary or cannot be spliced.
//!
//! Both paths converge on `SerializedRowGroupWriter::append_column`, so the two
//! sources are interchangeable within a single file and row order is preserved
//! by flushing any partially encoded row group before splicing.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::arrow::arrow_writer::{compute_leaves, ArrowColumnWriter, ArrowRowGroupWriterFactory};
use parquet::basic::Compression;
use parquet::column::writer::ColumnCloseResult;
use parquet::file::metadata::{KeyValue, ParquetMetaData, RowGroupMetaData};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::TypePtr;
use tempfile::{NamedTempFile, TempPath};

use crate::error::{Result, RubyAdapterError};

use super::io_error;
use super::plan::MAX_ROW_GROUPS_PER_FILE;

/// An output file whose bytes are complete but which has not yet been given its
/// final name. Keeping the rename until every output is closed means a failed
/// repack never publishes a partial file under a name a reader would trust.
pub struct CompletedOutput {
    pub temp_path: TempPath,
    pub final_path: PathBuf,
    pub num_rows: usize,
}

/// Rows currently accumulating into the next encoded row group.
struct EncodingRowGroup {
    writers: Vec<ArrowColumnWriter>,
    rows: usize,
}

pub struct OutputFile {
    writer: SerializedFileWriter<File>,
    column_writers: ArrowRowGroupWriterFactory,
    arrow_schema: SchemaRef,
    encoding: Option<EncodingRowGroup>,
    /// Upper bound on rows held in `encoding` before it is flushed. This is the
    /// writer-side counterpart to the reader's chunk bound, and it is what keeps
    /// peak memory independent of total input size.
    max_row_group_rows: usize,
    row_groups_written: usize,
    rows_written: usize,
    temp_file: NamedTempFile,
    final_path: PathBuf,
}

impl OutputFile {
    /// Create the `index`th output as a temporary file inside `output_dir`.
    ///
    /// `root_type` is the first input's Parquet root group, reused verbatim so
    /// the output's column descriptors are byte-identical to the inputs' and
    /// spliced chunks are accepted without translation.
    pub fn create(
        output_dir: &Path,
        final_path: PathBuf,
        root_type: TypePtr,
        arrow_schema: SchemaRef,
        key_value_metadata: Option<Vec<KeyValue>>,
        compression: Compression,
        max_row_group_rows: usize,
    ) -> Result<Self> {
        std::fs::create_dir_all(output_dir).map_err(|source| {
            io_error(
                format!("failed to create output directory {output_dir:?}"),
                source,
            )
        })?;

        let temp_file = NamedTempFile::new_in(output_dir).map_err(|source| {
            io_error(
                format!("failed to create temporary file for {final_path:?}"),
                source,
            )
        })?;
        let handle = temp_file.reopen().map_err(|source| {
            io_error(
                format!("failed to reopen temporary file for {final_path:?}"),
                source,
            )
        })?;

        let properties = WriterProperties::builder()
            .set_compression(compression)
            .set_max_row_group_row_count(Some(max_row_group_rows))
            .set_key_value_metadata(key_value_metadata)
            .build();

        let writer = SerializedFileWriter::new(handle, root_type, properties.into())
            .map_err(|source| parquet_error(&final_path, source))?;
        let column_writers = ArrowRowGroupWriterFactory::new(&writer, arrow_schema.clone());

        Ok(Self {
            writer,
            column_writers,
            arrow_schema,
            encoding: None,
            max_row_group_rows,
            row_groups_written: 0,
            rows_written: 0,
            temp_file,
            final_path,
        })
    }

    pub fn rows_written(&self) -> usize {
        self.rows_written
    }

    /// Whether an input row group of `rows` rows may be spliced in whole.
    ///
    /// Splicing preserves the input's exact encodings, so it is only correct
    /// when the caller has not asked for a different codec, and only possible
    /// when the whole row group fits the remaining budget — a spliced chunk
    /// cannot be cut in half.
    pub fn can_splice(
        &self,
        input_splice_compatible: bool,
        row_group: &RowGroupMetaData,
        compression: Compression,
        rows_remaining: Option<usize>,
    ) -> bool {
        if !input_splice_compatible || self.row_groups_written >= MAX_ROW_GROUPS_PER_FILE {
            return false;
        }

        let rows = row_group.num_rows() as usize;
        if rows == 0 || rows_remaining.is_some_and(|remaining| rows > remaining) {
            return false;
        }

        row_group
            .columns()
            .iter()
            .all(|column| same_codec(column.compression(), compression))
    }

    /// Copy every column chunk of `row_group_index` from `source` into a fresh
    /// output row group without decoding it.
    pub fn splice_row_group(
        &mut self,
        source: &File,
        metadata: &ParquetMetaData,
        row_group_index: usize,
    ) -> Result<()> {
        // Encoded rows read earlier must land before the spliced ones, or the
        // output would reorder rows relative to the inputs.
        self.flush_encoding()?;
        self.reserve_row_group()?;

        let row_group = metadata.row_group(row_group_index);
        let rows = row_group.num_rows() as u64;
        let mut row_group_writer = self
            .writer
            .next_row_group()
            .map_err(|source| parquet_error(&self.final_path, source))?;

        for (column_index, column) in row_group.columns().iter().enumerate() {
            let close = ColumnCloseResult {
                bytes_written: column.compressed_size() as u64,
                // Every column of a row group must agree on the row count; the
                // row group metadata is the authority.
                rows_written: rows,
                metadata: column.clone(),
                bloom_filter: None,
                column_index: metadata
                    .column_index()
                    .and_then(|index| index.get(row_group_index))
                    .and_then(|columns| columns.get(column_index))
                    .cloned(),
                offset_index: metadata
                    .offset_index()
                    .and_then(|index| index.get(row_group_index))
                    .and_then(|columns| columns.get(column_index))
                    .cloned(),
            };

            row_group_writer
                .append_column(source, close)
                .map_err(|source| parquet_error(&self.final_path, source))?;
        }

        row_group_writer
            .close()
            .map_err(|source| parquet_error(&self.final_path, source))?;

        self.row_groups_written += 1;
        self.rows_written += rows as usize;
        Ok(())
    }

    /// Encode `batch` into the row group currently being built, flushing it
    /// first if it has reached its row bound.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        if self
            .encoding
            .as_ref()
            .is_some_and(|open| open.rows >= self.max_row_group_rows)
        {
            self.flush_encoding()?;
        }

        if self.encoding.is_none() {
            self.reserve_row_group()?;
            let writers = self
                .column_writers
                .create_column_writers(self.row_groups_written)
                .map_err(|source| parquet_error(&self.final_path, source))?;
            self.encoding = Some(EncodingRowGroup { writers, rows: 0 });
        }

        let open = self
            .encoding
            .as_mut()
            .expect("an encoding row group was just ensured");

        // Column writers are ordered by leaf, grouped by top-level field, so a
        // single cursor walks them in step with the batch's columns.
        let mut leaf_index = 0;
        for (field, array) in self.arrow_schema.fields().iter().zip(batch.columns()) {
            for leaf in compute_leaves(field, array)
                .map_err(|source| parquet_error(&self.final_path, source))?
            {
                open.writers[leaf_index]
                    .write(&leaf)
                    .map_err(|source| parquet_error(&self.final_path, source))?;
                leaf_index += 1;
            }
        }
        debug_assert_eq!(
            leaf_index,
            open.writers.len(),
            "every leaf column must receive the batch"
        );

        open.rows += batch.num_rows();
        self.rows_written += batch.num_rows();
        Ok(())
    }

    /// Finish the file's bytes. The result still needs persisting under its
    /// final name.
    pub fn finish(mut self) -> Result<CompletedOutput> {
        self.flush_encoding()?;
        self.writer
            .close()
            .map_err(|source| parquet_error(&self.final_path, source))?;

        Ok(CompletedOutput {
            temp_path: self.temp_file.into_temp_path(),
            final_path: self.final_path,
            num_rows: self.rows_written,
        })
    }

    /// Close the in-progress encoded row group, if any, and append it.
    fn flush_encoding(&mut self) -> Result<()> {
        let Some(open) = self.encoding.take() else {
            return Ok(());
        };
        debug_assert!(open.rows > 0, "an empty row group must never be reserved");

        let mut row_group_writer = self
            .writer
            .next_row_group()
            .map_err(|source| parquet_error(&self.final_path, source))?;
        for writer in open.writers {
            writer
                .close()
                .map_err(|source| parquet_error(&self.final_path, source))?
                .append_to_row_group(&mut row_group_writer)
                .map_err(|source| parquet_error(&self.final_path, source))?;
        }
        row_group_writer
            .close()
            .map_err(|source| parquet_error(&self.final_path, source))?;

        self.row_groups_written += 1;
        Ok(())
    }

    /// Refuse to start a row group the Parquet format cannot address.
    ///
    /// Reaching this bound needs a single output holding more than
    /// `MAX_ROW_GROUPS_PER_FILE * max_row_group_rows` rows, which the caller can
    /// always avoid with `rows_per_file:`.
    fn reserve_row_group(&self) -> Result<()> {
        if self.row_groups_written >= MAX_ROW_GROUPS_PER_FILE {
            return Err(RubyAdapterError::runtime(format!(
                "output {:?} reached the Parquet limit of {MAX_ROW_GROUPS_PER_FILE} row groups \
                 per file; pass a smaller rows_per_file: to split the output",
                self.final_path
            )));
        }
        Ok(())
    }
}

/// Whether two compression settings name the same codec.
///
/// Parquet records only the codec in a column chunk, never the level a writer
/// used, so comparing whole `Compression` values would spuriously disable
/// splicing whenever levels differ.
fn same_codec(left: Compression, right: Compression) -> bool {
    std::mem::discriminant(&left) == std::mem::discriminant(&right)
}

fn parquet_error(path: &Path, source: parquet::errors::ParquetError) -> RubyAdapterError {
    RubyAdapterError::runtime(format!("failed writing {path:?}: {source}"))
}

/// `SerializedFileWriter` needs `W: Write + Send`; `File` satisfies both, but
/// state this explicitly so a future change of handle type fails here rather
/// than deep inside the writer.
const _: fn() = || {
    fn assert_writer<W: Write + Send>() {}
    assert_writer::<File>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};

    #[test]
    fn same_codec_ignores_compression_level() {
        assert!(same_codec(
            Compression::ZSTD(ZstdLevel::try_new(1).unwrap()),
            Compression::ZSTD(ZstdLevel::try_new(9).unwrap())
        ));
        assert!(same_codec(
            Compression::GZIP(GzipLevel::default()),
            Compression::GZIP(GzipLevel::try_new(9).unwrap())
        ));
        assert!(same_codec(Compression::SNAPPY, Compression::SNAPPY));
    }

    #[test]
    fn same_codec_separates_distinct_codecs() {
        assert!(!same_codec(
            Compression::SNAPPY,
            Compression::ZSTD(ZstdLevel::default())
        ));
        assert!(!same_codec(Compression::UNCOMPRESSED, Compression::SNAPPY));
        assert!(!same_codec(Compression::LZ4, Compression::LZ4_RAW));
        assert!(!same_codec(
            Compression::BROTLI(BrotliLevel::default()),
            Compression::GZIP(GzipLevel::default())
        ));
    }
}
