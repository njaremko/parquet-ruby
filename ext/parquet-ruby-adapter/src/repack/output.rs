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
use parquet::column::writer::ColumnCloseResult;
use parquet::file::metadata::{KeyValue, ParquetMetaData, RowGroupMetaData};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::TypePtr;
use tempfile::{NamedTempFile, TempPath};

use crate::error::{Result, RubyAdapterError};

use super::io_error;
use super::plan::{OutputCodec, MAX_ROW_GROUPS_PER_FILE};

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
    /// Whether this file builds a page index. Fixed at creation because every
    /// row group in a Parquet file must agree.
    write_page_index: bool,
    /// Upper bound on rows held in `encoding` before it is flushed. This is the
    /// writer-side counterpart to the reader's chunk bound, and it is what keeps
    /// peak memory independent of total input size.
    max_row_group_rows: usize,
    row_groups_written: usize,
    rows_written: usize,
    temp_file: NamedTempFile,
    final_path: PathBuf,
}

/// The parts of an output file that the plan fixes once and every output of a
/// run shares. Borrowed so rotating to the next output costs no copies beyond
/// what the writer itself must own.
pub struct OutputSpec<'a> {
    /// The first input's Parquet root group, reused verbatim so the output's
    /// column descriptors are byte-identical to the inputs' and copied chunks
    /// are accepted without translation.
    pub root_type: &'a TypePtr,
    pub arrow_schema: &'a SchemaRef,
    pub key_value_metadata: &'a Option<Vec<KeyValue>>,
    pub codec: &'a OutputCodec,
    pub write_page_index: bool,
    pub max_row_group_rows: usize,
}

impl OutputFile {
    /// Create one output as a temporary file inside `output_dir`.
    pub fn create(output_dir: &Path, final_path: PathBuf, spec: &OutputSpec<'_>) -> Result<Self> {
        let OutputSpec {
            root_type,
            arrow_schema,
            key_value_metadata,
            codec,
            write_page_index,
            max_row_group_rows,
        } = *spec;
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

        let mut builder = WriterProperties::builder()
            .set_max_row_group_row_count(Some(max_row_group_rows))
            .set_key_value_metadata(key_value_metadata.clone());
        builder = match codec {
            OutputCodec::Force(compression) => builder.set_compression(*compression),
            // Per-column so a re-encoded chunk lands on the same codec the
            // corresponding spliced chunks keep, making "preserve" true whichever
            // path a given row group takes.
            OutputCodec::Preserve {
                per_column,
                default,
            } => per_column.iter().fold(
                builder.set_compression(*default),
                |builder, (path, compression)| {
                    builder.set_column_compression(path.clone(), *compression)
                },
            ),
        };
        if !write_page_index {
            // A spliced row group can only contribute the index its source had,
            // so when any input lacks one the output must not build one either:
            // the footer writer cannot represent a file where some row groups
            // have an offset index and others do not, and panics trying.
            // Chunk-level statistics are unaffected; only page-level ones go.
            builder = builder
                .set_offset_index_disabled(true)
                .set_statistics_enabled(EnabledStatistics::Chunk);
        }

        let writer = SerializedFileWriter::new(handle, root_type.clone(), builder.build().into())
            .map_err(|source| parquet_error(&final_path, source))?;
        let column_writers = ArrowRowGroupWriterFactory::new(&writer, arrow_schema.clone());
        let arrow_schema = arrow_schema.clone();

        Ok(Self {
            writer,
            column_writers,
            arrow_schema,
            encoding: None,
            write_page_index,
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

    pub fn row_groups_written(&self) -> usize {
        self.row_groups_written
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
                // Bloom filters live outside the footer and would need a
                // separate read of the source. Dropping them matches the
                // re-encode path, which does not build them either, so both
                // strategies produce the same optional structures.
                bloom_filter: None,
                // Only pass indexes through when the output builds them at all.
                // Every row group in a file must agree, and `write_page_index`
                // is false exactly when some contributing input has none.
                column_index: self
                    .write_page_index
                    .then(|| {
                        metadata
                            .column_index()
                            .and_then(|index| index.get(row_group_index))
                            .and_then(|columns| columns.get(column_index))
                            .cloned()
                    })
                    .flatten(),
                offset_index: self
                    .write_page_index
                    .then(|| {
                        metadata
                            .offset_index()
                            .and_then(|index| index.get(row_group_index))
                            .and_then(|columns| columns.get(column_index))
                            .cloned()
                    })
                    .flatten(),
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
        // Not a debug assertion: a batch with fewer columns than the schema
        // would silently write a row group whose column chunks disagree on
        // value counts, i.e. a corrupt file rather than a crash.
        assert_eq!(
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

/// Everything the splice decision depends on, gathered by the caller from the
/// plan and the open output.
///
/// This is a value so [`can_splice`] can be a pure predicate. It used to be a
/// method that materialised an output file in order to answer, which meant a
/// query could publish an empty file.
pub struct SpliceBudget<'a> {
    pub input_splice_compatible: bool,
    /// Borrowed: the decision runs once per input row group, and `Preserve`
    /// carries a per-column table that must not be cloned that often.
    pub codec: &'a OutputCodec,
    /// Rows the open output may still accept; `None` when unbounded.
    pub rows_remaining: Option<usize>,
    /// Row groups the open output may still start.
    pub row_groups_remaining: usize,
    /// Row groups smaller than this are merged by the re-encode path instead.
    pub min_spliceable_rows: usize,
}

/// Whether an input row group may be copied into the output verbatim.
///
/// Copying is only possible when the whole group fits the remaining budget — a
/// spliced chunk cannot be cut in half — and only worthwhile when the group is
/// large enough that the output does not inherit a pathological layout.
pub fn can_splice(row_group: &RowGroupMetaData, budget: &SpliceBudget<'_>) -> bool {
    if !budget.input_splice_compatible || budget.row_groups_remaining == 0 {
        return false;
    }

    let rows = row_group.num_rows() as usize;
    if rows < budget.min_spliceable_rows {
        return false;
    }
    if budget
        .rows_remaining
        .is_some_and(|remaining| rows > remaining)
    {
        return false;
    }

    row_group
        .columns()
        .iter()
        .all(|column| budget.codec.accepts_spliced(column.compression()))
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
    use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
    use parquet::file::metadata::ColumnChunkMetaData;
    use parquet::schema::types::{SchemaDescriptor, Type};
    use std::sync::Arc;

    fn budget(codec: &OutputCodec) -> SpliceBudget<'_> {
        SpliceBudget {
            input_splice_compatible: true,
            codec,
            rows_remaining: None,
            row_groups_remaining: MAX_ROW_GROUPS_PER_FILE,
            min_spliceable_rows: 100,
        }
    }

    /// A row group of `rows` rows whose single column uses `codec`.
    fn row_group(rows: i64, codec: Compression) -> RowGroupMetaData {
        let leaf = Arc::new(
            Type::primitive_type_builder("id", parquet::basic::Type::INT64)
                .build()
                .unwrap(),
        );
        let root = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![leaf])
                .build()
                .unwrap(),
        );
        let descriptor = Arc::new(SchemaDescriptor::new(root));
        let column = ColumnChunkMetaData::builder(descriptor.column(0))
            .set_compression(codec)
            .build()
            .unwrap();
        RowGroupMetaData::builder(descriptor)
            .set_num_rows(rows)
            .set_column_metadata(vec![column])
            .build()
            .unwrap()
    }

    fn snappy() -> OutputCodec {
        OutputCodec::Force(Compression::SNAPPY)
    }

    fn preserve() -> OutputCodec {
        OutputCodec::Preserve {
            per_column: Vec::new(),
            default: Compression::SNAPPY,
        }
    }

    #[test]
    fn splices_a_large_enough_group_with_a_matching_codec() {
        let codec = snappy();
        assert!(can_splice(
            &row_group(1_000, Compression::SNAPPY),
            &budget(&codec)
        ));
    }

    #[test]
    fn refuses_groups_below_the_size_floor() {
        // Copying tiny groups would reproduce a fragmented layout in the output.
        let codec = snappy();
        assert!(!can_splice(
            &row_group(99, Compression::SNAPPY),
            &budget(&codec)
        ));
        assert!(!can_splice(
            &row_group(0, Compression::SNAPPY),
            &budget(&codec)
        ));
        assert!(can_splice(
            &row_group(100, Compression::SNAPPY),
            &budget(&codec)
        ));
    }

    #[test]
    fn refuses_a_group_that_does_not_fit_the_remaining_rows() {
        let codec = snappy();
        let mut budget = budget(&codec);
        budget.rows_remaining = Some(999);
        assert!(!can_splice(&row_group(1_000, Compression::SNAPPY), &budget));
        budget.rows_remaining = Some(1_000);
        assert!(can_splice(&row_group(1_000, Compression::SNAPPY), &budget));
    }

    #[test]
    fn refuses_when_the_input_is_not_splice_compatible_or_the_file_is_full() {
        let codec = snappy();
        let mut budget = budget(&codec);
        budget.input_splice_compatible = false;
        assert!(!can_splice(&row_group(1_000, Compression::SNAPPY), &budget));

        let mut budget = self::budget(&codec);
        budget.row_groups_remaining = 0;
        assert!(!can_splice(&row_group(1_000, Compression::SNAPPY), &budget));
    }

    #[test]
    fn forced_codec_matches_on_codec_identity_not_level() {
        let codec = OutputCodec::Force(Compression::ZSTD(ZstdLevel::try_new(9).unwrap()));
        let budget = budget(&codec);
        // Parquet records only the codec in a chunk, never the level a writer
        // used, so levels must not participate.
        assert!(can_splice(
            &row_group(1_000, Compression::ZSTD(ZstdLevel::try_new(1).unwrap())),
            &budget
        ));
        assert!(!can_splice(&row_group(1_000, Compression::SNAPPY), &budget));

        let snappy = snappy();
        assert!(!can_splice(
            &row_group(1_000, Compression::LZ4_RAW),
            &self::budget(&snappy)
        ));
    }

    #[test]
    fn preserving_codec_splices_whatever_the_chunk_already_has() {
        // `append_column` writes the source chunk's codec into the output
        // footer, so preserving needs no codec agreement at all.
        let codec = preserve();
        let budget = budget(&codec);
        for chunk in [
            Compression::GZIP(GzipLevel::default()),
            Compression::BROTLI(BrotliLevel::default()),
            Compression::UNCOMPRESSED,
            Compression::ZSTD(ZstdLevel::default()),
        ] {
            assert!(can_splice(&row_group(1_000, chunk), &budget), "{chunk:?}");
        }
    }
}
