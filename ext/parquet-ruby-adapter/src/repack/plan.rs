//! Everything `Parquet.repack` decides before it writes a byte.
//!
//! Planning reads each input's footer exactly once, proves the inputs can be
//! concatenated, resolves the resource bounds the transform will run under, and
//! establishes who owns the output filenames. Failing here means no output file
//! has been created yet, so a rejected request never leaves partial state.

use std::fs::File;
use std::path::PathBuf;

use arrow_schema::SchemaRef;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::basic::{Compression, ConvertedType, LogicalType, Type as PhysicalType};
use parquet::file::metadata::{KeyValue, PageIndexPolicy, ParquetMetaData};
use parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath, TypePtr};
use parquet_core::max_batch_size_for_column_count;

use crate::error::{Result, RubyAdapterError};
use crate::types::ParquetRepackArgs;

use super::io_error;
use super::output::OutputSpec;

/// Rows per read chunk when the caller does not choose. Small enough that a
/// wide schema does not materialise a huge Arrow batch, large enough to amortise
/// per-batch overhead.
const DEFAULT_MAX_READ_ROWS_PER_CHUNK: usize = 8192;

/// Parquet stores the row-group ordinal as a thrift `i16`, so a single file can
/// never hold more than this many row groups. This is a format limit, not a
/// policy we chose.
pub const MAX_ROW_GROUPS_PER_FILE: usize = i16::MAX as usize;

/// Read each input's page index when it has one so spliced chunks keep their
/// column and offset indexes. `Optional` rather than `Required`: most writers
/// omit the page index, and its absence must not make a valid input unreadable.
pub const PAGE_INDEX_POLICY: PageIndexPolicy = PageIndexPolicy::Optional;

/// Fraction of the output row-group target below which an input row group is
/// not worth splicing.
///
/// Splicing makes one output row group per input row group, so copying tiny
/// groups faithfully reproduces a bad layout: a compaction of many small files
/// would emit a file with as many row groups as it read, which is both slower to
/// produce and far slower for every later reader. Below this floor the rows go
/// through the re-encode path instead and merge into full-sized groups.
///
/// It also bounds the output's row-group count. A spliced group holds at least
/// `max_row_group_rows / SPLICE_ROW_GROUP_DIVISOR` rows, so reaching
/// `MAX_ROW_GROUPS_PER_FILE` needs more rows in one output than the format can
/// hold anyway.
const SPLICE_ROW_GROUP_DIVISOR: usize = 8;

/// One input file. Deliberately holds no parsed metadata: footers, and the page
/// indexes read with them, are re-read one input at a time during the transform
/// so peak memory does not grow with the number of inputs.
pub struct InputPlan {
    pub path: String,
    /// Whether this input's encoded column chunks may be spliced into the
    /// output verbatim. Splicing requires every leaf `ColumnDescriptor` to be
    /// *identical* to the output's, which is stricter than the shape equality
    /// that makes rows concatenable: it also pins Parquet field ids.
    pub splice_compatible: bool,
}

/// What codec the outputs should use.
///
/// Parquet records a codec per column chunk, so "keep what the inputs use" is
/// not expressible as a single `Compression`: a spliced chunk keeps whatever it
/// already had. Naming the two cases separately keeps "the caller demanded this
/// codec" distinct from "we had to pick one in order to encode".
#[derive(Debug, Clone)]
pub enum OutputCodec {
    /// Keep each column's codec. A spliced chunk keeps its own automatically,
    /// since `append_column` writes the source chunk's codec into the footer;
    /// a re-encoded chunk uses the codec observed for that column in the inputs,
    /// so both paths agree.
    Preserve {
        /// Per leaf column, in schema order, the codec observed in the inputs.
        per_column: Vec<(ColumnPath, Compression)>,
        /// The writer's default, for any column with nothing to observe.
        default: Compression,
    },
    /// The caller named a codec; every chunk must end up with it, so a chunk
    /// that does not already have it cannot be spliced.
    Force(Compression),
}

impl OutputCodec {
    /// Whether a chunk already compressed with `chunk` may be copied verbatim.
    pub fn accepts_spliced(&self, chunk: Compression) -> bool {
        match self {
            OutputCodec::Preserve { .. } => true,
            // Parquet stores only the codec in a chunk, never the level a writer
            // used, so comparing whole `Compression` values would spuriously
            // reject chunks whose level merely differs.
            OutputCodec::Force(requested) => {
                std::mem::discriminant(&chunk) == std::mem::discriminant(requested)
            }
        }
    }
}

/// The codec each leaf column is compressed with in the inputs.
///
/// Parquet records a codec per column chunk, so a file can legitimately use a
/// different one per column. Taking the first chunk observed for each column
/// makes "preserve" a per-column answer rather than one witness standing in for
/// the whole schema.
fn observe_column_codecs(
    metadata: &ParquetMetaData,
    columns: &[ColumnDescPtr],
    observed: &mut [Option<Compression>],
) {
    for row_group in metadata.row_groups() {
        for (index, column) in row_group.columns().iter().enumerate() {
            if let Some(slot) = observed.get_mut(index) {
                slot.get_or_insert_with(|| column.compression());
            }
        }
        if observed.iter().all(Option::is_some) {
            break;
        }
    }
    debug_assert_eq!(observed.len(), columns.len());
}

/// The resolved shape of one `Parquet.repack` call.
pub struct RepackPlan {
    pub inputs: Vec<InputPlan>,
    /// The first input's root group type, reused verbatim so every output has
    /// byte-identical column descriptors and spliced chunks are accepted.
    pub output_root_type: TypePtr,
    /// The first input's file-level key/value metadata, carried over so
    /// `ARROW:schema`, `pandas`, and similar producer metadata survive a repack.
    pub output_key_value_metadata: Option<Vec<KeyValue>>,
    /// Arrow view of `output_root_type`, used only by the re-encode path.
    pub arrow_schema: SchemaRef,
    pub codec: OutputCodec,
    /// Whether outputs carry a page index. An output's row groups must agree:
    /// the Parquet footer writer cannot represent a file where some row groups
    /// have an offset index and others do not. Since a spliced group can only
    /// contribute the index its source had, the output has one exactly when
    /// every contributing input does.
    pub write_page_index: bool,
    pub max_read_rows_per_chunk: usize,
    /// Upper bound on rows buffered in one output row group. Bounds peak writer
    /// memory the same way `max_read_rows_per_chunk` bounds the reader.
    pub max_row_group_rows: usize,
    /// Smallest input row group worth splicing; see `SPLICE_ROW_GROUP_DIVISOR`.
    pub min_spliceable_rows: usize,
    /// Rows across every input, used to check row preservation before anything
    /// is published. Summed here so the transform need not retain footers.
    pub total_input_rows: i64,
    pub namespace: OutputNamespace,
}

impl RepackPlan {
    /// The output configuration every file of this run shares.
    pub fn output_spec(&self) -> OutputSpec<'_> {
        OutputSpec {
            root_type: &self.output_root_type,
            arrow_schema: &self.arrow_schema,
            key_value_metadata: &self.output_key_value_metadata,
            codec: &self.codec,
            write_page_index: self.write_page_index,
            max_row_group_rows: self.max_row_group_rows,
        }
    }
}

/// The `{prefix}-{n}.parquet` filenames in `output_dir` that repack owns.
///
/// Treating this set as a single owned namespace is what makes the returned file
/// list equal to what a reader will find on disk: repack either has the
/// namespace to itself, or the caller explicitly authorised replacing it.
pub struct OutputNamespace {
    pub dir: PathBuf,
    pub prefix: String,
    /// Pre-existing members, as `(index, path)` sorted by index.
    pub existing: Vec<(usize, PathBuf)>,
}

impl OutputNamespace {
    /// Absolute-or-relative path of the `index`th output file.
    pub fn path_for(&self, index: usize) -> PathBuf {
        self.dir.join(format!("{}-{}.parquet", self.prefix, index))
    }
}

/// Read every input footer, prove the inputs are concatenable, and resolve the
/// bounds and filenames the transform will use.
pub fn build_plan(args: &ParquetRepackArgs) -> Result<RepackPlan> {
    // Upheld by `parse_parquet_repack_args`; asserted here because indexing
    // `read_from[0]` below depends on it and the two live in different modules.
    assert!(
        !args.read_from.is_empty(),
        "repack requires at least one input path"
    );

    // The first input defines the output. Read it once, keep what the output
    // needs as owned values, and let its metadata drop with the rest.
    let first = load_metadata(&args.read_from[0])?;
    let (output_root_type, arrow_schema, output_columns, output_key_value_metadata) = {
        let file_metadata = first.metadata().file_metadata();
        let descriptor = file_metadata.schema_descr();
        (
            descriptor.root_schema_ptr(),
            first.schema().clone(),
            descriptor.columns().to_vec(),
            file_metadata.key_value_metadata().cloned(),
        )
    };
    let leaf_column_count = output_columns.len();

    // One pass per input: validate shape, decide splice compatibility, and
    // accumulate the totals the plan needs. Each input's metadata is dropped
    // before the next is read, so peak memory is one footer, not all of them.
    let mut inputs = Vec::with_capacity(args.read_from.len());
    let mut total_input_rows = 0i64;
    let mut observed_codecs: Vec<Option<Compression>> = vec![None; output_columns.len()];
    let mut write_page_index = true;

    for (index, path) in args.read_from.iter().enumerate() {
        let metadata = if index == 0 {
            first.clone()
        } else {
            load_metadata(path)?
        };
        let parquet_metadata = metadata.metadata();
        let columns = parquet_metadata.file_metadata().schema_descr().columns();

        if index > 0 {
            if let Some(detail) = describe_shape_mismatch(&output_columns, columns) {
                return Err(RubyAdapterError::invalid_input(format!(
                    "input {path:?} schema does not match {:?}: {detail}",
                    args.read_from[0]
                )));
            }
        }

        // Stricter than the shape equality above: two inputs can hold
        // concatenable rows while differing in Parquet field ids, which
        // `append_column` refuses to splice.
        let splice_compatible = columns.len() == output_columns.len()
            && columns
                .iter()
                .zip(&output_columns)
                .all(|(actual, expected)| actual == expected);

        total_input_rows += parquet_metadata
            .row_groups()
            .iter()
            .map(|row_group| row_group.num_rows())
            .sum::<i64>();

        if splice_compatible {
            observe_column_codecs(parquet_metadata, &output_columns, &mut observed_codecs);
        }

        // An input with no row groups contributes no chunks, so it cannot force
        // a mixture and must not veto the page index for everything else.
        if !parquet_metadata.row_groups().is_empty() && parquet_metadata.offset_index().is_none() {
            write_page_index = false;
        }

        inputs.push(InputPlan {
            path: path.clone(),
            splice_compatible,
        });
    }

    let codec = match args.compression {
        Some(requested) => OutputCodec::Force(requested),
        None => OutputCodec::Preserve {
            per_column: output_columns
                .iter()
                .zip(&observed_codecs)
                .filter_map(|(column, codec)| codec.map(|codec| (column.path().clone(), codec)))
                .collect(),
            // With nothing to observe there is also nothing to compress, so fall
            // back to the gem-wide default.
            default: observed_codecs
                .iter()
                .find_map(|codec| *codec)
                .unwrap_or(Compression::SNAPPY),
        },
    };

    let slot_bound = max_batch_size_for_column_count(leaf_column_count);
    let max_read_rows_per_chunk = args
        .max_read_rows_per_chunk
        .unwrap_or(DEFAULT_MAX_READ_ROWS_PER_CHUNK)
        .min(slot_bound);
    assert!(max_read_rows_per_chunk > 0, "read chunk must make progress");

    let min_spliceable_rows = (slot_bound / SPLICE_ROW_GROUP_DIVISOR).max(1);

    Ok(RepackPlan {
        inputs,
        output_root_type,
        output_key_value_metadata,
        arrow_schema,
        codec,
        write_page_index,
        max_read_rows_per_chunk,
        max_row_group_rows: slot_bound,
        min_spliceable_rows,
        total_input_rows,
        namespace: scan_namespace(&args.output_dir, &args.output_file_prefix)?,
    })
}

/// Parse one input's footer, including its page index when it has one.
pub fn load_metadata(path: &str) -> Result<ArrowReaderMetadata> {
    let options = ArrowReaderOptions::new().with_page_index_policy(PAGE_INDEX_POLICY);
    let file = File::open(path)
        .map_err(|source| io_error(format!("failed to open input file {path:?}"), source))?;

    ArrowReaderMetadata::load(&file, options).map_err(|source| {
        RubyAdapterError::runtime(format!(
            "failed to read Parquet metadata from {path:?}: {source}"
        ))
    })
}

/// The part of a leaf column that must match for two files' rows to be
/// concatenable: where the column sits, how it is physically stored, and how it
/// nests.
///
/// Deliberately excludes Parquet field ids and Arrow key/value metadata. Those
/// describe provenance, not data, and rejecting on them makes repack refuse
/// files that differ only in which tool wrote them.
type ColumnShape<'a> = (
    &'a ColumnPath,
    PhysicalType,
    ConvertedType,
    Option<&'a LogicalType>,
    i16,
    i16,
    i16,
    i32,
    i32,
    i32,
);

fn column_shape(column: &ColumnDescriptor) -> ColumnShape<'_> {
    (
        column.path(),
        column.physical_type(),
        column.converted_type(),
        column.logical_type_ref(),
        column.max_def_level(),
        column.max_rep_level(),
        // Two columns can share both level maxima yet nest differently, e.g.
        // `optional group a { repeated group b { required int c } }` against
        // `repeated group a { optional group b { required int c } }`.
        column.repeated_ancestor_def_level(),
        column.type_length(),
        column.type_precision(),
        column.type_scale(),
    )
}

/// Describe the first way `actual` differs from `expected`, or `None` when the
/// two agree. The message names the column and both types so a mismatch is
/// actionable without the caller having to dump either schema.
fn describe_shape_mismatch(expected: &[ColumnDescPtr], actual: &[ColumnDescPtr]) -> Option<String> {
    if expected.len() != actual.len() {
        return Some(format!(
            "has {} leaf columns, expected {}",
            actual.len(),
            expected.len()
        ));
    }

    for (expected_column, actual_column) in expected.iter().zip(actual) {
        if column_shape(expected_column) != column_shape(actual_column) {
            return Some(format!(
                "column {:?} is {}, expected {}",
                actual_column.path().string(),
                describe_column(actual_column),
                describe_column(expected_column)
            ));
        }
    }

    None
}

/// A short, human-readable rendering of a leaf column's type, e.g.
/// `BYTE_ARRAY (String)` or `INT64 repeated`.
fn describe_column(column: &ColumnDescriptor) -> String {
    let mut description = column.physical_type().to_string();

    if column.type_length() > 0 {
        description.push_str(&format!("({})", column.type_length()));
    }
    if let Some(logical) = column.logical_type_ref() {
        description.push_str(&format!(" ({logical:?})"));
    } else if column.converted_type() != ConvertedType::NONE {
        description.push_str(&format!(" ({})", column.converted_type()));
    }
    if column.max_rep_level() > 0 {
        description.push_str(" repeated");
    } else if column.max_def_level() == 0 {
        description.push_str(" required");
    }

    description
}

/// Find the `{prefix}-{n}.parquet` files already present in `dir`.
///
/// A missing directory is an empty namespace, not an error: repack creates the
/// directory when it writes.
fn scan_namespace(dir: &str, prefix: &str) -> Result<OutputNamespace> {
    let dir = PathBuf::from(dir);
    let mut existing = Vec::new();

    match std::fs::read_dir(&dir) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry.map_err(|source| {
                    io_error(format!("failed to list output directory {dir:?}"), source)
                })?;
                let name = entry.file_name();
                if let Some(index) = name.to_str().and_then(|name| output_index(name, prefix)) {
                    existing.push((index, entry.path()));
                }
            }
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(io_error(
                format!("failed to list output directory {dir:?}"),
                source,
            ))
        }
    }

    existing.sort_unstable_by_key(|(index, _)| *index);

    Ok(OutputNamespace {
        dir,
        prefix: prefix.to_string(),
        existing,
    })
}

/// The `n` in `{prefix}-{n}.parquet`, when `name` is a member of the namespace.
fn output_index(name: &str, prefix: &str) -> Option<usize> {
    let rest = name.strip_prefix(prefix)?.strip_prefix('-')?;
    let digits = rest.strip_suffix(".parquet")?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_index_matches_only_its_own_namespace() {
        assert_eq!(output_index("batch-0.parquet", "batch"), Some(0));
        assert_eq!(output_index("batch-12.parquet", "batch"), Some(12));
        // Leading zeros still name the same slot; repack never writes them, but
        // an earlier run by another tool might.
        assert_eq!(output_index("batch-007.parquet", "batch"), Some(7));

        assert_eq!(output_index("batch-0.parquet", "part"), None);
        assert_eq!(output_index("batchx-0.parquet", "batch"), None);
        assert_eq!(output_index("batch-.parquet", "batch"), None);
        assert_eq!(output_index("batch-a.parquet", "batch"), None);
        assert_eq!(output_index("batch-0.txt", "batch"), None);
        assert_eq!(output_index("batch-0.parquet.bak", "batch"), None);
        // A longer prefix must not swallow a shorter one's files.
        assert_eq!(output_index("batch-extra-0.parquet", "batch"), None);
    }

    #[test]
    fn path_for_builds_namespace_members() {
        let namespace = OutputNamespace {
            dir: PathBuf::from("/out"),
            prefix: "part".to_string(),
            existing: Vec::new(),
        };
        assert_eq!(namespace.path_for(3), PathBuf::from("/out/part-3.parquet"));
    }
}
