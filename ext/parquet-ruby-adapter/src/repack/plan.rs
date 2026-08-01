//! Everything `Parquet.repack` decides before it writes a byte.
//!
//! Planning reads each input's footer exactly once, proves the inputs can be
//! concatenated, resolves the resource bounds the transform will run under, and
//! establishes who owns the output filenames. Failing here means no output file
//! has been created yet, so a rejected request never leaves partial state.

use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_schema::SchemaRef;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::basic::{Compression, ConvertedType, LogicalType, Type as PhysicalType};
use parquet::file::metadata::{KeyValue, PageIndexPolicy};
use parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath, TypePtr};
use parquet_core::max_batch_size_for_column_count;

use crate::error::{Result, RubyAdapterError};
use crate::types::ParquetRepackArgs;

use super::io_error;

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

/// One input file, with its footer already parsed.
pub struct InputPlan {
    pub path: String,
    pub reader_metadata: ArrowReaderMetadata,
    /// Whether this input's encoded column chunks may be spliced into the
    /// output verbatim. Splicing requires every leaf `ColumnDescriptor` to be
    /// *identical* to the output's, which is stricter than the shape equality
    /// that makes rows concatenable: it also pins Parquet field ids.
    pub splice_compatible: bool,
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
    pub compression: Compression,
    pub max_read_rows_per_chunk: usize,
    /// Upper bound on rows buffered in one output row group. Bounds peak writer
    /// memory the same way `max_read_rows_per_chunk` bounds the reader.
    pub max_row_group_rows: usize,
    pub namespace: OutputNamespace,
}

impl RepackPlan {
    /// Rows the current output may still accept before it must be closed.
    /// `None` means unbounded, i.e. the caller did not ask for splitting.
    pub fn rows_remaining(
        &self,
        rows_per_file: Option<usize>,
        rows_written: usize,
    ) -> Option<usize> {
        rows_per_file.map(|limit| {
            debug_assert!(rows_written <= limit, "output overshot rows_per_file");
            limit.saturating_sub(rows_written)
        })
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

    /// Members left over from an earlier, longer run once `written` files have
    /// been persisted. These are the files that would otherwise masquerade as
    /// part of the current result.
    pub fn stale_beyond(&self, written: usize) -> impl Iterator<Item = &Path> {
        self.existing
            .iter()
            .filter(move |(index, _)| *index >= written)
            .map(|(_, path)| path.as_path())
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

    let options = ArrowReaderOptions::new().with_page_index_policy(PAGE_INDEX_POLICY);
    let mut inputs = Vec::with_capacity(args.read_from.len());
    for path in &args.read_from {
        inputs.push(read_input(path, &options)?);
    }

    // Take everything the output needs from the first input as owned values, so
    // the per-input loops below can borrow `inputs` mutably.
    let (output_root_type, arrow_schema, output_columns, output_key_value_metadata) = {
        let file_metadata = inputs[0].reader_metadata.metadata().file_metadata();
        let descriptor = file_metadata.schema_descr();
        (
            descriptor.root_schema_ptr(),
            inputs[0].reader_metadata.schema().clone(),
            descriptor.columns().to_vec(),
            file_metadata.key_value_metadata().cloned(),
        )
    };
    let leaf_column_count = output_columns.len();

    for input in inputs.iter().skip(1) {
        let columns = input
            .reader_metadata
            .metadata()
            .file_metadata()
            .schema_descr()
            .columns();
        if let Some(detail) = describe_shape_mismatch(&output_columns, columns) {
            return Err(RubyAdapterError::invalid_input(format!(
                "input {:?} schema does not match {:?}: {detail}",
                input.path, args.read_from[0]
            )));
        }
    }

    // Splice compatibility is per input and stricter than the shape equality
    // checked above: two inputs can hold concatenable rows while differing in
    // Parquet field ids, which `append_column` refuses to splice.
    for input in inputs.iter_mut() {
        let columns = input
            .reader_metadata
            .metadata()
            .file_metadata()
            .schema_descr()
            .columns();
        input.splice_compatible = columns.len() == output_columns.len()
            && columns
                .iter()
                .zip(&output_columns)
                .all(|(actual, expected)| actual == expected);
    }

    let compression = args
        .compression
        .unwrap_or_else(|| observed_compression(&inputs[0]));

    let slot_bound = max_batch_size_for_column_count(leaf_column_count);
    let max_read_rows_per_chunk = args
        .max_read_rows_per_chunk
        .unwrap_or(DEFAULT_MAX_READ_ROWS_PER_CHUNK)
        .min(slot_bound);
    assert!(max_read_rows_per_chunk > 0, "read chunk must make progress");

    Ok(RepackPlan {
        inputs,
        output_root_type,
        output_key_value_metadata,
        arrow_schema,
        compression,
        max_read_rows_per_chunk,
        max_row_group_rows: slot_bound,
        namespace: scan_namespace(&args.output_dir, &args.output_file_prefix)?,
    })
}

fn read_input(path: &str, options: &ArrowReaderOptions) -> Result<InputPlan> {
    let file = File::open(path)
        .map_err(|source| io_error(format!("failed to open input file {path:?}"), source))?;
    let reader_metadata = ArrowReaderMetadata::load(&file, options.clone()).map_err(|source| {
        RubyAdapterError::runtime(format!(
            "failed to read Parquet metadata from {path:?}: {source}"
        ))
    })?;

    Ok(InputPlan {
        path: path.to_string(),
        reader_metadata,
        // Filled in once the output descriptor is known.
        splice_compatible: false,
    })
}

/// The codec to preserve when the caller did not name one.
///
/// A Parquet file records its codec per column chunk, so there is no single
/// file-level answer; the first chunk is the best available witness. With no
/// chunks to observe there is also no data to compress, so fall back to the
/// gem-wide default.
fn observed_compression(input: &InputPlan) -> Compression {
    input
        .reader_metadata
        .metadata()
        .row_groups()
        .first()
        .and_then(|row_group| row_group.columns().first())
        .map(|column| column.compression())
        .unwrap_or(Compression::SNAPPY)
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
    fn stale_beyond_reports_only_indexes_the_new_run_did_not_write() {
        let namespace = OutputNamespace {
            dir: PathBuf::from("/out"),
            prefix: "batch".to_string(),
            existing: vec![
                (0, PathBuf::from("/out/batch-0.parquet")),
                (1, PathBuf::from("/out/batch-1.parquet")),
                (2, PathBuf::from("/out/batch-2.parquet")),
            ],
        };

        assert_eq!(
            namespace.stale_beyond(1).collect::<Vec<_>>(),
            vec![
                Path::new("/out/batch-1.parquet"),
                Path::new("/out/batch-2.parquet")
            ]
        );
        assert_eq!(namespace.stale_beyond(3).count(), 0);
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
