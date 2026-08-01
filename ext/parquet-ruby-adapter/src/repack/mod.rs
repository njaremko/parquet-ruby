//! `Parquet.repack`: concatenate and re-split Parquet files without translating
//! rows through Ruby.
//!
//! # Meaning
//!
//! Treat a Parquet file as `(schema, key_value_metadata, [row])`. A repack of
//! inputs `p₀..pₙ` denotes
//!
//! ```text
//! rows    = concat [rows(pᵢ)]
//! outputs = partition(rows_per_file, rows)
//! result  = [(dir/prefix-i.parquet, (schema(p₀), kv(p₀), outputs[i]))]
//!
//! partition(None,    rs) = [rs]
//! partition(Some(n), []) = [[]]
//! partition(Some(n), rs) = splitEvery n rs
//! ```
//!
//! The laws that follow, each covered by a test:
//!
//! * rows are preserved in order, none added or dropped;
//! * every output but the last holds exactly `rows_per_file` rows;
//! * there is always at least one output, even for zero rows;
//! * `max_read_rows_per_chunk` does not appear in the denotation, so varying it
//!   cannot change the result;
//! * after a successful call the `{prefix}-{n}.parquet` files in `output_dir`
//!   are exactly the returned paths;
//! * every output's Parquet schema is byte-identical to the first input's.
//!
//! # Execution
//!
//! Argument parsing and result construction hold the GVL; all reading, writing,
//! and renaming happens with the GVL released so other Ruby threads keep
//! running. Ruby interrupts set a cancellation flag through the unblock
//! function, which the transform observes between row groups and batches.

mod output;
mod plan;

use std::fmt::Display;
use std::fs::File;
use std::os::raw::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::RecordBatch;
use magnus::value::ReprValue;
use magnus::{Error as MagnusError, Ruby, Value};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder,
};

use crate::error::{Result, RubyAdapterError};
use crate::types::ParquetRepackArgs;
use crate::utils::parse_parquet_repack_args;

use output::{CompletedOutput, OutputFile};
use plan::{build_plan, InputPlan, RepackPlan};

/// How many pre-existing output filenames to name before truncating the list.
const MAX_REPORTED_CONFLICTS: usize = 5;

struct RepackedFile {
    path: String,
    num_rows: usize,
}

pub fn repack(ruby: &Ruby, args: &[Value]) -> std::result::Result<Value, MagnusError> {
    let repack_args = parse_parquet_repack_args(ruby, args)?;
    let files = repack_without_gvl(repack_args)?;

    let result = ruby.ary_new_capa(files.len());
    for file in files {
        let hash = ruby.hash_new();
        hash.aset("path", file.path)?;
        hash.aset("num_rows", file.num_rows)?;
        result.push(hash)?;
    }

    Ok(result.as_value())
}

/// Attach a path to an IO failure while keeping its `ErrorKind`, so the Ruby
/// side still raises `IOError` rather than a generic `RuntimeError`.
fn io_error(context: impl Display, source: std::io::Error) -> RubyAdapterError {
    RubyAdapterError::Io(std::io::Error::new(
        source.kind(),
        format!("{context}: {source}"),
    ))
}

// ---------------------------------------------------------------------------
// GVL boundary
// ---------------------------------------------------------------------------

struct RepackWithoutGvlState {
    args: Option<ParquetRepackArgs>,
    result: Option<std::thread::Result<Result<Vec<RepackedFile>>>>,
    cancelled: *const AtomicBool,
}

fn repack_without_gvl(
    args: ParquetRepackArgs,
) -> std::result::Result<Vec<RepackedFile>, MagnusError> {
    let cancelled = AtomicBool::new(false);
    let mut state = RepackWithoutGvlState {
        args: Some(args),
        result: None,
        cancelled: &cancelled,
    };

    magnus::rb_sys::protect(|| {
        unsafe {
            rb_sys::rb_thread_call_without_gvl(
                Some(repack_without_gvl_trampoline),
                (&mut state as *mut RepackWithoutGvlState).cast::<c_void>(),
                Some(repack_without_gvl_unblock),
                (&cancelled as *const AtomicBool)
                    .cast_mut()
                    .cast::<c_void>(),
            );
        }
        rb_sys::Qnil as rb_sys::VALUE
    })?;

    match state
        .result
        .take()
        .expect("rb_thread_call_without_gvl must set a result")
    {
        Ok(Ok(files)) => Ok(files),
        Ok(Err(error)) => Err(error.into()),
        Err(payload) => Err(MagnusError::new(
            Ruby::get()
                .expect("the GVL is held again once rb_thread_call_without_gvl returns")
                .exception_runtime_error(),
            panic_message(payload),
        )),
    }
}

unsafe extern "C" fn repack_without_gvl_trampoline(data: *mut c_void) -> *mut c_void {
    let state = unsafe { &mut *data.cast::<RepackWithoutGvlState>() };
    state.result = Some(catch_unwind(AssertUnwindSafe(|| {
        let args = state.args.take().expect("repack arguments must be present");
        let cancelled = unsafe { &*state.cancelled };
        repack_files(&args, cancelled)
    })));

    ptr::null_mut()
}

unsafe extern "C" fn repack_without_gvl_unblock(data: *mut c_void) {
    let cancelled = unsafe { &*data.cast::<AtomicBool>() };
    cancelled.store(true, Ordering::SeqCst);
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        format!("Parquet.repack panicked: {message}")
    } else if let Some(message) = payload.downcast_ref::<String>() {
        format!("Parquet.repack panicked: {message}")
    } else {
        "Parquet.repack panicked".to_string()
    }
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<()> {
    if cancelled.load(Ordering::SeqCst) {
        // Ruby normally raises its own pending exception (Interrupt,
        // Timeout::Error) once the GVL is reacquired; this error only surfaces
        // when it does not.
        return Err(RubyAdapterError::runtime("Parquet.repack was interrupted"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Transform
// ---------------------------------------------------------------------------

fn repack_files(args: &ParquetRepackArgs, cancelled: &AtomicBool) -> Result<Vec<RepackedFile>> {
    let plan = build_plan(args)?;
    reject_occupied_namespace(&plan, args.overwrite)?;

    let mut sink = OutputSink::new(&plan, args.rows_per_file);

    for input in &plan.inputs {
        check_cancelled(cancelled)?;

        let file = File::open(&input.path).map_err(|source| {
            io_error(
                format!("failed to open input file {:?}", input.path),
                source,
            )
        })?;
        let metadata = input.reader_metadata.metadata().clone();
        let decode_metadata = decode_metadata(input, &plan)?;

        for row_group_index in 0..metadata.num_row_groups() {
            check_cancelled(cancelled)?;

            let row_group = metadata.row_group(row_group_index);
            if sink.can_splice(input.splice_compatible, row_group)? {
                sink.splice_row_group(&file, &metadata, row_group_index)?;
                continue;
            }

            let reader = row_group_reader(&file, &decode_metadata, &plan, row_group_index)?;
            for batch in reader {
                let batch = batch.map_err(|source| {
                    RubyAdapterError::runtime(format!(
                        "failed reading {:?} row group {row_group_index}: {source}",
                        input.path
                    ))
                })?;
                sink.write_batch(&batch)?;
                check_cancelled(cancelled)?;
            }
        }
    }

    let outputs = sink.finish()?;

    // Row preservation is the whole point of repack, and the two physical paths
    // account for rows differently — spliced groups from row-group metadata,
    // encoded ones from batch lengths. Check them against the row groups the
    // loop above consumed, so a miscount can never reach a caller. Summing the
    // row groups rather than the files' declared totals keeps this an assertion
    // about our own bookkeeping instead of about input validity.
    let rows_in: i64 = plan
        .inputs
        .iter()
        .flat_map(|input| input.reader_metadata.metadata().row_groups())
        .map(|row_group| row_group.num_rows())
        .sum();
    let rows_out: usize = outputs.iter().map(|output| output.num_rows).sum();
    assert_eq!(
        rows_out as i64, rows_in,
        "repack wrote {rows_out} rows from inputs holding {rows_in}"
    );

    persist_outputs(&plan, outputs)
}

/// Refuse to write into an output namespace someone else already occupies.
///
/// Without this, a shorter run leaves an earlier run's `{prefix}-{n}.parquet`
/// files in place and anything globbing the directory reads rows the returned
/// file list never mentioned.
fn reject_occupied_namespace(plan: &RepackPlan, overwrite: bool) -> Result<()> {
    if overwrite || plan.namespace.existing.is_empty() {
        return Ok(());
    }

    let mut names: Vec<String> = plan
        .namespace
        .existing
        .iter()
        .take(MAX_REPORTED_CONFLICTS)
        .map(|(_, path)| format!("{path:?}"))
        .collect();
    let total = plan.namespace.existing.len();
    if total > names.len() {
        names.push(format!("and {} more", total - names.len()));
    }

    Err(RubyAdapterError::invalid_input(format!(
        "output_dir {:?} already contains {total} {:?}-* file(s) ({}); \
         pass overwrite: true to replace them",
        plan.namespace.dir,
        plan.namespace.prefix,
        names.join(", ")
    )))
}

/// Reader metadata that decodes this input into the *output's* Arrow schema.
///
/// Two files can hold concatenable rows while their `ARROW:schema` hints differ,
/// which would otherwise yield arrays the output's column writers reject.
/// Pinning the schema makes every input decode to one Arrow shape.
fn decode_metadata(input: &InputPlan, plan: &RepackPlan) -> Result<ArrowReaderMetadata> {
    let options = ArrowReaderOptions::new()
        .with_page_index_policy(plan::PAGE_INDEX_POLICY)
        .with_schema(plan.arrow_schema.clone());

    ArrowReaderMetadata::try_new(input.reader_metadata.metadata().clone(), options).map_err(
        |source| {
            RubyAdapterError::invalid_input(format!(
                "input {:?} cannot be read with the schema of {:?}: {source}",
                input.path, plan.inputs[0].path
            ))
        },
    )
}

fn row_group_reader(
    file: &File,
    metadata: &ArrowReaderMetadata,
    plan: &RepackPlan,
    row_group_index: usize,
) -> Result<ParquetRecordBatchReader> {
    let handle = file
        .try_clone()
        .map_err(|source| io_error("failed to reopen input file", source))?;

    ParquetRecordBatchReaderBuilder::new_with_metadata(handle, metadata.clone())
        .with_row_groups(vec![row_group_index])
        .with_batch_size(plan.max_read_rows_per_chunk)
        .build()
        .map_err(|source| {
            RubyAdapterError::runtime(format!(
                "failed to read row group {row_group_index}: {source}"
            ))
        })
}

/// Owns output rotation: which file is open, when it is full, and how rows that
/// straddle a boundary are divided.
struct OutputSink<'a> {
    plan: &'a RepackPlan,
    rows_per_file: Option<usize>,
    completed: Vec<CompletedOutput>,
    current: Option<OutputFile>,
}

impl<'a> OutputSink<'a> {
    fn new(plan: &'a RepackPlan, rows_per_file: Option<usize>) -> Self {
        Self {
            plan,
            rows_per_file,
            completed: Vec::new(),
            current: None,
        }
    }

    fn current(&mut self) -> Result<&mut OutputFile> {
        if self.current.is_none() {
            let index = self.completed.len();
            self.current = Some(OutputFile::create(
                &self.plan.namespace.dir,
                self.plan.namespace.path_for(index),
                self.plan.output_root_type.clone(),
                self.plan.arrow_schema.clone(),
                self.plan.output_key_value_metadata.clone(),
                self.plan.compression,
                self.plan.max_row_group_rows,
            )?);
        }
        Ok(self
            .current
            .as_mut()
            .expect("an output file was just ensured"))
    }

    /// Rows the open output may still accept. `None` means unbounded, i.e. the
    /// caller did not ask for splitting.
    fn rows_remaining(&self) -> Option<usize> {
        let written = self.current.as_ref().map_or(0, OutputFile::rows_written);
        self.rows_per_file.map(|limit| {
            debug_assert!(written <= limit, "output overshot rows_per_file");
            limit.saturating_sub(written)
        })
    }

    fn can_splice(
        &mut self,
        splice_compatible: bool,
        row_group: &parquet::file::metadata::RowGroupMetaData,
    ) -> Result<bool> {
        // Creating the output first keeps the decision and the write on the same
        // file, so `rows_remaining` cannot describe a different output.
        self.current()?;
        let rows_remaining = self.rows_remaining();
        let compression = self.plan.compression;
        Ok(self
            .current
            .as_ref()
            .expect("an output file was just ensured")
            .can_splice(splice_compatible, row_group, compression, rows_remaining))
    }

    fn splice_row_group(
        &mut self,
        source: &File,
        metadata: &parquet::file::metadata::ParquetMetaData,
        row_group_index: usize,
    ) -> Result<()> {
        self.current()?
            .splice_row_group(source, metadata, row_group_index)?;
        self.close_if_full()
    }

    /// Write `batch`, splitting it across outputs when it crosses a boundary.
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut offset = 0;
        let rows_per_file = self.rows_per_file;
        while offset < batch.num_rows() {
            let output = self.current()?;
            let rows_remaining_in_batch = batch.num_rows() - offset;
            let rows_to_write = match rows_per_file {
                Some(limit) => {
                    let remaining = limit
                        .checked_sub(output.rows_written())
                        .expect("an output must never exceed rows_per_file");
                    assert!(remaining > 0, "a full output must be closed before writing");
                    rows_remaining_in_batch.min(remaining)
                }
                None => rows_remaining_in_batch,
            };

            output.write_batch(&batch.slice(offset, rows_to_write))?;
            offset += rows_to_write;
            self.close_if_full()?;
        }
        Ok(())
    }

    fn close_if_full(&mut self) -> Result<()> {
        let Some(limit) = self.rows_per_file else {
            return Ok(());
        };
        let full = self
            .current
            .as_ref()
            .is_some_and(|output| output.rows_written() >= limit);
        if full {
            let output = self.current.take().expect("a full output must be open");
            self.completed.push(output.finish()?);
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<CompletedOutput>> {
        if let Some(output) = self.current.take() {
            self.completed.push(output.finish()?);
        }

        // Zero input rows still denote one (empty) output, so a caller can
        // always read the result back without special-casing emptiness.
        if self.completed.is_empty() {
            self.current()?;
            let output = self
                .current
                .take()
                .expect("an output file was just ensured");
            self.completed.push(output.finish()?);
        }

        Ok(self.completed)
    }
}

// ---------------------------------------------------------------------------
// Publication
// ---------------------------------------------------------------------------

/// Move every finished output to its final name, then make the namespace match
/// the result exactly.
///
/// Renames happen only after all outputs are complete on disk, so a failure
/// mid-transform publishes nothing. If a rename itself fails, files this call
/// created are removed to restore the state the caller last observed; files it
/// replaced under `overwrite:` cannot be restored and the error says so.
fn persist_outputs(plan: &RepackPlan, outputs: Vec<CompletedOutput>) -> Result<Vec<RepackedFile>> {
    let preexisting: Vec<&Path> = plan
        .namespace
        .existing
        .iter()
        .map(|(_, path)| path.as_path())
        .collect();

    let total = outputs.len();
    let mut persisted = Vec::with_capacity(total);
    let mut created: Vec<PathBuf> = Vec::new();

    for output in outputs {
        let CompletedOutput {
            temp_path,
            final_path,
            num_rows,
        } = output;

        if let Err(error) = temp_path.persist(&final_path) {
            let source = error.error;
            let rollback = remove_files(&created);
            return Err(io_error(
                format!(
                    "failed to move temporary file to {final_path:?} after publishing {} of \
                     {total} output(s){rollback}",
                    persisted.len()
                ),
                source,
            ));
        }

        if !preexisting.contains(&final_path.as_path()) {
            created.push(final_path.clone());
        }
        persisted.push(RepackedFile {
            path: final_path.to_string_lossy().into_owned(),
            num_rows,
        });
    }

    // Under `overwrite:`, an earlier longer run may have left members past the
    // end of this result. Removing them is what makes the returned list equal
    // to what a reader finds in the directory.
    for stale in plan.namespace.stale_beyond(persisted.len()) {
        std::fs::remove_file(stale).map_err(|source| {
            io_error(
                format!("failed to remove superseded output {stale:?}"),
                source,
            )
        })?;
    }

    Ok(persisted)
}

/// Best-effort removal used only on the rename failure path. Returns a phrase
/// describing what happened, for inclusion in the error the caller sees.
fn remove_files(paths: &[PathBuf]) -> String {
    let mut failures = Vec::new();
    for path in paths {
        if let Err(error) = std::fs::remove_file(path) {
            failures.push(format!("{path:?} ({error})"));
        }
    }

    if paths.is_empty() {
        String::new()
    } else if failures.is_empty() {
        format!(
            "; the {} file(s) already published were removed",
            paths.len()
        )
    } else {
        format!(
            "; could not remove {} of the {} file(s) already published: {}",
            failures.len(),
            paths.len(),
            failures.join(", ")
        )
    }
}
