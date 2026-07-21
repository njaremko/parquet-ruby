use std::fs::{self, File};
use std::os::raw::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_schema::SchemaRef;
use magnus::value::ReprValue;
use magnus::{Error as MagnusError, Ruby, Value};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet_core::max_batch_size_for_column_count;
use tempfile::{NamedTempFile, TempPath};

use crate::types::ParquetRepackArgs;
use crate::utils::{parse_compression, parse_parquet_repack_args};

const DEFAULT_MAX_READ_ROWS_PER_CHUNK: usize = 8192;

struct RepackedFile {
    path: String,
    num_rows: usize,
}

struct PendingOutput {
    writer: ArrowWriter<File>,
    temp_file: NamedTempFile,
    final_path: PathBuf,
    final_path_string: String,
    num_rows: usize,
}

struct CompletedOutput {
    temp_path: TempPath,
    final_path: PathBuf,
    final_path_string: String,
    num_rows: usize,
}

struct RepackWithoutGvlState {
    args: Option<ParquetRepackArgs>,
    compression: Compression,
    result: Option<std::thread::Result<Result<Vec<RepackedFile>, String>>>,
    cancelled: *const AtomicBool,
}

pub fn repack(ruby: &Ruby, args: &[Value]) -> Result<Value, MagnusError> {
    let repack_args = parse_parquet_repack_args(ruby, args)?;
    let compression = parse_compression(ruby, repack_args.compression.clone())?;
    let files = repack_without_gvl(ruby, repack_args, compression)?;

    let result = ruby.ary_new_capa(files.len());
    for file in files {
        let hash = ruby.hash_new();
        hash.aset("path", file.path)?;
        hash.aset("num_rows", file.num_rows)?;
        result.push(hash)?;
    }

    Ok(result.as_value())
}

fn repack_without_gvl(
    ruby: &Ruby,
    args: ParquetRepackArgs,
    compression: Compression,
) -> Result<Vec<RepackedFile>, MagnusError> {
    let cancelled = AtomicBool::new(false);
    let mut state = RepackWithoutGvlState {
        args: Some(args),
        compression,
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
        Ok(Err(message)) => Err(MagnusError::new(ruby.exception_runtime_error(), message)),
        Err(payload) => Err(MagnusError::new(
            ruby.exception_runtime_error(),
            panic_message(payload),
        )),
    }
}

unsafe extern "C" fn repack_without_gvl_trampoline(data: *mut c_void) -> *mut c_void {
    let state = unsafe { &mut *data.cast::<RepackWithoutGvlState>() };
    state.result = Some(catch_unwind(AssertUnwindSafe(|| {
        let args = state.args.take().expect("repack arguments must be present");
        let compression = state.compression;
        let cancelled = unsafe { &*state.cancelled };
        repack_files(&args, compression, cancelled)
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

fn repack_files(
    args: &ParquetRepackArgs,
    compression: Compression,
    cancelled: &AtomicBool,
) -> Result<Vec<RepackedFile>, String> {
    let schema = read_schema(&args.read_from[0])?;
    validate_input_schemas(args, schema.clone())?;
    let max_read_rows_per_chunk =
        effective_max_read_rows_per_chunk(args.max_read_rows_per_chunk, schema.fields().len());

    let mut completed_outputs = Vec::new();
    let mut output_index = 0usize;
    let mut current_output_rows = 0usize;
    let mut current_output = None;

    for input_path in &args.read_from {
        let reader = create_reader_builder(input_path)?
            .with_batch_size(max_read_rows_per_chunk)
            .build()
            .map_err(|e| e.to_string())?;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| e.to_string())?;
            let mut offset = 0usize;

            while offset < batch.num_rows() {
                if current_output.is_none() {
                    current_output = Some(create_output(
                        args,
                        schema.clone(),
                        output_index,
                        compression,
                    )?);
                }

                let rows_remaining_in_batch = batch.num_rows() - offset;
                let rows_to_write = match args.rows_per_file {
                    Some(rows_per_file) => {
                        let rows_remaining_in_output = rows_per_file - current_output_rows;
                        rows_remaining_in_batch.min(rows_remaining_in_output)
                    }
                    None => rows_remaining_in_batch,
                };

                let batch_slice = batch.slice(offset, rows_to_write);
                let output = current_output.as_mut().expect("output must be present");
                output
                    .writer
                    .write(&batch_slice)
                    .map_err(|e| e.to_string())?;
                check_cancelled(cancelled)?;
                output.num_rows += rows_to_write;

                offset += rows_to_write;
                current_output_rows += rows_to_write;

                if args.rows_per_file == Some(current_output_rows) {
                    completed_outputs.push(close_output(
                        current_output.take().expect("output must be present"),
                    )?);
                    output_index += 1;
                    current_output_rows = 0;
                }
            }
        }
    }

    if let Some(output) = current_output {
        completed_outputs.push(close_output(output)?);
    }

    if completed_outputs.is_empty() {
        completed_outputs.push(close_output(create_output(args, schema, 0, compression)?)?);
    }

    persist_outputs(completed_outputs)
}

fn effective_max_read_rows_per_chunk(requested: Option<usize>, column_count: usize) -> usize {
    requested
        .unwrap_or(DEFAULT_MAX_READ_ROWS_PER_CHUNK)
        .min(max_batch_size_for_column_count(column_count))
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<(), String> {
    if cancelled.load(Ordering::SeqCst) {
        Err("Parquet.repack interrupted".to_string())
    } else {
        Ok(())
    }
}

fn persist_outputs(outputs: Vec<CompletedOutput>) -> Result<Vec<RepackedFile>, String> {
    let mut repacked_files = Vec::with_capacity(outputs.len());

    for output in outputs {
        let CompletedOutput {
            temp_path,
            final_path,
            final_path_string,
            num_rows,
        } = output;

        match temp_path.persist(&final_path) {
            Ok(_) => {
                repacked_files.push(RepackedFile {
                    path: final_path_string,
                    num_rows,
                });
            }
            Err(error) => {
                return Err(format!(
                    "Failed to move temporary file to '{}': {}",
                    final_path_string, error.error
                ));
            }
        }
    }

    Ok(repacked_files)
}

fn close_output(output: PendingOutput) -> Result<CompletedOutput, String> {
    let PendingOutput {
        writer,
        temp_file,
        final_path,
        final_path_string,
        num_rows,
    } = output;

    writer.close().map_err(|e| e.to_string())?;

    Ok(CompletedOutput {
        temp_path: temp_file.into_temp_path(),
        final_path,
        final_path_string,
        num_rows,
    })
}

fn validate_output_file_prefix(prefix: &str) -> Result<(), String> {
    if prefix.is_empty() {
        return Err("output_file_prefix must not be empty".to_string());
    }
    let p = PathBuf::from(prefix);
    // Reject absolute paths and any path traversal components.
    if p.is_absolute() {
        return Err(format!(
            "output_file_prefix must be a plain filename, not an absolute path: '{prefix}'"
        ));
    }
    for component in p.components() {
        match component {
            std::path::Component::Normal(_) => {}
            _ => {
                return Err(format!(
                    "output_file_prefix must not contain path separators or '..' components: '{prefix}'"
                ));
            }
        }
    }
    Ok(())
}

fn create_output(
    args: &ParquetRepackArgs,
    schema: SchemaRef,
    output_index: usize,
    compression: Compression,
) -> Result<PendingOutput, String> {
    validate_output_file_prefix(&args.output_file_prefix)?;

    fs::create_dir_all(&args.output_dir).map_err(|e| {
        format!(
            "Failed to create output directory '{}': {}",
            args.output_dir, e
        )
    })?;

    let final_path = PathBuf::from(&args.output_dir).join(format!(
        "{}-{}.parquet",
        args.output_file_prefix, output_index
    ));
    let final_path_string = final_path.to_string_lossy().into_owned();

    let temp_file = NamedTempFile::new_in(&args.output_dir).map_err(|e| {
        format!(
            "Failed to create temporary output file for '{}': {}",
            final_path_string, e
        )
    })?;

    let file = temp_file.reopen().map_err(|e| {
        format!(
            "Failed to reopen temporary output file for '{}': {}",
            final_path_string, e
        )
    })?;

    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();

    let writer = ArrowWriter::try_new(file, schema, Some(props)).map_err(|e| e.to_string())?;

    Ok(PendingOutput {
        writer,
        temp_file,
        final_path,
        final_path_string,
        num_rows: 0,
    })
}

fn validate_input_schemas(args: &ParquetRepackArgs, schema: SchemaRef) -> Result<(), String> {
    for input_path in args.read_from.iter().skip(1) {
        let input_schema = read_schema(input_path)?;

        if input_schema.as_ref() != schema.as_ref() {
            return Err(format!(
                "Input file schema does not match first file: {}",
                input_path
            ));
        }
    }

    Ok(())
}

fn read_schema(path: &str) -> Result<SchemaRef, String> {
    let builder = create_reader_builder(path)?;
    Ok(builder.schema().clone())
}

fn create_reader_builder(path: &str) -> Result<ParquetRecordBatchReaderBuilder<File>, String> {
    let file =
        File::open(path).map_err(|e| format!("Failed to open input file '{}': {}", path, e))?;

    ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_max_read_rows_per_chunk_uses_default_when_unset() {
        assert_eq!(
            effective_max_read_rows_per_chunk(None, 1),
            DEFAULT_MAX_READ_ROWS_PER_CHUNK
        );
    }

    #[test]
    fn effective_max_read_rows_per_chunk_is_bounded_by_schema_width() {
        assert_eq!(
            effective_max_read_rows_per_chunk(Some(1_000_000), 2),
            max_batch_size_for_column_count(2)
        );
        assert_eq!(
            effective_max_read_rows_per_chunk(None, 2_000),
            max_batch_size_for_column_count(2_000)
        );
    }

    #[test]
    fn effective_max_read_rows_per_chunk_preserves_smaller_requests() {
        assert_eq!(effective_max_read_rows_per_chunk(Some(64), 2_000), 64);
    }
}
