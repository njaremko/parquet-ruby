use std::fs::{self, File};
use std::path::PathBuf;

use arrow_schema::SchemaRef;
use magnus::value::ReprValue;
use magnus::{Error as MagnusError, Ruby, Value};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::{NamedTempFile, TempPath};

use crate::types::ParquetRepackArgs;
use crate::utils::{parse_compression, parse_parquet_repack_args};

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

pub fn repack(ruby: &Ruby, args: &[Value]) -> Result<Value, MagnusError> {
    let repack_args = parse_parquet_repack_args(ruby, args)?;
    let files = repack_files(ruby, &repack_args)?;

    let result = ruby.ary_new_capa(files.len());
    for file in files {
        let hash = ruby.hash_new();
        hash.aset("path", file.path)?;
        hash.aset("num_rows", file.num_rows)?;
        result.push(hash)?;
    }

    Ok(result.as_value())
}

fn repack_files(
    ruby: &Ruby,
    args: &ParquetRepackArgs,
) -> Result<Vec<RepackedFile>, MagnusError> {
    let schema = read_schema(ruby, &args.read_from[0])?;
    validate_input_schemas(ruby, args, schema.clone())?;

    let mut completed_outputs = Vec::new();
    let mut output_index = 0usize;
    let mut current_output_rows = 0usize;
    let mut current_output = None;

    for input_path in &args.read_from {
        let reader = create_reader_builder(ruby, input_path)?
            .with_batch_size(args.max_read_rows_per_chunk)
            .build()
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
            let mut offset = 0usize;

            while offset < batch.num_rows() {
                if current_output.is_none() {
                    current_output = Some(create_output(ruby, args, schema.clone(), output_index)?);
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
                    .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
                output.num_rows += rows_to_write;

                offset += rows_to_write;
                current_output_rows += rows_to_write;

                if args.rows_per_file == Some(current_output_rows) {
                    completed_outputs.push(close_output(
                        ruby,
                        current_output.take().expect("output must be present"),
                    )?);
                    output_index += 1;
                    current_output_rows = 0;
                }
            }
        }
    }

    if let Some(output) = current_output {
        completed_outputs.push(close_output(ruby, output)?);
    }

    persist_outputs(ruby, completed_outputs)
}

fn persist_outputs(
    ruby: &Ruby,
    outputs: Vec<CompletedOutput>,
) -> Result<Vec<RepackedFile>, MagnusError> {
    let mut repacked_files = Vec::with_capacity(outputs.len());
    let mut persisted_paths = Vec::with_capacity(outputs.len());

    for output in outputs {
        let CompletedOutput {
            temp_path,
            final_path,
            final_path_string,
            num_rows,
        } = output;

        match temp_path.persist(&final_path) {
            Ok(_) => {
                persisted_paths.push(final_path);
                repacked_files.push(RepackedFile {
                    path: final_path_string,
                    num_rows,
                });
            }
            Err(error) => {
                for persisted_path in persisted_paths {
                    let _ = fs::remove_file(persisted_path);
                }
                return Err(MagnusError::new(
                    ruby.exception_runtime_error(),
                    format!(
                        "Failed to move temporary file to '{}': {}",
                        final_path_string, error.error
                    ),
                ));
            }
        }
    }

    Ok(repacked_files)
}

fn close_output(ruby: &Ruby, output: PendingOutput) -> Result<CompletedOutput, MagnusError> {
    let PendingOutput {
        writer,
        temp_file,
        final_path,
        final_path_string,
        num_rows,
    } = output;

    writer
        .close()
        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

    Ok(CompletedOutput {
        temp_path: temp_file.into_temp_path(),
        final_path,
        final_path_string,
        num_rows,
    })
}

fn create_output(
    ruby: &Ruby,
    args: &ParquetRepackArgs,
    schema: SchemaRef,
    output_index: usize,
) -> Result<PendingOutput, MagnusError> {
    fs::create_dir_all(&args.output_dir).map_err(|e| {
        MagnusError::new(
            ruby.exception_runtime_error(),
            format!(
                "Failed to create output directory '{}': {}",
                args.output_dir, e
            ),
        )
    })?;

    let final_path = PathBuf::from(&args.output_dir).join(format!(
        "{}-{}.parquet",
        args.output_file_prefix, output_index
    ));
    let final_path_string = final_path.to_string_lossy().into_owned();

    let temp_file = NamedTempFile::new_in(&args.output_dir).map_err(|e| {
        MagnusError::new(
            ruby.exception_runtime_error(),
            format!(
                "Failed to create temporary output file for '{}': {}",
                final_path_string, e
            ),
        )
    })?;

    let file = temp_file.reopen().map_err(|e| {
        MagnusError::new(
            ruby.exception_runtime_error(),
            format!(
                "Failed to reopen temporary output file for '{}': {}",
                final_path_string, e
            ),
        )
    })?;

    let props = WriterProperties::builder()
        .set_compression(parse_compression(ruby, args.compression.clone())?)
        .build();

    let writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

    Ok(PendingOutput {
        writer,
        temp_file,
        final_path,
        final_path_string,
        num_rows: 0,
    })
}

fn validate_input_schemas(
    ruby: &Ruby,
    args: &ParquetRepackArgs,
    schema: SchemaRef,
) -> Result<(), MagnusError> {
    for input_path in args.read_from.iter().skip(1) {
        let input_schema = read_schema(ruby, input_path)?;

        if input_schema.as_ref() != schema.as_ref() {
            return Err(MagnusError::new(
                ruby.exception_runtime_error(),
                format!("Input file schema does not match first file: {}", input_path),
            ));
        }
    }

    Ok(())
}

fn read_schema(ruby: &Ruby, path: &str) -> Result<SchemaRef, MagnusError> {
    let builder = create_reader_builder(ruby, path)?;
    Ok(builder.schema().clone())
}

fn create_reader_builder(
    ruby: &Ruby,
    path: &str,
) -> Result<ParquetRecordBatchReaderBuilder<File>, MagnusError> {
    let file = File::open(path).map_err(|e| {
        MagnusError::new(
            ruby.exception_runtime_error(),
            format!("Failed to open input file '{}': {}", path, e),
        )
    })?;

    ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))
}
