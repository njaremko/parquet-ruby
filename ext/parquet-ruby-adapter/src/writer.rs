use magnus::value::ReprValue;
use magnus::{Enumerator, Error as MagnusError, RArray, Ruby, TryConvert, Value};
use parquet_core::writer::WriterBuilder;
use parquet_core::{Schema, SchemaNode};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use tempfile::{NamedTempFile, TempPath};

#[cfg(unix)]
use std::fs::OpenOptions;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

use crate::converter::RubyValueConverter;
use crate::io::RubyIOWriter;
use crate::logger::RubyLogger;
use crate::schema::{extract_field_schemas, process_schema_value, ruby_schema_to_parquet};
use crate::string_cache::StringCache;
use crate::utils::parse_compression;

enum WriterOutput {
    Path {
        writer: parquet_core::Writer<File>,
        staging_path: TempPath,
        destination: PathBuf,
        final_state: PathFinalState,
    },
    Io {
        writer: parquet_core::Writer<File>,
        staging_path: TempPath,
        io_object: Value,
    },
}

enum PreparedWriterOutput {
    Path {
        temp_file: NamedTempFile,
        destination: PathBuf,
        final_state: PathFinalState,
    },
    Io {
        temp_file: NamedTempFile,
        io_object: Value,
    },
}

#[cfg(unix)]
#[derive(Clone, Copy)]
struct UnixFileMetadata {
    uid: u32,
    gid: u32,
    mode: u32,
}

#[cfg(unix)]
enum PublicationMode {
    ReplaceExisting(UnixFileMetadata),
    CreateNew,
}

struct PathFinalState {
    #[cfg(unix)]
    publication_path: PathBuf,
    #[cfg(unix)]
    publication_mode: PublicationMode,
}

impl WriterOutput {
    fn writer_mut(&mut self) -> &mut parquet_core::Writer<File> {
        match self {
            Self::Path { writer, .. } | Self::Io { writer, .. } => writer,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct BatchSizingOptions {
    batch_size: Option<usize>,
    flush_threshold: Option<usize>,
    sample_size: Option<usize>,
}

fn create_writer(
    ruby: &Ruby,
    prepared_output: PreparedWriterOutput,
    schema: Schema,
    compression: Option<String>,
    options: BatchSizingOptions,
) -> Result<WriterOutput, MagnusError> {
    let mut builder = WriterBuilder::new().with_compression(parse_compression(ruby, compression)?);
    if let Some(size) = options.batch_size {
        builder = builder.with_batch_size(size);
    }
    if let Some(threshold) = options.flush_threshold {
        builder = builder.with_memory_threshold(threshold);
    }
    if let Some(size) = options.sample_size {
        builder = builder.with_sample_size(size);
    }

    match prepared_output {
        PreparedWriterOutput::Path {
            temp_file,
            destination,
            final_state,
        } => {
            let (writer, staging_path) = build_staged_writer(ruby, builder, temp_file, schema)?;
            Ok(WriterOutput::Path {
                writer,
                staging_path,
                destination,
                final_state,
            })
        }
        PreparedWriterOutput::Io {
            temp_file,
            io_object,
        } => {
            let (writer, staging_path) = build_staged_writer(ruby, builder, temp_file, schema)?;
            Ok(WriterOutput::Io {
                writer,
                staging_path,
                io_object,
            })
        }
    }
}

fn prepare_writer_output(
    ruby: &Ruby,
    write_to: Value,
) -> Result<PreparedWriterOutput, MagnusError> {
    if write_to.is_kind_of(ruby.class_string()) {
        let path_str: String = TryConvert::try_convert(write_to)?;
        let destination = PathBuf::from(path_str);
        let (temp_file, final_state) = create_path_staging_file(&destination).map_err(|error| {
            MagnusError::new(
                ruby.exception_runtime_error(),
                format!("Failed to create staging file: {error}"),
            )
        })?;
        Ok(PreparedWriterOutput::Path {
            temp_file,
            destination,
            final_state,
        })
    } else {
        let temp_file = NamedTempFile::new().map_err(|error| {
            MagnusError::new(
                ruby.exception_runtime_error(),
                format!("Failed to create temporary file: {error}"),
            )
        })?;
        Ok(PreparedWriterOutput::Io {
            temp_file,
            io_object: write_to,
        })
    }
}

fn build_staged_writer(
    ruby: &Ruby,
    builder: WriterBuilder,
    temp_file: NamedTempFile,
    schema: Schema,
) -> Result<(parquet_core::Writer<File>, TempPath), MagnusError> {
    let file = temp_file.reopen().map_err(|error| {
        MagnusError::new(
            ruby.exception_runtime_error(),
            format!("Failed to reopen staging file: {error}"),
        )
    })?;
    let staging_path = temp_file.into_temp_path();
    let writer = builder
        .build(file, schema)
        .map_err(|error| MagnusError::new(ruby.exception_runtime_error(), error.to_string()))?;
    Ok((writer, staging_path))
}

#[cfg(unix)]
fn create_path_staging_file(
    destination: &Path,
) -> std::io::Result<(NamedTempFile, PathFinalState)> {
    match std::fs::symlink_metadata(destination) {
        Ok(_) => create_existing_path_staging_file(destination),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            create_new_path_staging_file(destination)
        }
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn create_existing_path_staging_file(
    destination: &Path,
) -> std::io::Result<(NamedTempFile, PathFinalState)> {
    let publication_path = std::fs::canonicalize(destination)?;
    let writable_file = OpenOptions::new()
        .write(true)
        .open(&publication_path)
        .map_err(|error| writable_destination_error(destination, error))?;
    let metadata = writable_file.metadata()?;
    let hard_link_count = metadata.nlink();
    if hard_link_count > 1 {
        return Err(std::io::Error::other(format!(
            "refusing to atomically replace {}: destination has {hard_link_count} hard links",
            destination.display()
        )));
    }
    let parent = publication_path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("destination has no parent: {}", destination.display()),
        )
    })?;
    let temp_file = tempfile::Builder::new().tempfile_in(parent)?;
    let expected_metadata = unix_file_metadata(&metadata);
    preserve_existing_path_metadata(temp_file.path(), destination, expected_metadata)?;
    let final_state = PathFinalState {
        publication_path,
        publication_mode: PublicationMode::ReplaceExisting(expected_metadata),
    };
    Ok((temp_file, final_state))
}

#[cfg(unix)]
fn create_new_path_staging_file(
    destination: &Path,
) -> std::io::Result<(NamedTempFile, PathFinalState)> {
    let publication_path = publication_path_for_absent_destination(destination)?;
    let parent = publication_path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("destination has no parent: {}", destination.display()),
        )
    })?;
    let mut builder = tempfile::Builder::new();
    builder.permissions(std::fs::Permissions::from_mode(0o666));
    let temp_file = builder.tempfile_in(parent)?;
    let final_state = PathFinalState {
        publication_path,
        publication_mode: PublicationMode::CreateNew,
    };
    Ok((temp_file, final_state))
}

#[cfg(unix)]
fn publication_path_for_absent_destination(destination: &Path) -> std::io::Result<PathBuf> {
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = destination.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("destination has no file name: {}", destination.display()),
        )
    })?;
    Ok(std::fs::canonicalize(parent)?.join(file_name))
}

#[cfg(unix)]
fn writable_destination_error(destination: &Path, error: std::io::Error) -> std::io::Error {
    if error.kind() == std::io::ErrorKind::PermissionDenied {
        std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("destination is not writable: {}", destination.display()),
        )
    } else {
        error
    }
}

#[cfg(unix)]
fn preserve_existing_path_metadata(
    staging_path: &Path,
    destination: &Path,
    expected: UnixFileMetadata,
) -> std::io::Result<()> {
    std::os::unix::fs::chown(staging_path, Some(expected.uid), Some(expected.gid))
        .map_err(|error| metadata_preservation_error(destination, "ownership", error))?;
    std::fs::set_permissions(staging_path, std::fs::Permissions::from_mode(expected.mode))
        .map_err(|error| metadata_preservation_error(destination, "permissions", error))?;

    let staged_metadata = std::fs::metadata(staging_path)?;
    let staged_mode = staged_metadata.permissions().mode() & 0o7777;
    if staged_metadata.uid() != expected.uid
        || staged_metadata.gid() != expected.gid
        || staged_mode != expected.mode
    {
        return Err(std::io::Error::other(format!(
            "failed to preserve destination ownership and permissions: {}",
            destination.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn unix_file_metadata(metadata: &std::fs::Metadata) -> UnixFileMetadata {
    UnixFileMetadata {
        uid: metadata.uid(),
        gid: metadata.gid(),
        mode: metadata.permissions().mode() & 0o7777,
    }
}

#[cfg(unix)]
fn metadata_preservation_error(
    destination: &Path,
    metadata_kind: &str,
    error: std::io::Error,
) -> std::io::Error {
    std::io::Error::new(
        error.kind(),
        format!(
            "failed to preserve destination {metadata_kind} for {}: {error}",
            destination.display()
        ),
    )
}

#[cfg(not(unix))]
fn create_path_staging_file(
    destination: &Path,
) -> std::io::Result<(NamedTempFile, PathFinalState)> {
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    tempfile::Builder::new()
        .tempfile_in(parent)
        .map(|temp_file| (temp_file, PathFinalState {}))
}

#[cfg(unix)]
fn publish_path(
    staging_path: TempPath,
    destination: &Path,
    final_state: PathFinalState,
) -> std::io::Result<()> {
    let PathFinalState {
        publication_path,
        publication_mode,
    } = final_state;

    match publication_mode {
        PublicationMode::ReplaceExisting(expected_metadata) => {
            preserve_existing_path_metadata(&staging_path, destination, expected_metadata)?;
            staging_path
                .persist(publication_path)
                .map_err(|error| error.error)
        }
        PublicationMode::CreateNew => {
            let current_publication_path = publication_path_for_absent_destination(destination)?;
            if current_publication_path != publication_path {
                return Err(destination_changed_error(destination));
            }
            staging_path
                .persist_noclobber(publication_path)
                .map_err(|error| {
                    if error.error.kind() == std::io::ErrorKind::AlreadyExists {
                        destination_changed_error(destination)
                    } else {
                        error.error
                    }
                })
        }
    }
}

#[cfg(unix)]
fn destination_changed_error(destination: &Path) -> std::io::Error {
    std::io::Error::other(format!(
        "destination changed while writing: {}",
        destination.display()
    ))
}

#[cfg(not(unix))]
fn publish_path(
    staging_path: TempPath,
    destination: &Path,
    _final_state: PathFinalState,
) -> std::io::Result<()> {
    staging_path
        .persist(destination)
        .map_err(|error| error.error)
}

/// Close the footer before making any bytes visible at the requested output.
fn finalize_writer(ruby: &Ruby, writer_output: WriterOutput) -> Result<(), MagnusError> {
    match writer_output {
        WriterOutput::Path {
            writer,
            staging_path,
            destination,
            final_state,
        } => {
            writer.close().map_err(|error| {
                MagnusError::new(ruby.exception_runtime_error(), error.to_string())
            })?;
            publish_path(staging_path, &destination, final_state).map_err(|error| {
                MagnusError::new(
                    ruby.exception_runtime_error(),
                    format!(
                        "Failed to publish staging file to {}: {}",
                        destination.display(),
                        error
                    ),
                )
            })
        }
        WriterOutput::Io {
            writer,
            staging_path,
            io_object,
        } => {
            writer.close().map_err(|error| {
                MagnusError::new(ruby.exception_runtime_error(), error.to_string())
            })?;
            copy_temp_file_to_io(ruby, staging_path, io_object)
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn final_metadata_failure_leaves_existing_destination_unchanged() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("destination.parquet");
        std::fs::write(&destination, b"existing destination").unwrap();
        let expected_metadata = unix_file_metadata(&std::fs::metadata(&destination).unwrap());

        let mut staging_file = tempfile::Builder::new()
            .tempfile_in(directory.path())
            .unwrap();
        staging_file.write_all(b"complete replacement").unwrap();
        let staging_path = staging_file.into_temp_path();
        std::fs::remove_file(&staging_path).unwrap();

        let result = publish_path(
            staging_path,
            &destination,
            PathFinalState {
                publication_path: destination.clone(),
                publication_mode: PublicationMode::ReplaceExisting(expected_metadata),
            },
        );

        assert!(result.is_err());
        assert_eq!(
            std::fs::read(&destination).unwrap(),
            b"existing destination"
        );
    }
}

fn copy_temp_file_to_io(
    ruby: &Ruby,
    staging_path: TempPath,
    io_object: Value,
) -> Result<(), MagnusError> {
    let file = File::open(&staging_path).map_err(|error| {
        MagnusError::new(
            ruby.exception_runtime_error(),
            format!("Failed to reopen temporary file: {error}"),
        )
    })?;
    let mut reader = BufReader::new(file);
    let mut writer = BufWriter::new(RubyIOWriter::new(io_object));

    std::io::copy(&mut reader, &mut writer).map_err(|error| {
        MagnusError::new(
            ruby.exception_runtime_error(),
            format!("Failed to copy temp file to IO object: {error}"),
        )
    })?;
    writer.flush().map_err(|error| {
        MagnusError::new(
            ruby.exception_runtime_error(),
            format!("Failed to flush IO object: {error}"),
        )
    })
}

fn input_enumerator(ruby: &Ruby, input: Value) -> Result<Enumerator, MagnusError> {
    if !input.respond_to("each", false)? {
        return Err(MagnusError::new(
            ruby.exception_type_error(),
            "data must respond to 'each'",
        ));
    }
    Ok(input.enumeratorize("each", ()))
}

fn conversion_error(ruby: &Ruby, error: impl ToString) -> MagnusError {
    let message = error.to_string();
    if message.contains("EncodingError") || message.contains("invalid utf-8") {
        let message = message
            .find("EncodingError: ")
            .map(|position| message[position + 15..].to_string())
            .unwrap_or(message);
        MagnusError::new(ruby.exception_encoding_error(), message)
    } else {
        MagnusError::new(ruby.exception_runtime_error(), message)
    }
}

fn apply_non_fatal_logging_policy(result: Result<(), MagnusError>) {
    if let Err(_logger_error) = result {
        // Logging is observational: caller logger failures must not change file
        // contents, publication, or the result of the write operation.
    }
}

fn array_entry(ruby: &Ruby, array: RArray, index: usize) -> Result<Value, MagnusError> {
    let index = isize::try_from(index).map_err(|_| {
        MagnusError::new(ruby.exception_runtime_error(), "array index exceeds isize")
    })?;
    array.entry(index)
}

fn write_row_value(
    ruby: &Ruby,
    writer: &mut parquet_core::Writer<File>,
    converter: &mut RubyValueConverter,
    field_schemas: &[SchemaNode],
    row_value: Value,
) -> Result<(), MagnusError> {
    if !row_value.is_kind_of(ruby.class_array()) {
        return Err(MagnusError::new(
            ruby.exception_type_error(),
            "each row must be an array",
        ));
    }

    let row_array: RArray = TryConvert::try_convert(row_value)?;
    if row_array.len() != field_schemas.len() {
        return Err(MagnusError::new(
            ruby.exception_runtime_error(),
            format!(
                "Row has {} values but schema has {} fields",
                row_array.len(),
                field_schemas.len()
            ),
        ));
    }

    let mut row = Vec::with_capacity(field_schemas.len());
    for (column_index, field_schema) in field_schemas.iter().enumerate() {
        let value = array_entry(ruby, row_array, column_index)?;
        row.push(
            converter
                .to_parquet_with_schema_hint(value, Some(field_schema))
                .map_err(|error| conversion_error(ruby, error))?,
        );
    }

    writer
        .write_row(row)
        .map_err(|error| MagnusError::new(ruby.exception_runtime_error(), error.to_string()))
}

fn write_column_batch(
    ruby: &Ruby,
    writer: &mut parquet_core::Writer<File>,
    converter: &mut RubyValueConverter,
    field_schemas: &[SchemaNode],
    batch_index: usize,
    batch_value: Value,
) -> Result<usize, MagnusError> {
    if !batch_value.is_kind_of(ruby.class_array()) {
        return Err(MagnusError::new(
            ruby.exception_type_error(),
            format!("batch {batch_index} must be an array of column values"),
        ));
    }

    let batch: RArray = TryConvert::try_convert(batch_value)?;
    if batch.len() != field_schemas.len() {
        return Err(MagnusError::new(
            ruby.exception_runtime_error(),
            format!(
                "Batch {batch_index} has {} columns but schema has {}",
                batch.len(),
                field_schemas.len()
            ),
        ));
    }

    let mut columns = Vec::with_capacity(field_schemas.len());
    let mut row_count = None;
    for column_index in 0..field_schemas.len() {
        let values = array_entry(ruby, batch, column_index)?;
        if !values.is_kind_of(ruby.class_array()) {
            return Err(MagnusError::new(
                ruby.exception_type_error(),
                format!("batch {batch_index} column {column_index} must be an array"),
            ));
        }
        let values: RArray = TryConvert::try_convert(values)?;
        match row_count {
            None => row_count = Some(values.len()),
            Some(expected) if values.len() != expected => {
                return Err(MagnusError::new(
                    ruby.exception_runtime_error(),
                    format!(
                        "batch {batch_index} column {column_index} has {} values but expected {expected}",
                        values.len()
                    ),
                ));
            }
            Some(_) => {}
        }
        columns.push(values);
    }

    let row_count = row_count.unwrap_or(0);
    for row_index in 0..row_count {
        let mut row = Vec::with_capacity(field_schemas.len());
        for (column, field_schema) in columns.iter().zip(field_schemas) {
            let value = array_entry(ruby, *column, row_index)?;
            row.push(
                converter
                    .to_parquet_with_schema_hint(value, Some(field_schema))
                    .map_err(|error| conversion_error(ruby, error))?,
            );
        }
        writer
            .write_row(row)
            .map_err(|error| MagnusError::new(ruby.exception_runtime_error(), error.to_string()))?;
    }

    Ok(row_count)
}

/// Write a finite enumeration of logical rows without retaining prior input.
pub fn write_rows(
    ruby: &Ruby,
    write_args: crate::types::ParquetWriteArgs,
) -> Result<Value, MagnusError> {
    let logger = RubyLogger::new(write_args.logger)?;
    let prepared_output = prepare_writer_output(ruby, write_args.write_to)?;
    let mut input = input_enumerator(ruby, write_args.read_from)?;
    let first_row = input.next().transpose()?;
    let schema_hash = process_schema_value(ruby, write_args.schema_value, first_row)
        .map_err(|error| MagnusError::new(ruby.exception_runtime_error(), error.to_string()))?;
    let schema = ruby_schema_to_parquet(schema_hash)
        .map_err(|error| MagnusError::new(ruby.exception_runtime_error(), error.to_string()))?;
    let field_schemas = extract_field_schemas(&schema);
    let mut writer_output = create_writer(
        ruby,
        prepared_output,
        schema,
        write_args.compression,
        BatchSizingOptions {
            batch_size: write_args.batch_size,
            flush_threshold: write_args.flush_threshold,
            sample_size: write_args.sample_size,
        },
    )?;

    apply_non_fatal_logging_policy(logger.info(|| "Starting to write parquet file".to_string()));
    let mut converter = if let Some(capacity) = write_args.string_cache {
        apply_non_fatal_logging_policy(
            logger.debug(|| format!("String cache enabled (capacity {capacity})")),
        );
        RubyValueConverter::with_string_cache(StringCache::new(capacity))
    } else {
        RubyValueConverter::new()
    };
    let mut total_rows = 0u64;

    if let Some(row) = first_row {
        write_row_value(
            ruby,
            writer_output.writer_mut(),
            &mut converter,
            &field_schemas,
            row,
        )?;
        total_rows += 1;
    }
    for row in input {
        write_row_value(
            ruby,
            writer_output.writer_mut(),
            &mut converter,
            &field_schemas,
            row?,
        )?;
        total_rows = total_rows.checked_add(1).ok_or_else(|| {
            MagnusError::new(ruby.exception_runtime_error(), "row count exceeds u64")
        })?;
    }

    if let Some(stats) = converter.string_cache_stats() {
        apply_non_fatal_logging_policy(logger.info(|| {
            format!(
                "String cache stats: {} cache misses, {} hits ({:.1}% hit rate)",
                stats.misses,
                stats.hits,
                stats.hit_rate * 100.0
            )
        }));
    }
    finalize_writer(ruby, writer_output)?;
    apply_non_fatal_logging_policy(
        logger.info(|| format!("Finished writing {total_rows} rows to parquet file")),
    );

    Ok(ruby.qnil().as_value())
}

/// Write a finite enumeration of column batches without retaining prior batches.
pub fn write_columns(
    ruby: &Ruby,
    write_args: crate::types::ParquetWriteArgs,
) -> Result<Value, MagnusError> {
    let logger = RubyLogger::new(write_args.logger)?;
    let prepared_output = prepare_writer_output(ruby, write_args.write_to)?;
    let mut input = input_enumerator(ruby, write_args.read_from)?;
    let first_batch = input.next().transpose()?;
    let schema_hash = process_schema_value(ruby, write_args.schema_value, first_batch)
        .map_err(|error| MagnusError::new(ruby.exception_runtime_error(), error.to_string()))?;
    let schema = ruby_schema_to_parquet(schema_hash)
        .map_err(|error| MagnusError::new(ruby.exception_runtime_error(), error.to_string()))?;
    if !matches!(schema.root, SchemaNode::Struct { .. }) {
        return Err(MagnusError::new(
            ruby.exception_runtime_error(),
            "Schema root must be a struct",
        ));
    }
    let field_schemas = extract_field_schemas(&schema);
    let mut writer_output = create_writer(
        ruby,
        prepared_output,
        schema,
        write_args.compression,
        BatchSizingOptions {
            batch_size: None,
            flush_threshold: write_args.flush_threshold,
            sample_size: None,
        },
    )?;

    apply_non_fatal_logging_policy(
        logger.info(|| "Starting to write parquet file columns".to_string()),
    );
    let mut converter = RubyValueConverter::new();
    let mut total_rows = 0u64;
    let mut batch_index = 0usize;

    if let Some(batch) = first_batch {
        let rows = write_column_batch(
            ruby,
            writer_output.writer_mut(),
            &mut converter,
            &field_schemas,
            batch_index,
            batch,
        )?;
        total_rows = u64::try_from(rows).map_err(|_| {
            MagnusError::new(ruby.exception_runtime_error(), "row count exceeds u64")
        })?;
        batch_index += 1;
    }
    for batch in input {
        let rows = write_column_batch(
            ruby,
            writer_output.writer_mut(),
            &mut converter,
            &field_schemas,
            batch_index,
            batch?,
        )?;
        total_rows = total_rows
            .checked_add(u64::try_from(rows).map_err(|_| {
                MagnusError::new(ruby.exception_runtime_error(), "row count exceeds u64")
            })?)
            .ok_or_else(|| {
                MagnusError::new(ruby.exception_runtime_error(), "row count exceeds u64")
            })?;
        batch_index += 1;
    }

    finalize_writer(ruby, writer_output)?;
    apply_non_fatal_logging_policy(
        logger.info(|| format!("Finished writing {total_rows} rows to parquet file columns")),
    );

    Ok(ruby.qnil().as_value())
}
