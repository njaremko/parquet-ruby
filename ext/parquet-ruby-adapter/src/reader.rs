use magnus::value::ReprValue;
use magnus::{Error as MagnusError, IntoValue, RArray, RHash, Ruby, TryConvert, Value};
use parquet_core::reader::Reader;

use crate::{
    converter::parquet_to_ruby,
    io::{RubyIOReader, ThreadSafeRubyIOReader},
    logger::RubyLogger,
    types::{ColumnEnumeratorArgs, ParserResultType, RowEnumeratorArgs},
    utils::{create_column_enumerator, create_row_enumerator, handle_block_or_enum},
    CloneableChunkReader,
};

/// Read parquet file row by row
pub fn each_row(
    ruby: &Ruby,
    rb_self: Value,
    to_read: Value,
    result_type: ParserResultType,
    columns: Option<Vec<String>>,
    strict: bool,
    logger: RubyLogger,
) -> Result<Value, MagnusError> {
    if let Some(enum_value) = handle_block_or_enum(ruby.block_given(), || {
        create_row_enumerator(RowEnumeratorArgs {
            rb_self,
            to_read,
            result_type,
            columns: columns.clone(),
            strict,
            logger: logger.inner(),
        })
        .map(|yield_enum| yield_enum.into_value_with(ruby))
    })? {
        return Ok(enum_value);
    }

    // Log start of processing
    let _ = logger.info(|| "Starting to read parquet file".to_string());

    // Create a streaming reader based on input type
    let chunk_reader = if to_read.is_kind_of(ruby.class_string()) {
        let path_str: String = TryConvert::try_convert(to_read)?;
        let _ = logger.debug(|| format!("Reading from file: {}", path_str));
        CloneableChunkReader::from_path(&path_str)
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?
    } else if to_read.respond_to("read", false)? {
        // Handle IO objects with streaming
        let _ = logger.debug(|| "Reading from IO object".to_string());
        let ruby_reader = RubyIOReader::new(to_read)
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
        let thread_safe_reader = ThreadSafeRubyIOReader::new(ruby_reader);

        CloneableChunkReader::from_ruby_io(thread_safe_reader)
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?
    } else {
        return Err(MagnusError::new(
            ruby.exception_runtime_error(),
            format!(
                "Invalid input type: expected String or IO object with read method, got {}",
                to_read.class()
            ),
        ));
    };

    let reader = Reader::new(chunk_reader.clone());
    let mut reader_for_metadata = Reader::new(chunk_reader);

    // Get metadata to extract column names
    let metadata = reader_for_metadata
        .metadata()
        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
    let schema = metadata.schema();
    let all_column_names: Vec<String> = schema
        .get_fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    let _ = logger.info(|| format!("Processing {} columns", all_column_names.len()));

    // Get the row iterator
    let (row_iter, column_names) = if let Some(ref cols) = columns {
        let iter = reader
            .read_rows_with_projection(cols)
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
        (iter, cols.clone())
    } else {
        let iter = reader
            .read_rows()
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
        (iter, all_column_names)
    };

    // Process with block
    let proc = ruby.block_proc().map_err(|e| {
        MagnusError::new(
            ruby.exception_runtime_error(),
            format!("Failed to get block: {}", e),
        )
    })?;
    let mut row_count = 0u64;

    for row_result in row_iter {
        let row = row_result
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

        // Convert row to Ruby value based on result_type
        let ruby_row = match result_type {
            ParserResultType::Array => {
                let array: RArray = ruby.ary_new_capa(row.len());
                for value in row {
                    let ruby_value = parquet_to_ruby(value).map_err(|e| {
                        MagnusError::new(ruby.exception_runtime_error(), e.to_string())
                    })?;
                    array.push(ruby_value)?;
                }
                array.as_value()
            }
            ParserResultType::Hash => {
                let hash: RHash = ruby.hash_new();
                for (idx, value) in row.into_iter().enumerate() {
                    if idx < column_names.len() {
                        let ruby_value = parquet_to_ruby(value).map_err(|e| {
                            MagnusError::new(ruby.exception_runtime_error(), e.to_string())
                        })?;
                        hash.aset(column_names[idx].as_str(), ruby_value)?;
                    }
                }
                hash.as_value()
            }
        };

        proc.call::<_, Value>((ruby_row,))?;

        row_count += 1;
        if row_count % 1000 == 0 {
            let _ = logger.debug(|| format!("Processed {} rows", row_count));
        }
    }

    let _ = logger.info(|| format!("Finished processing {} rows", row_count));

    Ok(ruby.qnil().as_value())
}

/// Arguments for each_column function
struct EachColumnArgs {
    rb_self: Value,
    to_read: Value,
    result_type: ParserResultType,
    columns: Option<Vec<String>>,
    batch_size: Option<usize>,
    strict: bool,
    logger: RubyLogger,
}

/// Read parquet file column by column
#[allow(clippy::too_many_arguments)]
pub fn each_column(
    ruby: &Ruby,
    rb_self: Value,
    to_read: Value,
    result_type: ParserResultType,
    columns: Option<Vec<String>>,
    batch_size: Option<usize>,
    strict: bool,
    logger: RubyLogger,
) -> Result<Value, MagnusError> {
    let args = EachColumnArgs {
        rb_self,
        to_read,
        result_type,
        columns,
        batch_size,
        strict,
        logger,
    };
    each_column_impl(ruby, args)
}

fn each_column_impl(ruby: &Ruby, args: EachColumnArgs) -> Result<Value, MagnusError> {
    if let Some(enum_value) = handle_block_or_enum(ruby.block_given(), || {
        create_column_enumerator(ColumnEnumeratorArgs {
            rb_self: args.rb_self,
            to_read: args.to_read,
            result_type: args.result_type,
            columns: args.columns.clone(),
            batch_size: args.batch_size,
            strict: args.strict,
            logger: args.logger.inner(),
        })
        .map(|yield_enum| yield_enum.into_value_with(ruby))
    })? {
        return Ok(enum_value);
    }

    // Log start of processing
    let _ = args
        .logger
        .info(|| "Starting to read parquet file columns".to_string());

    // Create a streaming reader based on input type
    let chunk_reader = if args.to_read.is_kind_of(ruby.class_string()) {
        let path_str: String = TryConvert::try_convert(args.to_read)?;
        let _ = args
            .logger
            .debug(|| format!("Reading columns from file: {}", path_str));
        CloneableChunkReader::from_path(&path_str)
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?
    } else if args.to_read.respond_to("read", false)? {
        // Handle IO objects with streaming
        let _ = args
            .logger
            .debug(|| "Reading columns from IO object".to_string());
        let ruby_reader = RubyIOReader::new(args.to_read)
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
        let thread_safe_reader = ThreadSafeRubyIOReader::new(ruby_reader);

        CloneableChunkReader::from_ruby_io(thread_safe_reader)
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?
    } else {
        return Err(MagnusError::new(
            ruby.exception_runtime_error(),
            format!(
                "Invalid input type: expected String or IO object with read method, got {}",
                args.to_read.class()
            ),
        ));
    };

    let reader = Reader::new(chunk_reader.clone());
    let mut reader_for_metadata = Reader::new(chunk_reader);

    // Get metadata to extract column names
    let metadata = reader_for_metadata
        .metadata()
        .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
    let schema = metadata.schema();
    let all_column_names: Vec<String> = schema
        .get_fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    // Get the column iterator
    let (col_iter, _column_names) = if let Some(ref cols) = args.columns {
        let iter = reader
            .read_columns_with_projection(cols, args.batch_size)
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
        (iter, cols.clone())
    } else {
        let iter = reader
            .read_columns(args.batch_size)
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;
        (iter, all_column_names)
    };

    // Process with block
    let proc = ruby.block_proc().map_err(|e| {
        MagnusError::new(
            ruby.exception_runtime_error(),
            format!("Failed to get block: {}", e),
        )
    })?;
    let mut batch_count = 0u64;

    for batch_result in col_iter {
        let batch = batch_result
            .map_err(|e| MagnusError::new(ruby.exception_runtime_error(), e.to_string()))?;

        // Convert batch to Ruby value based on result_type
        let ruby_batch = match args.result_type {
            ParserResultType::Array => {
                let array: RArray = ruby.ary_new_capa(batch.columns.len());
                for (_name, values) in batch.columns {
                    let col_array: RArray = ruby.ary_new_capa(values.len());
                    for value in values {
                        let ruby_value = parquet_to_ruby(value).map_err(|e| {
                            MagnusError::new(ruby.exception_runtime_error(), e.to_string())
                        })?;
                        col_array.push(ruby_value)?;
                    }
                    array.push(col_array)?;
                }
                array.as_value()
            }
            ParserResultType::Hash => {
                let hash: RHash = ruby.hash_new();
                for (name, values) in batch.columns {
                    let col_array: RArray = ruby.ary_new_capa(values.len());
                    for value in values {
                        let ruby_value = parquet_to_ruby(value).map_err(|e| {
                            MagnusError::new(ruby.exception_runtime_error(), e.to_string())
                        })?;
                        col_array.push(ruby_value)?;
                    }
                    hash.aset(name, col_array)?;
                }
                hash.as_value()
            }
        };

        proc.call::<_, Value>((ruby_batch,))?;

        batch_count += 1;
        let _ = args
            .logger
            .debug(|| format!("Processed batch {}", batch_count));
    }

    let _ = args
        .logger
        .info(|| format!("Finished processing {} batches", batch_count));

    Ok(ruby.qnil().as_value())
}
