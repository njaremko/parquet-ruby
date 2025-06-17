use crate::header_cache::StringCache;
use crate::logger::RubyLogger;
use crate::types::TryIntoValue;
use crate::{
    create_column_enumerator, create_row_enumerator, ColumnEnumeratorArgs, ColumnRecord,
    ParquetField, ParquetGemError, ParquetValueVec, ParserResultType, RowEnumeratorArgs, RowRecord,
};
use ahash::RandomState;
use either::Either;
use magnus::IntoValue;
use magnus::{Error as MagnusError, Ruby, Value};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::OnceLock;

use super::arrow_reader::{
    process_arrow_column_data, process_arrow_file_column_data, process_arrow_row_data,
};
use super::common::{
    create_batch_reader, handle_block_or_enum, handle_empty_file, open_data_source, DataSource,
};
use crate::types::ArrayWrapper;

/// A unified parser configuration that can be used for both row and column parsing
pub enum ParserType {
    Row {
        strict: bool,
    },
    Column {
        batch_size: Option<usize>,
        strict: bool,
    },
}

/// Unified parser arguments structure
pub struct UnifiedParserArgs {
    pub to_read: Value,
    pub result_type: ParserResultType,
    pub columns: Option<Vec<String>>,
    pub parser_type: ParserType,
    pub logger: Option<Value>,
}

/// Unified implementation for parsing Parquet data (both rows and columns)
pub fn parse_parquet_unified(
    ruby: Rc<Ruby>,
    rb_self: Value,
    args: UnifiedParserArgs,
) -> Result<Value, ParquetGemError> {
    let UnifiedParserArgs {
        to_read,
        result_type,
        columns,
        parser_type,
        logger,
    } = args;

    // Initialize the logger if provided
    let ruby_logger = RubyLogger::new(&ruby, logger)?;

    // Clone values for the closure to avoid move issues
    let columns_clone = columns.clone();

    // Determine if we're handling rows or columns for enumerator creation
    match &parser_type {
        ParserType::Row { strict } => {
            // Handle block or create row enumerator
            if let Some(enum_value) = handle_block_or_enum(&ruby, ruby.block_given(), || {
                create_row_enumerator(RowEnumeratorArgs {
                    rb_self,
                    to_read,
                    result_type,
                    columns: columns_clone,
                    strict: *strict,
                    logger,
                })
                .map(|yield_enum| yield_enum.into_value_with(&ruby))
            })? {
                return Ok(enum_value);
            }
        }
        ParserType::Column { batch_size, strict } => {
            // For column-based parsing, log the batch size if present
            if let Some(ref bs) = batch_size {
                ruby_logger.debug(|| format!("Using batch size: {}", bs))?;
            }

            // Handle block or create column enumerator
            if let Some(enum_value) = handle_block_or_enum(&ruby, ruby.block_given(), || {
                create_column_enumerator(ColumnEnumeratorArgs {
                    rb_self,
                    to_read,
                    result_type,
                    columns: columns_clone,
                    batch_size: *batch_size,
                    strict: *strict,
                    logger: logger.as_ref().map(|_| to_read),
                })
                .map(|yield_enum| yield_enum.into_value_with(&ruby))
            })? {
                return Ok(enum_value);
            }
        }
    }

    // Open the data source and detect format
    let source = open_data_source(ruby.clone(), to_read, &ruby_logger)?;

    // Based on the source format and parser type, handle the data differently
    match (source, &parser_type) {
        (DataSource::Parquet(reader), ParserType::Row { strict }) => {
            // Handle Parquet row-based parsing
            process_row_data(
                ruby.clone(),
                reader,
                &columns,
                result_type,
                *strict,
                &ruby_logger,
            )?;
        }
        (DataSource::Parquet(reader), ParserType::Column { batch_size, strict }) => {
            // Handle Parquet column-based parsing
            process_column_data(
                ruby.clone(),
                reader,
                &columns,
                result_type,
                *batch_size,
                *strict,
                &ruby_logger,
            )?;
        }
        (DataSource::Arrow(reader), ParserType::Row { strict }) => {
            // Handle Arrow row-based parsing
            match reader {
                Either::Left(file) => {
                    // For seekable files, use FileReader which handles IPC file format
                    use arrow_ipc::reader::FileReader;
                    let file_reader = FileReader::try_new(file, None)
                        .map_err(|e| ParquetGemError::ArrowIpc(e.to_string()))?;

                    use super::arrow_reader::process_arrow_file_row_data;
                    process_arrow_file_row_data(
                        ruby.clone(),
                        file_reader,
                        &columns,
                        result_type,
                        *strict,
                        &ruby_logger,
                    )?;
                }
                Either::Right(readable) => {
                    use arrow_ipc::reader::StreamReader;
                    let stream_reader = StreamReader::try_new(readable, None)
                        .map_err(|e| ParquetGemError::ArrowIpc(e.to_string()))?;
                    process_arrow_row_data(
                        ruby.clone(),
                        stream_reader,
                        &columns,
                        result_type,
                        *strict,
                        &ruby_logger,
                    )?;
                }
            }
        }
        (DataSource::Arrow(reader), ParserType::Column { batch_size, strict }) => {
            // Handle Arrow column-based parsing
            match reader {
                Either::Left(file) => {
                    // For seekable files, we can use the optimized FileReader
                    process_arrow_file_column_data(
                        ruby.clone(),
                        file,
                        &columns,
                        result_type,
                        *batch_size,
                        *strict,
                        &ruby_logger,
                    )?;
                }
                Either::Right(readable) => {
                    use arrow_ipc::reader::StreamReader;
                    let stream_reader = StreamReader::try_new(readable, None)
                        .map_err(|e| ParquetGemError::ArrowIpc(e.to_string()))?;
                    process_arrow_column_data(
                        ruby.clone(),
                        stream_reader,
                        &columns,
                        result_type,
                        *batch_size,
                        *strict,
                        &ruby_logger,
                    )?;
                }
            }
        }
    }

    Ok(ruby.qnil().into_value_with(&ruby))
}

/// Process row-based Parquet data
fn process_row_data(
    ruby: Rc<Ruby>,
    source: Either<std::fs::File, crate::ruby_reader::ThreadSafeRubyReader>,
    columns: &Option<Vec<String>>,
    result_type: ParserResultType,
    strict: bool,
    ruby_logger: &RubyLogger,
) -> Result<(), ParquetGemError> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::reader::RowIter as ParquetRowIter;

    // Create the row-based reader
    let reader: Box<dyn FileReader> = match source {
        Either::Left(file) => {
            Box::new(SerializedFileReader::new(file).map_err(ParquetGemError::from)?)
        }
        Either::Right(readable) => {
            Box::new(SerializedFileReader::new(readable).map_err(ParquetGemError::from)?)
        }
    };

    let schema = reader.metadata().file_metadata().schema().clone();
    ruby_logger.debug(|| format!("Schema loaded: {:?}", schema))?;

    let mut iter = ParquetRowIter::from_file_into(reader);
    if let Some(cols) = columns {
        ruby_logger.debug(|| format!("Projecting columns: {:?}", cols))?;
        let projection = create_projection_schema(&schema, cols);
        iter = iter.project(Some(projection.to_owned())).map_err(|e| {
            MagnusError::new(
                ruby.exception_runtime_error(),
                format!("Failed to create projection: {}", e),
            )
        })?;
    }

    match result_type {
        ParserResultType::Hash => {
            let headers = OnceLock::new();
            let headers_clone = headers.clone();
            let iter = iter.map(move |row| {
                row.map(|row| {
                    let headers = headers_clone.get_or_init(|| {
                        let column_count = row.get_column_iter().count();

                        let mut header_string = Vec::with_capacity(column_count);
                        for (k, _) in row.get_column_iter() {
                            header_string.push(k.to_owned());
                        }

                        StringCache::intern_many(&header_string).expect("Failed to intern headers")
                    });

                    let mut map =
                        HashMap::with_capacity_and_hasher(headers.len(), RandomState::default());
                    for (i, ((_, v), t)) in
                        row.get_column_iter().zip(schema.get_fields()).enumerate()
                    {
                        let type_info = t.get_basic_info();
                        map.insert(
                            headers[i],
                            ParquetField {
                                field: v.clone(),
                                converted_type: type_info.converted_type(),
                                logical_type: type_info.logical_type().clone(),
                                strict,
                            },
                        );
                    }
                    map
                })
                .map(RowRecord::Map::<RandomState>)
                .map_err(ParquetGemError::from)
            });

            for result in iter {
                let record = result?;
                let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
            }
        }
        ParserResultType::Array => {
            let iter = iter.map(|row| {
                row.map(|row| {
                    let column_count = row.get_column_iter().count();
                    let mut vec = Vec::with_capacity(column_count);
                    for ((_, v), t) in row.get_column_iter().zip(schema.get_fields()) {
                        let type_info = t.get_basic_info();
                        vec.push(ParquetField {
                            field: v.clone(),
                            converted_type: type_info.converted_type(),
                            logical_type: type_info.logical_type().clone(),
                            strict,
                        });
                    }
                    vec
                })
                .map(RowRecord::Vec::<RandomState>)
                .map_err(ParquetGemError::from)
            });

            for result in iter {
                let record = result?;
                let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
            }
        }
    }

    Ok(())
}

/// Process column-based Parquet data
fn process_column_data(
    ruby: Rc<Ruby>,
    source: Either<std::fs::File, crate::ruby_reader::ThreadSafeRubyReader>,
    columns: &Option<Vec<String>>,
    result_type: ParserResultType,
    batch_size: Option<usize>,
    strict: bool,
    _ruby_logger: &RubyLogger,
) -> Result<(), ParquetGemError> {
    // Create the batch reader
    let (batch_reader, schema, num_rows) = match source {
        Either::Left(file) => create_batch_reader(file, columns, batch_size)?,
        Either::Right(readable) => create_batch_reader(readable, columns, batch_size)?,
    };

    match result_type {
        ParserResultType::Hash => {
            // For hash return type, we need to return a hash with column names pointing at empty arrays
            if handle_empty_file(&ruby, &schema, num_rows)? {
                return Ok(());
            }

            let headers = OnceLock::new();
            let headers_clone = headers.clone();
            let iter = batch_reader.map(move |batch| {
                batch.map_err(ParquetGemError::Arrow).and_then(|batch| {
                    let local_headers = headers_clone
                        .get_or_init(|| {
                            let schema = batch.schema();
                            let fields = schema.fields();
                            let mut header_string = Vec::with_capacity(fields.len());
                            for field in fields {
                                header_string.push(field.name().to_owned());
                            }
                            StringCache::intern_many(&header_string)
                        })
                        .as_ref()
                        .map_err(|e| ParquetGemError::HeaderIntern(e.clone()))?;

                    let mut map = HashMap::with_capacity_and_hasher(
                        local_headers.len(),
                        RandomState::default(),
                    );

                    batch
                        .columns()
                        .iter()
                        .enumerate()
                        .try_for_each(|(i, column)| {
                            let header = local_headers[i];
                            let values = ParquetValueVec::try_from(ArrayWrapper {
                                array: column,
                                strict,
                            })?;
                            map.insert(header, values.into_inner());
                            Ok::<_, ParquetGemError>(())
                        })?;

                    Ok(ColumnRecord::Map::<RandomState>(map))
                })
            });

            for result in iter {
                let record = result?;
                let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
            }
        }
        ParserResultType::Array => {
            let iter = batch_reader.map(|batch| {
                batch.map_err(ParquetGemError::Arrow).and_then(|batch| {
                    let vec = batch
                        .columns()
                        .iter()
                        .map(|column| {
                            let values = ParquetValueVec::try_from(ArrayWrapper {
                                array: column,
                                strict,
                            })?;
                            Ok::<_, ParquetGemError>(values.into_inner())
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(ColumnRecord::Vec::<RandomState>(vec))
                })
            });

            for result in iter {
                let record = result?;
                let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
            }
        }
    }

    Ok(())
}

/// Helper function to create a projection schema
fn create_projection_schema(
    schema: &parquet::schema::types::Type,
    columns: &[String],
) -> parquet::schema::types::Type {
    if let parquet::schema::types::Type::GroupType { fields, .. } = schema {
        let projected_fields: Vec<std::sync::Arc<parquet::schema::types::Type>> = fields
            .iter()
            .filter(|field| columns.contains(&field.name().to_string()))
            .cloned()
            .collect();

        parquet::schema::types::Type::GroupType {
            basic_info: schema.get_basic_info().clone(),
            fields: projected_fields,
        }
    } else {
        // Return original schema if not a group type
        schema.clone()
    }
}
