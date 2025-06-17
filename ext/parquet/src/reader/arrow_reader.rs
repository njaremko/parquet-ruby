use crate::header_cache::StringCache;
use crate::logger::RubyLogger;
use crate::types::ArrayWrapper;
use crate::types::{
    ColumnRecord, ParquetGemError, ParquetValueVec, ParserResultType, RowRecord, TryIntoValue,
};
use ahash::RandomState;
use arrow_array::RecordBatch;
use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_schema::Schema;
use magnus::{Ruby, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::rc::Rc;
use std::sync::{Arc, OnceLock};

/// Process Arrow IPC file data for column-based parsing
pub fn process_arrow_column_data<R: Read>(
    ruby: Rc<Ruby>,
    reader: StreamReader<R>,
    columns: &Option<Vec<String>>,
    result_type: ParserResultType,
    _batch_size: Option<usize>,
    strict: bool,
    ruby_logger: &RubyLogger,
) -> Result<(), ParquetGemError> {
    let schema = reader.schema();
    ruby_logger.debug(|| format!("Arrow schema loaded: {:?}", schema))?;

    // Filter schema if columns are specified
    let _filtered_schema = if let Some(cols) = columns {
        let mut fields = Vec::new();
        for field in schema.fields() {
            if cols.contains(&field.name().to_string()) {
                fields.push(field.clone());
            }
        }
        Arc::new(Schema::new(fields))
    } else {
        schema.clone()
    };

    match result_type {
        ParserResultType::Hash => {
            let headers = OnceLock::new();

            for batch_result in reader {
                let batch = batch_result.map_err(|e| ParquetGemError::ArrowIpc(e.to_string()))?;

                // Filter columns if needed
                let batch = if let Some(cols) = columns {
                    filter_record_batch(&batch, cols)?
                } else {
                    batch
                };

                let local_headers = headers
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

                let mut map =
                    HashMap::with_capacity_and_hasher(local_headers.len(), RandomState::default());

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

                let record = ColumnRecord::Map::<RandomState>(map);
                let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
            }
        }
        ParserResultType::Array => {
            for batch_result in reader {
                let batch = batch_result.map_err(|e| ParquetGemError::ArrowIpc(e.to_string()))?;

                // Filter columns if needed
                let batch = if let Some(cols) = columns {
                    filter_record_batch(&batch, cols)?
                } else {
                    batch
                };

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

                let record = ColumnRecord::Vec::<RandomState>(vec);
                let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
            }
        }
    }

    Ok(())
}

/// Process Arrow IPC file data for row-based parsing
pub fn process_arrow_row_data<R: Read>(
    ruby: Rc<Ruby>,
    reader: StreamReader<R>,
    columns: &Option<Vec<String>>,
    result_type: ParserResultType,
    strict: bool,
    ruby_logger: &RubyLogger,
) -> Result<(), ParquetGemError> {
    let schema = reader.schema();
    ruby_logger.debug(|| format!("Arrow schema loaded: {:?}", schema))?;

    match result_type {
        ParserResultType::Hash => {
            let headers = OnceLock::new();

            for batch_result in reader {
                let batch = batch_result.map_err(|e| ParquetGemError::ArrowIpc(e.to_string()))?;

                // Filter columns if needed
                let batch = if let Some(cols) = columns {
                    filter_record_batch(&batch, cols)?
                } else {
                    batch
                };

                let local_headers = headers
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

                // Convert columnar data to rows
                for row_idx in 0..batch.num_rows() {
                    let mut map = HashMap::with_capacity_and_hasher(
                        local_headers.len(),
                        RandomState::default(),
                    );

                    for (col_idx, column) in batch.columns().iter().enumerate() {
                        let header = local_headers[col_idx];
                        let value = extract_value_at_index(column, row_idx, strict)?;
                        map.insert(header, value);
                    }

                    let record = RowRecord::Map::<RandomState>(map);
                    let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
                }
            }
        }
        ParserResultType::Array => {
            for batch_result in reader {
                let batch = batch_result.map_err(|e| ParquetGemError::ArrowIpc(e.to_string()))?;

                // Filter columns if needed
                let batch = if let Some(cols) = columns {
                    filter_record_batch(&batch, cols)?
                } else {
                    batch
                };

                // Convert columnar data to rows
                for row_idx in 0..batch.num_rows() {
                    let mut row_vec = Vec::with_capacity(batch.num_columns());

                    for column in batch.columns() {
                        let value = extract_value_at_index(column, row_idx, strict)?;
                        row_vec.push(value);
                    }

                    let record = RowRecord::Vec::<RandomState>(row_vec);
                    let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
                }
            }
        }
    }

    Ok(())
}

/// Process Arrow IPC file with FileReader for row-based parsing
pub fn process_arrow_file_row_data(
    ruby: Rc<Ruby>,
    reader: FileReader<File>,
    columns: &Option<Vec<String>>,
    result_type: ParserResultType,
    strict: bool,
    ruby_logger: &RubyLogger,
) -> Result<(), ParquetGemError> {
    let schema = reader.schema();
    ruby_logger.debug(|| format!("Arrow file schema loaded: {:?}", schema))?;

    match result_type {
        ParserResultType::Hash => {
            let headers = OnceLock::new();

            for batch_result in reader {
                let batch = batch_result.map_err(|e| ParquetGemError::ArrowIpc(e.to_string()))?;

                // Filter columns if needed
                let batch = if let Some(cols) = columns {
                    filter_record_batch(&batch, cols)?
                } else {
                    batch
                };

                let local_headers = headers
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

                // Convert columnar data to rows
                for row_idx in 0..batch.num_rows() {
                    let mut map = HashMap::with_capacity_and_hasher(
                        local_headers.len(),
                        RandomState::default(),
                    );

                    for (col_idx, column) in batch.columns().iter().enumerate() {
                        let header = local_headers[col_idx];
                        let value = extract_value_at_index(column, row_idx, strict)?;
                        map.insert(header, value);
                    }

                    let record = RowRecord::Map::<RandomState>(map);
                    let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
                }
            }
        }
        ParserResultType::Array => {
            for batch_result in reader {
                let batch = batch_result.map_err(|e| ParquetGemError::ArrowIpc(e.to_string()))?;

                // Filter columns if needed
                let batch = if let Some(cols) = columns {
                    filter_record_batch(&batch, cols)?
                } else {
                    batch
                };

                // Convert columnar data to rows
                for row_idx in 0..batch.num_rows() {
                    let mut row_vec = Vec::with_capacity(batch.num_columns());

                    for column in batch.columns() {
                        let value = extract_value_at_index(column, row_idx, strict)?;
                        row_vec.push(value);
                    }

                    let record = RowRecord::Vec::<RandomState>(row_vec);
                    let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
                }
            }
        }
    }

    Ok(())
}

/// Process Arrow IPC file with FileReader (for seekable sources)
pub fn process_arrow_file_column_data(
    ruby: Rc<Ruby>,
    file: File,
    columns: &Option<Vec<String>>,
    result_type: ParserResultType,
    _batch_size: Option<usize>,
    strict: bool,
    ruby_logger: &RubyLogger,
) -> Result<(), ParquetGemError> {
    let reader =
        FileReader::try_new(file, None).map_err(|e| ParquetGemError::ArrowIpc(e.to_string()))?;

    let schema = reader.schema();
    ruby_logger.debug(|| format!("Arrow file schema loaded: {:?}", schema))?;

    // FileReader implements Iterator<Item = Result<RecordBatch, ArrowError>>
    match result_type {
        ParserResultType::Hash => {
            let headers = OnceLock::new();

            for batch_result in reader {
                let batch = batch_result.map_err(|e| ParquetGemError::ArrowIpc(e.to_string()))?;

                // Filter columns if needed
                let batch = if let Some(cols) = columns {
                    filter_record_batch(&batch, cols)?
                } else {
                    batch
                };

                let local_headers = headers
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

                let mut map =
                    HashMap::with_capacity_and_hasher(local_headers.len(), RandomState::default());

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

                let record = ColumnRecord::Map::<RandomState>(map);
                let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
            }
        }
        ParserResultType::Array => {
            for batch_result in reader {
                let batch = batch_result.map_err(|e| ParquetGemError::ArrowIpc(e.to_string()))?;

                // Filter columns if needed
                let batch = if let Some(cols) = columns {
                    filter_record_batch(&batch, cols)?
                } else {
                    batch
                };

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

                let record = ColumnRecord::Vec::<RandomState>(vec);
                let _: Value = ruby.yield_value(record.try_into_value_with(&ruby)?)?;
            }
        }
    }

    Ok(())
}

/// Extract a single value from an Arrow array at a specific index
fn extract_value_at_index(
    array: &Arc<dyn arrow_array::Array>,
    index: usize,
    strict: bool,
) -> Result<crate::types::ParquetField, ParquetGemError> {
    use crate::types::ParquetField;
    use arrow_array::*;
    use arrow_schema::DataType;
    use parquet::record::Field;

    // Convert Arrow array value at index to Parquet Field
    let field = match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            if arr.is_null(index) {
                Field::Null
            } else {
                Field::Bool(arr.value(index))
            }
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            if arr.is_null(index) {
                Field::Null
            } else {
                Field::Byte(arr.value(index) as i8)
            }
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            if arr.is_null(index) {
                Field::Null
            } else {
                Field::Short(arr.value(index))
            }
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            if arr.is_null(index) {
                Field::Null
            } else {
                Field::Int(arr.value(index))
            }
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            if arr.is_null(index) {
                Field::Null
            } else {
                Field::Long(arr.value(index))
            }
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            if arr.is_null(index) {
                Field::Null
            } else {
                Field::UByte(arr.value(index))
            }
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            if arr.is_null(index) {
                Field::Null
            } else {
                Field::UShort(arr.value(index))
            }
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            if arr.is_null(index) {
                Field::Null
            } else {
                Field::UInt(arr.value(index))
            }
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            if arr.is_null(index) {
                Field::Null
            } else {
                Field::ULong(arr.value(index))
            }
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            if arr.is_null(index) {
                Field::Null
            } else {
                Field::Float(arr.value(index))
            }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            if arr.is_null(index) {
                Field::Null
            } else {
                Field::Double(arr.value(index))
            }
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            if arr.is_null(index) {
                Field::Null
            } else {
                Field::Str(arr.value(index).to_string())
            }
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            if arr.is_null(index) {
                Field::Null
            } else {
                Field::Bytes(arr.value(index).into())
            }
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            if arr.is_null(index) {
                Field::Null
            } else {
                Field::Date(arr.value(index))
            }
        }
        DataType::Timestamp(unit, _tz) => match unit {
            arrow_schema::TimeUnit::Millisecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                if arr.is_null(index) {
                    Field::Null
                } else {
                    Field::TimestampMillis(arr.value(index))
                }
            }
            arrow_schema::TimeUnit::Microsecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                if arr.is_null(index) {
                    Field::Null
                } else {
                    Field::TimestampMicros(arr.value(index))
                }
            }
            _ => Field::Null,
        },
        // Add more type handling as needed
        _ => Field::Null,
    };

    // For Arrow files, we don't have Parquet logical types, so we use defaults
    Ok(ParquetField {
        field,
        converted_type: parquet::basic::ConvertedType::NONE,
        logical_type: None,
        strict,
    })
}

/// Filter a RecordBatch to only include specified columns
fn filter_record_batch(
    batch: &RecordBatch,
    columns: &[String],
) -> Result<RecordBatch, ParquetGemError> {
    let schema = batch.schema();
    let mut indices = Vec::new();
    let mut fields = Vec::new();

    for (i, field) in schema.fields().iter().enumerate() {
        if columns.contains(&field.name().to_string()) {
            indices.push(i);
            fields.push(field.clone());
        }
    }

    let new_schema = Arc::new(Schema::new(fields));
    let new_columns: Vec<_> = indices.iter().map(|&i| batch.column(i).clone()).collect();

    RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e| ParquetGemError::ArrowIpc(e.to_string()))
}
