//! Core Parquet writing functionality

use crate::{
    arrow_conversion::parquet_values_to_arrow_array, ParquetError, ParquetValue, Result, Schema,
    SchemaNode,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::Rng;
use std::sync::Arc;

// Default configuration constants
const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_MEMORY_THRESHOLD: usize = 100 * 1024 * 1024; // 100MB
const DEFAULT_SAMPLE_SIZE: usize = 100;
const MIN_BATCH_SIZE: usize = 10;
const MIN_SAMPLES_FOR_ESTIMATE: usize = 10;

/// Builder for creating a configured Writer
pub struct WriterBuilder {
    compression: Compression,
    batch_size: Option<usize>,
    memory_threshold: usize,
    sample_size: usize,
}

impl Default for WriterBuilder {
    fn default() -> Self {
        Self {
            compression: Compression::SNAPPY,
            batch_size: None,
            memory_threshold: DEFAULT_MEMORY_THRESHOLD,
            sample_size: DEFAULT_SAMPLE_SIZE,
        }
    }
}

impl WriterBuilder {
    /// Create a new WriterBuilder with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the compression algorithm
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Set a fixed batch size (disables dynamic sizing)
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }

    /// Set the memory threshold for flushing
    pub fn with_memory_threshold(mut self, threshold: usize) -> Self {
        self.memory_threshold = threshold;
        self
    }

    /// Set the sample size for row size estimation
    pub fn with_sample_size(mut self, size: usize) -> Self {
        self.sample_size = size;
        self
    }

    /// Build a Writer with the configured settings
    pub fn build<W: std::io::Write + Send>(self, writer: W, schema: Schema) -> Result<Writer<W>> {
        let arrow_schema = schema_to_arrow(&schema)?;

        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .build();

        let arrow_writer = ArrowWriter::try_new(writer, arrow_schema.clone(), Some(props))?;

        Ok(Writer {
            arrow_writer: Some(arrow_writer),
            arrow_schema,
            buffered_rows: Vec::new(),
            current_batch_size: self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE),
            memory_threshold: self.memory_threshold,
            sample_size: self.sample_size,
            size_samples: Vec::with_capacity(self.sample_size),
            total_rows_written: 0,
            fixed_batch_size: self.batch_size,
        })
    }
}

/// Core Parquet writer that works with any type implementing Write
pub struct Writer<W: std::io::Write> {
    arrow_writer: Option<ArrowWriter<W>>,
    arrow_schema: Arc<arrow_schema::Schema>,
    buffered_rows: Vec<Vec<ParquetValue>>,
    current_batch_size: usize,
    memory_threshold: usize,
    sample_size: usize,
    size_samples: Vec<usize>,
    total_rows_written: usize,
    fixed_batch_size: Option<usize>,
}

impl<W> Writer<W>
where
    W: std::io::Write + Send,
{
    /// Create a new writer with default settings
    pub fn new(writer: W, schema: Schema) -> Result<Self> {
        WriterBuilder::new().build(writer, schema)
    }

    /// Create a new writer with custom properties
    pub fn new_with_properties(writer: W, schema: Schema, props: WriterProperties) -> Result<Self> {
        let arrow_schema = schema_to_arrow(&schema)?;

        let arrow_writer = ArrowWriter::try_new(writer, arrow_schema.clone(), Some(props))?;

        Ok(Self {
            arrow_writer: Some(arrow_writer),
            arrow_schema,
            buffered_rows: Vec::new(),
            current_batch_size: DEFAULT_BATCH_SIZE,
            memory_threshold: DEFAULT_MEMORY_THRESHOLD,
            sample_size: DEFAULT_SAMPLE_SIZE,
            size_samples: Vec::with_capacity(DEFAULT_SAMPLE_SIZE),
            total_rows_written: 0,
            fixed_batch_size: None,
        })
    }

    /// Write a batch of rows to the Parquet file
    ///
    /// Each row is a vector of values corresponding to the schema fields
    pub fn write_rows(&mut self, rows: Vec<Vec<ParquetValue>>) -> Result<()> {
        for row in rows {
            self.write_row(row)?;
        }
        Ok(())
    }

    /// Write a single row to the Parquet file
    ///
    /// Rows are buffered internally and written in batches to optimize memory usage
    pub fn write_row(&mut self, row: Vec<ParquetValue>) -> Result<()> {
        // Validate row length
        let num_cols = self.arrow_schema.fields().len();
        if row.len() != num_cols {
            return Err(ParquetError::Schema(format!(
                "Row has {} values but schema has {} fields",
                row.len(),
                num_cols
            )));
        }

        // Validate each value matches its schema
        for (idx, (value, field)) in row.iter().zip(self.arrow_schema.fields()).enumerate() {
            validate_value_against_field(value, field, &format!("row[{}]", idx))?;
        }

        // Sample row size for dynamic batch sizing
        if self.fixed_batch_size.is_none() {
            self.sample_row_size(&row)?;
        }

        // Add row to buffer
        self.buffered_rows.push(row);

        // Check if we need to flush
        if self.buffered_rows.len() >= self.current_batch_size {
            self.flush_buffered_rows()?;
        }

        Ok(())
    }

    /// Sample row size for dynamic batch sizing using reservoir sampling
    fn sample_row_size(&mut self, row: &[ParquetValue]) -> Result<()> {
        let row_size = self.estimate_row_size(row)?;

        if self.size_samples.len() < self.sample_size {
            self.size_samples.push(row_size);
        } else {
            // Reservoir sampling
            let mut rng = rand::rng();
            let idx = rng.random_range(0..=self.total_rows_written);
            if idx < self.sample_size {
                self.size_samples[idx] = row_size;
            }
        }

        // Update batch size if we have enough samples
        if self.size_samples.len() >= MIN_SAMPLES_FOR_ESTIMATE {
            self.update_batch_size();
        }

        Ok(())
    }

    /// Estimate the memory size of a single row
    fn estimate_row_size(&self, row: &[ParquetValue]) -> Result<usize> {
        let mut size = 0;
        for (idx, value) in row.iter().enumerate() {
            let field = &self.arrow_schema.fields()[idx];
            size += self.estimate_value_size(value, field.data_type())?;
        }
        Ok(size)
    }

    /// Estimate the memory footprint of a single value
    #[allow(clippy::only_used_in_recursion)]
    fn estimate_value_size(&self, value: &ParquetValue, data_type: &DataType) -> Result<usize> {
        use ParquetValue::*;

        Ok(match (value, data_type) {
            (Null, _) => 0,

            // Fixed size types
            (Boolean(_), DataType::Boolean) => 1,
            (Int8(_), DataType::Int8) => 1,
            (UInt8(_), DataType::UInt8) => 1,
            (Int16(_), DataType::Int16) => 2,
            (UInt16(_), DataType::UInt16) => 2,
            (Int32(_), DataType::Int32) => 4,
            (UInt32(_), DataType::UInt32) => 4,
            (Float32(_), DataType::Float32) => 4,
            (Int64(_), DataType::Int64) => 8,
            (UInt64(_), DataType::UInt64) => 8,
            (Float64(_), DataType::Float64) => 8,
            (Date32(_), DataType::Date32) => 4,
            (Date64(_), DataType::Date64) => 8,
            (TimeMillis(_), DataType::Time32(_)) => 4,
            (TimeMicros(_), DataType::Time64(_)) => 8,
            (TimestampSecond(_, _), DataType::Timestamp(_, _)) => 8,
            (TimestampMillis(_, _), DataType::Timestamp(_, _)) => 8,
            (TimestampMicros(_, _), DataType::Timestamp(_, _)) => 8,
            (TimestampNanos(_, _), DataType::Timestamp(_, _)) => 8,
            (Decimal128(_, _), DataType::Decimal128(_, _)) => 16,

            // Variable size types
            (String(s), DataType::Utf8) => s.len() + std::mem::size_of::<usize>() * 3,
            (Bytes(b), DataType::Binary) => b.len() + std::mem::size_of::<usize>() * 3,
            (Bytes(_), DataType::FixedSizeBinary(len)) => *len as usize,

            (Decimal256(v, _), DataType::Decimal256(_, _)) => {
                let bytes = v.to_signed_bytes_le();
                32 + bytes.len()
            }

            // Complex types
            (List(items), DataType::List(field)) => {
                let base_size = std::mem::size_of::<usize>() * 3;
                if items.is_empty() {
                    base_size
                } else {
                    // Sample up to 5 elements
                    let sample_count = items.len().min(5);
                    let sample_size: usize = items
                        .iter()
                        .take(sample_count)
                        .map(|item| {
                            self.estimate_value_size(item, field.data_type())
                                .unwrap_or(0)
                        })
                        .sum();
                    let avg_size = sample_size / sample_count;
                    base_size + (avg_size * items.len())
                }
            }

            (Map(entries), DataType::Map(entries_field, _)) => {
                if let DataType::Struct(fields) = entries_field.data_type() {
                    let base_size = std::mem::size_of::<usize>() * 4;
                    if entries.is_empty() || fields.len() < 2 {
                        base_size
                    } else {
                        // Sample up to 5 entries
                        let sample_count = entries.len().min(5);
                        let mut total_size = base_size;

                        for (key, val) in entries.iter().take(sample_count) {
                            total_size += self
                                .estimate_value_size(key, fields[0].data_type())
                                .unwrap_or(0);
                            total_size += self
                                .estimate_value_size(val, fields[1].data_type())
                                .unwrap_or(0);
                        }

                        let avg_entry_size = (total_size - base_size) / sample_count;
                        base_size + (avg_entry_size * entries.len())
                    }
                } else {
                    100 // Default estimate
                }
            }

            (Record(fields), DataType::Struct(schema_fields)) => {
                let base_size = std::mem::size_of::<usize>() * 3;
                let field_sizes: usize = fields
                    .iter()
                    .zip(schema_fields.iter())
                    .map(|((_, val), field)| {
                        self.estimate_value_size(val, field.data_type())
                            .unwrap_or(0)
                    })
                    .sum();
                base_size + field_sizes
            }

            _ => 100, // Default estimate for mismatched types
        })
    }

    /// Update dynamic batch size based on current samples
    fn update_batch_size(&mut self) {
        if self.size_samples.is_empty() {
            return;
        }

        let total_size: usize = self.size_samples.iter().sum();
        let avg_row_size = (total_size as f64 / self.size_samples.len() as f64).max(1.0);
        let suggested_batch_size = (self.memory_threshold as f64 / avg_row_size).floor() as usize;
        self.current_batch_size = suggested_batch_size.max(MIN_BATCH_SIZE);
    }

    /// Flush buffered rows to the Parquet file
    fn flush_buffered_rows(&mut self) -> Result<()> {
        if self.buffered_rows.is_empty() {
            return Ok(());
        }

        let rows = std::mem::take(&mut self.buffered_rows);
        let num_rows = rows.len();
        self.total_rows_written += num_rows;

        // Convert rows to columnar format
        let num_cols = self.arrow_schema.fields().len();
        let mut columns: Vec<Vec<ParquetValue>> = vec![Vec::with_capacity(num_rows); num_cols];

        // Transpose rows to columns
        for row in rows {
            for (col_idx, value) in row.into_iter().enumerate() {
                columns[col_idx].push(value);
            }
        }

        // Convert columns to Arrow arrays
        let arrow_columns = columns
            .into_iter()
            .zip(self.arrow_schema.fields())
            .map(|(values, field)| parquet_values_to_arrow_array(values, field))
            .collect::<Result<Vec<_>>>()?;

        // Create RecordBatch
        let batch = RecordBatch::try_new(self.arrow_schema.clone(), arrow_columns)?;

        // Write the batch
        if let Some(writer) = &mut self.arrow_writer {
            writer.write(&batch)?;

            // Check if we need to flush based on memory usage
            if writer.in_progress_size() >= self.memory_threshold {
                writer.flush()?;
            }
        } else {
            return Err(ParquetError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Writer has been closed",
            )));
        }

        Ok(())
    }

    /// Write columns to the Parquet file
    ///
    /// Each element is a tuple of (column_name, values)
    pub fn write_columns(&mut self, columns: Vec<(String, Vec<ParquetValue>)>) -> Result<()> {
        if columns.is_empty() {
            return Ok(());
        }

        // Verify column names match schema
        let schema_fields = self.arrow_schema.fields();
        if columns.len() != schema_fields.len() {
            return Err(ParquetError::Schema(format!(
                "Provided {} columns but schema has {} fields",
                columns.len(),
                schema_fields.len()
            )));
        }

        // Sort columns to match schema order and convert to arrays
        let mut arrow_columns = Vec::with_capacity(columns.len());

        for field in schema_fields {
            let column_data = columns
                .iter()
                .find(|(name, _)| name == field.name())
                .ok_or_else(|| ParquetError::Schema(format!("Missing column: {}", field.name())))?;

            let array = parquet_values_to_arrow_array(column_data.1.clone(), field)?;
            arrow_columns.push(array);
        }

        // Create RecordBatch
        let batch = RecordBatch::try_new(self.arrow_schema.clone(), arrow_columns)?;

        // Write the batch
        if let Some(writer) = &mut self.arrow_writer {
            writer.write(&batch)?;
        } else {
            return Err(ParquetError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Writer has been closed",
            )));
        }

        Ok(())
    }

    /// Flush any buffered data
    pub fn flush(&mut self) -> Result<()> {
        // First flush any buffered rows
        self.flush_buffered_rows()?;

        // Then flush the arrow writer
        if let Some(writer) = &mut self.arrow_writer {
            writer.flush()?;
        }
        Ok(())
    }

    /// Close the writer and write the file footer
    ///
    /// This must be called to finalize the Parquet file
    pub fn close(mut self) -> Result<()> {
        // Flush any remaining buffered rows
        self.flush_buffered_rows()?;

        // Close the arrow writer
        if let Some(writer) = self.arrow_writer.take() {
            writer.close()?;
        }
        Ok(())
    }
}

/// Validate a value against its field schema
fn validate_value_against_field(value: &ParquetValue, field: &Field, path: &str) -> Result<()> {
    use ParquetValue::*;

    // Null handling
    if matches!(value, Null) {
        if !field.is_nullable() {
            return Err(ParquetError::Schema(format!(
                "Found null value for non-nullable field at {}",
                path
            )));
        }
        return Ok(());
    }

    // Type validation
    match (value, field.data_type()) {
        // Boolean
        (Boolean(_), DataType::Boolean) => Ok(()),

        // Integer types
        (Int8(_), DataType::Int8) => Ok(()),
        (Int16(_), DataType::Int16) => Ok(()),
        (Int32(_), DataType::Int32) => Ok(()),
        (Int64(_), DataType::Int64) => Ok(()),
        (UInt8(_), DataType::UInt8) => Ok(()),
        (UInt16(_), DataType::UInt16) => Ok(()),
        (UInt32(_), DataType::UInt32) => Ok(()),
        (UInt64(_), DataType::UInt64) => Ok(()),

        // Float types
        (Float16(_), DataType::Float16) => Ok(()),
        (Float32(_), DataType::Float32) => Ok(()),
        (Float64(_), DataType::Float64) => Ok(()),

        // String and binary
        (String(_), DataType::Utf8) => Ok(()),
        (Bytes(_), DataType::Binary) => Ok(()),
        (Bytes(_), DataType::FixedSizeBinary(_)) => Ok(()), // Size check done during conversion

        // Date/time types
        (Date32(_), DataType::Date32) => Ok(()),
        (Date64(_), DataType::Date64) => Ok(()),
        (TimeMillis(_), DataType::Time32(_)) => Ok(()),
        (TimeMicros(_), DataType::Time64(_)) => Ok(()),
        (TimestampSecond(_, _), DataType::Timestamp(_, _)) => Ok(()),
        (TimestampMillis(_, _), DataType::Timestamp(_, _)) => Ok(()),
        (TimestampMicros(_, _), DataType::Timestamp(_, _)) => Ok(()),
        (TimestampNanos(_, _), DataType::Timestamp(_, _)) => Ok(()),

        // Decimal types
        (Decimal128(_, _), DataType::Decimal128(_, _)) => Ok(()),
        (Decimal256(_, _), DataType::Decimal256(_, _)) => Ok(()),

        // List type
        (List(items), DataType::List(item_field)) => {
            for (idx, item) in items.iter().enumerate() {
                validate_value_against_field(item, item_field, &format!("{}[{}]", path, idx))?;
            }
            Ok(())
        }

        // Map type
        (Map(entries), DataType::Map(entries_field, _)) => {
            if let DataType::Struct(fields) = entries_field.data_type() {
                if fields.len() >= 2 {
                    let key_field = &fields[0];
                    let value_field = &fields[1];

                    for (idx, (key, val)) in entries.iter().enumerate() {
                        validate_value_against_field(
                            key,
                            key_field,
                            &format!("{}.key[{}]", path, idx),
                        )?;
                        validate_value_against_field(
                            val,
                            value_field,
                            &format!("{}.value[{}]", path, idx),
                        )?;
                    }
                }
            }
            Ok(())
        }

        // Struct type
        (Record(record_fields), DataType::Struct(schema_fields)) => {
            for field in schema_fields {
                let field_name = field.name();
                if let Some(value) = record_fields.get(field_name.as_str()) {
                    validate_value_against_field(
                        value,
                        field,
                        &format!("{}.{}", path, field_name),
                    )?;
                } else if !field.is_nullable() {
                    return Err(ParquetError::Schema(format!(
                        "Required field '{}' is missing in struct at {}",
                        field_name, path
                    )));
                }
            }
            Ok(())
        }

        // Type mismatch
        (value, expected_type) => Err(ParquetError::Schema(format!(
            "Type mismatch at {}: expected {:?}, got {:?}",
            path,
            expected_type,
            value.type_name()
        ))),
    }
}

/// Convert our Schema to Arrow Schema
fn schema_to_arrow(schema: &Schema) -> Result<Arc<arrow_schema::Schema>> {
    match &schema.root {
        SchemaNode::Struct { fields, .. } => {
            let arrow_fields = fields
                .iter()
                .map(schema_node_to_arrow_field)
                .collect::<Result<Vec<_>>>()?;

            Ok(Arc::new(arrow_schema::Schema::new(arrow_fields)))
        }
        _ => Err(ParquetError::Schema(
            "Root schema node must be a struct".to_string(),
        )),
    }
}

/// Convert a SchemaNode to an Arrow Field
fn schema_node_to_arrow_field(node: &SchemaNode) -> Result<Field> {
    match node {
        SchemaNode::Primitive {
            name,
            primitive_type,
            nullable,
            ..
        } => {
            let data_type = primitive_type_to_arrow(primitive_type)?;
            Ok(Field::new(name, data_type, *nullable))
        }
        SchemaNode::List {
            name,
            item,
            nullable,
        } => {
            let item_field = schema_node_to_arrow_field(item)?;
            let list_type = DataType::List(Arc::new(Field::new(
                "item",
                item_field.data_type().clone(),
                true,
            )));
            Ok(Field::new(name, list_type, *nullable))
        }
        SchemaNode::Map {
            name,
            key,
            value,
            nullable,
        } => {
            let key_field = schema_node_to_arrow_field(key)?;
            let value_field = schema_node_to_arrow_field(value)?;

            let struct_fields = vec![
                Field::new("key", key_field.data_type().clone(), false),
                Field::new("value", value_field.data_type().clone(), true),
            ];

            let map_type = DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(struct_fields.into()),
                    false,
                )),
                false, // keys_sorted
            );

            Ok(Field::new(name, map_type, *nullable))
        }
        SchemaNode::Struct {
            name,
            fields,
            nullable,
        } => {
            let struct_fields = fields
                .iter()
                .map(schema_node_to_arrow_field)
                .collect::<Result<Vec<_>>>()?;

            let struct_type = DataType::Struct(struct_fields.into());
            Ok(Field::new(name, struct_type, *nullable))
        }
    }
}

/// Convert PrimitiveType to Arrow DataType
fn primitive_type_to_arrow(ptype: &crate::PrimitiveType) -> Result<DataType> {
    use crate::PrimitiveType::*;

    Ok(match ptype {
        Boolean => DataType::Boolean,
        Int8 => DataType::Int8,
        Int16 => DataType::Int16,
        Int32 => DataType::Int32,
        Int64 => DataType::Int64,
        UInt8 => DataType::UInt8,
        UInt16 => DataType::UInt16,
        UInt32 => DataType::UInt32,
        UInt64 => DataType::UInt64,
        Float32 => DataType::Float32,
        Float64 => DataType::Float64,
        String => DataType::Utf8,
        Binary => DataType::Binary,
        Date32 => DataType::Date32,
        TimeMillis => DataType::Time32(arrow_schema::TimeUnit::Millisecond),
        TimeMicros => DataType::Time64(arrow_schema::TimeUnit::Microsecond),
        TimestampMillis(tz) => DataType::Timestamp(
            arrow_schema::TimeUnit::Millisecond,
            // PARQUET SPEC: ANY timezone (e.g., "+09:00", "America/New_York") means
            // UTC-normalized storage (isAdjustedToUTC = true). Original timezone is lost.
            tz.as_ref().map(|_| Arc::from("UTC")),
        ),
        TimestampMicros(tz) => DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            // PARQUET SPEC: ANY timezone (e.g., "+09:00", "America/New_York") means
            // UTC-normalized storage (isAdjustedToUTC = true). Original timezone is lost.
            tz.as_ref().map(|_| Arc::from("UTC")),
        ),
        Decimal128(precision, scale) => DataType::Decimal128(*precision, *scale),
        Decimal256(precision, scale) => DataType::Decimal256(*precision, *scale),
        Date64 => DataType::Date64,
        TimestampSecond(tz) => DataType::Timestamp(
            arrow_schema::TimeUnit::Second,
            // PARQUET SPEC: ANY timezone (e.g., "+09:00", "America/New_York") means
            // UTC-normalized storage (isAdjustedToUTC = true). Original timezone is lost.
            tz.as_ref().map(|_| Arc::from("UTC")),
        ),
        TimestampNanos(tz) => DataType::Timestamp(
            arrow_schema::TimeUnit::Nanosecond,
            // PARQUET SPEC: ANY timezone (e.g., "+09:00", "America/New_York") means
            // UTC-normalized storage (isAdjustedToUTC = true). Original timezone is lost.
            tz.as_ref().map(|_| Arc::from("UTC")),
        ),
        FixedLenByteArray(len) => DataType::FixedSizeBinary(*len),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SchemaBuilder;

    #[test]
    fn test_writer_creation() {
        let schema = SchemaBuilder::new()
            .with_root(SchemaNode::Struct {
                name: "root".to_string(),
                nullable: false,
                fields: vec![SchemaNode::Primitive {
                    name: "id".to_string(),
                    primitive_type: crate::PrimitiveType::Int64,
                    nullable: false,
                    format: None,
                }],
            })
            .build()
            .unwrap();

        let buffer = Vec::new();
        let _writer = Writer::new(buffer, schema).unwrap();
    }

    #[test]
    fn test_writer_builder() {
        let schema = SchemaBuilder::new()
            .with_root(SchemaNode::Struct {
                name: "root".to_string(),
                nullable: false,
                fields: vec![SchemaNode::Primitive {
                    name: "id".to_string(),
                    primitive_type: crate::PrimitiveType::Int64,
                    nullable: false,
                    format: None,
                }],
            })
            .build()
            .unwrap();

        let buffer = Vec::new();
        let _writer = WriterBuilder::new()
            .with_compression(Compression::ZSTD(parquet::basic::ZstdLevel::default()))
            .with_batch_size(500)
            .with_memory_threshold(50 * 1024 * 1024)
            .with_sample_size(50)
            .build(buffer, schema)
            .unwrap();
    }

    #[test]
    fn test_buffered_writing() {
        let schema = SchemaBuilder::new()
            .with_root(SchemaNode::Struct {
                name: "root".to_string(),
                nullable: false,
                fields: vec![
                    SchemaNode::Primitive {
                        name: "id".to_string(),
                        primitive_type: crate::PrimitiveType::Int64,
                        nullable: false,
                        format: None,
                    },
                    SchemaNode::Primitive {
                        name: "name".to_string(),
                        primitive_type: crate::PrimitiveType::String,
                        nullable: true,
                        format: None,
                    },
                ],
            })
            .build()
            .unwrap();

        let buffer = Vec::new();
        let mut writer = WriterBuilder::new()
            .with_batch_size(10) // Small batch for testing
            .build(buffer, schema)
            .unwrap();

        // Write 25 rows - should trigger 2 flushes with batch size 10
        for i in 0..25 {
            writer
                .write_row(vec![
                    ParquetValue::Int64(i),
                    ParquetValue::String(Arc::from(format!("row_{}", i))),
                ])
                .unwrap();
        }

        // Close to flush remaining rows
        writer.close().unwrap();
    }

    #[test]
    fn test_row_size_estimation() {
        let schema = SchemaBuilder::new()
            .with_root(SchemaNode::Struct {
                name: "root".to_string(),
                nullable: false,
                fields: vec![
                    SchemaNode::Primitive {
                        name: "id".to_string(),
                        primitive_type: crate::PrimitiveType::Int64,
                        nullable: false,
                        format: None,
                    },
                    SchemaNode::Primitive {
                        name: "data".to_string(),
                        primitive_type: crate::PrimitiveType::String,
                        nullable: false,
                        format: None,
                    },
                ],
            })
            .build()
            .unwrap();

        let buffer = Vec::new();
        let writer = Writer::new(buffer, schema).unwrap();

        // Test size estimation for different value types
        let row = vec![
            ParquetValue::Int64(12345),
            ParquetValue::String(Arc::from("Hello, World!")),
        ];

        let size = writer.estimate_row_size(&row).unwrap();
        assert!(size > 0);

        // Int64 = 8 bytes, String = 13 chars + overhead
        assert!(size >= 8 + 13);
    }
}
