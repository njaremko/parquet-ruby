//! Core Parquet writing functionality

use crate::{
    arrow_conversion::parquet_values_to_arrow_array, streaming_file_writer::StreamingFileWriter,
    ParquetError, ParquetValue, Result, Schema, SchemaNode,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::Rng;
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc as StdArc;

// Default configuration constants
const DEFAULT_BATCH_SIZE: usize = 1000;
// Default converted-value memory quantum: 100 MiB.
const DEFAULT_MEMORY_THRESHOLD: usize = 100 * 1024 * 1024;
// A caller may choose a very small converted-row quantum, but making every such
// quantum a row group would exhaust Parquet's i16 row-group ordinal. Encoded
// groups have an independent minimum while their metadata spills to disk.
const MIN_ROW_GROUP_TARGET_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_SAMPLE_SIZE: usize = 100;
const MIN_BATCH_SIZE: usize = 10;
// Ceiling for a fixed or dynamically-estimated batch size on a single-column
// schema. The effective cap is also limited by schema width below.
pub const MAX_BATCH_SIZE: usize = 1_000_000;
// `sample_size` also backs an eager Vec reservation during writer creation.
// Keep user-provided estimates from becoming an unbounded upfront allocation.
pub const MAX_SAMPLE_SIZE: usize = 10_000;
// Maximum live value slots across all per-column buffers. This keeps wide
// schemas from multiplying a row-count cap into an unbounded allocation.
const MAX_BUFFERED_VALUE_SLOTS: usize = 1_000_000;
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

        validate_memory_threshold(self.memory_threshold)?;
        validate_column_count(arrow_schema.fields().len())?;
        let current_batch_size = match self.batch_size {
            Some(size) => validate_fixed_batch_size(size, arrow_schema.fields().len())?,
            None => default_batch_size_for_column_count(arrow_schema.fields().len()),
        };
        let sample_size = validate_sample_size(self.sample_size)?;

        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .set_max_row_group_row_count(None)
            .set_max_row_group_bytes(None)
            .build();

        let file_writer = StreamingFileWriter::new(
            writer,
            arrow_schema.clone(),
            props,
            self.memory_threshold.max(MIN_ROW_GROUP_TARGET_BYTES),
        )?;
        let buffered_columns = new_buffered_columns(&arrow_schema);

        Ok(Writer {
            file_writer: Some(file_writer),
            arrow_schema,
            buffered_columns,
            buffered_row_count: 0,
            buffered_retained_bytes: 0,
            current_batch_size,
            memory_threshold: self.memory_threshold,
            sample_size,
            size_samples: Vec::with_capacity(sample_size),
            total_rows_seen: 0,
            fixed_batch_size: self.batch_size,
        })
    }
}

/// Core Parquet writer that works with any type implementing Write
pub struct Writer<W: std::io::Write> {
    file_writer: Option<StreamingFileWriter<W>>,
    arrow_schema: StdArc<arrow_schema::Schema>,
    buffered_columns: Vec<Vec<ParquetValue>>,
    buffered_row_count: usize,
    buffered_retained_bytes: usize,
    current_batch_size: usize,
    memory_threshold: usize,
    sample_size: usize,
    size_samples: Vec<usize>,
    total_rows_seen: usize,
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
        validate_column_count(arrow_schema.fields().len())?;
        let current_batch_size = default_batch_size_for_column_count(arrow_schema.fields().len());
        let row_group_target_bytes = props
            .max_row_group_bytes()
            .unwrap_or(DEFAULT_MEMORY_THRESHOLD);
        let file_writer =
            StreamingFileWriter::new(writer, arrow_schema.clone(), props, row_group_target_bytes)?;
        let buffered_columns = new_buffered_columns(&arrow_schema);

        Ok(Self {
            file_writer: Some(file_writer),
            arrow_schema,
            buffered_columns,
            buffered_row_count: 0,
            buffered_retained_bytes: 0,
            current_batch_size,
            memory_threshold: DEFAULT_MEMORY_THRESHOLD,
            sample_size: DEFAULT_SAMPLE_SIZE,
            size_samples: Vec::with_capacity(DEFAULT_SAMPLE_SIZE),
            total_rows_seen: 0,
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

        let row_retained_bytes = row.iter().fold(0usize, |total, value| {
            total.saturating_add(value.retained_size_bytes())
        });

        // Sample row size for dynamic batch sizing
        if self.fixed_batch_size.is_none() {
            self.sample_row_size(row_retained_bytes);
        }

        // A row that would cross either quantum starts the next batch. A
        // singleton row larger than the byte quantum is still valid and makes
        // progress by being written on its own.
        if self.buffered_row_count > 0
            && (self.buffered_row_count >= self.current_batch_size
                || self
                    .buffered_retained_bytes
                    .saturating_add(row_retained_bytes)
                    > self.memory_threshold)
        {
            self.flush_buffered_rows()?;
        }

        for (col_idx, value) in row.into_iter().enumerate() {
            self.buffered_columns[col_idx].push(value);
        }
        self.buffered_row_count += 1;
        self.buffered_retained_bytes = self
            .buffered_retained_bytes
            .saturating_add(row_retained_bytes);

        if self.buffered_row_count >= self.current_batch_size
            || self.buffered_retained_bytes >= self.memory_threshold
        {
            self.flush_buffered_rows()?;
        }

        Ok(())
    }

    /// Sample row size for dynamic batch sizing using reservoir sampling
    fn sample_row_size(&mut self, row_size: usize) {
        self.total_rows_seen = self.total_rows_seen.saturating_add(1);

        if self.size_samples.len() < self.sample_size {
            self.size_samples.push(row_size);
        } else {
            // Reservoir sampling
            let mut rng = rand::rng();
            let idx = rng.random_range(0..self.total_rows_seen);
            if idx < self.sample_size {
                self.size_samples[idx] = row_size;
            }
        }

        // Update batch size once the requested sample has been collected. Small
        // explicit sample sizes are valid because they bound how long large rows
        // may keep using the default batch size.
        let samples_required = self.sample_size.min(MIN_SAMPLES_FOR_ESTIMATE);
        if self.size_samples.len() >= samples_required {
            self.update_batch_size();
        }
    }

    /// Update dynamic batch size based on current samples
    fn update_batch_size(&mut self) {
        if self.size_samples.is_empty() {
            return;
        }

        let total_size = self
            .size_samples
            .iter()
            .fold(0usize, |total, size| total.saturating_add(*size));
        let avg_row_size = (total_size as f64 / self.size_samples.len() as f64).max(1.0);
        let suggested_batch_size = (self.memory_threshold as f64 / avg_row_size).floor() as usize;
        self.current_batch_size = dynamic_batch_size_for_column_count(
            suggested_batch_size,
            self.arrow_schema.fields().len(),
        );
    }

    /// Flush buffered rows to the Parquet file
    fn flush_buffered_rows(&mut self) -> Result<()> {
        if self.buffered_row_count == 0 {
            return Ok(());
        }

        // Convert columns to Arrow arrays
        let arrow_columns = self
            .buffered_columns
            .iter()
            .zip(self.arrow_schema.fields())
            .map(|(values, field)| parquet_values_to_arrow_array(values, field))
            .collect::<Result<Vec<_>>>()?;

        // Create RecordBatch
        let batch = RecordBatch::try_new(self.arrow_schema.clone(), arrow_columns)?;

        // Write the batch
        if let Some(writer) = &mut self.file_writer {
            writer.write(&batch)?;

            self.buffered_row_count = 0;
            self.buffered_retained_bytes = 0;
            for column in &mut self.buffered_columns {
                column.clear();
            }
        } else {
            return Err(ParquetError::Io(std::io::Error::other(
                "Writer has been closed",
            )));
        }

        Ok(())
    }

    /// Write columns to the Parquet file
    ///
    /// Each element is one bounded caller-owned column batch. Values are moved
    /// through the same byte-aware row admission path as `write_row`, so Arrow
    /// conversion never materializes a second batch proportional to the full
    /// file.
    pub fn write_columns(&mut self, columns: Vec<(String, Vec<ParquetValue>)>) -> Result<()> {
        if columns.is_empty() {
            return self.flush_buffered_rows();
        }

        if columns.len() != self.arrow_schema.fields().len() {
            return Err(ParquetError::Schema(format!(
                "Provided {} columns but schema has {} fields",
                columns.len(),
                self.arrow_schema.fields().len()
            )));
        }

        let mut columns_by_name = HashMap::with_capacity(columns.len());
        for (name, values) in columns {
            match columns_by_name.entry(name) {
                Entry::Vacant(entry) => {
                    entry.insert(values);
                }
                Entry::Occupied(entry) => {
                    return Err(ParquetError::Schema(format!(
                        "Duplicate column: {}",
                        entry.key()
                    )));
                }
            }
        }

        let ordered_columns = {
            let schema_fields = self.arrow_schema.fields();
            // Anchor the expected length to the first schema column and report
            // mismatches in schema order, independent of HashMap iteration.
            let expected_len = schema_fields
                .first()
                .and_then(|field| columns_by_name.get(field.name().as_str()))
                .map_or(0, Vec::len);
            for field in schema_fields {
                if let Some(values) = columns_by_name.get(field.name().as_str()) {
                    if values.len() != expected_len {
                        return Err(ParquetError::Schema(format!(
                            "Column '{}' has {} values but expected {}",
                            field.name(),
                            values.len(),
                            expected_len
                        )));
                    }
                }
            }

            let mut ordered = Vec::with_capacity(schema_fields.len());
            for field in schema_fields {
                let values = columns_by_name
                    .remove(field.name().as_str())
                    .ok_or_else(|| {
                        ParquetError::Schema(format!("Missing column: {}", field.name()))
                    })?;
                for (index, value) in values.iter().enumerate() {
                    validate_value_against_field(
                        value,
                        field,
                        &format!("column '{}'[{}]", field.name(), index),
                    )?;
                }
                ordered.push(values);
            }
            ordered
        };

        let row_count = ordered_columns.first().map_or(0, Vec::len);
        let mut columns = ordered_columns
            .into_iter()
            .map(Vec::into_iter)
            .collect::<Vec<_>>();
        for _ in 0..row_count {
            let row = columns
                .iter_mut()
                .map(|column| {
                    column.next().ok_or_else(|| {
                        ParquetError::Schema(
                            "validated column batch ended unexpectedly".to_string(),
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            self.write_row(row)?;
        }

        self.flush_buffered_rows()
    }

    /// Flush any buffered data
    pub fn flush(&mut self) -> Result<()> {
        // First flush any buffered rows
        self.flush_buffered_rows()?;

        // Then flush the arrow writer
        if let Some(writer) = &mut self.file_writer {
            writer.flush_row_group()?;
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
        if let Some(writer) = self.file_writer.take() {
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
        (Bytes(b), DataType::FixedSizeBinary(size)) => {
            // Validate up front so a wrong-length value is rejected at write_row
            // rather than poisoning the buffer at flush time.
            if b.len() != *size as usize {
                return Err(ParquetError::Schema(format!(
                    "Fixed size binary expected {} bytes, got {} at {}",
                    size,
                    b.len(),
                    path
                )));
            }
            Ok(())
        }

        // Date/time types
        (Date32(_), DataType::Date32) => Ok(()),
        (Date64(_), DataType::Date64) => Ok(()),
        (TimeMillis(_), DataType::Time32(_)) => Ok(()),
        (TimeMicros(_), DataType::Time64(_)) => Ok(()),
        (TimeNanos(_), DataType::Time64(_)) => Ok(()),
        (TimestampSecond(_, _), DataType::Timestamp(_, _)) => Ok(()),
        (TimestampMillis(_, _), DataType::Timestamp(_, _)) => Ok(()),
        (TimestampMicros(_, _), DataType::Timestamp(_, _)) => Ok(()),
        (TimestampNanos(_, _), DataType::Timestamp(_, _)) => Ok(()),

        // Decimal types
        (Decimal128(decimal, value_scale), DataType::Decimal128(precision, scale)) => {
            validate_decimal128_schema(*decimal, *value_scale, *precision, *scale, path)
        }
        (Decimal256(decimal, value_scale), DataType::Decimal256(precision, scale)) => {
            validate_decimal256_schema(decimal, *value_scale, *precision, *scale, path)
        }

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
fn schema_to_arrow(schema: &Schema) -> Result<StdArc<arrow_schema::Schema>> {
    schema.validate().map_err(ParquetError::Schema)?;
    match &schema.root {
        SchemaNode::Struct { fields, .. } => {
            let arrow_fields = fields
                .iter()
                .map(schema_node_to_arrow_field)
                .collect::<Result<Vec<_>>>()?;

            Ok(StdArc::new(arrow_schema::Schema::new(arrow_fields)))
        }
        _ => Err(ParquetError::Schema(
            "Root schema node must be a struct".to_string(),
        )),
    }
}

fn validate_column_count(column_count: usize) -> Result<()> {
    if column_count > MAX_BUFFERED_VALUE_SLOTS {
        return Err(ParquetError::Schema(format!(
            "Schema has {} columns, exceeding the writer buffer slot limit of {}",
            column_count, MAX_BUFFERED_VALUE_SLOTS
        )));
    }
    Ok(())
}

fn validate_memory_threshold(memory_threshold: usize) -> Result<()> {
    if memory_threshold == 0 {
        return Err(ParquetError::Schema(
            "memory threshold must be greater than 0".to_string(),
        ));
    }
    Ok(())
}

pub fn max_batch_size_for_column_count(column_count: usize) -> usize {
    let width = column_count.max(1);
    (MAX_BUFFERED_VALUE_SLOTS / width).clamp(1, MAX_BATCH_SIZE)
}

fn default_batch_size_for_column_count(column_count: usize) -> usize {
    DEFAULT_BATCH_SIZE.min(max_batch_size_for_column_count(column_count))
}

fn validate_fixed_batch_size(batch_size: usize, column_count: usize) -> Result<usize> {
    if batch_size == 0 {
        return Err(ParquetError::Schema(
            "batch_size must be greater than 0".to_string(),
        ));
    }

    let max_batch_size = max_batch_size_for_column_count(column_count);
    if batch_size > max_batch_size {
        return Err(ParquetError::Schema(format!(
            "batch_size {} exceeds maximum {} for {} columns",
            batch_size, max_batch_size, column_count
        )));
    }

    Ok(batch_size)
}

fn validate_sample_size(sample_size: usize) -> Result<usize> {
    if sample_size == 0 {
        return Err(ParquetError::Schema(
            "sample_size must be greater than 0".to_string(),
        ));
    }
    if sample_size > MAX_SAMPLE_SIZE {
        return Err(ParquetError::Schema(format!(
            "sample_size {} exceeds maximum {}",
            sample_size, MAX_SAMPLE_SIZE
        )));
    }
    Ok(sample_size)
}

fn dynamic_batch_size_for_column_count(suggested_batch_size: usize, column_count: usize) -> usize {
    let max_batch_size = max_batch_size_for_column_count(column_count);
    let min_batch_size = MIN_BATCH_SIZE.min(max_batch_size);
    suggested_batch_size.clamp(min_batch_size, max_batch_size)
}

/// Convert a SchemaNode to an Arrow Field
fn schema_node_to_arrow_field(node: &SchemaNode) -> Result<Field> {
    match node {
        SchemaNode::Primitive {
            name,
            primitive_type,
            nullable,
            format,
        } => {
            let data_type = primitive_type_to_arrow(primitive_type)?;
            let field = Field::new(name, data_type, *nullable);
            let extended_field = if format.as_deref() == Some("uuid") {
                field.with_extension_type(arrow_schema::extension::Uuid)
            } else {
                field
            };
            Ok(extended_field)
        }
        SchemaNode::List {
            name,
            item,
            nullable,
        } => {
            let item_field = schema_node_to_arrow_field(item)?;
            // Use the conventional Arrow list element name "item" rather than the
            // schema node's internal name (e.g. "<field>_item"), so written files
            // interoperate with external Parquet readers. The element's data type
            // and nullability still come from the schema node.
            let list_type = DataType::List(StdArc::new(Field::new(
                "item",
                item_field.data_type().clone(),
                item_field.is_nullable(),
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
                Field::new(
                    key_field.name().clone(),
                    key_field.data_type().clone(),
                    false,
                ),
                Field::new(
                    value_field.name().clone(),
                    value_field.data_type().clone(),
                    value_field.is_nullable(),
                ),
            ];

            let map_type = DataType::Map(
                StdArc::new(Field::new(
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

fn new_buffered_columns(arrow_schema: &arrow_schema::Schema) -> Vec<Vec<ParquetValue>> {
    let column_count = arrow_schema.fields().len();
    debug_assert!(column_count <= MAX_BUFFERED_VALUE_SLOTS);

    arrow_schema.fields().iter().map(|_| Vec::new()).collect()
}

fn validate_decimal128_schema(
    value: i128,
    value_scale: i8,
    precision: u8,
    scale: i8,
    path: &str,
) -> Result<()> {
    if value_scale != scale {
        return Err(ParquetError::Schema(format!(
            "Decimal scale mismatch at {}: schema scale {}, value scale {}",
            path, scale, value_scale
        )));
    }

    validate_decimal_precision(decimal128_digit_count(value), precision, path)
}

fn validate_decimal256_schema(
    value: &num::BigInt,
    value_scale: i8,
    precision: u8,
    scale: i8,
    path: &str,
) -> Result<()> {
    if value_scale != scale {
        return Err(ParquetError::Schema(format!(
            "Decimal scale mismatch at {}: schema scale {}, value scale {}",
            path, scale, value_scale
        )));
    }

    validate_decimal_precision(decimal256_digit_count(value), precision, path)
}

fn validate_decimal_precision(value_digits: usize, precision: u8, path: &str) -> Result<()> {
    if value_digits > precision as usize {
        return Err(ParquetError::Schema(format!(
            "Decimal precision overflow at {}: schema precision {}, value has {} digits",
            path, precision, value_digits
        )));
    }

    Ok(())
}

fn decimal128_digit_count(value: i128) -> usize {
    value.unsigned_abs().to_string().len()
}

fn decimal256_digit_count(value: &num::BigInt) -> usize {
    value.to_str_radix(10).trim_start_matches('-').len()
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
        TimeNanos => DataType::Time64(arrow_schema::TimeUnit::Nanosecond),
        TimestampMillis(tz) => DataType::Timestamp(
            arrow_schema::TimeUnit::Millisecond,
            // PARQUET SPEC: ANY timezone (e.g., "+09:00", "America/New_York") means
            // UTC-normalized storage (isAdjustedToUTC = true). Original timezone is lost.
            tz.as_ref().map(|_| StdArc::from("UTC")),
        ),
        TimestampMicros(tz) => DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            // PARQUET SPEC: ANY timezone (e.g., "+09:00", "America/New_York") means
            // UTC-normalized storage (isAdjustedToUTC = true). Original timezone is lost.
            tz.as_ref().map(|_| StdArc::from("UTC")),
        ),
        Decimal128(precision, scale) => DataType::Decimal128(*precision, *scale),
        Decimal256(precision, scale) => DataType::Decimal256(*precision, *scale),
        Date64 => DataType::Date64,
        TimestampSecond(tz) => DataType::Timestamp(
            arrow_schema::TimeUnit::Second,
            // PARQUET SPEC: ANY timezone (e.g., "+09:00", "America/New_York") means
            // UTC-normalized storage (isAdjustedToUTC = true). Original timezone is lost.
            tz.as_ref().map(|_| StdArc::from("UTC")),
        ),
        TimestampNanos(tz) => DataType::Timestamp(
            arrow_schema::TimeUnit::Nanosecond,
            // PARQUET SPEC: ANY timezone (e.g., "+09:00", "America/New_York") means
            // UTC-normalized storage (isAdjustedToUTC = true). Original timezone is lost.
            tz.as_ref().map(|_| StdArc::from("UTC")),
        ),
        FixedLenByteArray(len) => DataType::FixedSizeBinary(*len),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SchemaBuilder;
    use triomphe::Arc;

    fn int64_schema(column_count: usize) -> Schema {
        SchemaBuilder::new()
            .with_root(SchemaNode::Struct {
                name: "root".to_string(),
                nullable: false,
                fields: (0..column_count)
                    .map(|index| SchemaNode::Primitive {
                        name: format!("field_{index}"),
                        primitive_type: crate::PrimitiveType::Int64,
                        nullable: false,
                        format: None,
                    })
                    .collect(),
            })
            .build()
            .unwrap()
    }

    fn single_int64_schema() -> Schema {
        int64_schema(1)
    }

    fn single_int64_writer(buffer: Vec<u8>) -> Writer<Vec<u8>> {
        Writer::new(buffer, single_int64_schema()).unwrap()
    }

    #[test]
    fn dynamic_batch_size_is_clamped_to_max() {
        let mut writer = single_int64_writer(Vec::new());
        // A pathological tiny average row size would otherwise drive the batch
        // size toward memory_threshold rows; it must be capped at MAX_BATCH_SIZE.
        writer.size_samples = vec![1; MIN_SAMPLES_FOR_ESTIMATE];
        writer.update_batch_size();
        assert_eq!(writer.current_batch_size, MAX_BATCH_SIZE);

        // A realistic average stays below the cap.
        writer.size_samples = vec![DEFAULT_MEMORY_THRESHOLD / 1000; MIN_SAMPLES_FOR_ESTIMATE];
        writer.update_batch_size();
        assert!(writer.current_batch_size <= MAX_BATCH_SIZE);
        assert!(writer.current_batch_size >= MIN_BATCH_SIZE);
    }

    #[test]
    fn dynamic_batch_size_is_clamped_to_width_bound() {
        let mut writer = WriterBuilder::new()
            .build(Vec::new(), int64_schema(2))
            .unwrap();

        writer.size_samples = vec![1; MIN_SAMPLES_FOR_ESTIMATE];
        writer.update_batch_size();

        assert_eq!(
            writer.current_batch_size,
            max_batch_size_for_column_count(2)
        );
        assert_eq!(
            writer.current_batch_size * writer.buffered_columns.len(),
            MAX_BUFFERED_VALUE_SLOTS
        );
    }

    #[test]
    fn fixed_batch_size_preserves_small_user_value_without_eager_allocation() {
        let writer = WriterBuilder::new()
            .with_batch_size(1)
            .build(Vec::new(), single_int64_schema())
            .unwrap();

        assert_eq!(writer.current_batch_size, 1);
        assert_eq!(writer.buffered_columns[0].capacity(), 0);
    }

    #[test]
    fn oversized_fixed_batch_size_is_rejected_before_initial_buffer_allocation() {
        let mut output = Vec::new();
        {
            let result = WriterBuilder::new()
                .with_batch_size(MAX_BATCH_SIZE + 1)
                .build(&mut output, single_int64_schema());
            assert!(result.is_err());
        }
        assert!(output.is_empty());
    }

    #[test]
    fn wide_schema_fixed_batch_size_is_rejected_by_total_slot_bound() {
        let result = WriterBuilder::new()
            .with_batch_size(MAX_BATCH_SIZE)
            .build(Vec::new(), int64_schema(2));

        assert!(result.is_err());
    }

    #[test]
    fn sample_size_preserves_small_user_value() {
        let writer = WriterBuilder::new()
            .with_sample_size(1)
            .build(Vec::new(), single_int64_schema())
            .unwrap();

        assert_eq!(writer.sample_size, 1);
        assert_eq!(writer.size_samples.capacity(), 1);
    }

    #[test]
    fn small_sample_size_updates_after_requested_sample_count() {
        let mut writer = WriterBuilder::new()
            .with_memory_threshold(128)
            .with_sample_size(1)
            .build(Vec::new(), single_int64_schema())
            .unwrap();

        writer.write_row(vec![ParquetValue::Int64(1)]).unwrap();

        assert_eq!(writer.size_samples.len(), 1);
        assert_eq!(
            writer.current_batch_size,
            dynamic_batch_size_for_column_count(
                128 / ParquetValue::Int64(1).retained_size_bytes(),
                1
            )
        );
    }

    #[test]
    fn oversized_sample_size_is_rejected_before_initial_buffer_allocation() {
        let mut output = Vec::new();
        {
            let result = WriterBuilder::new()
                .with_sample_size(usize::MAX)
                .build(&mut output, single_int64_schema());
            assert!(result.is_err());
        }
        assert!(output.is_empty());
    }

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
}
