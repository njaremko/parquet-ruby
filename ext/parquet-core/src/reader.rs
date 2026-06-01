//! Core Parquet reading functionality

use crate::{arrow_conversion::arrow_to_parquet_value, ParquetError, ParquetValue, Result};
use arrow::record_batch::RecordBatch;
use arrow_array::Array;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::file::metadata::{FileMetaData, ParquetMetaData};
use parquet::schema::types::{Type, TypePtr};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Core Parquet reader that works with any source implementing Read + Seek
#[derive(Clone)]
pub struct Reader<R> {
    inner: R,
}

impl<R> Reader<R>
where
    R: parquet::file::reader::ChunkReader + Clone + 'static,
{
    /// Create a new reader
    pub fn new(reader: R) -> Self {
        Self { inner: reader }
    }

    /// Get the Parquet file metadata
    pub fn metadata(&mut self) -> Result<FileMetaData> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(self.inner.clone())?;
        Ok(builder.metadata().file_metadata().clone())
    }

    /// Read rows from the Parquet file
    ///
    /// Returns an iterator over rows where each row is a vector of ParquetValues
    pub fn read_rows(self) -> Result<RowIterator<R>> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(self.inner)?;
        let schema = builder.schema().clone();
        let metadata = builder.metadata().clone();
        let aligned_parquet_fields = build_alignment(&schema, &metadata)?;
        let reader = builder.build()?;

        Ok(RowIterator {
            batch_reader: reader,
            schema,
            current_batch: None,
            current_row: 0,
            aligned_parquet_fields,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Read rows with column projection
    ///
    /// Only the specified columns will be read, which can significantly
    /// improve performance for wide tables. Projected row values are returned
    /// in file schema order, not request order.
    pub fn read_rows_with_projection(self, columns: &[String]) -> Result<RowIterator<R>> {
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(self.inner)?;
        let arrow_schema = builder.schema();
        let requested_columns = columns.iter().map(String::as_str).collect::<HashSet<_>>();

        // Create projection mask based on column names
        let mut column_indices = Vec::new();
        for (idx, field) in arrow_schema.fields().iter().enumerate() {
            if requested_columns.contains(field.name().as_str()) {
                column_indices.push(idx);
            }
        }
        // The projected batches are emitted in file order over the selected
        // columns; build that schema so alignment and field access match.
        let projected_schema = Arc::new(arrow_schema::Schema::new(
            column_indices
                .iter()
                .map(|idx| arrow_schema.field(*idx).clone())
                .collect::<Vec<_>>(),
        ));

        // Allow empty column projections to match v1 behavior
        // This will result in rows with no fields

        let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), column_indices);
        builder = builder.with_projection(mask);
        let metadata = builder.metadata().clone();
        let aligned_parquet_fields = build_alignment(&projected_schema, &metadata)?;
        let reader = builder.build()?;

        Ok(RowIterator {
            batch_reader: reader,
            schema: projected_schema,
            current_batch: None,
            current_row: 0,
            aligned_parquet_fields,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Read columns from the Parquet file
    ///
    /// Returns an iterator over column batches where each batch contains
    /// arrays of values for each column.
    pub fn read_columns(self, batch_size: Option<usize>) -> Result<ColumnIterator<R>> {
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(self.inner)?;

        let is_empty = builder.metadata().file_metadata().num_rows() == 0;

        if let Some(size) = batch_size {
            builder = builder.with_batch_size(size);
        }

        let schema = builder.schema().clone();
        let metadata = builder.metadata().clone();
        let aligned_parquet_fields = build_alignment(&schema, &metadata)?;
        let reader = builder.build()?;

        Ok(ColumnIterator {
            batch_reader: reader,
            schema,
            returned_empty_batch: false,
            is_empty_file: is_empty,
            aligned_parquet_fields,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Read columns with projection
    pub fn read_columns_with_projection(
        self,
        columns: &[String],
        batch_size: Option<usize>,
    ) -> Result<ColumnIterator<R>> {
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(self.inner)?;
        let arrow_schema = builder.schema();
        let requested_columns = columns.iter().map(String::as_str).collect::<HashSet<_>>();

        let is_empty = builder.metadata().file_metadata().num_rows() == 0;

        // Create projection mask
        let mut column_indices = Vec::new();
        for (idx, field) in arrow_schema.fields().iter().enumerate() {
            if requested_columns.contains(field.name().as_str()) {
                column_indices.push(idx);
            }
        }
        let projected_schema = Arc::new(arrow_schema::Schema::new(
            column_indices
                .iter()
                .map(|idx| arrow_schema.field(*idx).clone())
                .collect::<Vec<_>>(),
        ));

        // Allow empty column projections to match v1 behavior
        // This will result in rows with no fields

        let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), column_indices);
        builder = builder.with_projection(mask);

        if let Some(size) = batch_size {
            builder = builder.with_batch_size(size);
        }

        let metadata = builder.metadata().clone();
        let aligned_parquet_fields = build_alignment(&projected_schema, &metadata)?;
        let reader = builder.build()?;

        Ok(ColumnIterator {
            batch_reader: reader,
            schema: projected_schema,
            returned_empty_batch: false,
            is_empty_file: is_empty,
            aligned_parquet_fields,
            _phantom: std::marker::PhantomData,
        })
    }
}

/// Build a column-aligned list of parquet root fields for `arrow_schema`,
/// matching each arrow field to a parquet root field by name once.
///
/// The arrow schema may be a projection (a subset of the file's columns in file
/// order), so positional indexing into the full parquet root is wrong; we match
/// by name. Computed once per read and indexed by column position thereafter,
/// turning a per-row O(columns^2) scan into a one-time O(columns) build. If the
/// file has duplicate root column names the first occurrence wins, matching the
/// previous lookup behavior.
fn align_parquet_fields(
    arrow_schema: &arrow_schema::Schema,
    parquet_fields: &[TypePtr],
) -> Result<Vec<TypePtr>> {
    let mut by_name: HashMap<&str, &TypePtr> = HashMap::with_capacity(parquet_fields.len());
    for field in parquet_fields {
        by_name.entry(field.name()).or_insert(field);
    }
    arrow_schema
        .fields()
        .iter()
        .map(|field| {
            by_name
                .get(field.name().as_str())
                .map(|matched| (*matched).clone())
                .ok_or_else(|| {
                    ParquetError::Conversion(format!(
                        "No matching parquet field for arrow field '{}'",
                        field.name()
                    ))
                })
        })
        .collect()
}

/// Extract the parquet root group's fields from file metadata.
fn root_parquet_fields(metadata: &ParquetMetaData) -> Result<Vec<TypePtr>> {
    match metadata.file_metadata().schema_descr().root_schema() {
        Type::GroupType { fields, .. } => Ok(fields.clone()),
        _ => Err(ParquetError::Conversion(
            "Root schema must be a group type".to_string(),
        )),
    }
}

/// Compute the column-aligned parquet fields for an (output) arrow schema. The
/// schema is fixed for the whole read, so this is computed once at construction
/// and then indexed by column position for every batch and row.
fn build_alignment(
    schema: &arrow_schema::Schema,
    metadata: &ParquetMetaData,
) -> Result<Vec<TypePtr>> {
    align_parquet_fields(schema, &root_parquet_fields(metadata)?)
}

/// Iterator over rows in a Parquet file
pub struct RowIterator<R> {
    batch_reader: ParquetRecordBatchReader,
    /// Output arrow schema (projected subset in file order, or the full schema),
    /// fixed for every batch.
    schema: Arc<arrow_schema::Schema>,
    current_batch: Option<RecordBatch>,
    current_row: usize,
    /// Parquet root fields aligned to `schema` column order, computed once.
    aligned_parquet_fields: Vec<TypePtr>,
    _phantom: std::marker::PhantomData<R>,
}

impl<R> Iterator for RowIterator<R>
where
    R: parquet::file::reader::ChunkReader + 'static,
{
    type Item = Result<Vec<ParquetValue>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we have a current batch and haven't exhausted it
            if let Some(ref batch) = self.current_batch {
                if self.current_row < batch.num_rows() {
                    // Extract values from current row, using the column-aligned
                    // parquet fields computed once at construction.
                    let mut row_values = Vec::with_capacity(batch.num_columns());

                    for (i, column) in batch.columns().iter().enumerate() {
                        let field = self.schema.field(i);
                        let value = match arrow_to_parquet_value(
                            field,
                            &self.aligned_parquet_fields[i],
                            column,
                            self.current_row,
                        ) {
                            Ok(v) => v,
                            Err(e) => return Some(Err(e)),
                        };
                        row_values.push(value);
                    }

                    self.current_row += 1;
                    return Some(Ok(row_values));
                }
            }

            // Need to fetch next batch
            match self.batch_reader.next() {
                Some(Ok(batch)) => {
                    self.current_batch = Some(batch);
                    self.current_row = 0;
                }
                Some(Err(e)) => return Some(Err(e.into())),
                None => return None,
            }
        }
    }
}

/// Iterator over column batches in a Parquet file
pub struct ColumnIterator<R> {
    batch_reader: ParquetRecordBatchReader,
    schema: Arc<arrow_schema::Schema>,
    returned_empty_batch: bool,
    is_empty_file: bool,
    /// Parquet root fields aligned to `schema` column order, computed once.
    aligned_parquet_fields: Vec<TypePtr>,
    _phantom: std::marker::PhantomData<R>,
}

/// A batch of columns with their names
pub struct ColumnBatch {
    pub columns: Vec<(String, Vec<ParquetValue>)>,
}

impl<R> Iterator for ColumnIterator<R>
where
    R: parquet::file::reader::ChunkReader + 'static,
{
    type Item = Result<ColumnBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        // Check if this is the first call and we have no data
        if self.is_empty_file && !self.returned_empty_batch {
            // Return one batch with empty columns to show schema
            self.returned_empty_batch = true;
            let mut columns = Vec::with_capacity(self.schema.fields().len());

            for field in self.schema.fields() {
                columns.push((field.name().to_string(), Vec::new()));
            }

            return Some(Ok(ColumnBatch { columns }));
        }

        match self.batch_reader.next() {
            Some(Ok(batch)) => {
                let mut columns = Vec::with_capacity(batch.num_columns());

                for (idx, column) in batch.columns().iter().enumerate() {
                    let field = self.schema.field(idx);
                    let column_name = field.name().to_string();
                    let parquet_field = &self.aligned_parquet_fields[idx];

                    // Convert entire column to ParquetValues
                    let mut values = Vec::with_capacity(column.len());
                    for row_idx in 0..column.len() {
                        match arrow_to_parquet_value(field, parquet_field, column, row_idx) {
                            Ok(value) => values.push(value),
                            Err(e) => return Some(Err(e)),
                        }
                    }

                    columns.push((column_name, values));
                }

                Some(Ok(ColumnBatch { columns }))
            }
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reader_creation() {
        let data = vec![0u8; 1024];
        let bytes = bytes::Bytes::from(data);
        let _reader = Reader::new(bytes);
    }
}
