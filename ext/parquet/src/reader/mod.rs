mod common;
mod parquet_column_reader;
mod parquet_row_reader;
mod unified;
use std::{fs::File, rc::Rc};

use magnus::{value::ReprValue, Error as MagnusError, Ruby, Value};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
pub use parquet_column_reader::parse_parquet_columns;
pub use parquet_row_reader::parse_parquet_rows;

use crate::{
    ruby_reader::{RubyReader, ThreadSafeRubyReader},
    types::{ParquetGemError, TryIntoValue},
};

struct RubyParquetMetaData(ParquetMetaData);

impl TryIntoValue for RubyParquetMetaData {
    fn try_into_value_with(self, handle: &Ruby) -> Result<Value, ParquetGemError> {
        let metadata = self.0;
        let file_metadata = metadata.file_metadata();
        let row_groups = metadata.row_groups();

        // Construct a hash with the metadata
        let hash = handle.hash_new();
        hash.aset("num_rows", file_metadata.num_rows())?;
        hash.aset("created_by", file_metadata.created_by())?;
        // Convert key_value_metadata to a Ruby array if it exists
        if let Some(key_value_metadata) = file_metadata.key_value_metadata() {
            let kv_array = handle.ary_new();
            for kv in key_value_metadata {
                let kv_hash = handle.hash_new();
                kv_hash.aset("key", kv.key.clone())?;
                kv_hash.aset("value", kv.value.clone())?;
                kv_array.push(kv_hash)?;
            }
            hash.aset("key_value_metadata", kv_array)?;
        } else {
            hash.aset("key_value_metadata", None::<Value>)?;
        }

        // Convert schema to a Ruby hash since &Type doesn't implement IntoValue
        let schema_hash = handle.hash_new();
        let schema = file_metadata.schema();
        schema_hash.aset("name", schema.name())?;
        // Add schema fields information
        let fields_array = handle.ary_new();
        for field in schema.get_fields() {
            let field_hash = handle.hash_new();
            field_hash.aset("name", field.name())?;

            // Handle different field types
            match field.as_ref() {
                parquet::schema::types::Type::PrimitiveType {
                    physical_type,
                    type_length,
                    scale,
                    precision,
                    ..
                } => {
                    field_hash.aset("type", "primitive")?;
                    field_hash.aset("physical_type", format!("{:?}", physical_type))?;
                    field_hash.aset("type_length", *type_length)?;
                    field_hash.aset("scale", *scale)?;
                    field_hash.aset("precision", *precision)?;
                }
                parquet::schema::types::Type::GroupType { .. } => {
                    field_hash.aset("type", "group")?;
                }
            }

            // Add basic info
            let basic_info = field.get_basic_info();
            field_hash.aset("repetition", format!("{:?}", basic_info.repetition()))?;
            field_hash.aset(
                "converted_type",
                format!("{:?}", basic_info.converted_type()),
            )?;
            if let Some(logical_type) = basic_info.logical_type() {
                field_hash.aset("logical_type", format!("{:?}", logical_type))?;
            }

            fields_array.push(field_hash)?;
        }
        schema_hash.aset("fields", fields_array)?;

        hash.aset("schema", schema_hash)?;

        // Convert row_groups to a Ruby array since &[RowGroupMetaData] doesn't implement IntoValue
        let row_groups_array = handle.ary_new();
        for row_group in row_groups.iter() {
            let rg_hash = handle.hash_new();
            rg_hash.aset("num_columns", row_group.num_columns())?;
            rg_hash.aset("num_rows", row_group.num_rows())?;
            rg_hash.aset("total_byte_size", row_group.total_byte_size())?;
            rg_hash.aset("file_offset", row_group.file_offset())?;
            rg_hash.aset("ordinal", row_group.ordinal())?;
            rg_hash.aset("compressed_size", row_group.compressed_size())?;

            // Add column chunks metadata
            let columns_array = handle.ary_new();
            for col_idx in 0..row_group.num_columns() {
                let column = row_group.column(col_idx);
                let col_hash = handle.hash_new();

                col_hash.aset("column_path", column.column_path().string())?;
                col_hash.aset("file_path", column.file_path())?;
                col_hash.aset("file_offset", column.file_offset())?;
                col_hash.aset("num_values", column.num_values())?;
                col_hash.aset("compression", format!("{:?}", column.compression()))?;
                col_hash.aset("total_compressed_size", column.compressed_size())?;
                col_hash.aset("total_uncompressed_size", column.uncompressed_size())?;
                col_hash.aset("data_page_offset", column.data_page_offset())?;

                if let Some(offset) = column.dictionary_page_offset() {
                    col_hash.aset("dictionary_page_offset", offset)?;
                }

                if let Some(offset) = column.bloom_filter_offset() {
                    col_hash.aset("bloom_filter_offset", offset)?;
                }

                if let Some(length) = column.bloom_filter_length() {
                    col_hash.aset("bloom_filter_length", length)?;
                }

                if let Some(offset) = column.offset_index_offset() {
                    col_hash.aset("offset_index_offset", offset)?;
                }

                if let Some(length) = column.offset_index_length() {
                    col_hash.aset("offset_index_length", length)?;
                }

                if let Some(offset) = column.column_index_offset() {
                    col_hash.aset("column_index_offset", offset)?;
                }

                if let Some(length) = column.column_index_length() {
                    col_hash.aset("column_index_length", length)?;
                }

                // Add encodings
                let encodings_array = handle.ary_new();
                for encoding in column.encodings() {
                    encodings_array.push(format!("{:?}", encoding))?;
                }
                col_hash.aset("encodings", encodings_array)?;

                // Add statistics if available
                if let Some(stats) = column.statistics() {
                    let stats_hash = handle.hash_new();
                    stats_hash.aset("min_is_exact", stats.min_is_exact())?;
                    stats_hash.aset("max_is_exact", stats.max_is_exact())?;

                    col_hash.aset("statistics", stats_hash)?;
                }

                // Add page encoding stats if available
                if let Some(page_encoding_stats) = column.page_encoding_stats() {
                    let page_stats_array = handle.ary_new();
                    for stat in page_encoding_stats {
                        let stat_hash = handle.hash_new();
                        stat_hash.aset("page_type", format!("{:?}", stat.page_type))?;
                        stat_hash.aset("encoding", format!("{:?}", stat.encoding))?;
                        stat_hash.aset("count", stat.count)?;
                        page_stats_array.push(stat_hash)?;
                    }
                    col_hash.aset("page_encoding_stats", page_stats_array)?;
                }

                columns_array.push(col_hash)?;
            }
            rg_hash.aset("columns", columns_array)?;

            row_groups_array.push(rg_hash)?;
        }
        hash.aset("row_groups", row_groups_array)?;

        Ok(handle.into_value(hash))
    }
}

pub fn parse_metadata(_rb_self: Value, args: &[Value]) -> Result<Value, MagnusError> {
    let ruby = unsafe { Ruby::get_unchecked() };

    if args.len() != 1 {
        return Err(MagnusError::new(
            magnus::exception::arg_error(),
            format!("metadata expects exactly 1 argument (file path or IO-like object), got {}", args.len()),
        ));
    }

    let ruby = Rc::new(ruby);
    let arg = args[0];

    let mut reader = ParquetMetaDataReader::new();
    if arg.is_kind_of(ruby.class_string()) {
        let path = arg.to_r_string()?.to_string()?;
        let file = File::open(path).map_err(ParquetGemError::FileOpen)?;
        reader.try_parse(&file).map_err(ParquetGemError::Parquet)?;
    } else {
        let file = ThreadSafeRubyReader::new(RubyReader::new(ruby.clone(), arg)?);
        reader.try_parse(&file).map_err(ParquetGemError::Parquet)?;
    }

    let metadata = reader.finish().map_err(ParquetGemError::Parquet)?;

    Ok(RubyParquetMetaData(metadata).try_into_value_with(&ruby)?)
}