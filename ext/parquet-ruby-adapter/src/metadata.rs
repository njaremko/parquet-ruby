use magnus::value::ReprValue;
use magnus::{Error as MagnusError, IntoValue, Ruby, Value};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use std::fs::File;

use crate::error::{IntoMagnusError, Result, RubyAdapterError};
use crate::io::{RubyIOReader, ThreadSafeRubyIOReader};
use crate::TryIntoValue;

/// Wrapper for ParquetMetaData to implement IntoValue trait
pub struct RubyParquetMetaData(pub ParquetMetaData);

impl TryIntoValue for RubyParquetMetaData {
    fn try_into_value(self, handle: &Ruby) -> Result<Value> {
        let metadata = &self.0;
        let file_metadata = metadata.file_metadata();
        let row_groups = metadata.row_groups();

        // Construct a hash with the metadata
        let hash = handle.hash_new();
        hash.aset("num_rows", file_metadata.num_rows())
            .map_err(|e| RubyAdapterError::metadata(format!("Failed to set num_rows: {}", e)))?;
        hash.aset("created_by", file_metadata.created_by())
            .map_err(|e| RubyAdapterError::metadata(format!("Failed to set created_by: {}", e)))?;

        // Convert key_value_metadata to a Ruby array if it exists
        if let Some(key_value_metadata) = file_metadata.key_value_metadata() {
            let kv_array = handle.ary_new();
            for kv in key_value_metadata {
                let kv_hash = handle.hash_new();
                kv_hash
                    .aset("key", kv.key.clone())
                    .map_err(|e| RubyAdapterError::metadata(format!("Failed to set key: {}", e)))?;
                kv_hash.aset("value", kv.value.clone()).map_err(|e| {
                    RubyAdapterError::metadata(format!("Failed to set value: {}", e))
                })?;
                kv_array.push(kv_hash).map_err(|e| {
                    RubyAdapterError::metadata(format!("Failed to push kv_hash: {}", e))
                })?;
            }
            hash.aset("key_value_metadata", kv_array).map_err(|e| {
                RubyAdapterError::metadata(format!("Failed to set key_value_metadata: {}", e))
            })?;
        } else {
            hash.aset("key_value_metadata", None::<Value>)
                .map_err(|e| {
                    RubyAdapterError::metadata(format!("Failed to set key_value_metadata: {}", e))
                })?;
        }

        // Convert schema to a Ruby hash since &Type doesn't implement IntoValue
        let schema_hash = handle.hash_new();
        let schema = file_metadata.schema();
        schema_hash
            .aset("name", schema.name())
            .map_err(|e| RubyAdapterError::metadata(format!("Failed to set schema name: {}", e)))?;

        // Add schema fields information
        let fields_array = handle.ary_new();
        for field in schema.get_fields() {
            let field_hash = handle.hash_new();
            field_hash.aset("name", field.name()).map_err(|e| {
                RubyAdapterError::metadata(format!("Failed to set field name: {}", e))
            })?;

            // Handle different field types
            match field.as_ref() {
                parquet::schema::types::Type::PrimitiveType {
                    physical_type,
                    type_length,
                    scale,
                    precision,
                    ..
                } => {
                    field_hash.aset("type", "primitive").map_err(|e| {
                        RubyAdapterError::metadata(format!("Failed to set type: {}", e))
                    })?;
                    field_hash
                        .aset("physical_type", format!("{:?}", physical_type))
                        .map_err(|e| {
                            RubyAdapterError::metadata(format!(
                                "Failed to set physical_type: {}",
                                e
                            ))
                        })?;
                    field_hash.aset("type_length", *type_length).map_err(|e| {
                        RubyAdapterError::metadata(format!("Failed to set type_length: {}", e))
                    })?;
                    field_hash.aset("scale", *scale).map_err(|e| {
                        RubyAdapterError::metadata(format!("Failed to set scale: {}", e))
                    })?;
                    field_hash.aset("precision", *precision).map_err(|e| {
                        RubyAdapterError::metadata(format!("Failed to set precision: {}", e))
                    })?;
                }
                parquet::schema::types::Type::GroupType { .. } => {
                    field_hash.aset("type", "group").map_err(|e| {
                        RubyAdapterError::metadata(format!("Failed to set type: {}", e))
                    })?;
                }
            }

            // Add basic info
            let basic_info = field.get_basic_info();
            field_hash
                .aset("repetition", format!("{:?}", basic_info.repetition()))
                .map_err(|e| {
                    RubyAdapterError::metadata(format!("Failed to set repetition: {}", e))
                })?;
            field_hash
                .aset(
                    "converted_type",
                    format!("{:?}", basic_info.converted_type()),
                )
                .map_err(|e| {
                    RubyAdapterError::metadata(format!("Failed to set converted_type: {}", e))
                })?;
            if let Some(logical_type) = basic_info.logical_type() {
                field_hash
                    .aset("logical_type", format!("{:?}", logical_type))
                    .map_err(|e| {
                        RubyAdapterError::metadata(format!("Failed to set logical_type: {}", e))
                    })?;
            }

            fields_array.push(field_hash).map_err(|e| {
                RubyAdapterError::metadata(format!("Failed to push field_hash: {}", e))
            })?;
        }
        schema_hash
            .aset("fields", fields_array)
            .map_err(|e| RubyAdapterError::metadata(format!("Failed to set fields: {}", e)))?;

        hash.aset("schema", schema_hash)
            .map_err(|e| RubyAdapterError::metadata(format!("Failed to set schema: {}", e)))?;

        // Convert row_groups to a Ruby array since &[RowGroupMetaData] doesn't implement IntoValue
        let row_groups_array = handle.ary_new();
        for row_group in row_groups.iter() {
            let rg_hash = handle.hash_new();
            rg_hash
                .aset("num_columns", row_group.num_columns())
                .map_err(|e| {
                    RubyAdapterError::metadata(format!("Failed to set num_columns: {}", e))
                })?;
            rg_hash
                .aset("num_rows", row_group.num_rows())
                .map_err(|e| {
                    RubyAdapterError::metadata(format!("Failed to set num_rows: {}", e))
                })?;
            rg_hash
                .aset("total_byte_size", row_group.total_byte_size())
                .map_err(|e| {
                    RubyAdapterError::metadata(format!("Failed to set total_byte_size: {}", e))
                })?;
            rg_hash
                .aset("file_offset", row_group.file_offset())
                .map_err(|e| {
                    RubyAdapterError::metadata(format!("Failed to set file_offset: {}", e))
                })?;
            rg_hash
                .aset("ordinal", row_group.ordinal())
                .map_err(|e| RubyAdapterError::metadata(format!("Failed to set ordinal: {}", e)))?;
            rg_hash
                .aset("compressed_size", row_group.compressed_size())
                .map_err(|e| {
                    RubyAdapterError::metadata(format!("Failed to set compressed_size: {}", e))
                })?;

            // Add column chunks metadata
            let columns_array = handle.ary_new();
            for col_idx in 0..row_group.num_columns() {
                let column = row_group.column(col_idx);
                let col_hash = handle.hash_new();

                col_hash
                    .aset("column_path", column.column_path().string())
                    .map_err(|e| {
                        RubyAdapterError::metadata(format!("Failed to set column_path: {}", e))
                    })?;
                col_hash
                    .aset("file_path", column.file_path())
                    .map_err(|e| {
                        RubyAdapterError::metadata(format!("Failed to set file_path: {}", e))
                    })?;
                col_hash
                    .aset("file_offset", column.file_offset())
                    .map_err(|e| {
                        RubyAdapterError::metadata(format!("Failed to set file_offset: {}", e))
                    })?;
                col_hash
                    .aset("num_values", column.num_values())
                    .map_err(|e| {
                        RubyAdapterError::metadata(format!("Failed to set num_values: {}", e))
                    })?;
                col_hash
                    .aset("compression", format!("{:?}", column.compression()))
                    .map_err(|e| {
                        RubyAdapterError::metadata(format!("Failed to set compression: {}", e))
                    })?;
                col_hash
                    .aset("total_compressed_size", column.compressed_size())
                    .map_err(|e| {
                        RubyAdapterError::metadata(format!(
                            "Failed to set total_compressed_size: {}",
                            e
                        ))
                    })?;
                col_hash
                    .aset("total_uncompressed_size", column.uncompressed_size())
                    .map_err(|e| {
                        RubyAdapterError::metadata(format!(
                            "Failed to set total_uncompressed_size: {}",
                            e
                        ))
                    })?;
                col_hash
                    .aset("data_page_offset", column.data_page_offset())
                    .map_err(|e| {
                        RubyAdapterError::metadata(format!("Failed to set data_page_offset: {}", e))
                    })?;

                if let Some(offset) = column.dictionary_page_offset() {
                    col_hash
                        .aset("dictionary_page_offset", offset)
                        .map_err(|e| {
                            RubyAdapterError::metadata(format!(
                                "Failed to set dictionary_page_offset: {}",
                                e
                            ))
                        })?;
                }

                if let Some(offset) = column.bloom_filter_offset() {
                    col_hash.aset("bloom_filter_offset", offset).map_err(|e| {
                        RubyAdapterError::metadata(format!(
                            "Failed to set bloom_filter_offset: {}",
                            e
                        ))
                    })?;
                }

                if let Some(length) = column.bloom_filter_length() {
                    col_hash.aset("bloom_filter_length", length).map_err(|e| {
                        RubyAdapterError::metadata(format!(
                            "Failed to set bloom_filter_length: {}",
                            e
                        ))
                    })?;
                }

                if let Some(offset) = column.offset_index_offset() {
                    col_hash.aset("offset_index_offset", offset).map_err(|e| {
                        RubyAdapterError::metadata(format!(
                            "Failed to set offset_index_offset: {}",
                            e
                        ))
                    })?;
                }

                if let Some(length) = column.offset_index_length() {
                    col_hash.aset("offset_index_length", length).map_err(|e| {
                        RubyAdapterError::metadata(format!(
                            "Failed to set offset_index_length: {}",
                            e
                        ))
                    })?;
                }

                if let Some(offset) = column.column_index_offset() {
                    col_hash.aset("column_index_offset", offset).map_err(|e| {
                        RubyAdapterError::metadata(format!(
                            "Failed to set column_index_offset: {}",
                            e
                        ))
                    })?;
                }

                if let Some(length) = column.column_index_length() {
                    col_hash.aset("column_index_length", length).map_err(|e| {
                        RubyAdapterError::metadata(format!(
                            "Failed to set column_index_length: {}",
                            e
                        ))
                    })?;
                }

                // Add encodings
                let encodings_array = handle.ary_new();
                for encoding in column.encodings() {
                    encodings_array
                        .push(format!("{:?}", encoding))
                        .map_err(|e| {
                            RubyAdapterError::metadata(format!("Failed to push encoding: {}", e))
                        })?;
                }
                col_hash.aset("encodings", encodings_array).map_err(|e| {
                    RubyAdapterError::metadata(format!("Failed to set encodings: {}", e))
                })?;

                // Add statistics if available
                if let Some(stats) = column.statistics() {
                    let stats_hash = handle.hash_new();
                    stats_hash
                        .aset("min_is_exact", stats.min_is_exact())
                        .map_err(|e| {
                            RubyAdapterError::metadata(format!("Failed to set min_is_exact: {}", e))
                        })?;
                    stats_hash
                        .aset("max_is_exact", stats.max_is_exact())
                        .map_err(|e| {
                            RubyAdapterError::metadata(format!("Failed to set max_is_exact: {}", e))
                        })?;

                    col_hash.aset("statistics", stats_hash).map_err(|e| {
                        RubyAdapterError::metadata(format!("Failed to set statistics: {}", e))
                    })?;
                }

                // Add page encoding stats if available
                if let Some(page_encoding_stats) = column.page_encoding_stats() {
                    let page_stats_array = handle.ary_new();
                    for stat in page_encoding_stats {
                        let stat_hash = handle.hash_new();
                        stat_hash
                            .aset("page_type", format!("{:?}", stat.page_type))
                            .map_err(|e| {
                                RubyAdapterError::metadata(format!(
                                    "Failed to set page_type: {}",
                                    e
                                ))
                            })?;
                        stat_hash
                            .aset("encoding", format!("{:?}", stat.encoding))
                            .map_err(|e| {
                                RubyAdapterError::metadata(format!("Failed to set encoding: {}", e))
                            })?;
                        stat_hash.aset("count", stat.count).map_err(|e| {
                            RubyAdapterError::metadata(format!("Failed to set count: {}", e))
                        })?;
                        page_stats_array.push(stat_hash).map_err(|e| {
                            RubyAdapterError::metadata(format!("Failed to push stat_hash: {}", e))
                        })?;
                    }
                    col_hash
                        .aset("page_encoding_stats", page_stats_array)
                        .map_err(|e| {
                            RubyAdapterError::metadata(format!(
                                "Failed to set page_encoding_stats: {}",
                                e
                            ))
                        })?;
                }

                columns_array.push(col_hash).map_err(|e| {
                    RubyAdapterError::metadata(format!("Failed to push col_hash: {}", e))
                })?;
            }
            rg_hash
                .aset("columns", columns_array)
                .map_err(|e| RubyAdapterError::metadata(format!("Failed to set columns: {}", e)))?;

            row_groups_array.push(rg_hash).map_err(|e| {
                RubyAdapterError::metadata(format!("Failed to push rg_hash: {}", e))
            })?;
        }
        hash.aset("row_groups", row_groups_array)
            .map_err(|e| RubyAdapterError::metadata(format!("Failed to set row_groups: {}", e)))?;

        Ok(handle.into_value(hash))
    }
}

// Also implement IntoValue for backwards compatibility
impl IntoValue for RubyParquetMetaData {
    fn into_value_with(self, handle: &Ruby) -> Value {
        // Use TryIntoValue and handle errors by returning an error hash
        match self.try_into_value(handle) {
            Ok(value) => value,
            Err(e) => {
                // Create an error hash instead of panicking
                let error_hash = handle.hash_new();
                let _ = error_hash.aset("error", true);
                let _ = error_hash.aset("message", e.to_string());
                handle.into_value(error_hash)
            }
        }
    }
}

/// Parse metadata from a file path or Ruby IO object
pub fn parse_metadata(arg: Value) -> std::result::Result<Value, MagnusError> {
    parse_metadata_impl(arg).into_magnus_error()
}

fn parse_metadata_impl(arg: Value) -> Result<Value> {
    let ruby = Ruby::get().map_err(|_| RubyAdapterError::runtime("Failed to get Ruby runtime"))?;

    let mut reader = ParquetMetaDataReader::new();
    if arg.is_kind_of(ruby.class_string()) {
        let path = arg
            .to_r_string()
            .map_err(|e| {
                RubyAdapterError::invalid_input(format!("Failed to convert to string: {}", e))
            })?
            .to_string()
            .map_err(|e| {
                RubyAdapterError::invalid_input(format!("Failed to convert to Rust string: {}", e))
            })?;
        let file = File::open(path).map_err(RubyAdapterError::Io)?;
        reader
            .try_parse(&file)
            .map_err(|e| RubyAdapterError::Parquet(parquet_core::ParquetError::Parquet(e)))?;
    } else {
        let file = RubyIOReader::new(arg).map_err(RubyAdapterError::Io)?;
        reader
            .try_parse(&ThreadSafeRubyIOReader::new(file))
            .map_err(|e| RubyAdapterError::Parquet(parquet_core::ParquetError::Parquet(e)))?;
    }

    let metadata = reader
        .finish()
        .map_err(|e| RubyAdapterError::Parquet(parquet_core::ParquetError::Parquet(e)))?;

    // Use TryIntoValue instead of IntoValue
    RubyParquetMetaData(metadata).try_into_value(&ruby)
}
