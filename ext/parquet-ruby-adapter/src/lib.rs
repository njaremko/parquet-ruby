//! Ruby-specific adapter for parquet-core
//!
//! This crate provides Ruby-specific implementations of the parquet-core traits,
//! enabling seamless integration between Ruby and the core Parquet functionality.
//!
//! # Overview
//!
//! The adapter implements three main components:
//!
//! ## Value Conversion
//!
//! The [`RubyValueConverter`] implements the `ValueConverter` trait to handle
//! conversions between Ruby values (via Magnus) and Parquet values:
//!
//! - Ruby integers ↔ Parquet int types
//! - Ruby floats ↔ Parquet float/double
//! - Ruby strings ↔ Parquet strings/binary
//! - Ruby BigDecimal ↔ Parquet decimal types
//! - Ruby Time/DateTime ↔ Parquet temporal types
//! - Ruby arrays/hashes ↔ Parquet lists/maps/structs
//!
//! ## I/O Operations
//!
//! The I/O module provides [`RubyIOReader`] and [`RubyIOWriter`] which implement
//! parquet-core's `ChunkReader` trait for Ruby IO objects:
//!
//! - File objects
//! - StringIO for in-memory operations
//! - Any Ruby object implementing read/write/seek methods
//!
//! ## Schema Conversion
//!
//! Schema utilities for converting between Ruby schema representations and
//! parquet-core's schema types:
//!
//! - Legacy hash-based schemas
//! - New DSL-based schemas
//! - Automatic type inference from data

pub mod error;
pub use error::{ErrorContext, IntoMagnusError, Result, RubyAdapterError};

pub mod chunk_reader;
pub use chunk_reader::CloneableChunkReader;

pub mod converter;
pub use converter::RubyValueConverter;

pub mod io;
pub use io::{create_reader, is_io_like, RubyIO, RubyIOReader, RubyIOWriter};

pub mod logger;
pub use logger::RubyLogger;

pub mod schema;
pub use schema::{
    convert_legacy_schema, extract_field_schemas, is_dsl_schema, parquet_schema_to_ruby,
    process_schema_value, ruby_schema_to_parquet, RubySchemaBuilder,
};

pub mod string_cache;
pub use string_cache::StringCache;

pub mod batch_manager;
pub use batch_manager::{
    BatchSizeManager, DEFAULT_MEMORY_THRESHOLD, INITIAL_BATCH_SIZE, MIN_BATCH_SIZE, SAMPLE_SIZE,
};

pub mod metadata;
pub use metadata::{parse_metadata, RubyParquetMetaData};

pub mod types;
pub use types::{
    ColumnEnumeratorArgs, ParquetWriteArgs, ParserResultType, RowEnumeratorArgs, WriterOutput,
};

pub mod utils;
pub use utils::{
    create_column_enumerator, create_row_enumerator, estimate_parquet_value_size,
    estimate_row_size, handle_block_or_enum, parse_compression, parse_parquet_write_args,
};

pub mod reader;
pub use reader::{each_column, each_row};

pub mod writer;
pub use writer::{create_writer, finalize_writer, write_columns, write_rows};

pub mod try_into_value;
pub use try_into_value::TryIntoValue;
