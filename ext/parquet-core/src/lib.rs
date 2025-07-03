//! Language-agnostic core functionality for Parquet operations
//!
//! `parquet-core` provides core Parquet functionality that can be reused
//! across different language integrations. It wraps the Apache parquet-rs
//! crate with a simplified API focused on common use cases.
//!
//! # Key Components
//!
//! - **Reader**: High-performance Parquet file reader
//!   - Row-wise iteration through [`reader::Reader`]
//!   - Column-wise batch reading for analytics workloads
//!   - Uses `parquet::file::reader::ChunkReader` for flexible input sources
//!   
//! - **Writer**: Efficient Parquet file writer
//!   - Supports both row and columnar data input
//!   - Configurable compression and encoding options
//!   - Dynamic batch sizing based on memory usage
//!   - Uses `std::io::Write + Send` for output flexibility
//!   
//! - **Schema**: Type-safe schema representation
//!   - Builder API for constructing schemas
//!   - Support for nested types (structs, lists, maps)
//!   - Schema introspection through the [`traits::SchemaInspector`] trait
//!   
//! - **Values**: Core value types without external dependencies
//!   - All Parquet primitive types
//!   - Decimal support (128 and 256 bit)
//!   - Temporal types (dates, times, timestamps)
//!
//! - **Arrow Conversion**: Bidirectional conversion between Arrow and Parquet
//!   - Zero-copy where possible
//!   - Handles all supported types including nested structures
//!
//! # Design Philosophy
//!
//! This crate focuses on providing concrete implementations rather than
//! abstract traits. Language-specific adapters (like `parquet-ruby-adapter`)
//! handle the translation between language types and Parquet values.
//!
//! # Example Usage
//!
//! This crate is designed to be used through language-specific adapters.
//! See `parquet-ruby-adapter` for Ruby integration.

pub mod arrow_conversion;
pub mod error;
pub mod reader;
pub mod schema;
pub mod traits;
pub mod value;
pub mod writer;

#[cfg(test)]
pub mod test_utils;

pub use error::{ErrorContext, ParquetError, Result};
pub use reader::Reader;
pub use schema::{PrimitiveType, Repetition, Schema, SchemaBuilder, SchemaNode};
pub use value::ParquetValue;
pub use writer::{Writer, WriterBuilder};
