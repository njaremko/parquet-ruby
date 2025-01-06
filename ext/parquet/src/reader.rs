use std::io;

use magnus::{Error as MagnusError, Ruby};
use thiserror::Error;

use crate::header_cache::CacheError;
pub use crate::parquet_column_reader::parse_parquet_columns;
pub use crate::parquet_row_reader::parse_parquet_rows;

#[derive(Error, Debug)]
pub enum ReaderError {
    #[error("Failed to get file descriptor: {0}")]
    FileDescriptor(String),
    #[error("Invalid file descriptor")]
    InvalidFileDescriptor,
    #[error("Failed to open file: {0}")]
    FileOpen(#[from] io::Error),
    #[error("Failed to intern headers: {0}")]
    HeaderIntern(#[from] CacheError),
    #[error("Ruby error: {0}")]
    Ruby(String),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}

impl From<MagnusError> for ReaderError {
    fn from(err: MagnusError) -> Self {
        Self::Ruby(err.to_string())
    }
}

impl From<ReaderError> for MagnusError {
    fn from(err: ReaderError) -> Self {
        MagnusError::new(
            Ruby::get().unwrap().exception_runtime_error(),
            err.to_string(),
        )
    }
}
