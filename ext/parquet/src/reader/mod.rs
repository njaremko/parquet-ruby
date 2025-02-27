mod common;
mod parquet_column_reader;
mod parquet_row_reader;

use std::io;

use magnus::Error as MagnusError;
use thiserror::Error;

use crate::header_cache::CacheError;
pub use parquet_column_reader::parse_parquet_columns;
pub use parquet_row_reader::parse_parquet_rows;

#[derive(Error, Debug)]
pub enum ReaderError {
    #[error("Failed to open file: {0}")]
    FileOpen(#[from] io::Error),
    #[error("Failed to intern headers: {0}")]
    HeaderIntern(#[from] CacheError),
    #[error("Ruby error: {0}")]
    Ruby(#[from] MagnusErrorWrapper),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("UTF-8 error: {0}")]
    Utf8Error(#[from] simdutf8::basic::Utf8Error),
    #[error("Jiff error: {0}")]
    Jiff(#[from] jiff::Error),
}

#[derive(Debug)]
pub struct MagnusErrorWrapper(pub MagnusError);

impl From<MagnusError> for MagnusErrorWrapper {
    fn from(err: MagnusError) -> Self {
        Self(err)
    }
}

impl std::fmt::Display for MagnusErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for MagnusErrorWrapper {}

impl From<MagnusError> for ReaderError {
    fn from(err: MagnusError) -> Self {
        Self::Ruby(MagnusErrorWrapper(err))
    }
}

impl Into<MagnusError> for ReaderError {
    fn into(self) -> MagnusError {
        match self {
            Self::Ruby(MagnusErrorWrapper(err)) => err.into(),
            _ => MagnusError::new(magnus::exception::runtime_error(), self.to_string()),
        }
    }
}
