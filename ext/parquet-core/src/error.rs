use thiserror::Error;

/// Core error type for Parquet operations
#[derive(Error, Debug)]
pub enum ParquetError {
    /// IO errors from file operations
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Arrow errors from Arrow operations
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// Parquet format errors
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Schema-related errors
    #[error("Schema error: {0}")]
    Schema(String),

    /// Type conversion errors
    #[error("Conversion error: {0}")]
    Conversion(String),

    /// Invalid argument errors
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// Data validation errors
    #[error("Data validation error: {0}")]
    DataValidation(String),

    /// Unsupported operation errors
    #[error("Unsupported operation: {0}")]
    Unsupported(String),

    /// Internal errors that shouldn't happen
    #[error("Internal error: {0}")]
    Internal(String),

    /// UTF-8 decoding errors
    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    /// Number parsing errors
    #[error("Parse error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),

    /// Float parsing errors
    #[error("Parse float error: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
}

/// Result type alias for Parquet operations
pub type Result<T> = std::result::Result<T, ParquetError>;

impl ParquetError {
    /// Create a new schema error
    pub fn schema<S: Into<String>>(msg: S) -> Self {
        ParquetError::Schema(msg.into())
    }

    /// Create a new conversion error
    pub fn conversion<S: Into<String>>(msg: S) -> Self {
        ParquetError::Conversion(msg.into())
    }

    /// Create a new invalid argument error
    pub fn invalid_argument<S: Into<String>>(msg: S) -> Self {
        ParquetError::InvalidArgument(msg.into())
    }

    /// Create a new data validation error
    pub fn data_validation<S: Into<String>>(msg: S) -> Self {
        ParquetError::DataValidation(msg.into())
    }

    /// Create a new unsupported operation error
    pub fn unsupported<S: Into<String>>(msg: S) -> Self {
        ParquetError::Unsupported(msg.into())
    }

    /// Create a new internal error
    pub fn internal<S: Into<String>>(msg: S) -> Self {
        ParquetError::Internal(msg.into())
    }
}

/// Extension trait to add context to errors
pub trait ErrorContext<T> {
    /// Add context to an error
    fn context<S: Into<String>>(self, ctx: S) -> Result<T>;

    /// Add context with a closure that's only called on error
    fn with_context<S: Into<String>, F: FnOnce() -> S>(self, f: F) -> Result<T>;
}

impl<T, E> ErrorContext<T> for std::result::Result<T, E>
where
    E: Into<ParquetError>,
{
    fn context<S: Into<String>>(self, ctx: S) -> Result<T> {
        self.map_err(|e| {
            let base_error = e.into();
            ParquetError::Internal(format!("{}: {}", ctx.into(), base_error))
        })
    }

    fn with_context<S: Into<String>, F: FnOnce() -> S>(self, f: F) -> Result<T> {
        self.map_err(|e| {
            let base_error = e.into();
            ParquetError::Internal(format!("{}: {}", f().into(), base_error))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = ParquetError::schema("Invalid schema");
        assert_eq!(err.to_string(), "Schema error: Invalid schema");

        let err = ParquetError::conversion("Cannot convert value");
        assert_eq!(err.to_string(), "Conversion error: Cannot convert value");
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "File not found");
        let err: ParquetError = io_err.into();
        assert!(err.to_string().contains("IO error"));
    }

    #[test]
    fn test_error_context() {
        fn failing_operation() -> Result<()> {
            Err(ParquetError::invalid_argument("bad input"))
        }

        let result = failing_operation().context("During file read");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("During file read"));
    }

    #[test]
    fn test_error_with_context() {
        fn failing_operation() -> Result<()> {
            Err(ParquetError::data_validation("Invalid data"))
        }

        let filename = "test.parquet";
        let result = failing_operation().with_context(|| format!("Processing file: {}", filename));

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Processing file: test.parquet"));
    }
}
