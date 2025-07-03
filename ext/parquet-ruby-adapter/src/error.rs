use magnus::{Error as MagnusError, Ruby};
use parquet_core::ParquetError as CoreParquetError;
use std::fmt::Display;
use thiserror::Error;

/// Error type for parquet-ruby-adapter
#[derive(Error, Debug)]
pub enum RubyAdapterError {
    /// Core parquet errors
    #[error("Parquet error: {0}")]
    Parquet(#[from] CoreParquetError),

    /// Magnus/Ruby errors
    #[error("Ruby error: {0}")]
    Ruby(String),

    /// IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Type conversion errors
    #[error("Type conversion error: {0}")]
    TypeConversion(String),

    /// Schema conversion errors
    #[error("Schema conversion error: {0}")]
    SchemaConversion(String),

    /// Metadata extraction errors
    #[error("Metadata error: {0}")]
    Metadata(String),

    /// Invalid input errors
    #[error("Invalid input: {0}")]
    InvalidInput(String),

    /// Runtime errors
    #[error("Runtime error: {0}")]
    Runtime(String),
}

pub type Result<T> = std::result::Result<T, RubyAdapterError>;

impl RubyAdapterError {
    /// Create a new Ruby error
    pub fn ruby<S: Into<String>>(msg: S) -> Self {
        RubyAdapterError::Ruby(msg.into())
    }

    /// Create a new type conversion error
    pub fn type_conversion<S: Into<String>>(msg: S) -> Self {
        RubyAdapterError::TypeConversion(msg.into())
    }

    /// Create a new schema conversion error
    pub fn schema_conversion<S: Into<String>>(msg: S) -> Self {
        RubyAdapterError::SchemaConversion(msg.into())
    }

    /// Create a new metadata error
    pub fn metadata<S: Into<String>>(msg: S) -> Self {
        RubyAdapterError::Metadata(msg.into())
    }

    /// Create a new invalid input error
    pub fn invalid_input<S: Into<String>>(msg: S) -> Self {
        RubyAdapterError::InvalidInput(msg.into())
    }

    /// Create a new runtime error
    pub fn runtime<S: Into<String>>(msg: S) -> Self {
        RubyAdapterError::Runtime(msg.into())
    }
}

/// Convert RubyAdapterError to MagnusError
impl From<RubyAdapterError> for MagnusError {
    fn from(err: RubyAdapterError) -> Self {
        match Ruby::get() {
            Ok(ruby) => match &err {
                RubyAdapterError::Io(_) => {
                    MagnusError::new(ruby.exception_io_error(), err.to_string())
                }
                RubyAdapterError::TypeConversion(_) => {
                    MagnusError::new(ruby.exception_type_error(), err.to_string())
                }
                RubyAdapterError::InvalidInput(_) => {
                    MagnusError::new(ruby.exception_arg_error(), err.to_string())
                }
                _ => MagnusError::new(ruby.exception_runtime_error(), err.to_string()),
            },
            Err(_) => {
                // Fallback if we can't get Ruby runtime
                MagnusError::new(
                    magnus::exception::runtime_error(),
                    format!("Failed to get Ruby runtime: {}", err),
                )
            }
        }
    }
}

/// Extension trait to convert errors to MagnusError at the boundary
pub trait IntoMagnusError<T> {
    /// Convert to MagnusError
    fn into_magnus_error(self) -> std::result::Result<T, MagnusError>;
}

impl<T> IntoMagnusError<T> for Result<T> {
    fn into_magnus_error(self) -> std::result::Result<T, MagnusError> {
        self.map_err(Into::into)
    }
}

/// Extension trait to add context to errors
pub trait ErrorContext<T> {
    /// Add context to an error
    fn context<S: Display>(self, ctx: S) -> Result<T>;

    /// Add context with a closure that's only called on error
    fn with_context<S: Display, F: FnOnce() -> S>(self, f: F) -> Result<T>;
}

impl<T, E> ErrorContext<T> for std::result::Result<T, E>
where
    E: Into<RubyAdapterError>,
{
    fn context<S: Display>(self, ctx: S) -> Result<T> {
        self.map_err(|e| {
            let base_error = e.into();
            RubyAdapterError::Runtime(format!("{}: {}", ctx, base_error))
        })
    }

    fn with_context<S: Display, F: FnOnce() -> S>(self, f: F) -> Result<T> {
        self.map_err(|e| {
            let base_error = e.into();
            RubyAdapterError::Runtime(format!("{}: {}", f(), base_error))
        })
    }
}

/// Convert from MagnusError to RubyAdapterError
impl From<MagnusError> for RubyAdapterError {
    fn from(err: MagnusError) -> Self {
        RubyAdapterError::Ruby(err.to_string())
    }
}
