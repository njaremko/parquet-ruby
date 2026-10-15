//! The exception classes that the gem raises for its own errors.
//!
//! `Parquet::Error` is a `RuntimeError`, so `rescue RuntimeError` still
//! catches every error that the gem raised as a plain `RuntimeError` before
//! these classes existed, and each message is unchanged. A subclass says where
//! the error arose. Errors that Ruby already has a class for keep it:
//! `ArgumentError`, `TypeError`, `IOError` and `EncodingError` are raised as
//! before.

use std::borrow::Cow;
use std::sync::OnceLock;

use arrow_schema::ArrowError;
use magnus::value::Opaque;
use magnus::{gc, prelude::*, Error as MagnusError, ExceptionClass, RModule, Ruby};
use parquet::errors::ParquetError as ParquetRsError;
use parquet_core::ParquetError as CoreParquetError;

/// A Ruby exception class that the gem raises for its own errors
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorClass {
    /// `Parquet::Error`, for an error that no subclass describes, such as an
    /// I/O failure that was never raised as `IOError`
    Error,
    /// `Parquet::InvalidFileError`: the input is not a Parquet file, or its
    /// footer or pages are damaged
    InvalidFile,
    /// `Parquet::UnsupportedError`: valid Parquet that the gem cannot read
    Unsupported,
    /// `Parquet::SchemaError`: the schema given to a write is invalid
    Schema,
    /// `Parquet::ConversionError`: a row or column batch given to a write does
    /// not fit its schema
    Conversion,
}

/// The classes, indexed by `ErrorClass::index`, set once by
/// `ErrorClass::define_all`
static CLASSES: OnceLock<[Opaque<ExceptionClass>; 5]> = OnceLock::new();

impl ErrorClass {
    const SUBCLASSES: [ErrorClass; 4] = [
        ErrorClass::InvalidFile,
        ErrorClass::Unsupported,
        ErrorClass::Schema,
        ErrorClass::Conversion,
    ];

    /// The name of the class under `Parquet`
    fn name(self) -> &'static str {
        match self {
            ErrorClass::Error => "Error",
            ErrorClass::InvalidFile => "InvalidFileError",
            ErrorClass::Unsupported => "UnsupportedError",
            ErrorClass::Schema => "SchemaError",
            ErrorClass::Conversion => "ConversionError",
        }
    }

    fn index(self) -> usize {
        match self {
            ErrorClass::Error => 0,
            ErrorClass::InvalidFile => 1,
            ErrorClass::Unsupported => 2,
            ErrorClass::Schema => 3,
            ErrorClass::Conversion => 4,
        }
    }

    /// Define `Parquet::Error` as a `RuntimeError`, and the other classes as
    /// subclasses of it. The extension calls this once, when it is loaded.
    pub fn define_all(ruby: &Ruby, parquet: RModule) -> Result<(), MagnusError> {
        let base =
            parquet.define_error(ErrorClass::Error.name(), ruby.exception_runtime_error())?;
        let mut classes = [base; 5];
        for class in ErrorClass::SUBCLASSES {
            classes[class.index()] = parquet.define_error(class.name(), base)?;
        }
        for class in classes {
            // Pins the class, which a static must not lose to GC compaction.
            gc::register_mark_object(class);
        }
        let _ = CLASSES.set(classes.map(Opaque::from));
        Ok(())
    }

    /// The Ruby class
    pub fn exception_class(self, ruby: &Ruby) -> ExceptionClass {
        let classes = CLASSES
            .get()
            .expect("the Parquet error classes are defined when the extension is loaded");
        ruby.get_inner(classes[self.index()])
    }

    /// Build an exception of this class with `message`
    pub fn error(self, ruby: &Ruby, message: impl Into<Cow<'static, str>>) -> MagnusError {
        MagnusError::new(self.exception_class(ruby), message)
    }

    /// The class of an error that arose while reading a Parquet file: an I/O
    /// failure is a `Parquet::Error`, valid Parquet that the gem cannot read
    /// or convert to Ruby is a `Parquet::UnsupportedError`, and an error in
    /// the file's footer or pages is a `Parquet::InvalidFileError`. The match
    /// names every variant, so that a new one cannot go unclassified.
    pub fn reading(error: &CoreParquetError) -> Self {
        match error {
            CoreParquetError::Io(_) => ErrorClass::Error,
            CoreParquetError::Parquet(error) => ErrorClass::reading_parquet(error),
            CoreParquetError::Arrow(error) => ErrorClass::reading_arrow(error),
            CoreParquetError::Unsupported(_) | CoreParquetError::Conversion(_) => {
                ErrorClass::Unsupported
            }
            CoreParquetError::Schema(_)
            | CoreParquetError::InvalidArgument(_)
            | CoreParquetError::DataValidation(_)
            | CoreParquetError::Internal(_)
            | CoreParquetError::Utf8(_)
            | CoreParquetError::ParseInt(_)
            | CoreParquetError::ParseFloat(_) => ErrorClass::Error,
        }
    }

    /// The class of an error that parquet-rs returned while reading a file
    pub fn reading_parquet(error: &ParquetRsError) -> Self {
        match error {
            ParquetRsError::NYI(_) => ErrorClass::Unsupported,
            ParquetRsError::External(source) if source.is::<std::io::Error>() => ErrorClass::Error,
            _ => ErrorClass::InvalidFile,
        }
    }

    /// The class of an error that arrow-rs returned while decoding a file's
    /// pages, where parquet-rs errors arrive as text
    pub fn reading_arrow(error: &ArrowError) -> Self {
        match error {
            ArrowError::NotYetImplemented(_) => ErrorClass::Unsupported,
            ArrowError::ParquetError(message) if message.starts_with("NYI: ") => {
                ErrorClass::Unsupported
            }
            ArrowError::IoError(..) => ErrorClass::Error,
            _ => ErrorClass::InvalidFile,
        }
    }

    /// The class of an error that the core writer returned while it was being
    /// built from a schema
    pub fn building_writer(error: &CoreParquetError) -> Self {
        match error {
            CoreParquetError::Io(_)
            | CoreParquetError::InvalidArgument(_)
            | CoreParquetError::Internal(_) => ErrorClass::Error,
            CoreParquetError::Arrow(_)
            | CoreParquetError::Parquet(_)
            | CoreParquetError::Schema(_)
            | CoreParquetError::Conversion(_)
            | CoreParquetError::DataValidation(_)
            | CoreParquetError::Unsupported(_)
            | CoreParquetError::Utf8(_)
            | CoreParquetError::ParseInt(_)
            | CoreParquetError::ParseFloat(_) => ErrorClass::Schema,
        }
    }

    /// The class of an error that the core writer returned for the rows it
    /// was given, or while it finished the file
    pub fn writing(error: &CoreParquetError) -> Self {
        match error {
            CoreParquetError::Schema(_)
            | CoreParquetError::Conversion(_)
            | CoreParquetError::InvalidArgument(_)
            | CoreParquetError::DataValidation(_)
            | CoreParquetError::Utf8(_)
            | CoreParquetError::ParseInt(_)
            | CoreParquetError::ParseFloat(_) => ErrorClass::Conversion,
            CoreParquetError::Unsupported(_) => ErrorClass::Unsupported,
            CoreParquetError::Io(_)
            | CoreParquetError::Arrow(_)
            | CoreParquetError::Parquet(_)
            | CoreParquetError::Internal(_) => ErrorClass::Error,
        }
    }
}

/// Build the exception for an error that arose while reading a Parquet file
pub fn read_error(ruby: &Ruby, error: CoreParquetError) -> MagnusError {
    ErrorClass::reading(&error).error(ruby, error.to_string())
}

/// Build the exception for an error that the core writer returned for the
/// rows it was given, or while it finished the file
pub fn write_error(ruby: &Ruby, error: CoreParquetError) -> MagnusError {
    ErrorClass::writing(&error).error(ruby, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn io_error() -> std::io::Error {
        std::io::Error::other("connection reset")
    }

    #[test]
    fn reading_tells_a_damaged_file_from_unsupported_parquet_and_io_failures() {
        let cases = [
            (
                CoreParquetError::Parquet(ParquetRsError::General("Corrupt footer".into())),
                ErrorClass::InvalidFile,
            ),
            (
                CoreParquetError::Parquet(ParquetRsError::EOF("Invalid page header".into())),
                ErrorClass::InvalidFile,
            ),
            (
                CoreParquetError::Parquet(ParquetRsError::External(Box::new(std::fmt::Error))),
                ErrorClass::InvalidFile,
            ),
            (
                CoreParquetError::Arrow(ArrowError::ParquetError(
                    "EOF: Invalid page header".into(),
                )),
                ErrorClass::InvalidFile,
            ),
            (
                CoreParquetError::Parquet(ParquetRsError::NYI("BIT_PACKED".into())),
                ErrorClass::Unsupported,
            ),
            (
                CoreParquetError::Arrow(ArrowError::ParquetError(
                    "NYI: Encoding BIT_PACKED is not supported".into(),
                )),
                ErrorClass::Unsupported,
            ),
            (
                CoreParquetError::Arrow(ArrowError::NotYetImplemented("Float16".into())),
                ErrorClass::Unsupported,
            ),
            (
                CoreParquetError::Conversion("Unsupported data type for conversion".into()),
                ErrorClass::Unsupported,
            ),
            (CoreParquetError::Io(io_error()), ErrorClass::Error),
            (
                CoreParquetError::Parquet(ParquetRsError::External(Box::new(io_error()))),
                ErrorClass::Error,
            ),
            (
                CoreParquetError::Arrow(ArrowError::IoError("reset".into(), io_error())),
                ErrorClass::Error,
            ),
        ];

        for (error, class) in cases {
            assert_eq!(ErrorClass::reading(&error), class, "{error}");
        }
    }

    #[test]
    fn building_a_writer_blames_the_schema_unless_io_failed() {
        assert_eq!(
            ErrorClass::building_writer(&CoreParquetError::Schema("bad".into())),
            ErrorClass::Schema
        );
        assert_eq!(
            ErrorClass::building_writer(&CoreParquetError::Conversion("bad".into())),
            ErrorClass::Schema
        );
        assert_eq!(
            ErrorClass::building_writer(&CoreParquetError::Io(io_error())),
            ErrorClass::Error
        );
    }

    #[test]
    fn writing_blames_the_rows_unless_io_or_encoding_failed() {
        assert_eq!(
            ErrorClass::writing(&CoreParquetError::Schema("null in a required field".into())),
            ErrorClass::Conversion
        );
        assert_eq!(
            ErrorClass::writing(&CoreParquetError::Conversion("not a number".into())),
            ErrorClass::Conversion
        );
        assert_eq!(
            ErrorClass::writing(&CoreParquetError::Io(io_error())),
            ErrorClass::Error
        );
        assert_eq!(
            ErrorClass::writing(&CoreParquetError::Parquet(ParquetRsError::General(
                "encoding failed".into()
            ))),
            ErrorClass::Error
        );
    }
}
