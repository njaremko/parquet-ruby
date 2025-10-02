//! Remote data source support for range-based Parquet reads

use bytes::Bytes;
use magnus::value::{Opaque, ReprValue};
use magnus::{Error as MagnusError, RString, Ruby, Value};
use std::error::Error;
use std::fmt;
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Seek, SeekFrom};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub(crate) struct RemoteError(String);

impl fmt::Display for RemoteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for RemoteError {}

/// Expected Ruby side API for remote sources
///
/// ```ruby
/// class MySource
///   def byte_length
///     # => Integer
///   end
///
///   def read_range(offset, length)
///     # => binary String containing requested bytes
///   end
/// end
/// ```
pub(crate) struct RemoteSource {
    inner: Opaque<Value>,
}

impl RemoteSource {
    pub fn new(value: Value) -> Result<Self, MagnusError> {
        if !value.respond_to("read_range", false)? {
            return Err(MagnusError::new(
                magnus::exception::arg_error(),
                "Remote source must respond to #read_range(offset, length)",
            ));
        }

        if !value.respond_to("byte_length", false)? {
            return Err(MagnusError::new(
                magnus::exception::arg_error(),
                "Remote source must respond to #byte_length",
            ));
        }

        Ok(Self {
            inner: Opaque::from(value),
        })
    }

    fn with_inner<T, F>(&self, func: F) -> Result<T, MagnusError>
    where
        F: FnOnce(&Ruby, Value) -> Result<T, MagnusError>,
    {
        let ruby = Ruby::get().map_err(|_| {
            MagnusError::new(
                magnus::exception::runtime_error(),
                "Failed to get Ruby runtime",
            )
        })?;
        let value = ruby.get_inner(self.inner);
        func(&ruby, value)
    }

    pub fn len(&self) -> Result<u64, MagnusError> {
        self.with_inner(|_ruby, value| {
            let length: i64 = value.funcall("byte_length", ())?;
            if length < 0 {
                Err(MagnusError::new(
                    magnus::exception::range_error(),
                    "Remote source reported negative byte_length",
                ))
            } else {
                Ok(length as u64)
            }
        })
    }

    pub fn read_range(&self, offset: u64, length: usize) -> Result<Bytes, MagnusError> {
        if length == 0 {
            return Ok(Bytes::new());
        }

        self.with_inner(|_ruby, value| {
            let string: RString = value.funcall("read_range", (offset, length))?;
            let slice = unsafe { string.as_slice().to_vec() };
            Ok(Bytes::from(slice))
        })
    }
}

/// Thread-safe wrapper around the remote source
#[derive(Clone)]
pub(crate) struct ThreadSafeRemoteSource(Arc<Mutex<RemoteSource>>);

impl ThreadSafeRemoteSource {
    pub(crate) fn new(source: RemoteSource) -> Self {
        Self(Arc::new(Mutex::new(source)))
    }

    pub(crate) fn len(&self) -> Result<u64, RemoteError> {
        let guard = self
            .0
            .lock()
            .map_err(|e| RemoteError(format!("Remote source lock poisoned: {}", e)))?;
        guard.len().map_err(|e| RemoteError(e.to_string()))
    }

    pub(crate) fn read_range(&self, offset: u64, length: usize) -> Result<Bytes, RemoteError> {
        let guard = self
            .0
            .lock()
            .map_err(|e| RemoteError(format!("Remote source lock poisoned: {}", e)))?;
        guard
            .read_range(offset, length)
            .map_err(|e| RemoteError(e.to_string()))
    }
}

/// Reader that serves a specific byte range using repeated remote fetches
pub(crate) struct RemoteRangeReader {
    source: ThreadSafeRemoteSource,
    start: u64,
    end: u64,
    pos: u64,
}

impl RemoteRangeReader {
    pub fn new(source: ThreadSafeRemoteSource, start: u64, length: u64) -> Self {
        Self {
            source,
            start,
            end: start + length,
            pos: start,
        }
    }
}

impl Read for RemoteRangeReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let remaining = (self.end - self.pos) as usize;
        if remaining == 0 {
            return Ok(0);
        }

        let to_read = buf.len().min(remaining);
        let bytes = self
            .source
            .read_range(self.pos, to_read)
            .map_err(|e| IoError::new(ErrorKind::Other, e))?;

        if bytes.is_empty() {
            return Ok(0);
        }

        let len = bytes.len().min(buf.len());
        buf[..len].copy_from_slice(&bytes[..len]);
        self.pos += len as u64;
        Ok(len)
    }
}

impl Seek for RemoteRangeReader {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => self.start + offset,
            SeekFrom::Current(delta) => {
                let signed = self.pos as i64 + delta;
                if signed < self.start as i64 {
                    return Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "Attempted to seek before start of range",
                    ));
                }
                signed as u64
            }
            SeekFrom::End(delta) => {
                let signed = self.end as i64 + delta;
                if signed < self.start as i64 {
                    return Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "Attempted to seek before start of range",
                    ));
                }
                signed as u64
            }
        };

        if new_pos > self.end {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "Attempted to seek beyond end of range",
            ));
        }

        self.pos = new_pos;
        Ok(self.pos - self.start)
    }
}
