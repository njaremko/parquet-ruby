//! Cloneable ChunkReader implementation for streaming Parquet files
//!
//! This module provides a ChunkReader that implements Clone, enabling
//! true streaming of Parquet files without loading them entirely into memory.

use bytes::Bytes;
use parquet::file::reader::{ChunkReader, Length};
use parquet_core::Result;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::io::ThreadSafeRubyIOReader;

/// A ChunkReader that can be cloned for parallel reading
#[derive(Clone)]
pub enum CloneableChunkReader {
    /// File-based reader that reopens files on clone
    File(FileChunkReader),
    /// Ruby IO-based reader using thread-safe wrapper
    RubyIO(RubyIOChunkReader),
    /// In-memory bytes (fallback for small files)
    Bytes(bytes::Bytes),
}

/// File-based chunk reader that reopens files for each clone
#[derive(Clone)]
pub struct FileChunkReader {
    path: PathBuf,
    file_len: u64,
}

impl FileChunkReader {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let metadata = file.metadata()?;
        let file_len = metadata.len();

        Ok(FileChunkReader { path, file_len })
    }
}

/// Ruby IO-based chunk reader using thread-safe wrapper
#[derive(Clone)]
pub struct RubyIOChunkReader {
    reader: ThreadSafeRubyIOReader,
    len: u64,
}

impl RubyIOChunkReader {
    pub fn new(reader: ThreadSafeRubyIOReader, len: u64) -> Self {
        RubyIOChunkReader { reader, len }
    }
}

/// A reader that reads a specific range from a ChunkReader
struct RangeReader<R> {
    inner: R,
    _start: u64,
    end: u64,
    pos: u64,
}

impl<R: Read + Seek> RangeReader<R> {
    fn new(mut inner: R, start: u64, length: u64) -> io::Result<Self> {
        inner.seek(SeekFrom::Start(start))?;
        Ok(RangeReader {
            inner,
            _start: start,
            end: start + length,
            pos: start,
        })
    }
}

impl<R: Read> Read for RangeReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = (self.end - self.pos) as usize;
        if remaining == 0 {
            return Ok(0);
        }

        let to_read = buf.len().min(remaining);
        let n = self.inner.read(&mut buf[..to_read])?;
        self.pos += n as u64;
        Ok(n)
    }
}

// Implement Length trait for our readers
impl Length for FileChunkReader {
    fn len(&self) -> u64 {
        self.file_len
    }
}

impl Length for RubyIOChunkReader {
    fn len(&self) -> u64 {
        self.len
    }
}

impl Length for CloneableChunkReader {
    fn len(&self) -> u64 {
        match self {
            CloneableChunkReader::File(f) => f.len(),
            CloneableChunkReader::RubyIO(r) => r.len(),
            CloneableChunkReader::Bytes(b) => b.len() as u64,
        }
    }
}

// Implement ChunkReader for FileChunkReader
impl ChunkReader for FileChunkReader {
    type T = Box<dyn Read + Send>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let file = File::open(&self.path)
            .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
        let reader = RangeReader::new(file, start, self.file_len - start)
            .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
        Ok(Box::new(reader))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let mut file = File::open(&self.path)
            .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
        file.seek(SeekFrom::Start(start))
            .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;

        let mut buf = vec![0; length];
        file.read_exact(&mut buf)
            .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
        Ok(Bytes::from(buf))
    }
}

// Implement ChunkReader for RubyIOChunkReader
impl ChunkReader for RubyIOChunkReader {
    type T = Box<dyn Read + Send>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        // Clone the reader for thread-safe access
        let mut reader = self.reader.clone();

        // Seek to the start position
        reader
            .seek(SeekFrom::Start(start))
            .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;

        // Create a range reader that limits reading to the available data
        let reader = RangeReader::new(reader, start, self.len - start)
            .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
        Ok(Box::new(reader))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let mut reader = self.reader.clone();
        reader
            .seek(SeekFrom::Start(start))
            .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;

        let mut buf = vec![0; length];
        reader
            .read_exact(&mut buf)
            .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
        Ok(Bytes::from(buf))
    }
}

// Implement ChunkReader for CloneableChunkReader
impl ChunkReader for CloneableChunkReader {
    type T = Box<dyn Read + Send>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        match self {
            CloneableChunkReader::File(f) => f.get_read(start),
            CloneableChunkReader::RubyIO(r) => r.get_read(start),
            CloneableChunkReader::Bytes(b) => {
                // For bytes, we can use the built-in implementation
                let bytes = b.clone();
                let len = bytes.len();
                if start as usize > len {
                    return Err(parquet::errors::ParquetError::IndexOutOfBound(
                        start as usize,
                        len,
                    ));
                }
                let reader = std::io::Cursor::new(bytes.slice(start as usize..));
                Ok(Box::new(reader))
            }
        }
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        match self {
            CloneableChunkReader::File(f) => f.get_bytes(start, length),
            CloneableChunkReader::RubyIO(r) => r.get_bytes(start, length),
            CloneableChunkReader::Bytes(b) => {
                // For bytes, use the built-in slice functionality
                let end = (start as usize).saturating_add(length).min(b.len());
                Ok(b.slice(start as usize..end))
            }
        }
    }
}

/// Create a CloneableChunkReader from various sources
impl CloneableChunkReader {
    /// Create from a file path
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(CloneableChunkReader::File(FileChunkReader::new(path)?))
    }

    /// Create from a Ruby IO object
    pub fn from_ruby_io(reader: ThreadSafeRubyIOReader) -> Result<Self> {
        // Get the length by seeking to the end and back
        let mut reader_clone = reader.clone();
        let len = reader_clone.seek(SeekFrom::End(0))?;
        reader_clone.seek(SeekFrom::Start(0))?;

        Ok(CloneableChunkReader::RubyIO(RubyIOChunkReader::new(
            reader, len,
        )))
    }

    /// Create from bytes (for small files or testing)
    pub fn from_bytes(bytes: Bytes) -> Self {
        CloneableChunkReader::Bytes(bytes)
    }

    /// Check if this reader should use streaming (based on size threshold)
    pub fn should_stream(&self, threshold_bytes: u64) -> bool {
        self.len() > threshold_bytes
    }
}
