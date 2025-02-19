use bytes::Bytes;
use magnus::{
    value::{Opaque, ReprValue},
    RString, Ruby, Value,
};
use parquet::{
    errors::ParquetError,
    file::reader::{ChunkReader, Length},
};
use std::{fs::File, sync::Mutex};
use std::{
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    sync::Arc,
};

/// A reader that can handle various Ruby input types (String, StringIO, IO-like objects)
/// and provide a standard Read implementation for them.
pub enum RubyReader {
    String {
        inner: Opaque<RString>,
        offset: usize,
    },
    RubyIoLike {
        inner: Opaque<Value>,
    },
    NativeProxyIoLike {
        proxy_file: File,
    },
}

impl RubyReader {
    fn is_io_like(value: &Value) -> bool {
        value.respond_to("read", false).unwrap_or(false)
    }

    // For now, don't use this. Having to use seek in length is scary.
    fn is_seekable_io_like(value: &Value) -> bool {
        Self::is_io_like(value) && value.respond_to("seek", false).unwrap_or(false)
    }
}

impl TryFrom<Value> for RubyReader {
    type Error = magnus::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let ruby = unsafe { Ruby::get_unchecked() };
        if RubyReader::is_seekable_io_like(&value) {
            Ok(RubyReader::RubyIoLike {
                inner: Opaque::from(value),
            })
        } else if RubyReader::is_io_like(&value) {
            let mut temp_file = tempfile::tempfile()
                .map_err(|e| magnus::Error::new(ruby.exception_runtime_error(), e.to_string()))?;

            // This is safe, because we won't call seek
            let inner_readable = RubyReader::RubyIoLike {
                inner: Opaque::from(value),
            };
            let mut reader = BufReader::new(inner_readable);
            io::copy(&mut reader, &mut temp_file)
                .map_err(|e| magnus::Error::new(ruby.exception_runtime_error(), e.to_string()))?;
            temp_file
                .seek(SeekFrom::Start(0))
                .map_err(|e| magnus::Error::new(ruby.exception_runtime_error(), e.to_string()))?;

            Ok(RubyReader::NativeProxyIoLike {
                proxy_file: temp_file,
            })
        } else {
            // Try calling `to_str`, and if that fails, try `to_s`
            let string_content = value
                .funcall::<_, _, RString>("to_str", ())
                .or_else(|_| value.funcall::<_, _, RString>("to_s", ()))?;
            Ok(RubyReader::String {
                inner: Opaque::from(string_content),
                offset: 0,
            })
        }
    }
}

impl Seek for RubyReader {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let ruby = unsafe { Ruby::get_unchecked() };
        match self {
            RubyReader::NativeProxyIoLike { proxy_file } => proxy_file.seek(pos),
            RubyReader::String {
                inner,
                offset: original_offset,
            } => {
                let unwrapped_inner = ruby.get_inner(*inner);

                let new_offset = match pos {
                    SeekFrom::Start(off) => off as usize,
                    SeekFrom::Current(off) => {
                        let signed = *original_offset as i64 + off;
                        signed.max(0) as usize
                    }
                    SeekFrom::End(off) => {
                        let signed = unwrapped_inner.len() as i64 + off;
                        signed.max(0) as usize
                    }
                };

                *original_offset = new_offset.min(unwrapped_inner.len());
                Ok(*original_offset as u64)
            }
            RubyReader::RubyIoLike { inner } => {
                let unwrapped_inner = ruby.get_inner(*inner);

                let (whence, ruby_offset) = match pos {
                    SeekFrom::Start(i) => (0, i as i64),
                    SeekFrom::Current(i) => (1, i),
                    SeekFrom::End(i) => (2, i),
                };

                unwrapped_inner
                    .funcall::<_, _, u64>("seek", (ruby_offset, whence))
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

                let new_position = unwrapped_inner
                    .funcall::<_, _, u64>("pos", ())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

                Ok(new_position)
            }
        }
    }
}

impl Read for RubyReader {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let ruby = unsafe { Ruby::get_unchecked() };
        match self {
            RubyReader::NativeProxyIoLike { proxy_file } => proxy_file.read(buf),
            RubyReader::String { inner, offset } => {
                let unwrapped_inner = ruby.get_inner(*inner);

                let string_buffer = unsafe { unwrapped_inner.as_slice() };
                if *offset >= string_buffer.len() {
                    return Ok(0); // EOF
                }

                let remaining = string_buffer.len() - *offset;
                let copy_size = remaining.min(buf.len());
                buf[..copy_size].copy_from_slice(&string_buffer[*offset..*offset + copy_size]);

                *offset += copy_size;

                Ok(copy_size)
            }
            RubyReader::RubyIoLike { inner } => {
                let unwrapped_inner = ruby.get_inner(*inner);

                let bytes = unwrapped_inner
                    .funcall::<_, _, Option<RString>>("read", (buf.len(),))
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

                match bytes {
                    Some(bytes) => {
                        let string_buffer = unsafe { bytes.as_slice() };
                        buf.write_all(string_buffer)?;
                        Ok(string_buffer.len())
                    }
                    None => {
                        return Ok(0);
                    }
                }
            }
        }
    }
}

impl Length for RubyReader {
    fn len(&self) -> u64 {
        let ruby = unsafe { Ruby::get_unchecked() };
        match self {
            RubyReader::NativeProxyIoLike { proxy_file } => proxy_file.len(),
            RubyReader::String { inner, offset: _ } => {
                let unwrapped_inner = ruby.get_inner(*inner);
                unwrapped_inner.len() as u64
            }
            RubyReader::RubyIoLike { inner } => {
                let unwrapped_inner = ruby.get_inner(*inner);
                let current_pos = unwrapped_inner.funcall::<_, _, u64>("seek", (0, 1));

                if let Err(e) = current_pos {
                    eprintln!("Error seeking: {}", e);
                    return 0;
                }

                if let Err(e) = unwrapped_inner.funcall::<_, _, u64>("seek", (0, 2)) {
                    eprintln!("Error seeking: {}", e);
                    return 0;
                }

                let size = unwrapped_inner.funcall::<_, _, u64>("pos", ());

                match size {
                    Ok(size) => {
                        // Restore original position
                        if let Err(e) = unwrapped_inner.funcall::<_, _, u64>(
                            "seek",
                            (current_pos.expect("Current position is not set!"), 0),
                        ) {
                            eprintln!("Error seeking: {}", e);
                            return 0;
                        }
                        size
                    }
                    Err(e) => {
                        eprintln!("Error seeking: {}", e);
                        return 0;
                    }
                }
            }
        }
    }
}

const READ_BUFFER_SIZE: usize = 16 * 1024;

#[derive(Clone)]
pub struct ThreadSafeRubyReader(Arc<Mutex<RubyReader>>);

impl ThreadSafeRubyReader {
    pub fn new(reader: RubyReader) -> Self {
        Self(Arc::new(Mutex::new(reader)))
    }
}

impl Length for ThreadSafeRubyReader {
    fn len(&self) -> u64 {
        self.0.lock().expect("Failed to lock mutex").len()
    }
}

impl Seek for ThreadSafeRubyReader {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let mut reader = self
            .0
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        reader.seek(pos)
    }
}

impl Read for ThreadSafeRubyReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut reader = self
            .0
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        reader.read(buf)
    }
}

impl ChunkReader for ThreadSafeRubyReader {
    type T = BufReader<ThreadSafeRubyReader>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let mut reader = self.clone();
        reader.seek(SeekFrom::Start(start))?;
        Ok(BufReader::with_capacity(READ_BUFFER_SIZE, reader))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let mut buffer = Vec::with_capacity(length);
        let mut reader = self.clone();
        reader.seek(SeekFrom::Start(start))?;
        let read = reader.take(length as _).read_to_end(&mut buffer)?;

        if read != length {
            return Err(ParquetError::EOF(format!(
                "Expected to read {} bytes, read only {}",
                length, read
            )));
        }
        Ok(buffer.into())
    }
}
