use bytes::Bytes;
use magnus::error::OpaqueError;
use magnus::gc;
use magnus::value::{Opaque, ReprValue};
use magnus::{Error as MagnusError, RString, Ruby, TryConvert, Value};
use std::io::{Error as IoError, ErrorKind, Read, Seek, SeekFrom, Write};

use parquet::{
    errors::ParquetError,
    file::reader::{ChunkReader, Length},
};
use std::{fs::File, sync::Mutex};
use std::{
    io::{self, BufReader},
    sync::Arc,
};

/// A reader that can handle various Ruby input types (String, StringIO, IO-like objects)
/// and provide a standard Read implementation for them.
pub enum RubyIOReader {
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

// Sending is technically not safe, but the only things that threatens to
// do this is the parquet gem, and they don't seem to actually do it.
unsafe impl Send for RubyIOReader {}

impl RubyIOReader {
    pub fn new(value: Value) -> std::io::Result<Self> {
        if RubyIOReader::is_seekable_io_like(&value) {
            Ok(RubyIOReader::RubyIoLike {
                inner: Opaque::from(value),
            })
        } else if RubyIOReader::is_io_like(&value) {
            let mut temp_file = tempfile::tempfile()?;

            // This is safe, because we won't call seek
            let inner_readable = RubyIOReader::RubyIoLike {
                inner: Opaque::from(value),
            };
            let mut reader = BufReader::new(inner_readable);
            io::copy(&mut reader, &mut temp_file)?;
            temp_file.seek(SeekFrom::Start(0))?;

            Ok(RubyIOReader::NativeProxyIoLike {
                proxy_file: temp_file,
            })
        } else {
            // Try calling `to_str`, and if that fails, try `to_s`
            let string_content = value
                .funcall::<_, _, RString>("to_str", ())
                .or_else(|_| value.funcall::<_, _, RString>("to_s", ()))
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            Ok(RubyIOReader::String {
                inner: Opaque::from(string_content),
                offset: 0,
            })
        }
    }

    fn is_io_like(value: &Value) -> bool {
        value.respond_to("read", false).unwrap_or(false)
    }

    // For now, don't use this. Having to use seek in length is scary.
    fn is_seekable_io_like(value: &Value) -> bool {
        Self::is_io_like(value)
            && value.respond_to("seek", false).unwrap_or(false)
            && value.respond_to("pos", false).unwrap_or(false)
    }
}

impl Seek for RubyIOReader {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let ruby = Ruby::get()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to get Ruby runtime"))?;
        match self {
            RubyIOReader::NativeProxyIoLike { proxy_file } => proxy_file.seek(pos),
            RubyIOReader::String {
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
            RubyIOReader::RubyIoLike { inner } => {
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

impl Read for RubyIOReader {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let ruby = Ruby::get()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to get Ruby runtime"))?;
        match self {
            RubyIOReader::NativeProxyIoLike { proxy_file } => proxy_file.read(buf),
            RubyIOReader::String { inner, offset } => {
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
            RubyIOReader::RubyIoLike { inner } => {
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
                    None => Ok(0),
                }
            }
        }
    }
}

impl Length for RubyIOReader {
    fn len(&self) -> u64 {
        let ruby = match Ruby::get() {
            Ok(r) => r,
            Err(_) => {
                eprintln!("Failed to get Ruby runtime in RubyIOReader::len");
                return 0;
            }
        };
        match self {
            RubyIOReader::NativeProxyIoLike { proxy_file } => proxy_file.len(),
            RubyIOReader::String { inner, offset: _ } => {
                let unwrapped_inner = ruby.get_inner(*inner);
                unwrapped_inner.len() as u64
            }
            RubyIOReader::RubyIoLike { inner } => {
                let unwrapped_inner = ruby.get_inner(*inner);

                // Get current position
                let current_pos = match unwrapped_inner.funcall::<_, _, u64>("pos", ()) {
                    Ok(pos) => pos,
                    Err(e) => {
                        eprintln!("Error seeking: {}", e);
                        return 0;
                    }
                };

                // Seek to end
                if let Err(e) = unwrapped_inner.funcall::<_, _, u64>("seek", (0, 2)) {
                    eprintln!("Error seeking: {}", e);
                    return 0;
                }

                // Offset at the end of the file is the length of the file
                let size = match unwrapped_inner.funcall::<_, _, u64>("pos", ()) {
                    Ok(pos) => pos,
                    Err(e) => {
                        eprintln!("Error seeking: {}", e);
                        return 0;
                    }
                };

                // Restore original position
                if let Err(e) = unwrapped_inner.funcall::<_, _, u64>("seek", (current_pos, 0)) {
                    eprintln!("Error seeking: {}", e);
                    return 0;
                }

                let final_pos = match unwrapped_inner.funcall::<_, _, u64>("pos", ()) {
                    Ok(pos) => pos,
                    Err(e) => {
                        eprintln!("Error seeking: {}", e);
                        return 0;
                    }
                };

                if current_pos != final_pos {
                    eprintln!(
                        "Failed to restore original position in seekable IO object. Started at position {}, but ended at {}",
                        current_pos,
                        final_pos
                    );
                }

                size
            }
        }
    }
}

const READ_BUFFER_SIZE: usize = 16 * 1024;

#[derive(Clone)]
pub struct ThreadSafeRubyIOReader(Arc<Mutex<RubyIOReader>>);

impl ThreadSafeRubyIOReader {
    pub fn new(reader: RubyIOReader) -> Self {
        Self(Arc::new(Mutex::new(reader)))
    }
}

impl Length for ThreadSafeRubyIOReader {
    fn len(&self) -> u64 {
        match self.0.lock() {
            Ok(reader) => reader.len(),
            Err(_) => {
                // If the mutex is poisoned, we can't recover, return 0
                eprintln!("Failed to lock mutex in ThreadSafeRubyIOReader::len");
                0
            }
        }
    }
}

impl Seek for ThreadSafeRubyIOReader {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let mut reader = self
            .0
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        reader.seek(pos)
    }
}

impl Read for ThreadSafeRubyIOReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut reader = self
            .0
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        reader.read(buf)
    }
}

impl ChunkReader for ThreadSafeRubyIOReader {
    type T = BufReader<ThreadSafeRubyIOReader>;

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

/// Adapter for Ruby IO objects that implements std::io::Write
pub struct RubyIOWriter {
    io: Value,
}

impl RubyIOWriter {
    pub fn new(io: Value) -> Self {
        Self { io }
    }
}

impl Write for RubyIOWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Call Ruby IO#write method
        let ruby = Ruby::get().map_err(|e| {
            IoError::new(
                ErrorKind::Other,
                format!("Failed to get Ruby runtime: {}", e),
            )
        })?;

        if buf.is_empty() {
            return Ok(0);
        }

        write_to_io(&ruby, self.io, buf)
            .map(|()| buf.len())
            .map_err(|error| IoError::other(error.to_string()))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // Call Ruby IO#flush method
        let result: Result<Value, MagnusError> = self.io.funcall("flush", ());

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(IoError::new(ErrorKind::Other, e.to_string())),
        }
    }
}

/// Bytes that a `RubyIoSink` collects before it calls the IO object's `write`, so
/// that the many small writes of a footer reach Ruby as a few large strings.
const IO_SINK_BUFFER_BYTES: usize = 1024 * 1024;

/// Carries the first error that a Ruby IO object raises while a `RubyIoSink`
/// writes to it, to the writer that raises it once the Parquet writer stops.
///
/// The Parquet writer owns the sink and turns a failed write into its own error,
/// whose message is all that survives the footer's thrift encoding, so the Ruby
/// error travels here instead: an exception, or a non-local exit such as `throw`.
/// The sink reports to it, and the writer takes the error from it.
#[derive(Clone, Default)]
pub struct RubyIoFailure(Arc<Mutex<Option<KeptError>>>);

impl RubyIoFailure {
    /// Keeps the error, unless an earlier one is already kept.
    fn report(&self, error: MagnusError) {
        if let Ok(mut kept) = self.0.lock() {
            if kept.is_none() {
                *kept = Some(KeptError::new(error));
            }
        }
    }

    /// Takes the error that the IO object raised, if it raised one.
    pub fn take(&self, ruby: &Ruby) -> Option<MagnusError> {
        let kept = self.0.lock().ok()?.take()?;
        Some(kept.into_error(ruby))
    }
}

/// An error kept on the heap until it is raised. Its Ruby object, the exception
/// or its class, is registered with the garbage collector meanwhile, since only
/// the stack would otherwise keep it alive.
struct KeptError {
    error: OpaqueError,
    root: Option<Box<Opaque<Value>>>,
}

impl KeptError {
    fn new(error: MagnusError) -> Self {
        let root = error.value().map(|value| {
            let root = Box::new(Opaque::from(value));
            gc::register_address(&*root);
            root
        });
        Self {
            error: OpaqueError::from(error),
            root,
        }
    }

    /// Returns the error, whose Ruby object the stack then keeps alive.
    fn into_error(self, ruby: &Ruby) -> MagnusError {
        OpaqueError::into_error_with(self.error.clone(), ruby)
    }
}

impl Drop for KeptError {
    fn drop(&mut self) {
        if let Some(root) = &self.root {
            gc::unregister_address(&**root);
        }
    }
}

/// Writes all of `bytes` to a Ruby IO object, passing the rest to `write` again
/// when it writes only some of them.
///
/// `IO#write` writes every byte and returns how many. An IO-like object may write
/// fewer, or return something other than an Integer, which is taken to mean that
/// it wrote them all. The bytes are copied into one String, and the rest after a
/// short write is a slice of it that shares its memory.
fn write_to_io(ruby: &Ruby, io: Value, bytes: &[u8]) -> Result<(), MagnusError> {
    let string = ruby.str_from_slice(bytes);
    let mut offset = 0;
    while offset < bytes.len() {
        let remaining = bytes.len() - offset;
        let chunk: Value = if offset == 0 {
            string.as_value()
        } else {
            string.funcall("byteslice", (offset, remaining))?
        };
        let result: Value = io.funcall("write", (chunk,))?;
        let written = if result.is_kind_of(ruby.class_integer()) {
            match i64::try_convert(result) {
                Ok(count) if count > 0 => usize::try_from(count).unwrap_or(usize::MAX),
                _ => {
                    return Err(MagnusError::new(
                        ruby.exception_io_error(),
                        format!("IO object's write returned {result}, rather than the number of bytes it wrote"),
                    ))
                }
            }
        } else {
            remaining
        };
        offset += written.min(remaining);
    }
    Ok(())
}

/// Writes the bytes of a Parquet file to a Ruby IO object as they are produced,
/// rather than staging the whole file on disk first.
///
/// Calls the IO object's `write` with up to `IO_SINK_BUFFER_BYTES` at a time, and
/// with whatever remains when the writer flushes, which it does after each row
/// group and at the end of the file. Once a write fails, it reports the error to
/// its `RubyIoFailure` and writes nothing more to the IO object. Bytes still
/// buffered when the sink is dropped are discarded, so a failed Parquet write
/// sends nothing more to the IO object either.
pub struct RubyIoSink {
    io: Opaque<Value>,
    buffer: Vec<u8>,
    failure: RubyIoFailure,
    failed: bool,
}

impl RubyIoSink {
    pub fn new(io: Value, failure: RubyIoFailure) -> Self {
        Self {
            io: Opaque::from(io),
            buffer: Vec::with_capacity(IO_SINK_BUFFER_BYTES),
            failure,
            failed: false,
        }
    }

    fn drain(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let result = send(self.io, &self.buffer);
        self.buffer.clear();
        result.map_err(|error| self.fail(error))
    }

    /// Stops writing to the IO object, and reports the Ruby error, when there is
    /// one, so that the writer raises it.
    fn fail(&mut self, error: Option<MagnusError>) -> IoError {
        self.failed = true;
        match error {
            Some(error) => {
                self.failure.report(error);
                IoError::other("the IO object raised an error")
            }
            None => IoError::other("a Ruby IO object can only be written from a Ruby thread"),
        }
    }

    fn check_usable(&self) -> io::Result<()> {
        if self.failed {
            Err(IoError::other("an earlier write to the IO object failed"))
        } else {
            Ok(())
        }
    }
}

/// Writes the bytes to the IO object, returning the Ruby error that it raised, or
/// no error when this is not a Ruby thread.
fn send(io: Opaque<Value>, bytes: &[u8]) -> Result<(), Option<MagnusError>> {
    let ruby = Ruby::get().map_err(|_| None)?;
    write_to_io(&ruby, ruby.get_inner(io), bytes).map_err(Some)
}

impl Write for RubyIoSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.check_usable()?;
        if self.buffer.len() + bytes.len() > IO_SINK_BUFFER_BYTES {
            self.drain()?;
        }
        if bytes.len() >= IO_SINK_BUFFER_BYTES {
            send(self.io, bytes).map_err(|error| self.fail(error))?;
        } else {
            self.buffer.extend_from_slice(bytes);
        }
        Ok(bytes.len())
    }

    /// Writes the buffered bytes to the IO object, without calling its `flush`.
    fn flush(&mut self) -> io::Result<()> {
        self.check_usable()?;
        self.drain()
    }
}

/// Wrapper that implements both Read and Write for Ruby IO objects
pub struct RubyIO {
    io: Value,
}

impl RubyIO {
    pub fn new(io: Value) -> Self {
        Self { io }
    }
}

impl Read for RubyIO {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        RubyIOReader::new(self.io)?.read(buf)
    }
}

impl Write for RubyIO {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        RubyIOWriter::new(self.io).write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        RubyIOWriter::new(self.io).flush()
    }
}

impl Seek for RubyIO {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        RubyIOReader::new(self.io)?.seek(pos)
    }
}

/// Check if a Ruby value responds to IO methods
pub fn is_io_like(value: Value) -> bool {
    // Check if the object responds to read/write methods
    let responds_to_read: Result<bool, MagnusError> = value.funcall("respond_to?", ("read",));
    let responds_to_write: Result<bool, MagnusError> = value.funcall("respond_to?", ("write",));

    matches!(
        (responds_to_read, responds_to_write),
        (Ok(true), _) | (_, Ok(true))
    )
}

/// Create a reader from a Ruby IO-like object
pub fn create_reader(io: Value) -> std::io::Result<RubyIOReader> {
    // Verify it has a read method
    let responds_to_read: Result<bool, MagnusError> = io.funcall("respond_to?", ("read",));

    match responds_to_read {
        Ok(true) => RubyIOReader::new(io),
        Ok(false) => Err(IoError::new(
            ErrorKind::InvalidInput,
            "Object does not respond to 'read' method",
        )),
        Err(e) => Err(IoError::new(ErrorKind::Other, e.to_string())),
    }
}

/// Create a writer from a Ruby IO-like object
pub fn create_writer(io: Value) -> std::io::Result<RubyIOWriter> {
    // Verify it has a write method
    let responds_to_write: Result<bool, MagnusError> = io.funcall("respond_to?", ("write",));

    match responds_to_write {
        Ok(true) => Ok(RubyIOWriter::new(io)),
        Ok(false) => Err(IoError::new(
            ErrorKind::InvalidInput,
            "Object does not respond to 'write' method",
        )),
        Err(e) => Err(IoError::new(ErrorKind::Other, e.to_string())),
    }
}
