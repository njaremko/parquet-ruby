use std::{
    fs::File,
    io::{BufReader, SeekFrom},
    mem::ManuallyDrop,
};

use bytes::Bytes;
use magnus::{value::Opaque, Ruby, Value};
use parquet::{
    errors::ParquetError,
    file::reader::{ChunkReader, Length},
};
use std::io::Read;

use crate::ruby_reader::{build_ruby_reader, SeekableRead};

const READ_BUFFER_SIZE: usize = 16 * 1024;

pub struct SeekableRubyValue(pub Opaque<Value>);

impl Length for SeekableRubyValue {
    fn len(&self) -> u64 {
        let ruby = unsafe { Ruby::get_unchecked() };
        let mut reader = build_ruby_reader(&ruby, ruby.get_inner(self.0)).unwrap();
        let current_pos = reader.seek(SeekFrom::Current(0)).unwrap();
        let file_len = reader.seek(SeekFrom::End(0)).unwrap();
        reader.seek(SeekFrom::Start(current_pos)).unwrap();
        file_len
    }
}

impl ChunkReader for SeekableRubyValue {
    type T = BufReader<Box<dyn SeekableRead>>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let ruby = unsafe { Ruby::get_unchecked() };
        let mut reader = build_ruby_reader(&ruby, ruby.get_inner(self.0)).unwrap();
        reader.seek(SeekFrom::Start(start))?;
        Ok(BufReader::with_capacity(READ_BUFFER_SIZE, reader))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let ruby = unsafe { Ruby::get_unchecked() };
        let mut buffer = Vec::with_capacity(length);
        let mut reader = build_ruby_reader(&ruby, ruby.get_inner(self.0)).unwrap();
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

pub struct ForgottenFileHandle(pub ManuallyDrop<File>);

impl Length for ForgottenFileHandle {
    fn len(&self) -> u64 {
        self.0.len()
    }
}

impl ChunkReader for ForgottenFileHandle {
    type T = BufReader<File>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        self.0.get_read(start)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        self.0.get_bytes(start, length)
    }
}
