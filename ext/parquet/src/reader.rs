use crate::header_cache::{CacheError, StringCache};
use crate::ruby_reader::{build_ruby_reader, SeekableRead};
use crate::utils::*;
use bytes::Bytes;
use magnus::rb_sys::AsRawValue;
use magnus::value::{Opaque, ReprValue};
use magnus::IntoValue;
use magnus::{block::Yield, Error as MagnusError, KwArgs, RHash, Ruby, Symbol, Value};
use parquet::errors::ParquetError;
use parquet::file::reader::{ChunkReader, Length, SerializedFileReader};
use parquet::record::Field;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::mem::ManuallyDrop;
use std::os::fd::FromRawFd;
use std::sync::OnceLock;
use std::{borrow::Cow, hash::BuildHasher};
use thiserror::Error;
use xxhash_rust::xxh3::Xxh3Builder;

use parquet::record::reader::RowIter as ParquetRowIter;

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

struct ForgottenFileHandle(ManuallyDrop<File>);

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

struct HeaderCacheCleanupIter<I> {
    inner: I,
    headers: OnceLock<Vec<&'static str>>,
}

impl<I: Iterator> Iterator for HeaderCacheCleanupIter<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<I> Drop for HeaderCacheCleanupIter<I> {
    fn drop(&mut self) {
        if let Some(headers) = self.headers.get() {
            StringCache::clear(&headers).unwrap();
        }
    }
}

pub fn parse_parquet<'a>(
    rb_self: Value,
    args: &[Value],
) -> Result<Yield<Box<dyn Iterator<Item = Record<Xxh3Builder>>>>, MagnusError> {
    let original = unsafe { Ruby::get_unchecked() };
    let ruby: &'static Ruby = Box::leak(Box::new(original));

    let ParquetArgs {
        to_read,
        result_type,
    } = parse_parquet_args(&ruby, args)?;

    if !ruby.block_given() {
        return create_enumerator(EnumeratorArgs {
            rb_self,
            to_read,
            result_type,
        });
    }

    let iter = if to_read.is_kind_of(ruby.class_string()) {
        let path_string = to_read.to_r_string()?;
        let file_path = unsafe { path_string.as_str()? };
        let file = File::open(file_path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        ParquetRowIter::from_file_into(Box::new(reader))
    } else if to_read.is_kind_of(ruby.class_io()) {
        let raw_value = to_read.as_raw();
        let fd = std::panic::catch_unwind(|| unsafe { rb_sys::rb_io_descriptor(raw_value) })
            .map_err(|_| {
                ReaderError::FileDescriptor("Failed to get file descriptor".to_string())
            })?;

        if fd < 0 {
            return Err(ReaderError::InvalidFileDescriptor.into());
        }

        let file = unsafe { File::from_raw_fd(fd) };
        let file = ForgottenFileHandle(ManuallyDrop::new(file));
        let reader = SerializedFileReader::new(file).unwrap();
        ParquetRowIter::from_file_into(Box::new(reader))
    } else {
        let readable = SeekableRubyValue(Opaque::from(to_read));
        let reader = SerializedFileReader::new(readable).unwrap();
        ParquetRowIter::from_file_into(Box::new(reader))
    };

    let iter: Box<dyn Iterator<Item = Record<Xxh3Builder>>> = match result_type.as_str() {
        "hash" => {
            let headers = OnceLock::new();
            let headers_clone = headers.clone();
            let iter = iter
                .filter_map(move |row| {
                    row.ok().map(|row| {
                        let headers = headers_clone.get_or_init(|| {
                            row.get_column_iter()
                                .map(|(k, _)| StringCache::intern(k.to_owned()).unwrap())
                                .collect::<Vec<_>>()
                        });

                        row.get_column_iter()
                            .enumerate()
                            .map(|(i, (_, v))| {
                                let key = headers[i];
                                (key, ParquetField(v.clone()))
                            })
                            .collect::<HashMap<&'static str, ParquetField, Xxh3Builder>>()
                    })
                })
                .map(|row| Record::Map(row));

            Box::new(HeaderCacheCleanupIter {
                inner: iter,
                headers,
            })
        }
        "array" => Box::new(
            iter.filter_map(|row| {
                row.ok().map(|row| {
                    row.get_column_iter()
                        .map(|(_, v)| ParquetField(v.clone()))
                        .collect::<Vec<ParquetField>>()
                })
            })
            .map(|row| Record::Vec(row)),
        ),
        _ => {
            return Err(MagnusError::new(
                ruby.exception_runtime_error(),
                "Invalid result type",
            ))
        }
    };

    Ok(Yield::Iter(iter))
}

struct EnumeratorArgs {
    rb_self: Value,
    to_read: Value,
    result_type: String,
}

fn create_enumerator(
    args: EnumeratorArgs,
) -> Result<Yield<Box<dyn Iterator<Item = Record<Xxh3Builder>>>>, MagnusError> {
    let kwargs = RHash::new();

    kwargs.aset(Symbol::new("result_type"), Symbol::new(args.result_type))?;

    let enumerator = args
        .rb_self
        .enumeratorize("for_each", (args.to_read, KwArgs(kwargs)));
    Ok(Yield::Enumerator(enumerator))
}

#[derive(Debug)]
pub enum Record<S: BuildHasher + Default> {
    Vec(Vec<ParquetField>),
    Map(HashMap<&'static str, ParquetField, S>),
}

impl<S: BuildHasher + Default> IntoValue for Record<S> {
    #[inline]
    fn into_value_with(self, handle: &Ruby) -> Value {
        match self {
            Record::Vec(vec) => {
                let ary = handle.ary_new_capa(vec.len());
                vec.into_iter().try_for_each(|v| ary.push(v)).unwrap();
                ary.into_value_with(handle)
            }
            Record::Map(map) => {
                // Pre-allocate the hash with the known size
                let hash = handle.hash_new_capa(map.len());
                map.into_iter()
                    .try_for_each(|(k, v)| hash.aset(k, v))
                    .unwrap();
                hash.into_value_with(handle)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct CowValue<'a>(pub Cow<'a, str>);

impl<'a> IntoValue for CowValue<'a> {
    fn into_value_with(self, handle: &Ruby) -> Value {
        self.0.into_value_with(handle)
    }
}

#[derive(Debug)]
pub struct ParquetField(Field);

impl<'a> IntoValue for ParquetField {
    fn into_value_with(self, handle: &Ruby) -> Value {
        match self.0 {
            Field::Byte(b) => b.into_value_with(handle),
            Field::Bool(b) => b.into_value_with(handle),
            Field::Short(s) => s.into_value_with(handle),
            Field::Int(i) => i.into_value_with(handle),
            Field::Long(l) => l.into_value_with(handle),
            Field::UByte(ub) => ub.into_value_with(handle),
            Field::UShort(us) => us.into_value_with(handle),
            Field::UInt(ui) => ui.into_value_with(handle),
            Field::ULong(ul) => ul.into_value_with(handle),
            Field::Float16(f) => f32::from(f).into_value_with(handle),
            Field::Float(f) => f.into_value_with(handle),
            Field::Double(d) => d.into_value_with(handle),

            Field::Str(s) => s.into_value_with(handle),
            Field::Bytes(b) => handle.str_from_slice(b.data()).as_value(),
            Field::Date(d) => d.into_value_with(handle),
            Field::TimestampMillis(ts) => ts.into_value_with(handle),
            Field::TimestampMicros(ts) => ts.into_value_with(handle),
            Field::ListInternal(list) => {
                let ary = handle.ary_new_capa(list.elements().len());
                list.elements()
                    .iter()
                    .try_for_each(|e| ary.push(ParquetField(e.clone()).into_value_with(handle)))
                    .unwrap();
                ary.into_value_with(handle)
            }
            Field::MapInternal(map) => {
                let hash = handle.hash_new_capa(map.entries().len());
                map.entries()
                    .iter()
                    .try_for_each(|(k, v)| {
                        hash.aset(
                            ParquetField(k.clone()).into_value_with(handle),
                            ParquetField(v.clone()).into_value_with(handle),
                        )
                    })
                    .unwrap();
                hash.into_value_with(handle)
            }
            // Field::Decimal(d) => d.to_string().into_value_with(handle),
            // Field::Group(row) => row.into_value_with(handle),
            Field::Null => handle.qnil().as_value(),
            _ => panic!("Unsupported field type"),
        }
    }
}

struct SeekableRubyValue(Opaque<Value>);

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
        Ok(BufReader::new(reader))
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
