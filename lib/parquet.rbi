# typed: true

module Parquet
  # Returns metadata information about a Parquet file
  #
  # The returned hash contains information about:
  # - Basic file metadata (num_rows, created_by)
  # - Schema information (fields, types, etc.)
  # - Row group details
  # - Column chunk information (compression, encodings, statistics)
  sig { params(path: String).returns(T::Hash[String, T.untyped]) }
  def self.metadata(path)
  end

  # Options:
  #   - `input`: String, File, or IO object containing parquet data
  #   - `result_type`: String specifying the output format
  #                    ("hash" or "array" or :hash or :array)
  #   - `columns`: When present, only the specified columns will be included in the output.
  #                This is useful for reducing how much data is read and improving performance.
  sig do
    params(
      input: T.any(String, File, StringIO, IO),
      result_type: T.nilable(T.any(String, Symbol)),
      columns: T.nilable(T::Array[String]),
      strict: T.nilable(T::Boolean)
    ).returns(T::Enumerator[T.any(T::Hash[String, T.untyped], T::Array[T.untyped])])
  end
  sig do
    params(
      input: T.any(String, File, StringIO, IO),
      result_type: T.nilable(T.any(String, Symbol)),
      columns: T.nilable(T::Array[String]),
      strict: T.nilable(T::Boolean),
      blk: T.nilable(T.proc.params(row: T.any(T::Hash[String, T.untyped], T::Array[T.untyped])).void)
    ).returns(NilClass)
  end
  def self.each_row(input, result_type: nil, columns: nil, strict: nil, &blk)
  end

  # Options:
  #   - `input`: String, File, or IO object containing parquet data
  #   - `result_type`: String specifying the output format
  #                    ("hash" or "array" or :hash or :array)
  #   - `columns`: When present, only the specified columns will be included in the output.
  #   - `batch_size`: When present, specifies the number of rows per batch
  sig do
    params(
      input: T.any(String, File, StringIO, IO),
      result_type: T.nilable(T.any(String, Symbol)),
      columns: T.nilable(T::Array[String]),
      batch_size: T.nilable(Integer),
      strict: T.nilable(T::Boolean)
    ).returns(T::Enumerator[T.any(T::Hash[String, T.untyped], T::Array[T.untyped])])
  end
  sig do
    params(
      input: T.any(String, File, StringIO, IO),
      result_type: T.nilable(T.any(String, Symbol)),
      columns: T.nilable(T::Array[String]),
      batch_size: T.nilable(Integer),
      strict: T.nilable(T::Boolean),
      blk:
        T.nilable(T.proc.params(batch: T.any(T::Hash[String, T::Array[T.untyped]], T::Array[T::Array[T.untyped]])).void)
    ).returns(NilClass)
  end
  def self.each_column(input, result_type: nil, columns: nil, batch_size: nil, strict: nil, &blk)
  end

  # Options:
  #   - `read_from`: An Enumerator yielding arrays of values representing each row
  #   - `schema`: Array of hashes specifying column names and types. Supported types:
  #     - `int8`, `int16`, `int32`, `int64`
  #     - `uint8`, `uint16`, `uint32`, `uint64`
  #     - `float`, `double`
  #     - `string`
  #     - `binary`
  #     - `boolean`
  #     - `date32`
  #     - `timestamp_millis`, `timestamp_micros`
  #   - `write_to`: String path or IO object to write the parquet file to
  #   - `batch_size`: Optional batch size for writing (defaults to 1000)
  #   - `flush_threshold`: Optional memory threshold in bytes before flushing (defaults to 64MB)
  #   - `compression`: Optional compression type to use (defaults to "zstd")
  #                   Supported values: "none", "uncompressed", "snappy", "gzip", "lz4", "zstd"
  #   - `sample_size`: Optional number of rows to sample for size estimation (defaults to 100)
  sig do
    params(
      read_from: T::Enumerator[T::Array[T.untyped]],
      schema: T::Array[T::Hash[String, String]],
      write_to: T.any(String, IO),
      batch_size: T.nilable(Integer),
      flush_threshold: T.nilable(Integer),
      compression: T.nilable(String),
      sample_size: T.nilable(Integer)
    ).void
  end
  def self.write_rows(
    read_from,
    schema:,
    write_to:,
    batch_size: nil,
    flush_threshold: nil,
    compression: nil,
    sample_size: nil
  )
  end

  # Options:
  #   - `read_from`: An Enumerator yielding arrays of column batches
  #   - `schema`: Array of hashes specifying column names and types. Supported types:
  #     - `int8`, `int16`, `int32`, `int64`
  #     - `uint8`, `uint16`, `uint32`, `uint64`
  #     - `float`, `double`
  #     - `string`
  #     - `binary`
  #     - `boolean`
  #     - `date32`
  #     - `timestamp_millis`, `timestamp_micros`
  #     - Looks like [{"column_name" => {"type" => "date32", "format" => "%Y-%m-%d"}}, {"column_name" => "int8"}]
  #   - `write_to`: String path or IO object to write the parquet file to
  #   - `flush_threshold`: Optional memory threshold in bytes before flushing (defaults to 64MB)
  #   - `compression`: Optional compression type to use (defaults to "zstd")
  #                   Supported values: "none", "uncompressed", "snappy", "gzip", "lz4", "zstd"
  sig do
    params(
      read_from: T::Enumerator[T::Array[T::Array[T.untyped]]],
      schema: T::Array[T::Hash[String, String]],
      write_to: T.any(String, IO),
      flush_threshold: T.nilable(Integer),
      compression: T.nilable(String)
    ).void
  end
  def self.write_columns(read_from, schema:, write_to:, flush_threshold: nil, compression: nil)
  end
end
