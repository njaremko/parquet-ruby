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
  #   - `string_storage`: How string *values* become Ruby strings (default `:copy`). Hash keys
  #                       (struct field names and top-level column names) are always interned and
  #                       reused regardless of this setting.
  #                       - `:copy` allocates a fresh mutable String per value.
  #                       - `:intern` deduplicates low-cardinality equal values into frozen interned
  #                         Strings up to a bounded per-read cache, then falls back to frozen copies.
  #                         A transient copy still happens per value, so it is not a per-value speedup.
  #                       - `:shared` returns frozen, zero-copy strings backed by Rust memory for
  #                         short, repeated, low-cardinality values. Each read returns at most the
  #                         configured number of shared values and only values up to the configured
  #                         byte size; values past those bounds become frozen copies. New process-wide
  #                         leaks are also capped by the requested budget and hard process ceilings.
  #                         All `:shared` results are frozen. Not recommended for high-cardinality or
  #                         large-blob string columns.
  #                       Pass a hash to set the `:shared` leak budget, e.g.
  #                       `{ mode: :shared, max_entries: 16_384, max_value_bytes: 1024 }`.
  sig do
    params(
      input: T.any(String, File, StringIO, IO),
      result_type: T.nilable(T.any(String, Symbol)),
      columns: T.nilable(T::Array[String]),
      strict: T.nilable(T::Boolean),
      string_storage: T.nilable(T.any(String, Symbol, T::Hash[Symbol, T.untyped]))
    ).returns(T::Enumerator[T.any(T::Hash[String, T.untyped], T::Array[T.untyped])])
  end
  sig do
    params(
      input: T.any(String, File, StringIO, IO),
      result_type: T.nilable(T.any(String, Symbol)),
      columns: T.nilable(T::Array[String]),
      strict: T.nilable(T::Boolean),
      string_storage: T.nilable(T.any(String, Symbol, T::Hash[Symbol, T.untyped])),
      blk: T.nilable(T.proc.params(row: T.any(T::Hash[String, T.untyped], T::Array[T.untyped])).void)
    ).returns(NilClass)
  end
  def self.each_row(input, result_type: nil, columns: nil, strict: nil, string_storage: nil, &blk)
  end

  # Options:
  #   - `input`: String, File, or IO object containing parquet data
  #   - `result_type`: String specifying the output format
  #                    ("hash" or "array" or :hash or :array)
  #   - `columns`: When present, only the specified columns will be included in the output.
  #   - `batch_size`: When present, specifies the number of rows per batch
  #   - `string_storage`: How string values become Ruby strings (`:copy` (default), `:intern`,
  #                       or `:shared`). See `each_row` for the semantics of each mode.
  sig do
    params(
      input: T.any(String, File, StringIO, IO),
      result_type: T.nilable(T.any(String, Symbol)),
      columns: T.nilable(T::Array[String]),
      batch_size: T.nilable(Integer),
      strict: T.nilable(T::Boolean),
      string_storage: T.nilable(T.any(String, Symbol, T::Hash[Symbol, T.untyped]))
    ).returns(T::Enumerator[T.any(T::Hash[String, T.untyped], T::Array[T.untyped])])
  end
  sig do
    params(
      input: T.any(String, File, StringIO, IO),
      result_type: T.nilable(T.any(String, Symbol)),
      columns: T.nilable(T::Array[String]),
      batch_size: T.nilable(Integer),
      strict: T.nilable(T::Boolean),
      string_storage: T.nilable(T.any(String, Symbol, T::Hash[Symbol, T.untyped])),
      blk:
        T.nilable(T.proc.params(batch: T.any(T::Hash[String, T::Array[T.untyped]], T::Array[T::Array[T.untyped]])).void)
    ).returns(NilClass)
  end
  def self.each_column(input, result_type: nil, columns: nil, batch_size: nil, strict: nil, string_storage: nil, &blk)
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
  #   - `batch_size`: Optional positive batch size for writing (defaults to 1000, at most 1_000_000
  #                   for one-column schemas; wide schemas may have a lower safety cap)
  #   - `flush_threshold`: Optional threshold in bytes for the writer's in-progress (encoded)
  #                        buffer before a row group is flushed (defaults to 100MB)
  #   - `compression`: Optional compression type to use (defaults to "zstd")
  #                   Supported values: "none", "uncompressed", "snappy", "gzip", "lz4", "zstd"
  #   - `sample_size`: Optional positive number of rows to sample for size estimation
  #                    (defaults to 100, at most 10_000)
  #   - `string_cache`: Deduplicate repeated string values while writing. `false` (default)
  #                     disables it, `true` enables it with a default capacity, and an Integer
  #                     enables it with that many retained distinct strings (at most 65_536).
  #                     Retention also skips values larger than 4KB and stops after 16MB of
  #                     cached string content.
  sig do
    params(
      read_from: T::Enumerator[T::Array[T.untyped]],
      schema: T::Array[T::Hash[String, String]],
      write_to: T.any(String, IO),
      batch_size: T.nilable(Integer),
      flush_threshold: T.nilable(Integer),
      compression: T.nilable(String),
      sample_size: T.nilable(Integer),
      string_cache: T.nilable(T.any(T::Boolean, Integer))
    ).void
  end
  def self.write_rows(
    read_from,
    schema:,
    write_to:,
    batch_size: nil,
    flush_threshold: nil,
    compression: nil,
    sample_size: nil,
    string_cache: nil
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
  #   - `flush_threshold`: Optional threshold in bytes for the writer's in-progress (encoded)
  #                        buffer before a row group is flushed (defaults to 100MB)
  #   - `compression`: Optional compression type to use (defaults to "zstd")
  #                   Supported values: "none", "uncompressed", "snappy", "gzip", "lz4", "zstd"
  #   - `logger`: Optional Ruby logger for column-write progress messages
  sig do
    params(
      read_from: T::Enumerator[T::Array[T::Array[T.untyped]]],
      schema: T::Array[T::Hash[String, String]],
      write_to: T.any(String, IO),
      flush_threshold: T.nilable(Integer),
      compression: T.nilable(String),
      logger: T.nilable(T.untyped)
    ).void
  end
  def self.write_columns(
    read_from,
    schema:,
    write_to:,
    flush_threshold: nil,
    compression: nil,
    logger: nil
  )
  end

  # Options:
  #   - `read_from`: String path or array of paths to Parquet files with matching schemas
  #   - `output_file_prefix`: File name prefix for outputs, default "batch"
  #   - `output_dir`: Directory where {output_file_prefix}-{n}.parquet files will be written
  #   - `rows_per_file`: Optional maximum number of rows per output file. When nil, all input rows are concatenated into one file.
  #   - `max_read_rows_per_chunk`: Optional maximum number of rows to read per chunk, default 8192
  #   - `compression`: Optional compression type to use, default "zstd"
  sig do
    params(
      read_from: T.any(String, T::Array[String]),
      output_file_prefix: T.nilable(String),
      output_dir: String,
      rows_per_file: T.nilable(Integer),
      max_read_rows_per_chunk: T.nilable(Integer),
      compression: T.nilable(String)
    ).returns(T::Array[T::Hash[String, T.any(String, Integer)]])
  end
  def self.repack(
    read_from,
    output_file_prefix: nil,
    output_dir:,
    rows_per_file: nil,
    max_read_rows_per_chunk: nil,
    compression: nil
  )
  end
end
