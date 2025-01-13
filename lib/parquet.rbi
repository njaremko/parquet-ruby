# typed: true

module Parquet
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
      columns: T.nilable(T::Array[String])
    ).returns(T::Enumerator[T.any(T::Hash[String, T.untyped], T::Array[T.untyped])])
  end
  sig do
    params(
      input: T.any(String, File, StringIO, IO),
      result_type: T.nilable(T.any(String, Symbol)),
      columns: T.nilable(T::Array[String]),
      blk: T.nilable(T.proc.params(row: T.any(T::Hash[String, T.untyped], T::Array[T.untyped])).void)
    ).returns(NilClass)
  end
  def self.each_row(input, result_type: nil, columns: nil, &blk)
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
      batch_size: T.nilable(Integer)
    ).returns(T::Enumerator[T.any(T::Hash[String, T.untyped], T::Array[T.untyped])])
  end
  sig do
    params(
      input: T.any(String, File, StringIO, IO),
      result_type: T.nilable(T.any(String, Symbol)),
      columns: T.nilable(T::Array[String]),
      batch_size: T.nilable(Integer),
      blk:
        T.nilable(T.proc.params(batch: T.any(T::Hash[String, T::Array[T.untyped]], T::Array[T::Array[T.untyped]])).void)
    ).returns(NilClass)
  end
  def self.each_column(input, result_type: nil, columns: nil, batch_size: nil, &blk)
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
  sig do
    params(
      read_from: T::Enumerator[T::Array[T.untyped]],
      schema: T::Array[T::Hash[String, String]],
      write_to: T.any(String, IO),
      batch_size: T.nilable(Integer)
    ).void
  end
  def self.write_rows(read_from, schema:, write_to:, batch_size: nil)
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
  sig do
    params(
      read_from: T::Enumerator[T::Array[T::Array[T.untyped]]],
      schema: T::Array[T::Hash[String, String]],
      write_to: T.any(String, IO)
    ).void
  end
  def self.write_columns(read_from, schema:, write_to:)
  end
end
