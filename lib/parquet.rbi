# typed: strict
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
      columns: T.nilable(T::Array[String]),
      blk: T.nilable(T.proc.params(row: T.any(T::Hash[String, T.untyped], T::Array[T.untyped])).void)
    ).returns(T.any(Enumerator, NilClass))
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
      batch_size: T.nilable(Integer),
      blk:
        T.nilable(T.proc.params(batch: T.any(T::Hash[String, T::Array[T.untyped]], T::Array[T::Array[T.untyped]])).void)
    ).returns(T.any(Enumerator, NilClass))
  end
  def self.each_column(input, result_type: nil, columns: nil, batch_size: nil, &blk)
  end
end
