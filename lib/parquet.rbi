# typed: strict
module Parquet
  # Options:
  #   - `input`: String specifying the input file
  #   - `result_type`: String specifying the output format
  #                    ("hash" or "array" or :hash or :array)
  #   - `columns`: When present, only the specified columns will be included in the output.
  #                This is useful for reducing how much data is read and improving performance.
  sig do
    params(
      input: T.any(String, IO),
      result_type: T.nilable(T.any(String, Symbol)),
      columns: T.nilable(T::Array[String]),
      blk: T.nilable(T.proc.params(row: T.any(T::Hash[String, T.untyped], T::Array[T.untyped])).void)
    ).returns(T.any(Enumerator, T.untyped))
  end
  def self.each_row(input, result_type: nil, columns: nil, &blk)
  end
end
