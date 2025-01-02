# typed: strict

module Parquet
  # Options:
  #   - `input`: String specifying the input file
  #   - `result_type`: String specifying the output format
  #                    ("hash" or "array" or :hash or :array)
  sig do
    params(
      input: T.any(String, IO),
      result_type: T.nilable(T.any(String, Symbol)),
      blk: T.nilable(T.proc.params(row: T.any(T::Hash[String, T.untyped], T::Array[T.untyped])).void)
    ).returns(T.any(Enumerator, T.untyped))
  end
  def self.each_row(input, result_type: nil, &blk)
  end
end
