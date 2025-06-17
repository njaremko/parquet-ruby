# frozen_string_literal: true
require "tempfile"

require "parquet"
require "minitest/autorun"

class ErrorTest < Minitest::Test
  def test_each_column_empty_file
    File.write("test/empty.parquet", "")
    assert_raises(RuntimeError) { Parquet.each_column("test/empty.parquet") { |_| } }
  ensure
    File.delete("test/empty.parquet")
  end

  def test_each_column_nonexistent_file
    assert_raises(RuntimeError) { Parquet.each_column("test/nonexistent.parquet") { |_| } }
  end

  def test_each_column_invalid_result_type
    assert_raises(RuntimeError) { Parquet.each_column("test/data.parquet", result_type: :invalid) { |_| } }
  end

  def test_strict_utf8_enforcement
    invalid_utf8_bytes = [0xC3, 0x28].pack("C*") # Example of an invalid sequence

    temp_path = "test/strict_utf8_enforcement.parquet"
    schema = [{ "payload" => "string" }]

    begin
      error =
        assert_raises(EncodingError) do
          Parquet.write_rows([[invalid_utf8_bytes]].each, schema: schema, write_to: temp_path)
        end

      assert_match(/invalid utf-8 sequence/i, error.message)
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_reading_non_parquet_file
    non_parquet_path = "test/non_parquet.txt"
    File.write(non_parquet_path, "Just some text data.")

    error = assert_raises(RuntimeError) { Parquet.each_row(non_parquet_path).to_a }

    assert_match(/Failed to open file|Invalid Parquet|Unknown file format/, error.message)
  ensure
    File.delete(non_parquet_path) if File.exist?(non_parquet_path)
  end

  def test_corrupted_parquet_file
    temp_path = "test/a_data.parquet"
    corrupted_path = "test/corrupted_data.parquet"

    begin
      # Create a simple parquet file first
      schema = [{ "id" => "int32" }, { "name" => "string" }]
      Parquet.write_rows([[1, "test"]].each, schema: schema, write_to: temp_path)

      original_data = File.binread(temp_path)
      # Truncate the file halfway to simulate corruption
      File.binwrite(corrupted_path, original_data[0, original_data.size / 2])

      error = assert_raises(RuntimeError) { Parquet.each_row(corrupted_path).to_a }

      assert_match(/Failed to open file|EOF|Parquet error/, error.message)
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
      File.delete(corrupted_path) if File.exist?(corrupted_path)
    end
  end

  def test_mismatched_schema_write_rows
    temp_path = "test/mismatched_schema_rows.parquet"
    schema = [{ "id" => "int64" }, { "name" => "string" }]

    # Our data enumerator incorrectly yields an array of length 3
    data = [
      [1, "Alice", "ExtraColumn"] # 3 columns, but schema has only 2
    ]

    begin
      error = assert_raises(RuntimeError) { Parquet.write_rows(data.each, schema: schema, write_to: temp_path) }
      assert_match(/Row length|schema length|mismatch/, error.message)
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_enumerator_interrupt_partial_write
    temp_path = "test/partial_write.parquet"
    schema = [{ "id" => "int64" }, { "name" => "string" }]

    enumerator =
      Enumerator.new do |yielder|
        yielder << [1, "Alice"]
        yielder << [2, "Bob"]
        raise "Simulated stream failure"
      end

    begin
      error = assert_raises(RuntimeError) { Parquet.write_rows(enumerator, schema: schema, write_to: temp_path) }
      assert_equal("Simulated stream failure", error.message)

      # The file may exist but might be partially written or truncated.
      assert File.exist?(temp_path)

      # Optionally: attempt to read what is there, or confirm it's not valid.
      # We'll just check that it doesn't catastrophically break reading API:
      assert_raises(RuntimeError) { Parquet.each_row(temp_path).to_a }
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_incorrect_coercion_int8_too_large
    schema = [{ "small_col" => "int8" }]
    data = [
      [9999] # too large for int8
    ]

    temp_path = "test_int8_too_large.parquet"
    begin
      error =
        assert_raises(RuntimeError, RangeError) { Parquet.write_rows(data.each, schema: schema, write_to: temp_path) }
      # depending on environment, might say "fixnum too big to convert" or "out of range"
      assert_match(/fixnum too big to convert into/i, error.message)
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end
end
