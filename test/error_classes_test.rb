# frozen_string_literal: true

# The gem raises its own errors as Parquet::Error, which is a RuntimeError, or
# as a subclass of it that says where the error arose. Each message is the one
# that the gem raised as a plain RuntimeError before these classes existed.
# Errors that Ruby has a class for keep it: ArgumentError, TypeError, IOError
# and EncodingError are raised as before.

require_relative "test_helper"
require "bigdecimal"
require "fileutils"
require "stringio"
require "tmpdir"

class ErrorClassesTest < Minitest::Test
  VALID = File.expand_path("data.parquet", __dir__)

  # One large_string (Arrow LargeUtf8) column, "name", holding "a", "b" and
  # "c", written by parquet-rs 58.3.0: valid Parquet whose values the gem
  # cannot convert to Ruby.
  LARGE_STRING = File.expand_path("large_string.parquet", __dir__)

  # A seekable IO whose read always fails.
  class FailingIO
    def initialize(bytes)
      @io = StringIO.new(bytes)
    end

    def read(*)
      raise IOError, "connection reset"
    end

    def seek(*args)
      @io.seek(*args)
    end

    def pos
      @io.pos
    end
  end

  def setup
    @dir = Dir.mktmpdir("parquet_error_classes_test")
  end

  def teardown
    FileUtils.remove_entry(@dir) if @dir && Dir.exist?(@dir)
  end

  def test_parquet_error_is_a_runtime_error_and_the_other_classes_are_parquet_errors
    assert_equal RuntimeError, Parquet::Error.superclass
    [Parquet::InvalidFileError, Parquet::UnsupportedError, Parquet::SchemaError, Parquet::ConversionError].each do |klass|
      assert_equal Parquet::Error, klass.superclass, klass.name
    end
  end

  def test_a_file_that_is_not_parquet_raises_invalid_file_error
    path = write_file("not_parquet.parquet", "id,name\n1,abc\n")

    from_rows = assert_raises(Parquet::InvalidFileError) { Parquet.each_row(path) { nil } }
    from_columns = assert_raises(Parquet::InvalidFileError) { Parquet.each_column(path) { nil } }
    from_metadata = assert_raises(Parquet::InvalidFileError) { Parquet.metadata(path) }

    assert_equal "Parquet error: Parquet error: Invalid Parquet file. Corrupt footer", from_rows.message
    assert_equal "Parquet error: Parquet error: Invalid Parquet file. Corrupt footer", from_columns.message
    assert_equal "Parquet error: Parquet error: Parquet error: Invalid Parquet file. Corrupt footer", from_metadata.message
  end

  def test_an_empty_file_raises_invalid_file_error
    path = write_file("empty.parquet", "")

    error = assert_raises(Parquet::InvalidFileError) { Parquet.each_row(path) { nil } }

    assert_equal "Parquet error: EOF: Parquet file too small. Size is 0 but need 8", error.message
  end

  def test_a_damaged_page_raises_invalid_file_error
    # Byte 7 of data.parquet is in the header of its first page.
    bytes = File.binread(VALID)
    bytes.setbyte(7, bytes.getbyte(7) ^ 0x01)
    path = write_file("damaged_page.parquet", bytes)

    error = assert_raises(Parquet::InvalidFileError) { Parquet.each_row(path) { nil } }

    assert_equal "Arrow error: Parquet argument error: EOF: Invalid page header", error.message
  end

  def test_repacking_a_file_that_is_not_parquet_raises_invalid_file_error
    path = write_file("not_parquet.parquet", "id,name\n1,abc\n")

    error = assert_raises(Parquet::InvalidFileError) { Parquet.repack([path], output_dir: File.join(@dir, "out")) }

    assert_equal(
      "Runtime error: failed to read Parquet metadata from #{path.inspect}: " \
      "Parquet error: Invalid Parquet file. Corrupt footer",
      error.message
    )
  end

  def test_a_type_that_the_gem_cannot_convert_raises_unsupported_error
    from_rows = assert_raises(Parquet::UnsupportedError) { Parquet.each_row(LARGE_STRING) { nil } }
    from_columns = assert_raises(Parquet::UnsupportedError) { Parquet.each_column(LARGE_STRING) { nil } }

    assert_equal "Conversion error: Unsupported data type for conversion: LargeUtf8", from_rows.message
    assert_equal "Conversion error: Unsupported data type for conversion: LargeUtf8", from_columns.message
  end

  def test_an_unknown_type_in_a_schema_raises_schema_error
    error =
      assert_raises(Parquet::SchemaError) do
        Parquet.write_rows([[1]].each, schema: [{ "id" => "not_a_type" }], write_to: output_path)
      end

    assert_equal "Invalid input: Unknown primitive type: not_a_type", error.message
  end

  def test_an_invalid_decimal_in_a_schema_raises_schema_error
    error =
      assert_raises(Parquet::SchemaError) do
        Parquet.write_columns([[[1]]].each, schema: [{ "amount" => "decimal(x,2)" }], write_to: output_path)
      end

    assert_equal "Parquet error: Schema error: Invalid decimal precision: x", error.message
  end

  def test_a_value_that_does_not_fit_its_column_raises_conversion_error
    error =
      assert_raises(Parquet::ConversionError) do
        Parquet.write_rows([["abc"]].each, schema: [{ "id" => "int32" }], write_to: output_path)
      end

    assert_equal "Conversion error: Failed to parse 'abc' as numeric: invalid digit found in string", error.message
  end

  def test_a_row_with_the_wrong_number_of_values_raises_conversion_error
    error =
      assert_raises(Parquet::ConversionError) do
        Parquet.write_rows([[1, 2]].each, schema: [{ "id" => "int32" }], write_to: output_path)
      end

    assert_equal "Row has 2 values but schema has 1 fields", error.message
  end

  def test_a_null_in_a_required_column_raises_conversion_error
    schema = { type: :struct, fields: [{ name: "id", type: :int32, nullable: false }] }

    error = assert_raises(Parquet::ConversionError) { Parquet.write_rows([[nil]].each, schema: schema, write_to: output_path) }

    assert_equal "Schema error: Found null value for non-nullable field at row[0]", error.message
  end

  def test_a_decimal_beyond_its_precision_raises_conversion_error
    schema = { type: :struct, fields: [{ name: "amount", type: :decimal, precision: 4, scale: 2 }] }

    error =
      assert_raises(Parquet::ConversionError) do
        Parquet.write_rows([[BigDecimal("12345.67")]].each, schema: schema, write_to: output_path)
      end

    assert_equal(
      "Schema error: Decimal precision overflow at row[0]: schema precision 4, value has 7 digits",
      error.message
    )
  end

  def test_a_column_batch_that_does_not_fit_the_schema_raises_conversion_error
    schema = [{ "id" => "int32" }, { "count" => "int32" }]

    columns = assert_raises(Parquet::ConversionError) { Parquet.write_columns([[[1]]].each, schema: schema, write_to: output_path) }
    values =
      assert_raises(Parquet::ConversionError) do
        Parquet.write_columns([[[1], [2, 3]]].each, schema: schema, write_to: output_path)
      end

    assert_equal "Batch 0 has 1 columns but schema has 2", columns.message
    assert_equal "batch 0 column 1 has 2 values but expected 1", values.message
  end

  def test_a_missing_file_raises_parquet_error
    error = assert_raises(Parquet::Error) { Parquet.each_row(File.join(@dir, "missing.parquet")) { nil } }

    assert_equal Parquet::Error, error.class
    assert_match(/\AIO error: /, error.message)
  end

  # An IO that fails says nothing about the file it was reading.
  def test_an_io_that_fails_while_the_footer_is_read_raises_parquet_error
    io = FailingIO.new(File.binread(VALID))

    from_rows = assert_raises(Parquet::Error) { Parquet.each_row(io) { nil } }
    from_metadata = assert_raises(Parquet::Error) { Parquet.metadata(io) }

    assert_equal Parquet::Error, from_rows.class
    assert_equal "Parquet error: External: connection reset", from_rows.message
    assert_equal Parquet::Error, from_metadata.class
    assert_equal "Parquet error: Parquet error: External: connection reset", from_metadata.message
  end

  def test_input_that_is_neither_a_path_nor_an_io_raises_parquet_error
    error = assert_raises(Parquet::Error) { Parquet.each_row(123) { nil } }

    assert_equal Parquet::Error, error.class
    assert_equal "Invalid input type: expected String or IO object with read method, got Integer", error.message
  end

  def test_errors_that_ruby_has_a_class_for_keep_it
    errors = [
      assert_raises(ArgumentError) { Parquet.each_row(VALID, result_type: :bogus) { nil } },
      assert_raises(TypeError) { Parquet.write_rows([1].each, schema: [{ "id" => "int32" }], write_to: output_path) },
      assert_raises(IOError) { Parquet.metadata(File.join(@dir, "missing.parquet")) },
      assert_raises(EncodingError) do
        invalid_utf8 = [0xC3, 0x28].pack("C*").force_encoding(Encoding::UTF_8)
        Parquet.write_rows([[invalid_utf8]].each, schema: [{ "name" => "string" }], write_to: output_path)
      end
    ]

    errors.each { |error| refute_kind_of Parquet::Error, error }
  end

  private

  def write_file(name, bytes)
    path = File.join(@dir, name)
    File.binwrite(path, bytes)
    path
  end

  def output_path
    File.join(@dir, "output.parquet")
  end
end
