require_relative "test_helper"
require_relative "io_objects"

# IO output that is staged on disk, the default, and copied to the IO once the
# file is complete.
class WriteStagedIoTest < Minitest::Test
  include IoObjects

  ROW_SCHEMA = [{ "id" => "int64" }, { "name" => "string" }].freeze

  def test_raises_the_error_of_the_io_unchanged_and_writes_no_more
    io = FailingIO.new

    error = assert_raises(FailingIO::Failure) do
      Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: io)
    end

    assert_same io.raised, error
    assert_equal 1, io.calls
  end

  def test_a_throw_from_the_io_leaves_the_write
    result = catch(:stop_writing) do
      Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: ThrowingIO.new)
      :not_thrown
    end

    assert_equal :thrown, result
  end

  def test_writes_to_an_io_without_flush_whose_write_returns_nil
    io = WriteOnlyIO.new

    Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: io)

    assert_equal expected_bytes, io.bytes
  end

  def test_writes_the_rest_of_a_short_write
    io = ShortWriteIO.new

    Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: io)

    assert_equal expected_bytes, io.bytes
  end

  def test_flushes_the_io_once_the_file_is_written
    io = RecordingIO.new

    Parquet.write_columns([[[1, 2, 3], ["one", "two", nil]]].each, schema: ROW_SCHEMA, write_to: io)

    assert_equal expected_bytes, io.bytes
    assert_equal 1, io.flushes
  end

  def test_rejects_an_object_that_cannot_be_written_to_before_reading_any_rows
    pulled = 0
    input = Enumerator.new do |yielder|
      pulled += 1
      yielder << [1, "one"]
    end

    error = assert_raises(TypeError) do
      Parquet.write_rows(input, schema: ROW_SCHEMA, write_to: Object.new)
    end

    assert_match(/responds to 'write'/, error.message)
    assert_equal 0, pulled
  end

  private

  def rows
    [[1, "one"], [2, "two"], [3, nil]].each
  end

  # The file that the rows make, as StringIO receives it.
  def expected_bytes
    io = StringIO.new
    Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: io)
    io.string.b
  end
end
