require_relative "test_helper"

class WriteUnstagedIoTest < Minitest::Test
  ROW_SCHEMA = [{ "id" => "int64" }, { "name" => "string" }].freeze

  # Enough uncompressed rows for more than one row group with `flush_threshold: 1`,
  # since a row group is then 8 MiB.
  LARGE_ROW_COUNT = 200_000
  LARGE_OPTIONS = { compression: "none", flush_threshold: 1, stage_io: false }.freeze

  # Records every string written to it.
  class RecordingIO
    attr_reader :writes, :flushes

    def initialize
      @writes = []
      @flushes = 0
    end

    def write(data)
      @writes << data.b
      data.bytesize
    end

    def flush
      @flushes += 1
      self
    end

    def bytes
      @writes.join.b
    end

    def bytesize
      @writes.sum(&:bytesize)
    end
  end

  # Accepts at most 1,000 bytes from each call to `write`, as a non-blocking
  # writer can.
  class ShortWriteIO
    attr_reader :bytes

    def initialize
      @bytes = String.new(encoding: Encoding::BINARY)
    end

    def write(data)
      chunk = data.byteslice(0, 1_000)
      @bytes << chunk
      chunk.bytesize
    end
  end

  # Has no `flush`, and returns nil from `write`.
  class WriteOnlyIO
    attr_reader :bytes

    def initialize
      @bytes = String.new(encoding: Encoding::BINARY)
    end

    def write(data)
      @bytes << data
      nil
    end
  end

  class FailingIO
    class Failure < StandardError; end

    attr_reader :calls, :raised

    def initialize
      @calls = 0
    end

    def write(_data)
      @calls += 1
      @raised = Failure.new("disk full")
      raise @raised
    end
  end

  class ThrowingIO
    def write(_data)
      throw :stop_writing, :thrown
    end
  end

  def test_writes_the_same_bytes_as_staged_output
    staged = StringIO.new
    Parquet.write_rows(small_rows, schema: ROW_SCHEMA, write_to: staged)
    unstaged = RecordingIO.new

    Parquet.write_rows(small_rows, schema: ROW_SCHEMA, write_to: unstaged, stage_io: false)

    assert_equal staged.string.b, unstaged.bytes
    assert_equal 1, unstaged.flushes
  end

  def test_write_columns_writes_the_same_bytes_as_staged_output
    batches = [[[1, 2], ["one", "two"]], [[3], ["three"]]]
    staged = StringIO.new
    Parquet.write_columns(batches.each, schema: ROW_SCHEMA, write_to: staged)
    unstaged = RecordingIO.new

    Parquet.write_columns(batches.each, schema: ROW_SCHEMA, write_to: unstaged, stage_io: false)

    assert_equal staged.string.b, unstaged.bytes
  end

  def test_each_row_group_reaches_the_io_once_it_is_complete
    io = RecordingIO.new
    received_before_last_row = nil
    rows = Enumerator.new do |yielder|
      LARGE_ROW_COUNT.times { |index| yielder << large_row(index) }
      received_before_last_row = io.bytesize
    end

    Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: io, **LARGE_OPTIONS)

    # Every row group but the last is complete before the last row is written, so
    # the IO holds exactly those row groups: it ends where a later one starts.
    starts = row_group_starts(io.bytes)
    assert_operator starts.length, :>, 1
    assert_includes starts.drop(1), received_before_last_row
    assert_equal LARGE_ROW_COUNT, Parquet.each_row(StringIO.new(io.bytes)).count
  end

  def test_writes_a_file_that_spans_row_groups_to_a_pipe
    reader, writer = IO.pipe
    received = Thread.new { reader.binmode.read }
    rows = (0...LARGE_ROW_COUNT).lazy.map { |index| large_row(index) }

    Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: writer, **LARGE_OPTIONS)
    writer.close
    bytes = received.value

    assert_equal LARGE_ROW_COUNT, Parquet.each_row(StringIO.new(bytes)).count
    assert_equal large_row(LARGE_ROW_COUNT - 1), Parquet.each_row(StringIO.new(bytes), result_type: :array).to_a.last
  ensure
    reader&.close
    writer&.close unless writer&.closed?
  end

  def test_the_small_writes_of_a_footer_reach_the_io_together
    io = RecordingIO.new

    Parquet.write_rows([].each, schema: ROW_SCHEMA, write_to: io, stage_io: false)

    # One write each time the writer flushes the footer, rather than one for each
    # of its fields.
    assert_operator io.writes.length, :<=, 3
    assert_equal [], Parquet.each_row(StringIO.new(io.bytes)).to_a
  end

  def test_writes_the_rest_of_a_short_write
    staged = StringIO.new
    Parquet.write_rows(small_rows, schema: ROW_SCHEMA, write_to: staged)
    io = ShortWriteIO.new

    Parquet.write_rows(small_rows, schema: ROW_SCHEMA, write_to: io, stage_io: false)

    assert_equal staged.string.b, io.bytes
  end

  def test_writes_to_an_io_without_flush_whose_write_returns_nil
    staged = StringIO.new
    Parquet.write_rows(small_rows, schema: ROW_SCHEMA, write_to: staged)
    io = WriteOnlyIO.new

    Parquet.write_rows(small_rows, schema: ROW_SCHEMA, write_to: io, stage_io: false)

    assert_equal staged.string.b, io.bytes
  end

  def test_raises_the_error_of_the_io_unchanged_and_writes_no_more
    io = FailingIO.new

    error = assert_raises(FailingIO::Failure) do
      Parquet.write_rows(small_rows, schema: ROW_SCHEMA, write_to: io, stage_io: false)
    end

    assert_same io.raised, error
    assert_equal 1, io.calls
  end

  def test_stops_pulling_rows_once_the_io_fails
    io = FailingIO.new
    pulled = 0
    rows = Enumerator.new do |yielder|
      LARGE_ROW_COUNT.times do |index|
        pulled += 1
        yielder << large_row(index)
      end
    end

    assert_raises(FailingIO::Failure) do
      Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: io, **LARGE_OPTIONS)
    end

    assert_operator pulled, :<, LARGE_ROW_COUNT
    assert_equal 1, io.calls
  end

  def test_raises_the_error_of_the_io_unchanged_while_the_gc_runs_constantly
    io = FailingIO.new

    error = assert_raises(FailingIO::Failure) do
      GC.stress = true
      begin
        Parquet.write_rows([[1, "one"]].each, schema: ROW_SCHEMA, write_to: io, stage_io: false)
      ensure
        GC.stress = false
      end
    end

    assert_same io.raised, error
  end

  def test_raises_when_write_returns_a_negative_count
    io = Class.new do
      def write(_data)
        -1
      end
    end.new

    error = assert_raises(IOError) do
      Parquet.write_rows(small_rows, schema: ROW_SCHEMA, write_to: io, stage_io: false)
    end

    assert_match(/write returned -1/, error.message)
  end

  def test_a_throw_from_the_io_leaves_the_write
    result = catch(:stop_writing) do
      Parquet.write_rows(small_rows, schema: ROW_SCHEMA, write_to: ThrowingIO.new, stage_io: false)
      :not_thrown
    end

    assert_equal :thrown, result
  end

  def test_an_input_failure_before_the_first_row_group_writes_nothing
    io = RecordingIO.new
    rows = Enumerator.new do |yielder|
      yielder << [1, "one"]
      raise "input failure"
    end

    error = assert_raises(RuntimeError) do
      Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: io, stage_io: false)
    end

    assert_equal "input failure", error.message
    assert_empty io.writes
  end

  def test_stage_io_true_stages_io_output
    staged = StringIO.new
    Parquet.write_rows(small_rows, schema: ROW_SCHEMA, write_to: staged)
    io = StringIO.new

    Parquet.write_rows(small_rows, schema: ROW_SCHEMA, write_to: io, stage_io: true)

    assert_equal staged.string, io.string
  end

  def test_rejects_stage_io_false_for_a_path
    path = File.join(Dir.tmpdir, "parquet-ruby-unstaged-path-#{Process.pid}.parquet")

    error = assert_raises(ArgumentError) do
      Parquet.write_rows(small_rows, schema: ROW_SCHEMA, write_to: path, stage_io: false)
    end

    assert_match(/stage_io: false only applies to an IO object/, error.message)
    refute File.exist?(path), "a rejected write must not create #{path}"
  end

  def test_rejects_a_stage_io_that_is_not_a_boolean
    error = assert_raises(TypeError) do
      Parquet.write_rows(small_rows, schema: ROW_SCHEMA, write_to: StringIO.new, stage_io: "no")
    end

    assert_equal "stage_io must be true or false", error.message
  end

  def test_rejects_an_object_that_cannot_be_written_to
    error = assert_raises(TypeError) do
      Parquet.write_rows(small_rows, schema: ROW_SCHEMA, write_to: Object.new, stage_io: false)
    end

    assert_match(/responds to 'write'/, error.message)
  end

  private

  def small_rows
    [[1, "one"], [2, "two"], [3, nil]].each
  end

  def large_row(index)
    [index, format("%0100d", index)]
  end

  # Returns the offset of the first page of each row group of the file.
  def row_group_starts(bytes)
    Parquet.metadata(StringIO.new(bytes)).fetch("row_groups").map do |row_group|
      row_group.fetch("columns").flat_map do |column|
        [column["dictionary_page_offset"], column["data_page_offset"]].compact
      end.min
    end
  end
end
