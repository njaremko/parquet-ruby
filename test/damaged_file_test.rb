# frozen_string_literal: true

# parquet-rs and arrow-rs panic on some damaged files instead of returning an
# error. Reading such a file raises a RuntimeError that carries the panic
# message, instead of Ruby's `fatal`, which no `rescue` catches: on the main
# thread it ends the process, and on any other thread it ends that thread.
#
# negative_dictionary_page_offset.parquet holds one int64 column, "id", with the
# rows 1, 2 and 3, written by this gem without compression. One byte of its
# footer is changed so that the dictionary_page_offset of the column chunk is
# -5 instead of 4, which makes parquet-rs 58.3.0 panic when it reads the chunk.
#
# The default panic hook still prints each panic to stderr, so these tests
# capture it to keep the test output readable.

require_relative "test_helper"
require "open3"
require "rbconfig"

class DamagedFileTest < Minitest::Test
  DAMAGED = File.expand_path("negative_dictionary_page_offset.parquet", __dir__)
  VALID = File.expand_path("data.parquet", __dir__)
  MESSAGE = "Failed to read Parquet file (reader panicked): column start and length should not be negative"

  def test_the_damaged_footer_holds_a_negative_page_offset
    column = Parquet.metadata(DAMAGED)["row_groups"].first["columns"].first

    assert_equal(-5, column["dictionary_page_offset"])
  end

  def test_each_row_raises_runtime_error
    error = assert_read_panic { Parquet.each_row(DAMAGED) { flunk "no row can be read" } }

    assert_equal MESSAGE, error.message
  end

  def test_each_row_with_columns_raises_runtime_error
    error = assert_read_panic { Parquet.each_row(DAMAGED, columns: ["id"]) { flunk "no row can be read" } }

    assert_equal MESSAGE, error.message
  end

  def test_each_row_from_an_io_raises_runtime_error
    error =
      File.open(DAMAGED, "rb") do |io|
        assert_read_panic { Parquet.each_row(io) { flunk "no row can be read" } }
      end

    assert_equal MESSAGE, error.message
  end

  def test_row_enumerator_raises_runtime_error
    enumerator = Parquet.each_row(DAMAGED)

    assert_equal MESSAGE, assert_read_panic { enumerator.to_a }.message
    assert_equal MESSAGE, assert_read_panic { enumerator.next }.message
  end

  def test_each_column_raises_runtime_error
    error = assert_read_panic { Parquet.each_column(DAMAGED) { flunk "no batch can be read" } }

    assert_equal MESSAGE, error.message
  end

  def test_another_thread_raises_runtime_error
    error =
      assert_read_panic do
        Thread.new do
          Thread.current.report_on_exception = false
          Parquet.each_row(DAMAGED) { flunk "no row can be read" }
        end.join
      end

    assert_equal MESSAGE, error.message
  end

  def test_the_process_keeps_running
    script = <<~RUBY
      require "parquet"
      begin
        Parquet.each_row(ARGV.fetch(0)) { nil }
      rescue RuntimeError => e
        puts "rescued: \#{e.message}"
      end
      puts "still running"
    RUBY

    stdout, _stderr, status =
      Open3.capture3(RbConfig.ruby, "-I", File.expand_path("../lib", __dir__), "-e", script, DAMAGED)

    assert_equal "rescued: #{MESSAGE}\nstill running\n", stdout
    assert status.success?, "the process exited with #{status.inspect}"
  end

  def test_an_exception_raised_by_the_block_is_raised_unchanged
    raised = Class.new(StandardError).new("raised by the block")

    from_rows = assert_raises(StandardError) { Parquet.each_row(VALID) { raise raised } }
    from_columns = assert_raises(StandardError) { Parquet.each_column(VALID) { raise raised } }

    assert_same raised, from_rows
    assert_same raised, from_columns
  end

  def test_break_and_throw_from_the_block_leave_the_read
    assert_equal :broke, Parquet.each_row(VALID) { break :broke }
    assert_equal :broke, Parquet.each_column(VALID) { break :broke }
    assert_equal :thrown, catch(:done) { Parquet.each_row(VALID) { throw :done, :thrown } }
    assert_equal :thrown, catch(:done) { Parquet.each_column(VALID) { throw :done, :thrown } }
  end

  private

  # Returns the RuntimeError that the block raises, with the panic that the
  # default panic hook prints to stderr captured.
  def assert_read_panic(&block)
    error = nil
    capture_subprocess_io { error = assert_raises(RuntimeError, &block) }
    error
  end
end
