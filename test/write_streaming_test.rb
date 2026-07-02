require_relative 'test_helper'
require 'tempfile'
require 'stringio'

# Regression tests for streaming writes: enumerator inputs must not be
# materialized up front, and completed row groups must reach the destination
# while the input is still being enumerated (bounded by flush_threshold).
class WriteStreamingTest < Minitest::Test
  def setup
    @test_file = File.join(Dir.tmpdir, "test_write_streaming_#{Process.pid}.parquet")
  end

  def teardown
    File.delete(@test_file) if File.exist?(@test_file)
  end

  def schema
    {
      fields: [
        { name: 'id', type: :int64 },
        { name: 'payload', type: :string, nullable: true }
      ]
    }
  end

  def payload_for(i, bytes)
    "#{i}-#{'x' * bytes}"
  end

  def test_write_rows_enumerator_flushes_to_destination_during_enumeration
    n_rows = 2_000
    payload_bytes = 10_000
    file_sizes = []

    rows = Enumerator.new do |y|
      n_rows.times do |i|
        if (i % 500).zero?
          file_sizes << (File.exist?(@test_file) ? File.size(@test_file) : -1)
        end
        y << [i, payload_for(i, payload_bytes)]
      end
    end

    # 2000 rows x ~10KB = ~20MB raw staged bytes against a 512KB threshold:
    # row groups must be flushed to the file long before enumeration ends.
    Parquet.write_rows(rows, schema: schema, write_to: @test_file,
                       batch_size: 100, flush_threshold: 512 * 1024,
                       compression: "zstd")

    assert file_sizes.last > 0,
           "expected destination file to grow during enumeration, sizes: #{file_sizes.inspect}"

    rows_read = Parquet.each_row(@test_file).to_a
    assert_equal n_rows, rows_read.length
    assert_equal 0, rows_read.first['id']
    assert_equal payload_for(1999, payload_bytes), rows_read.last['payload']
  end

  %w[none snappy gzip zstd].each do |compression|
    define_method("test_write_rows_enumerator_multi_row_group_round_trip_#{compression}") do
      n_rows = 3_000
      rows = Enumerator.new do |y|
        n_rows.times do |i|
          y << [i, (i % 7).zero? ? nil : payload_for(i, 1_000)]
        end
      end

      Parquet.write_rows(rows, schema: schema, write_to: @test_file,
                         batch_size: 200, flush_threshold: 256 * 1024,
                         compression: compression)

      row_groups = Parquet.metadata(@test_file)["row_groups"]
      assert row_groups.length > 1,
             "expected multiple row groups, got #{row_groups.length}"

      count = 0
      Parquet.each_row(@test_file) do |row|
        assert_equal count, row['id']
        if (count % 7).zero?
          assert_nil row['payload']
        else
          assert_equal payload_for(count, 1_000), row['payload']
        end
        count += 1
      end
      assert_equal n_rows, count
    end
  end

  def test_write_columns_enumerator_flushes_to_destination_during_enumeration
    n_batches = 20
    batch_rows = 200
    payload_bytes = 10_000
    file_sizes = []

    batches = Enumerator.new do |y|
      n_batches.times do |b|
        file_sizes << (File.exist?(@test_file) ? File.size(@test_file) : -1)
        ids = Array.new(batch_rows) { |i| b * batch_rows + i }
        payloads = ids.map { |id| payload_for(id, payload_bytes) }
        y << [ids, payloads]
      end
    end

    Parquet.write_columns(batches, schema: schema, write_to: @test_file,
                          flush_threshold: 512 * 1024, compression: "zstd")

    assert file_sizes.last > 0,
           "expected destination file to grow during enumeration, sizes: #{file_sizes.inspect}"

    rows_read = Parquet.each_row(@test_file).to_a
    assert_equal n_batches * batch_rows, rows_read.length
    rows_read.each_with_index do |row, i|
      assert_equal i, row['id']
    end
    assert_equal payload_for(3999, payload_bytes), rows_read.last['payload']
  end

  def test_write_columns_enumerator_round_trips_with_nils
    batches = Enumerator.new do |y|
      3.times do |b|
        ids = Array.new(50) { |i| b * 50 + i }
        payloads = ids.map { |id| (id % 5).zero? ? nil : "p#{id}" }
        y << [ids, payloads]
      end
    end

    Parquet.write_columns(batches, schema: schema, write_to: @test_file)

    rows_read = Parquet.each_row(@test_file).to_a
    assert_equal 150, rows_read.length
    rows_read.each_with_index do |row, i|
      assert_equal i, row['id']
      if (i % 5).zero?
        assert_nil row['payload']
      else
        assert_equal "p#{i}", row['payload']
      end
    end
  end

  def test_write_columns_empty_enumerator_with_schema
    batches = Enumerator.new { |_y| }

    Parquet.write_columns(batches, schema: schema, write_to: @test_file)

    assert File.exist?(@test_file)
    assert_equal [], Parquet.each_row(@test_file).to_a
  end

  def test_write_columns_enumerator_infers_schema_from_first_batch
    batches = Enumerator.new do |y|
      y << [%w[a b], %w[x y]]
      y << [%w[c d], %w[z w]]
    end

    Parquet.write_columns(batches, schema: nil, write_to: @test_file)

    rows_read = Parquet.each_row(@test_file).to_a
    assert_equal 4, rows_read.length
    assert_equal "a", rows_read.first['f0']
    assert_equal "w", rows_read.last['f1']
  end

  def test_write_rows_rejects_zero_flush_threshold
    error = assert_raises(ArgumentError) do
      Parquet.write_rows([[1, "a"]], schema: schema, write_to: @test_file,
                         flush_threshold: 0)
    end
    assert_match(/flush_threshold must be positive/, error.message)
  end

  def test_write_rows_empty_enumerator_with_schema
    rows = Enumerator.new { |_y| }

    Parquet.write_rows(rows, schema: schema, write_to: @test_file)

    assert File.exist?(@test_file)
    assert_equal [], Parquet.each_row(@test_file).to_a
  end

  def test_write_rows_enumerator_infers_schema_from_first_slice
    rows = Enumerator.new do |y|
      100.times { |i| y << ["name#{i}", "value#{i}"] }
    end

    Parquet.write_rows(rows, schema: nil, write_to: @test_file)

    rows_read = Parquet.each_row(@test_file).to_a
    assert_equal 100, rows_read.length
    assert_equal "name0", rows_read.first['f0']
    assert_equal "value99", rows_read.last['f1']
  end

  def test_write_rows_lazy_enumerator
    rows = (0...500).lazy.map { |i| [i, "payload#{i}"] }

    Parquet.write_rows(rows, schema: schema, write_to: @test_file, batch_size: 50)

    rows_read = Parquet.each_row(@test_file).to_a
    assert_equal 500, rows_read.length
    assert_equal "payload499", rows_read.last['payload']
  end

  def test_write_rows_object_with_only_to_a_still_works
    to_a_only = Object.new
    def to_a_only.to_a = (0...10).map { |i| [i, "p#{i}"] }

    Parquet.write_rows(to_a_only, schema: schema, write_to: @test_file)

    rows_read = Parquet.each_row(@test_file).to_a
    assert_equal 10, rows_read.length
    assert_equal "p9", rows_read.last['payload']
  end

  def test_write_rows_enumerator_to_io_destination
    rows = Enumerator.new do |y|
      1_000.times { |i| y << [i, payload_for(i, 1_000)] }
    end

    io = StringIO.new
    io.binmode
    Parquet.write_rows(rows, schema: schema, write_to: io,
                       batch_size: 100, flush_threshold: 64 * 1024)
    io.rewind

    rows_read = Parquet.each_row(io).to_a
    assert_equal 1_000, rows_read.length
    assert_equal payload_for(999, 1_000), rows_read.last['payload']
  end

  def test_write_rows_enumerator_error_propagates
    rows = Enumerator.new do |y|
      y << [0, "ok"]
      raise ArgumentError, "boom mid-stream"
    end

    error = assert_raises(ArgumentError) do
      Parquet.write_rows(rows, schema: schema, write_to: @test_file, batch_size: 1)
    end
    assert_match(/boom mid-stream/, error.message)
  end
end
