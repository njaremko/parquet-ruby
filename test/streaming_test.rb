require_relative 'test_helper'
require 'tempfile'
require 'stringio'

class StreamingTest < Minitest::Test
  def setup
    @test_file = File.join(Dir.tmpdir, "test_streaming_#{Process.pid}.parquet")
  end

  def teardown
    File.delete(@test_file) if File.exist?(@test_file)
  end

  def test_file_streaming_read
    # Create a larger test file with multiple batches
    data = (1..10000).map do |i|
      ["Row #{i}", i, i * 1.5, "Description for row #{i}"]
    end

    schema = {
      fields: [
        {name: 'name', type: :string},
        {name: 'id', type: :int32},
        {name: 'value', type: :float64},
        {name: 'description', type: :string}
      ]
    }

    # Write data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)

    # Read using streaming (file path)
    row_count = 0
    Parquet.each_row(@test_file) do |row|
      row_count += 1
      # Verify first few rows
      if row_count <= 3
        assert_equal "Row #{row_count}", row['name']
        assert_equal row_count, row['id']
      end
    end
    assert_equal 10000, row_count

    # Test column streaming
    batch_count = 0
    total_rows = 0
    Parquet.each_column(@test_file, batch_size: 1000) do |batch|
      batch_count += 1
      total_rows += batch['name'].length
    end
    assert batch_count > 1, "Should have multiple batches"
    assert_equal 10000, total_rows
  end

  def test_io_streaming_read
    # Create test data
    data = (1..1000).map do |i|
      ["Item #{i}", i, i % 2 == 0]
    end

    schema = {
      fields: [
        {name: 'name', type: :string},
        {name: 'id', type: :int32},
        {name: 'active', type: :boolean}
      ]
    }

    # Write to StringIO
    io = StringIO.new
    io.binmode
    Parquet.write_rows(data, schema: schema, write_to: io)
    io.rewind

    # Read from StringIO using streaming
    rows = []
    Parquet.each_row(io) { |row| rows << row }
    assert_equal 1000, rows.length
    assert_equal "Item 1", rows[0]['name']
    assert_equal "Item 1000", rows[999]['name']

    # Test with seek
    io.rewind
    columns = []
    Parquet.each_column(io, batch_size: 250) { |batch| columns << batch }
    assert_equal 4, columns.length  # 1000 rows / 250 batch size
  end

  def test_large_file_memory_efficiency
    # This test creates a file that would be problematic to load entirely into memory
    # but should work fine with streaming
    skip "Large file test - enable for performance testing" unless ENV['RUN_LARGE_TESTS']

    # Create a 100MB+ file
    large_data = (1..1_000_000).map do |i|
      ["X" * 1000, i, "Y" * 5000]  # Each row ~150 bytes
    end

    schema = {
      fields: [
        {name: 'data1', type: :string},
        {name: 'id', type: :int64},
        {name: 'data2', type: :string}
      ]
    }

    # Write large file
    Parquet.write_rows(large_data, schema: schema, write_to: @test_file)
    file_size = File.size(@test_file)
    puts "Created test file of size: #{file_size / 1024 / 1024}MB"

    # Read with streaming - should not consume excessive memory
    count = 0
    Parquet.each_row(@test_file) do |row|
      count += 1
      # Process row without keeping in memory
    end
    assert_equal 1_000_000, count
  end

  def test_concurrent_readers
    # Test that multiple readers can work on the same file
    data = (1..100).map { |i| [i, "Item #{i}"] }
    schema = {
      fields: [
        {name: 'id', type: :int32},
        {name: 'name', type: :string}
      ]
    }

    Parquet.write_rows(data, schema: schema, write_to: @test_file)

    # Create multiple concurrent readers
    results = []
    threads = 3.times.map do |thread_id|
      Thread.new do
        thread_results = []
        Parquet.each_row(@test_file) do |row|
          thread_results << [thread_id, row['id']]
        end
        thread_results
      end
    end

    # Collect results
    threads.each { |t| results.concat(t.value) }

    # Each thread should have read all 100 rows
    threads_data = results.group_by { |thread_id, _| thread_id }
    assert_equal 3, threads_data.keys.length
    threads_data.each do |thread_id, data|
      ids = data.map { |_, id| id }.sort
      assert_equal (1..100).to_a, ids, "Thread #{thread_id} should read all rows"
    end
  end

  def test_streaming_with_projection
    # Test that column projection works with streaming
    data = (1..1000).map do |i|
      [i, "Name #{i}", "Email #{i}", "Phone #{i}", "Address #{i}"]
    end

    schema = {
      fields: [
        {name: 'id', type: :int32},
        {name: 'name', type: :string},
        {name: 'email', type: :string},
        {name: 'phone', type: :string},
        {name: 'address', type: :string}
      ]
    }

    Parquet.write_rows(data, schema: schema, write_to: @test_file)

    # Read only specific columns
    rows = []
    Parquet.each_row(@test_file, columns: ['id', 'name']) do |row|
      rows << row
      # Verify only requested columns are present
      assert_equal ['id', 'name'].sort, row.keys.sort
    end
    assert_equal 1000, rows.length
    assert_equal 1, rows[0]['id']
    assert_equal "Name 1", rows[0]['name']
  end

  def test_enumerator_streaming
    # Test that enumerators work with streaming
    data = (1..100).map { |i| [i, i * 2] }
    schema = {
      fields: [
        {name: 'x', type: :int32},
        {name: 'y', type: :int32}
      ]
    }

    Parquet.write_rows(data, schema: schema, write_to: @test_file)

    # Use enumerator methods with streaming
    enum = Parquet.each_row(@test_file)

    # Take first 10
    first_10 = enum.take(10)
    assert_equal 10, first_10.length
    assert_equal 1, first_10[0]['x']
    assert_equal 20, first_10[9]['y']

    # Filter with lazy evaluation
    evens = Parquet.each_row(@test_file).select { |row| row['x'] % 2 == 0 }
    assert_equal 50, evens.length
    assert_equal 2, evens[0]['x']
  end
end
