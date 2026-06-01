require_relative 'test_helper'
require 'tempfile'
require 'objspace'

class MemoryUsageTest < Minitest::Test
  def setup
    skip unless ENV["RUN_SLOW_TESTS"]
    @test_file = File.join(Dir.tmpdir, "test_memory_#{Process.pid}.parquet")
    # Enable object allocation tracing
    ObjectSpace.trace_object_allocations_start
  end

  def teardown
    File.delete(@test_file) if File.exist?(@test_file)
    ObjectSpace.trace_object_allocations_stop
  end

  # Helper to get current memory usage in MB
  def current_memory_mb
    if RUBY_PLATFORM =~ /darwin|linux/
      # Use RSS (Resident Set Size) for actual memory usage
      `ps -o rss= -p #{Process.pid}`.to_i / 1024.0
    else
      # Fallback to Ruby's reported memory
      GC.stat[:heap_allocated_pages] * GC::INTERNAL_CONSTANTS[:HEAP_PAGE_SIZE] / 1024.0 / 1024.0
    end
  end

  # Helper to measure memory growth during a block
  def measure_memory_growth
    GC.start(full_mark: true, immediate_sweep: true)
    initial_memory = current_memory_mb
    initial_objects = ObjectSpace.count_objects[:TOTAL]

    yield

    GC.start(full_mark: true, immediate_sweep: true)
    final_memory = current_memory_mb
    final_objects = ObjectSpace.count_objects[:TOTAL]

    {
      memory_growth_mb: final_memory - initial_memory,
      object_growth: final_objects - initial_objects,
      initial_memory_mb: initial_memory,
      final_memory_mb: final_memory
    }
  end

  def test_reading_large_file_constant_memory
    # Create a large file (100MB+) with many rows
    row_count = 500_000
    large_string = "X" * 200  # Each row ~200 bytes

    schema = {
      fields: [
        {name: 'id', type: :int64},
        {name: 'data', type: :string},
        {name: 'value', type: :float64}
      ]
    }

    # Write once, then drop the source rows so they don't sit in the heap while
    # we measure read-side memory.
    data = (0...row_count).map { |i| [i, large_string, i * 1.5] }
    Parquet.write_rows(data, schema: schema, write_to: @test_file)
    data = nil
    GC.start(full_mark: true, immediate_sweep: true)

    file_size_mb = File.size(@test_file) / 1024.0 / 1024.0
    puts "Created test file: #{file_size_mb.round(2)}MB with #{row_count} rows" if ENV["VERBOSE"]

    # Constant memory means: reading the whole file must not retain live Ruby
    # objects proportional to the row count (i.e. the reader streams instead of
    # materializing the file). We assert on retained object growth, which is the
    # invariant that actually matters and is stable across platforms. RSS is
    # reported for humans but not asserted: a streaming read still triggers
    # large transient allocation, and the allocator (jemalloc) keeps freed
    # arenas resident, so an RSS delta measures allocator policy, not retained
    # state.
    object_budget = row_count / 10

    # Test 1: Streaming row read.
    stats = measure_memory_growth do
      processed_count = 0
      Parquet.each_row(@test_file) do |row|
        processed_count += 1
        _ = row['id']
        _ = row['data'].length
        GC.start if processed_count % 50_000 == 0
      end
      assert_equal row_count, processed_count
    end
    puts "Streaming read: #{stats[:object_growth]} objects retained, #{stats[:memory_growth_mb].round(2)}MB RSS" if ENV["VERBOSE"]
    assert_operator stats[:object_growth], :<, object_budget,
                    "Streaming read retained #{stats[:object_growth]} objects for #{row_count} rows; expected bounded (streaming) memory"

    # Test 2: Column batch reading should also stream.
    stats = measure_memory_growth do
      total_processed = 0
      Parquet.each_column(@test_file, batch_size: 5000) do |batch|
        total_processed += batch['id'].length
        _ = batch['data'].map(&:length).sum
      end
      assert_equal row_count, total_processed
    end
    puts "Column batch read: #{stats[:object_growth]} objects retained, #{stats[:memory_growth_mb].round(2)}MB RSS" if ENV["VERBOSE"]
    assert_operator stats[:object_growth], :<, object_budget,
                    "Column batch read retained #{stats[:object_growth]} objects for #{row_count} rows; expected bounded (streaming) memory"
  end

  def test_writing_large_rows_constant_memory
    # Test writing very large individual rows
    row_count = 10_000
    large_row_size = 10_000  # 10KB per row
    large_string = "Y" * large_row_size

    schema = {
      fields: [
        {name: 'id', type: :int64},
        {name: 'large_data', type: :string},
        {name: 'binary_data', type: :binary},
        {name: 'timestamp', type: :timestamp_millis}
      ]
    }

    # Test streaming write (if supported by API)
    memory_stats = measure_memory_growth do
      # Write rows one at a time to simulate streaming
      data = []
      row_count.times do |i|
        row = [
          i,
          large_string,
          large_string.b,  # Use binary encoding instead of bytes array
          Time.now
        ]
        data << row

        # Write in small batches to simulate streaming
        if data.length >= 100
          if i < 100
            Parquet.write_rows(data, schema: schema, write_to: @test_file)
          else
            # Would need append functionality here
            # For now, we'll test memory of accumulating data
          end
          data.clear
          GC.start if i % 1000 == 0
        end
      end

      # Write any remaining data
      if data.any?
        # This simulates the final write
        Parquet.write_rows(data, schema: schema, write_to: @test_file)
      end
    end

    file_size_mb = File.size(@test_file) / 1024.0 / 1024.0
    puts "Written file size: #{file_size_mb.round(2)}MB" if ENV["VERBOSE"]
    puts "Writing memory growth: #{memory_stats[:memory_growth_mb].round(2)}MB" if ENV["VERBOSE"]

    # Memory growth should be reasonable even with large rows
    assert memory_stats[:memory_growth_mb] < 100,
           "Memory grew by #{memory_stats[:memory_growth_mb].round(2)}MB, expected < 100MB"
  end

  def test_concurrent_reading_memory_efficiency
    # Create a moderately large file
    row_count = 100_000
    data = (0...row_count).map do |i|
      [i, "Name #{i}", "Description #{i}", i * 2.5]
    end

    schema = {
      fields: [
        {name: 'id', type: :int64},
        {name: 'name', type: :string},
        {name: 'description', type: :string},
        {name: 'value', type: :float64}
      ]
    }

    Parquet.write_rows(data, schema: schema, write_to: @test_file)
    # Drop the source rows so they don't sit in the heap while we measure the
    # read side.
    data = nil
    GC.start(full_mark: true, immediate_sweep: true)

    thread_count = 4

    # Concurrent readers must not multiply retained memory: every thread streams
    # the whole file, so the live Ruby objects left after the read must stay
    # bounded (the streaming window) rather than scale with
    # row_count * thread_count. We assert on retained object growth, which is the
    # invariant that actually matters and is stable across platforms. RSS is
    # reported for humans but not asserted: each thread triggers large transient
    # allocation, and the allocator (jemalloc) keeps freed arenas resident, so an
    # RSS delta measures allocator policy (it is routinely negative here)
    # rather than retained state.
    object_budget = row_count / 10

    memory_stats = measure_memory_growth do
      threads = thread_count.times.map do
        Thread.new do
          count = 0
          Parquet.each_row(@test_file, columns: ['id']) do |row|
            count += 1
            # Minimal processing
            _ = row['id']
          end
          count
        end
      end

      results = threads.map(&:value)
      assert_equal [row_count] * thread_count, results
    end

    puts "Concurrent reading: #{memory_stats[:object_growth]} objects retained, #{memory_stats[:memory_growth_mb].round(2)}MB RSS" if ENV["VERBOSE"]
    # Retained memory must not scale with thread count.
    assert_operator memory_stats[:object_growth], :<, object_budget,
                    "Concurrent read with #{thread_count} threads retained #{memory_stats[:object_growth]} objects for #{row_count} rows each; expected bounded (streaming) memory that does not scale with thread count"
  end

  def test_row_reader_reuses_hash_keys
    require 'memory_profiler' rescue skip "memory_profiler gem not available"

    row_count = 10_000
    data = (0...row_count).map do |i|
      [i, i + 1, i + 2, i * 2.5]
    end

    schema = {
      fields: [
        {name: 'id', type: :int64},
        {name: 'count', type: :int64},
        {name: 'other', type: :int64},
        {name: 'value', type: :float64}
      ]
    }

    Parquet.write_rows(data, schema: schema, write_to: @test_file)

    # The reader interns struct field names, so reading N rows allocates a
    # bounded number of key strings (one per distinct field) rather than a fresh
    # set per row. Measure only the reader's own allocations: a `row['id']`
    # lookup inside the block would allocate the test's non-frozen "id" literal
    # on every iteration (Ruby elides this via the opt_aref_with bytecode only
    # on some versions), which says nothing about whether the reader reuses keys.
    report = MemoryProfiler.report do
      Parquet.each_row(@test_file) { |_row| }
    end

    string_allocations = report
      .allocated_objects_by_class
      .find { |allocation| allocation[:data] == "String" }
      &.fetch(:count, 0) || 0

    assert string_allocations < row_count,
           "Expected hash keys to be reused, but the reader allocated #{string_allocations} strings for #{row_count} rows"

    # Stronger, Ruby-version-independent check: every row must share the exact
    # same frozen key objects, not merely equal ones.
    reference_keys = nil
    Parquet.each_row(@test_file) do |row|
      if reference_keys.nil?
        reference_keys = row.keys
        reference_keys.each { |key| assert_predicate key, :frozen? }
      else
        row.keys.each_with_index { |key, i| assert_same reference_keys[i], key }
      end
    end
  end

  def test_memory_with_complex_types
    # Test with nested types and large arrays
    row_count = 50_000

    schema = {
      fields: [
        {name: 'id', type: :int64},
        {name: 'tags', type: :list, item: {type: :string}},
        {name: 'metadata', type: :map, key: {type: :string}, value: {type: :string}},
        {name: 'scores', type: :list, item: {type: :float64}}
      ]
    }

    # Generate data with complex types
    data = (0...row_count).map do |i|
      [
        i,
        Array.new(20) { |j| "tag_#{i}_#{j}" },  # 20 tags per row
        (0...10).map { |j| ["key_#{j}", "value_#{i}_#{j}"] }.to_h,  # 10 key-value pairs
        Array.new(50) { rand }  # 50 float values
      ]
    end

    # Write the complex data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)

    file_size_mb = File.size(@test_file) / 1024.0 / 1024.0
    puts "Complex types file size: #{file_size_mb.round(2)}MB" if ENV["VERBOSE"]

    # Test reading complex types with constant memory
    memory_stats = measure_memory_growth do
      processed = 0
      Parquet.each_row(@test_file) do |row|
        processed += 1
        # Access complex fields
        _ = row['tags'].length
        _ = row['metadata'].size
        _ = row['scores'].sum

        GC.start if processed % 5_000 == 0
      end
      assert_equal row_count, processed
    end

    puts "Complex types reading memory growth: #{memory_stats[:memory_growth_mb].round(2)}MB" if ENV["VERBOSE"]
    assert memory_stats[:memory_growth_mb] < 75,
           "Memory grew by #{memory_stats[:memory_growth_mb].round(2)}MB, expected < 75MB"
  end

  def test_memory_profile_output
    # This test generates a detailed memory profile
    require 'memory_profiler' rescue skip "memory_profiler gem not available"

    # Create test data with very large rows (1MB each) and 1000 rows
    row_count = 1_000
    schema = {
      fields: [
        {name: 'id', type: :int64},
        {name: 'data', type: :string}
      ]
    }

    # Each string is 1MB: 1_048_576 bytes
    one_mb_string = "X" * 1_048_576
    data = (0...row_count).map { |i| [i, one_mb_string] }
    Parquet.write_rows(data, schema: schema, write_to: @test_file)

    # Profile reading
    report = MemoryProfiler.report do
      Parquet.each_row(@test_file) do |row|
        _ = row['id']
        _ = row['data']
      end
    end

    puts "\n=== Memory Profile Report ===" if ENV["VERBOSE"]
    puts "Total allocated: #{report.total_allocated_memsize / 1024.0 / 1024.0} MB" if ENV["VERBOSE"]
    puts "Total retained: #{report.total_retained_memsize / 1024.0 / 1024.0} MB" if ENV["VERBOSE"]

    # Show top allocations
    puts "\nTop allocations by gem:" if ENV["VERBOSE"]
    report.allocated_memory_by_gem.each do |allocation|
      puts "  #{allocation[:data]}: #{allocation[:count] / 1024.0 / 1024.0} MB" if ENV["VERBOSE"]
    end

    report.pretty_print(to_file: 'memory_profile.txt')
    puts "\nDetailed report written to memory_profile.txt"
  end
end
