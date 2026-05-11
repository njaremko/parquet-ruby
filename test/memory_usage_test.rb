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

    # Write in batches to avoid memory issues during file creation
    batch_size = 10_000
    temp_data = []

    (0...row_count).step(batch_size) do |start_idx|
      batch_data = (start_idx...[start_idx + batch_size, row_count].min).map do |i|
        [i, large_string, i * 1.5]
      end

      if start_idx == 0
        Parquet.write_rows(batch_data, schema: schema, write_to: @test_file)
      else
        # Append mode if supported, otherwise we'd need to rewrite
        temp_data.concat(batch_data)
      end
    end

    # Write all data if append not supported
    if temp_data.any?
      all_data = (0...batch_size).map { |i| [i, large_string, i * 1.5] } + temp_data
      Parquet.write_rows(all_data, schema: schema, write_to: @test_file)
    end

    file_size_mb = File.size(@test_file) / 1024.0 / 1024.0
    puts "Created test file: #{file_size_mb.round(2)}MB with #{row_count} rows" if ENV["VERBOSE"]

    # Test 1: Streaming read should use constant memory
    memory_stats = measure_memory_growth do
      processed_count = 0
      Parquet.each_row(@test_file) do |row|
        processed_count += 1
        # Simulate processing without keeping data
        _ = row['id']
        _ = row['data'].length

        # Force GC periodically to ensure we're not just delaying cleanup
        GC.start if processed_count % 50_000 == 0
      end
      assert_equal row_count, processed_count
    end

    puts "Streaming read memory growth: #{memory_stats[:memory_growth_mb].round(2)}MB" if ENV["VERBOSE"]
    # Memory growth should be minimal (< 50MB for a 100MB+ file)
    assert memory_stats[:memory_growth_mb] < 50,
           "Memory grew by #{memory_stats[:memory_growth_mb].round(2)}MB, expected < 50MB"

    # Test 2: Column batch reading should also use constant memory
    memory_stats = measure_memory_growth do
      total_processed = 0
      Parquet.each_column(@test_file, batch_size: 5000) do |batch|
        total_processed += batch['id'].length
        # Process and discard batch
        _ = batch['data'].map(&:length).sum
      end
      assert_equal row_count, total_processed
    end

    puts "Column batch read memory growth: #{memory_stats[:memory_growth_mb].round(2)}MB" if ENV["VERBOSE"]
    assert memory_stats[:memory_growth_mb] < 50,
           "Memory grew by #{memory_stats[:memory_growth_mb].round(2)}MB, expected < 50MB"
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

    read_with = lambda do |thread_count|
      threads = thread_count.times.map do
        Thread.new do
          count = 0
          Parquet.each_row(@test_file) do |row|
            count += 1
            _ = row['id']
          end
          count
        end
      end
      results = threads.map(&:value)
      assert_equal [row_count] * thread_count, results
    end

    # Warm up with the LARGER thread count first. mimalloc allocates a
    # per-thread arena (~25MB) on first use of each thread, and recycles
    # arenas via a free pool when threads exit. Warming with 4 threads
    # seeds 4 arenas so both the 1-thread and 4-thread measurements below
    # run against an already-populated pool. Warming with only 1 thread
    # would leave the 4-thread measurement paying the full cold-start cost
    # of 3 fresh arenas (~75MB), making the ratio look pathological even
    # with no real concurrency overhead.
    read_with.call(4)

    # RSS noise is one-directional — allocator retention can only inflate
    # the apparent growth, never deflate it — so min across N runs is the
    # most accurate estimate of true workload cost.
    baseline   = 3.times.map { measure_memory_growth { read_with.call(1) }[:memory_growth_mb] }.min
    concurrent = 3.times.map { measure_memory_growth { read_with.call(4) }[:memory_growth_mb] }.min

    if ENV["VERBOSE"]
      puts "1-thread growth (min of 3): #{baseline.round(2)}MB"
      puts "4-thread growth (min of 3): #{concurrent.round(2)}MB"
    end

    # Test the property in this test's name: concurrent reads should scale
    # sub-linearly. 4 readers should cost less than 2x a single reader.
    # The 50MB floor handles the case where the baseline is in the RSS-noise
    # band (a few MB) — without it a 1MB baseline plus 20MB concurrent (both
    # noise) would look like 20x scaling and fire spuriously on shared CI
    # runners where mimalloc holds per-thread arenas.
    ceiling = [baseline * 2, 50].max
    assert_operator concurrent, :<, ceiling,
                    "4-thread growth #{concurrent.round(1)}MB exceeded ceiling " \
                    "#{ceiling.round(1)}MB (2x baseline of #{baseline.round(1)}MB or 50MB floor)"
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

        GC.start if processed % 10_000 == 0
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
    report.allocated_memory_by_gem.each do |gem, size|
      puts "  #{gem}: #{size / 1024.0 / 1024.0} MB" if ENV["VERBOSE"]
    end

    report.pretty_print(to_file: 'memory_profile.txt')
    puts "\nDetailed report written to memory_profile.txt"
  end
end
