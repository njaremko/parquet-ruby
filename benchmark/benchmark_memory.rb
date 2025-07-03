#!/usr/bin/env ruby

require 'bundler/setup'
require 'parquet'
require 'tempfile'
require 'benchmark'

# Memory tracking for macOS
class MemoryTracker
  def self.current_memory_mb
    # On macOS, use ps to get RSS (Resident Set Size)
    `ps -o rss= -p #{Process.pid}`.to_i / 1024.0
  end

  def self.track
    GC.start(full_mark: true, immediate_sweep: true)
    start_memory = current_memory_mb

    result = yield

    GC.start(full_mark: true, immediate_sweep: true)
    end_memory = current_memory_mb

    {
      result: result,
      memory_used_mb: end_memory - start_memory,
      start_memory_mb: start_memory,
      end_memory_mb: end_memory
    }
  end

  def self.monitor_continuously
    initial_memory = current_memory_mb
    samples = [initial_memory]
    stop_monitoring = false

    monitor_thread = Thread.new do
      until stop_monitoring
        samples << current_memory_mb
        sleep 0.05  # Sample every 50ms for better resolution
      end
    end

    # Ensure monitoring thread is running
    sleep 0.1

    result = yield

    # Get final sample
    samples << current_memory_mb
    stop_monitoring = true
    monitor_thread.join(0.1)

    # Calculate statistics
    min_memory = samples.min || initial_memory
    max_memory = samples.max || initial_memory

    {
      result: result,
      samples: samples,
      initial_mb: initial_memory,
      min_mb: min_memory,
      max_mb: max_memory,
      peak_growth_mb: max_memory - initial_memory,
      final_mb: samples.last || initial_memory
    }
  end
end

# Test scenarios
class MemoryBenchmarks
  def initialize
    @results = {}
  end

  def run_all
    puts "=== Parquet Memory Usage Benchmarks (macOS) ==="
    puts "Process ID: #{Process.pid}"
    puts "Initial memory: #{MemoryTracker.current_memory_mb.round(2)}MB\n\n"

    test_reading_constant_memory
    test_writing_constant_memory
    test_large_row_handling
    test_streaming_vs_batch

    print_summary
  end

  private

  def test_reading_constant_memory
    puts "1. Testing reading large files with constant memory"
    puts "-" * 50

    # Create files of increasing sizes
    [10, 50, 100, 200].each do |size_mb|
      file = create_test_file_mb(size_mb)

      begin
        # Monitor memory while reading
        stats = MemoryTracker.monitor_continuously do
          row_count = 0
          start_time = Time.now

          Parquet.each_row(file.path) do |row|
            row_count += 1
            # Minimal processing to simulate real usage
            _ = row['id']
            _ = row['data'].length if row_count % 1000 == 0
          end

          { row_count: row_count, duration: Time.now - start_time }
        end

        result = stats[:result]
        puts "File size: #{size_mb}MB"
        puts "  Rows read: #{result[:row_count]}"
        puts "  Time: #{result[:duration].round(2)}s"
        puts "  Memory: #{stats[:initial_mb].round(2)}MB → #{stats[:max_mb].round(2)}MB (growth: #{stats[:peak_growth_mb].round(2)}MB)"
        puts "  Samples taken: #{stats[:samples].length}"
        puts "  Rate: #{(result[:row_count] / result[:duration]).round(0)} rows/sec"

        @results["read_#{size_mb}mb"] = {
          size_mb: size_mb,
          peak_memory_growth_mb: stats[:peak_growth_mb],
          rows_per_second: result[:row_count] / result[:duration]
        }

        # Memory growth should be sub-linear with file size
        if size_mb >= 50
          memory_ratio = stats[:peak_growth_mb] / size_mb
          puts "  Memory/File ratio: #{memory_ratio.round(3)}"
          puts "  ✓ GOOD" if memory_ratio < 0.5
          puts "  ✗ BAD - Using too much memory!" if memory_ratio >= 0.5
        end
        puts
      ensure
        file.close
        file.unlink
      end
    end
  end

  def test_writing_constant_memory
    puts "\n2. Testing writing with constant memory"
    puts "-" * 50

    # Test writing files with different row sizes
    [1, 10, 100].each do |row_size_kb|
      row_data = "X" * (row_size_kb * 1024)
      row_count = 10_000
      batch_size = 100

      file = Tempfile.new(['write_test', '.parquet'])

      begin
        schema = {
          fields: [
            {name: 'id', type: :int64},
            {name: 'data', type: :string},
            {name: 'timestamp', type: :timestamp_millis}
          ]
        }

        # Monitor memory during write
        stats = MemoryTracker.monitor_continuously do
          start_time = Time.now

          # Write in batches
          data = []

          row_count.times do |i|
            data << [i, row_data, Time.now]

            if data.length >= batch_size
              if i < batch_size
                Parquet.write_rows(data, schema: schema, write_to: file.path)
              else
                # Simulating append (would need actual append API)
                # For now, just clear data to test memory
                data.clear
              end
            end
          end

          # Write final batch
          Parquet.write_rows(data, schema: schema, write_to: file.path) if data.any?

          { duration: Time.now - start_time }
        end

        file_size_mb = File.size(file.path) / 1024.0 / 1024.0

        puts "Row size: #{row_size_kb}KB x #{row_count} rows"
        puts "  File size: #{file_size_mb.round(2)}MB"
        puts "  Time: #{stats[:result][:duration].round(2)}s"
        puts "  Memory: #{stats[:initial_mb].round(2)}MB → #{stats[:max_mb].round(2)}MB (growth: #{stats[:peak_growth_mb].round(2)}MB)"
        puts "  Samples taken: #{stats[:samples].length}"

        @results["write_#{row_size_kb}kb_rows"] = {
          row_size_kb: row_size_kb,
          file_size_mb: file_size_mb,
          peak_memory_growth_mb: stats[:peak_growth_mb]
        }

        # Check if memory usage is reasonable
        expected_max_memory = [row_size_kb * batch_size / 1024.0 * 2, 50].max
        if stats[:peak_growth_mb] > expected_max_memory
          puts "  ✗ WARNING: Memory usage higher than expected!"
        else
          puts "  ✓ Memory usage within expected bounds"
        end
        puts
      ensure
        file.close
        file.unlink
      end
    end
  end

  def test_large_row_handling
    puts "\n3. Testing very large individual rows"
    puts "-" * 50

    # Test with increasingly large individual rows
    [1, 5, 10, 20].each do |row_mb|
      next if row_mb > 10 && !ENV['TEST_VERY_LARGE_ROWS']

      large_data = "Y" * (row_mb * 1024 * 1024)
      file = Tempfile.new(['large_row', '.parquet'])

      begin
        schema = {
          fields: [
            {name: 'id', type: :int32},
            {name: 'huge_data', type: :string}
          ]
        }

        # Write a few large rows
        write_stats = MemoryTracker.track do
          data = (0...10).map { |i| [i, large_data] }
          Parquet.write_rows(data, schema: schema, write_to: file.path)
        end

        # Read them back
        read_stats = MemoryTracker.monitor_continuously do
          count = 0
          Parquet.each_row(file.path) do |row|
            count += 1
            # Just check we can access the data
            assert row['huge_data'].length == large_data.length
          end
          count
        end

        puts "Row size: #{row_mb}MB per row (10 rows total)"
        puts "  Write memory: #{write_stats[:start_memory_mb].round(2)}MB → #{write_stats[:end_memory_mb].round(2)}MB (growth: #{write_stats[:memory_used_mb].round(2)}MB)"
        puts "  Read memory: #{read_stats[:initial_mb].round(2)}MB → #{read_stats[:max_mb].round(2)}MB (growth: #{read_stats[:peak_growth_mb].round(2)}MB)"
        puts "  Rows read: #{read_stats[:result]}"

        @results["large_row_#{row_mb}mb"] = {
          row_size_mb: row_mb,
          write_memory_mb: write_stats[:memory_used_mb],
          read_memory_mb: read_stats[:peak_growth_mb]
        }
        puts
      ensure
        file.close
        file.unlink
      end
    end
  end

  def test_streaming_vs_batch
    puts "\n4. Comparing streaming vs batch operations"
    puts "-" * 50

    # Create a medium-sized test file
    file = create_test_file_mb(50)

    begin
      # Test 1: Row-by-row streaming
      streaming_stats = MemoryTracker.monitor_continuously do
        count = 0
        start = Time.now
        Parquet.each_row(file.path) do |row|
          count += 1
        end
        { count: count, duration: Time.now - start }
      end

      # Test 2: Column batch reading
      batch_stats = MemoryTracker.monitor_continuously do
        count = 0
        start = Time.now
        Parquet.each_column(file.path, batch_size: 5000) do |batch|
          count += batch['id'].length
        end
        { count: count, duration: Time.now - start }
      end

      # Test 3: Reading with projection (fewer columns)
      projection_stats = MemoryTracker.monitor_continuously do
        count = 0
        start = Time.now
        Parquet.each_row(file.path, columns: ['id']) do |row|
          count += 1
        end
        { count: count, duration: Time.now - start }
      end

      puts "Streaming (all columns):"
      puts "  Memory: #{streaming_stats[:initial_mb].round(2)}MB → #{streaming_stats[:max_mb].round(2)}MB (growth: #{streaming_stats[:peak_growth_mb].round(2)}MB)"
      puts "  Time: #{streaming_stats[:result][:duration].round(2)}s"

      puts "\nColumn batches (5000 rows):"
      puts "  Memory: #{batch_stats[:initial_mb].round(2)}MB → #{batch_stats[:max_mb].round(2)}MB (growth: #{batch_stats[:peak_growth_mb].round(2)}MB)"
      puts "  Time: #{batch_stats[:result][:duration].round(2)}s"

      puts "\nStreaming (projection - 1 column):"
      puts "  Memory: #{projection_stats[:initial_mb].round(2)}MB → #{projection_stats[:max_mb].round(2)}MB (growth: #{projection_stats[:peak_growth_mb].round(2)}MB)"
      puts "  Time: #{projection_stats[:result][:duration].round(2)}s"

      # Calculate memory efficiency
      puts "\nMemory efficiency:"
      puts "  Batch vs Streaming: #{(batch_stats[:peak_growth_mb] / streaming_stats[:peak_growth_mb]).round(2)}x"
      puts "  Projection vs Full: #{(projection_stats[:peak_growth_mb] / streaming_stats[:peak_growth_mb]).round(2)}x"

    ensure
      file.close
      file.unlink
    end
  end

  def create_test_file_mb(size_mb)
    file = Tempfile.new(['test', '.parquet'])

    # Calculate rows to approximate the target file size
    # Each row is approximately 1KB when compressed
    estimated_rows_per_mb = 250  # Adjusted for Parquet compression
    total_rows = size_mb * estimated_rows_per_mb

    schema = {
      fields: [
        {name: 'id', type: :int64},
        {name: 'data', type: :string},
        {name: 'value', type: :float64},
        {name: 'active', type: :boolean},
        {name: 'description', type: :string}
      ]
    }

    # Generate all data at once for accurate file size
    data = (0...total_rows).map do |i|
      [
        i,
        "Data_#{i}_" + "X" * 1000,  # ~1KB of string data
        i * 1.5,
        i % 2 == 0,
        "Description for row #{i} with additional padding " * 5
      ]
    end

    # Write all data
    Parquet.write_rows(data, schema: schema, write_to: file.path)

    # Log actual file size
    actual_size_mb = File.size(file.path) / 1024.0 / 1024.0
    if (actual_size_mb - size_mb).abs > size_mb * 0.2
      puts "  Note: Target #{size_mb}MB, actual #{actual_size_mb.round(2)}MB"
    end

    file
  end

  def print_summary
    puts "\n" + "=" * 60
    puts "SUMMARY - Memory Efficiency Analysis"
    puts "=" * 60

    # Check if memory usage scales sub-linearly with file size
    read_results = @results.select { |k, _| k.start_with?('read_') }
    if read_results.any?
      puts "\nReading efficiency:"
      read_results.each do |key, data|
        efficiency = data[:peak_memory_growth_mb] / data[:size_mb]
        status = efficiency < 0.5 ? "✓ GOOD" : "✗ NEEDS OPTIMIZATION"
        puts "  #{data[:size_mb]}MB file: #{efficiency.round(3)} memory/file ratio - #{status}"
      end
    end

    # Check write memory usage
    write_results = @results.select { |k, _| k.start_with?('write_') }
    if write_results.any?
      puts "\nWriting efficiency:"
      write_results.each do |key, data|
        status = data[:peak_memory_growth_mb] < 100 ? "✓ GOOD" : "✗ HIGH"
        puts "  #{data[:row_size_kb]}KB rows: #{data[:peak_memory_growth_mb].round(2)}MB peak - #{status}"
      end
    end
  end

  def assert(condition)
    raise "Assertion failed" unless condition
  end
end

# Run benchmarks
if __FILE__ == $0
  benchmarks = MemoryBenchmarks.new
  benchmarks.run_all
end
