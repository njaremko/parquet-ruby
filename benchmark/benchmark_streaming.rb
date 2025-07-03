#!/usr/bin/env ruby

require 'bundler/setup'
require 'parquet'
require 'tempfile'
require 'benchmark/memory' if Gem.loaded_specs['benchmark-memory']
require 'securerandom'
require 'benchmark'

# Create a test file
def create_test_file(size_mb: 50)
  file = Tempfile.new(['benchmark', '.parquet'])
  puts "Creating #{size_mb}MB test file..."

  # Each row is approximately 1KB
  rows_per_mb = 1024
  total_rows = size_mb * rows_per_mb

  schema = {
    fields: [
      {name: 'id', type: :int64},
      {name: 'uuid', type: :binary, format: 'uuid'},
      {name: 'name', type: :string},
      {name: 'description', type: :string},
      {name: 'value', type: :float64},
      {name: 'active', type: :boolean},
      {name: 'created_at', type: :timestamp_millis},
      {name: 'data', type: :string}
    ]
  }

  # Write in batches to avoid memory issues during creation
  batch_size = 10_000
  batches = (total_rows / batch_size.to_f).ceil

  data = []
  batches.times do |batch_num|
    batch_data = []
    batch_size.times do |i|
      row_id = batch_num * batch_size + i
      break if row_id >= total_rows

      batch_data << [
        row_id,
        SecureRandom.uuid,
        "User #{row_id}",
        "Description for user #{row_id} with some additional text",
        row_id * 1.5,
        row_id % 2 == 0,
        Time.now,
        "X" * 800  # Pad to make each row ~1KB
      ]
    end
    data.concat(batch_data)
  end

  Parquet.write_rows(data, schema: schema, write_to: file.path)

  file_size = File.size(file.path) / 1024.0 / 1024.0
  puts "Created test file: #{file.path} (#{file_size.round(2)}MB)"
  puts "Total rows: #{total_rows}"

  file
end

# Test streaming read performance
def benchmark_streaming_read(file_path)
  puts "\n=== Benchmarking Streaming Read ==="

  count = 0
  time = Benchmark.realtime do
    Parquet.each_row(file_path) do |row|
      count += 1
      # Process row without keeping in memory
      _ = row['id']
      _ = row['name']
    end
  end

  puts "Read #{count} rows in #{time.round(2)} seconds"
  puts "Rate: #{(count / time).round(0)} rows/second"

  # Memory usage tracking if benchmark-memory is available
  if defined?(Benchmark::Memory)
    puts "\nMemory usage:"
    Benchmark.memory do |x|
      x.report("streaming read") do
        Parquet.each_row(file_path) { |row| _ = row['id'] }
      end
    end
  end
end

# Test column batch reading
def benchmark_column_read(file_path)
  puts "\n=== Benchmarking Column Batch Read ==="

  batch_count = 0
  row_count = 0
  time = Benchmark.realtime do
    Parquet.each_column(file_path, batch_size: 5000) do |batch|
      batch_count += 1
      row_count += batch['id'].length
    end
  end

  puts "Read #{row_count} rows in #{batch_count} batches in #{time.round(2)} seconds"
  puts "Rate: #{(row_count / time).round(0)} rows/second"
end

# Test concurrent readers
def benchmark_concurrent_reads(file_path)
  puts "\n=== Benchmarking Concurrent Reads ==="

  thread_count = 4
  time = Benchmark.realtime do
    threads = thread_count.times.map do |thread_id|
      Thread.new do
        count = 0
        Parquet.each_row(file_path, columns: ['id', 'name', 'value']) do |row|
          count += 1
        end
        count
      end
    end

    total = threads.map(&:value).sum
    puts "#{thread_count} threads read #{total / thread_count} rows each"
  end

  puts "Total time: #{time.round(2)} seconds"
end

# Main benchmark
if __FILE__ == $0
  size_mb = (ARGV[0] || 50).to_i
  file = create_test_file(size_mb: size_mb)

  begin
    benchmark_streaming_read(file.path)
    benchmark_column_read(file.path)
    benchmark_concurrent_reads(file.path)

    puts "\n=== File Statistics ==="
    metadata = Parquet.metadata(file.path)
    puts "Row count: #{metadata[:num_rows]}"
    puts "Column count: #{metadata[:num_columns]}"
    puts "Row groups: #{metadata[:num_row_groups]}"
    puts "Created by: #{metadata[:created_by]}"
  ensure
    file.close
    file.unlink
  end
end
