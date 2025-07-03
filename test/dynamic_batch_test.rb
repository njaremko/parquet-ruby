require_relative 'test_helper'
require 'logger'
require 'stringio'

class DynamicBatchTest < Minitest::Test
  def setup
    @test_file = File.join(Dir.tmpdir, "test_batch_#{Process.pid}.parquet")
  end

  def teardown
    File.delete(@test_file) if File.exist?(@test_file)
  end

  def test_fixed_batch_size
    # Create logger to capture batch sizes
    log_output = StringIO.new
    logger = Logger.new(log_output)
    logger.level = Logger::DEBUG
    
    # Create data with 250 rows
    data = []
    250.times do |i|
      data << [i, "string #{i}"]
    end
    
    schema = {
      fields: [
        {name: 'id', type: :int32},
        {name: 'value', type: :string}
      ]
    }
    
    # Write with fixed batch size of 50
    Parquet.write_rows(data, schema: schema, write_to: @test_file, 
                      batch_size: 50, logger: logger)
    
    # Check that batches were written with fixed size
    log_content = log_output.string
    assert_match(/Writing batch of 50 rows/, log_content)
    # Should have 5 batches of 50
    assert_equal 5, log_content.scan(/Writing batch of 50 rows/).count
    
    # Verify data was written correctly
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }
    assert_equal 250, rows.length
  end
  
  def test_memory_threshold_batching
    # Create logger to capture batch sizes
    log_output = StringIO.new
    logger = Logger.new(log_output)
    logger.level = Logger::DEBUG
    
    # Create data with varying string sizes
    data = []
    100.times do |i|
      # Create larger strings to trigger memory-based flushing
      large_string = "x" * 10000  # ~10KB per string
      data << [i, large_string]
    end
    
    schema = {
      fields: [
        {name: 'id', type: :int32},
        {name: 'value', type: :string}
      ]
    }
    
    # Write with small memory threshold (1MB)
    Parquet.write_rows(data, schema: schema, write_to: @test_file,
                      flush_threshold: 1024 * 1024, logger: logger)
    
    # Check that batches were written
    log_content = log_output.string
    assert_match(/Writing batch of \d+ rows/, log_content)
    assert_match(/avg row size: \d+ bytes/, log_content)
    
    # Should have multiple batches due to memory threshold
    batch_count = log_content.scan(/Writing batch of \d+ rows/).count
    assert batch_count > 1, "Expected multiple batches due to memory threshold"
    
    # Verify data was written correctly
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }
    assert_equal 100, rows.length
  end
  
  def test_dynamic_batch_sizing_with_sample_size
    # Create logger to capture batch sizes
    log_output = StringIO.new
    logger = Logger.new(log_output)
    logger.level = Logger::DEBUG
    
    # Create data with varying sizes
    data = []
    # First 20 rows are small
    20.times do |i|
      data << [i, "small"]
    end
    # Next 80 rows are large
    80.times do |i|
      data << [i + 20, "x" * 5000]
    end
    
    schema = {
      fields: [
        {name: 'id', type: :int32},
        {name: 'value', type: :string}
      ]
    }
    
    # Write with sample size of 10 (should sample from small rows)
    Parquet.write_rows(data, schema: schema, write_to: @test_file,
                      sample_size: 10, flush_threshold: 512 * 1024, logger: logger)
    
    # Check the logs
    log_content = log_output.string
    assert_match(/Batch sizing:.*sample_size=10/, log_content)
    
    # Verify data was written correctly
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }
    assert_equal 100, rows.length
  end
  
  def test_no_batch_size_parameters_uses_defaults
    # Create logger to capture batch sizes
    log_output = StringIO.new
    logger = Logger.new(log_output)
    logger.level = Logger::DEBUG
    
    # Create data
    data = []
    1500.times do |i|
      data << [i, "string #{i}"]
    end
    
    schema = {
      fields: [
        {name: 'id', type: :int32},
        {name: 'value', type: :string}
      ]
    }
    
    # Write without any batch size parameters
    Parquet.write_rows(data, schema: schema, write_to: @test_file, logger: logger)
    
    # Check that default batch sizing was used
    log_content = log_output.string
    assert_match(/Batch sizing:.*memory_threshold=67108864/, log_content) # 64MB default
    assert_match(/Writing batch of \d+ rows/, log_content)
    
    # Verify data was written correctly
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }
    assert_equal 1500, rows.length
  end
  
  def test_batch_size_adjusts_based_on_row_size
    # Create logger to capture batch sizes
    log_output = StringIO.new
    logger = Logger.new(log_output)
    logger.level = Logger::DEBUG
    
    # Create data with mixed sizes
    data = []
    200.times do |i|
      if i < 50
        # Small rows
        data << [i, "small"]
      else
        # Larger rows
        data << [i, "x" * 1000]
      end
    end
    
    schema = {
      fields: [
        {name: 'id', type: :int32},
        {name: 'value', type: :string}
      ]
    }
    
    # Write with moderate memory threshold
    Parquet.write_rows(data, schema: schema, write_to: @test_file,
                      flush_threshold: 100_000, sample_size: 30, logger: logger)
    
    # Extract batch sizes from logs
    log_content = log_output.string
    batch_sizes = log_content.scan(/Writing batch of (\d+) rows/).map { |m| m[0].to_i }
    
    # Should have multiple batches
    assert batch_sizes.length > 1, "Expected multiple batches"
    
    # Batch sizes should vary based on row sizes
    # Initial batches (small rows) should be larger
    # Later batches (large rows) should be smaller
    assert batch_sizes.any? { |size| size != batch_sizes.first }, 
           "Expected varying batch sizes based on row content"
    
    # Verify data was written correctly
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }
    assert_equal 200, rows.length
  end
end