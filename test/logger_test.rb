require_relative 'test_helper'
require 'logger'
require 'stringio'

class LoggerTest < Minitest::Test
  def setup
    @test_file = File.join(Dir.tmpdir, "test_logger_#{Process.pid}.parquet")
  end

  def teardown
    File.delete(@test_file) if File.exist?(@test_file)
  end

  def test_logger_in_read_operations
    # Create test data
    data = []
    100.times do |i|
      data << [i, "string #{i}"]
    end
    
    schema = {
      fields: [
        {name: 'id', type: :int32},
        {name: 'value', type: :string}
      ]
    }
    
    # Write test file
    Parquet.write_rows(data, schema: schema, write_to: @test_file)
    
    # Create a logger that captures to StringIO
    log_output = StringIO.new
    logger = Logger.new(log_output)
    logger.level = Logger::DEBUG
    
    # Read with logger
    rows = []
    Parquet.each_row(@test_file, logger: logger) do |row|
      rows << row
    end
    
    assert_equal 100, rows.length
    
    # Check that logger was called
    log_content = log_output.string
    assert_match(/Starting to read parquet file/, log_content)
    assert_match(/Processing 2 columns/, log_content)
    assert_match(/Finished processing 100 rows/, log_content)
  end
  
  def test_logger_in_write_operations
    # Create a logger that captures to StringIO
    log_output = StringIO.new
    logger = Logger.new(log_output)
    logger.level = Logger::DEBUG
    
    # Test data - use 2100 to ensure a final partial batch
    data = []
    2100.times do |i|
      data << [i, "string #{i}"]
    end
    
    schema = {
      fields: [
        {name: 'id', type: :int32},
        {name: 'value', type: :string}
      ]
    }
    
    # Write with logger
    Parquet.write_rows(data, schema: schema, write_to: @test_file, logger: logger)
    
    # Check that logger was called
    log_content = log_output.string
    assert_match(/Starting to write parquet file/, log_content)
    assert_match(/Writing batch of \d+ rows/, log_content)
    assert_match(/Finished writing 2100 rows to parquet file/, log_content)
  end
  
  def test_logger_validation
    # Test that logger must respond to required methods
    invalid_logger = Object.new
    
    assert_raises(ArgumentError) do
      Parquet.each_row(@test_file, logger: invalid_logger) { |row| row }
    end
  end
  
  def test_logger_in_column_operations
    # Create test data
    data = []
    1000.times do |i|
      data << [i, "string #{i}"]
    end
    
    schema = {
      fields: [
        {name: 'id', type: :int32},
        {name: 'value', type: :string}
      ]
    }
    
    # Write test file
    Parquet.write_rows(data, schema: schema, write_to: @test_file)
    
    # Create a logger that captures to StringIO
    log_output = StringIO.new
    logger = Logger.new(log_output)
    logger.level = Logger::DEBUG
    
    # Read columns with logger
    batches = []
    Parquet.each_column(@test_file, batch_size: 100, logger: logger) do |batch|
      batches << batch
    end
    
    # Check that logger was called
    log_content = log_output.string
    assert_match(/Starting to read parquet file columns/, log_content)
    assert_match(/Processed batch/, log_content)
    assert_match(/Finished processing \d+ batches/, log_content)
  end
end