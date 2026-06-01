require_relative 'test_helper'
require 'logger'
require 'stringio'

class StringCacheTest < Minitest::Test
  def setup
    @test_file = File.join(Dir.tmpdir, "test_string_cache_#{Process.pid}.parquet")
  end

  def teardown
    File.delete(@test_file) if File.exist?(@test_file)
  end

  def test_string_cache_with_repeated_values
    # Create logger to capture cache statistics
    log_output = StringIO.new
    logger = Logger.new(log_output)
    logger.level = Logger::INFO
    
    # Create data with many repeated string values
    data = []
    categories = ["apple", "banana", "cherry", "date", "elderberry"]
    500.times do |i|
      # Use modulo to repeat categories
      category = categories[i % categories.length]
      data << [i, category, "user_#{i % 10}"]
    end
    
    schema = {
      fields: [
        {name: 'id', type: :int32},
        {name: 'category', type: :string},
        {name: 'user', type: :string}
      ]
    }
    
    # Write with string cache enabled
    Parquet.write_rows(data, schema: schema, write_to: @test_file, 
                      string_cache: true, logger: logger)
    
    # Check that string cache was used
    log_content = log_output.string
    assert_match(/String cache stats:/, log_content)
    assert_match(/cache misses.*hits.*hit rate/, log_content)
    match = log_content.match(/String cache stats: (\d+) cache misses, (\d+) hits \((\d+\.\d)% hit rate\)/)
    assert match, "Could not parse string cache statistics from log"
    # ~1000 writes of a small set of distinct values: nearly all are cache hits.
    assert_operator match[2].to_i, :>=, 980
    
    # Verify data was written correctly
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }
    assert_equal 500, rows.length
    assert_equal "apple", rows[0]["category"]
    assert_equal "user_0", rows[0]["user"]
  end

  CATEGORY_SCHEMA = {
    fields: [
      { name: 'id', type: :int32 },
      { name: 'category', type: :string },
      { name: 'user', type: :string }
    ]
  }.freeze

  def cache_hits(data, string_cache)
    log_output = StringIO.new
    logger = Logger.new(log_output).tap { |l| l.level = Logger::INFO }
    Parquet.write_rows(data, schema: CATEGORY_SCHEMA, write_to: @test_file, string_cache: string_cache, logger: logger)
    log_output.string.match(/String cache stats: \d+ cache misses, (\d+) hits/)[1].to_i
  end

  # A larger capacity retains more distinct strings, so a column with more
  # distinct values than the default cap (100) yields more hits.
  def test_larger_string_cache_capacity_increases_hits
    data = (0...450).map { |i| [i, "cat_#{i % 150}", "u"] } # 150 distinct categories

    default_hits = cache_hits(data, true) # default cap (100)
    large_hits = cache_hits(data, 1000)   # retains all 150

    assert_operator large_hits, :>, default_hits
  end

  def test_string_cache_integer_round_trips
    data = (0...300).map { |i| [i, "cat_#{i % 5}", "u"] }
    Parquet.write_rows(data, schema: CATEGORY_SCHEMA, write_to: @test_file, string_cache: 256)

    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }
    assert_equal 300, rows.length
    assert_equal "cat_0", rows[0]["category"]
  end

  def test_string_cache_rejects_zero
    data = [[1, "a", "b"]]
    assert_raises(ArgumentError) do
      Parquet.write_rows(data, schema: CATEGORY_SCHEMA, write_to: @test_file, string_cache: 0)
    end
  end

  def test_string_cache_rejects_excessive_capacity
    data = [[1, "a", "b"]]
    assert_raises(ArgumentError) do
      Parquet.write_rows(data, schema: CATEGORY_SCHEMA, write_to: @test_file, string_cache: 65_537)
    end
  end

  def test_string_cache_rejects_invalid_type
    data = [[1, "a", "b"]]
    assert_raises(TypeError, ArgumentError) do
      Parquet.write_rows(data, schema: CATEGORY_SCHEMA, write_to: @test_file, string_cache: "yes")
    end
  end
  
  def test_string_cache_disabled_by_default
    # Create logger to capture output
    log_output = StringIO.new
    logger = Logger.new(log_output)
    logger.level = Logger::INFO
    
    # Create simple data
    data = []
    100.times do |i|
      data << [i, "value_#{i}"]
    end
    
    schema = {
      fields: [
        {name: 'id', type: :int32},
        {name: 'value', type: :string}
      ]
    }
    
    # Write without string_cache parameter (should be disabled by default)
    Parquet.write_rows(data, schema: schema, write_to: @test_file, logger: logger)
    
    # Check that string cache stats are NOT logged
    log_content = log_output.string
    refute_match(/String cache stats:/, log_content)
    
    # Verify data was written correctly
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }
    assert_equal 100, rows.length
  end
  
  def test_string_cache_with_high_cardinality
    # Create logger to capture cache statistics
    log_output = StringIO.new
    logger = Logger.new(log_output)
    logger.level = Logger::INFO
    
    # Create data with unique strings (low cache hit rate expected)
    data = []
    200.times do |i|
      data << [i, "unique_string_#{i}_#{rand(1000)}"]
    end
    
    schema = {
      fields: [
        {name: 'id', type: :int32},
        {name: 'value', type: :string}
      ]
    }
    
    # Write with string cache enabled
    Parquet.write_rows(data, schema: schema, write_to: @test_file, 
                      string_cache: true, logger: logger)
    
    # Check cache statistics
    log_content = log_output.string
    if log_content =~ /String cache stats: (\d+) cache misses, (\d+) hits \((\d+\.\d)% hit rate\)/
      misses = $1.to_i
      hit_rate = $3.to_f

      # With unique strings, we expect many cache misses and a low hit rate.
      assert misses >= 190, "Expected at least 190 cache misses"
      assert hit_rate < 10.0, "Expected low hit rate for unique strings"
    else
      flunk "Could not parse string cache statistics from log"
    end
  end
  
  def test_string_cache_explicit_false
    # Create logger to capture output
    log_output = StringIO.new
    logger = Logger.new(log_output)
    logger.level = Logger::INFO
    
    # Create simple data
    data = []
    50.times do |i|
      data << [i, "value"]
    end
    
    schema = {
      fields: [
        {name: 'id', type: :int32},
        {name: 'value', type: :string}
      ]
    }
    
    # Write with string_cache explicitly set to false
    Parquet.write_rows(data, schema: schema, write_to: @test_file, 
                      string_cache: false, logger: logger)
    
    # Check that string cache stats are NOT logged
    log_content = log_output.string
    refute_match(/String cache stats:/, log_content)
  end
end
