require_relative 'test_helper'
require 'tempfile'

class BloomFilterTest < Minitest::Test
  def setup
    @file = File.join(Dir.tmpdir, "bf_#{Process.pid}.parquet")
  end

  def teardown
    File.delete(@file) if File.exist?(@file)
  end

  def schema
    {
      fields: [
        { name: 'uuid', type: :fixed_len_byte_array, length: 16, format: 'uuid', nullable: false },
        { name: 'device_id', type: :string },
        { name: 'value', type: :int64 }
      ]
    }
  end

  def test_write_rows_with_bloom_filters
    require 'securerandom'
    # Generate data
    data = (0...10_000).map do |i|
      [SecureRandom.uuid, "dev-#{i % 1000}", i]
    end

    # Write with bloom filters on uuid and device_id
    Parquet.write_rows(
      data,
      schema: schema,
      write_to: @file,
      bloom_filters: [
        { path: ['uuid'], false_positive_probability: 0.01, n_distinct_values: 10_000 },
        { path: ['device_id'], false_positive_probability: 0.01, n_distinct_values: 1_000 }
      ]
    )

    md = Parquet.metadata(@file)
    # Ensure bloom filter metadata is present for at least one column chunk
    has_bf = md["row_groups"].any? do |rg|
      rg["columns"].any? { |c| c.key?("bloom_filter_offset") || c.key?("bloom_filter_length") }
    end
    assert has_bf, "Expected bloom filter metadata to be present"
  end

  def test_write_columns_with_bloom_filters
    require 'securerandom'
    uuids = Array.new(5000) { SecureRandom.uuid }
    devices = Array.new(5000) { |i| "dev-#{i % 500}" }
    values = (0...5000).to_a

    batches = [[uuids, devices, values]]

    Parquet.write_columns(
      batches.each,
      schema: schema,
      write_to: @file,
      bloom_filters: [
        { path: ['uuid'], false_positive_probability: 0.01, n_distinct_values: 5000 },
        { path: ['device_id'], false_positive_probability: 0.01, n_distinct_values: 500 }
      ]
    )

    md = Parquet.metadata(@file)
    has_bf = md["row_groups"].any? do |rg|
      rg["columns"].any? { |c| c.key?("bloom_filter_offset") || c.key?("bloom_filter_length") }
    end
    assert has_bf, "Expected bloom filter metadata to be present for column write"
  end

  def test_bloom_filter_with_defaults
    require 'securerandom'
    # Generate data
    data = (0...1000).map do |i|
      [SecureRandom.uuid, "dev-#{i % 100}", i]
    end

    # Write with bloom filters using defaults
    Parquet.write_rows(
      data,
      schema: schema,
      write_to: @file,
      bloom_filters: [
        { path: ['uuid'] },  # Use default fpp and ndv
        { path: ['device_id'], false_positive_probability: 0.05 }  # Custom fpp, default ndv
      ]
    )

    md = Parquet.metadata(@file)
    # Ensure bloom filter metadata is present
    has_bf = md["row_groups"].any? do |rg|
      rg["columns"].any? { |c| c.key?("bloom_filter_offset") || c.key?("bloom_filter_length") }
    end
    assert has_bf, "Expected bloom filter metadata when using defaults"
  end

  def test_bloom_filter_single_element_array
    require 'securerandom'
    # Generate data
    data = (0...1000).map do |i|
      [SecureRandom.uuid, "dev-#{i % 100}", i]
    end

    # Test that single-element arrays work correctly
    Parquet.write_rows(
      data,
      schema: schema,
      write_to: @file,
      bloom_filters: [
        { path: ['uuid'], false_positive_probability: 0.01, n_distinct_values: 1000 },
        { path: ['device_id'], false_positive_probability: 0.01, n_distinct_values: 100 }
      ]
    )

    md = Parquet.metadata(@file)
    # Ensure bloom filter metadata is present
    has_bf = md["row_groups"].any? do |rg|
      rg["columns"].any? { |c| c.key?("bloom_filter_offset") || c.key?("bloom_filter_length") }
    end
    assert has_bf, "Expected bloom filter metadata for single-element path arrays"
  end

  def test_bloom_filter_nested_column
    nested_schema = {
      fields: [
        { name: 'id', type: :int64 },
        { 
          name: 'user', 
          type: :struct,
          fields: [
            { name: 'email', type: :string },
            { name: 'name', type: :string }
          ]
        }
      ]
    }

    data = (0...1000).map do |i|
      [i, { "email" => "user#{i}@example.com", "name" => "User #{i}" }]
    end

    # Write with bloom filter on nested column
    Parquet.write_rows(
      data,
      schema: nested_schema,
      write_to: @file,
      bloom_filters: [
        { path: ['user', 'email'], false_positive_probability: 0.01, n_distinct_values: 1000 }
      ]
    )

    md = Parquet.metadata(@file)
    # Ensure bloom filter metadata is present
    has_bf = md["row_groups"].any? do |rg|
      rg["columns"].any? { |c| c.key?("bloom_filter_offset") || c.key?("bloom_filter_length") }
    end
    assert has_bf, "Expected bloom filter metadata for nested column"
  end

  def test_no_bloom_filters
    require 'securerandom'
    # Generate data
    data = (0...1000).map do |i|
      [SecureRandom.uuid, "dev-#{i % 100}", i]
    end

    # Write without bloom filters
    Parquet.write_rows(
      data,
      schema: schema,
      write_to: @file
    )

    md = Parquet.metadata(@file)
    # Ensure NO bloom filter metadata is present
    has_bf = md["row_groups"].any? do |rg|
      rg["columns"].any? { |c| c.key?("bloom_filter_offset") || c.key?("bloom_filter_length") }
    end
    refute has_bf, "Should not have bloom filter metadata when not specified"
  end

  def test_ndv_capping_to_row_group_size
    require 'securerandom'
    # Generate small dataset
    data = (0...100).map do |i|
      [SecureRandom.uuid, "dev-#{i}", i]
    end

    # Write with NDV much larger than actual data
    # The NDV should be capped to max row group size (1M) internally
    # This ensures bloom filter isn't unnecessarily large
    Parquet.write_rows(
      data,
      schema: schema,
      write_to: @file,
      bloom_filters: [
        { path: ['uuid'], false_positive_probability: 0.01, n_distinct_values: 10_000_000 },  # 10M NDV
        { path: ['device_id'], false_positive_probability: 0.01, n_distinct_values: 5_000_000 }  # 5M NDV
      ]
    )

    md = Parquet.metadata(@file)
    # Verify bloom filters were created
    has_bf = md["row_groups"].any? do |rg|
      rg["columns"].any? { |c| c.key?("bloom_filter_offset") || c.key?("bloom_filter_length") }
    end
    assert has_bf, "Expected bloom filter metadata even with large NDV"
    
    # The bloom filter size should be reasonable (not excessively large)
    # With NDV capped at 1M and FPP 0.01, theoretical size is ~1.2MB
    # But the actual implementation may round up to power of 2, so ~2MB is expected
    md["row_groups"].each do |rg|
      rg["columns"].each do |col|
        if col["bloom_filter_length"]
          # Bloom filter for 10M values at 0.01 FPP would be ~14.3MB
          # With capping to 1M, it should be much smaller (~2MB)
          assert col["bloom_filter_length"] < 3_000_000, 
            "Bloom filter size should be capped (got #{col["bloom_filter_length"]} bytes)"
          # Also verify it's not too small (should be at least 1KB for any reasonable filter)
          assert col["bloom_filter_length"] > 1000,
            "Bloom filter seems too small (got #{col["bloom_filter_length"]} bytes)"
        end
      end
    end
  end
end


