require_relative 'test_helper'
require 'time'

class TimezoneTest < Minitest::Test
  def setup
    @test_file = File.join(Dir.tmpdir, "test_timezone_#{Process.pid}.parquet")
  end

  def teardown
    File.delete(@test_file) if File.exist?(@test_file)
  end

  def test_timezone_preservation_roundtrip
    # Create Time objects with different timezone scenarios
    times = [
      # Local time (no explicit timezone)
      Time.local(2024, 1, 15, 10, 30, 45.123456),
      # UTC time
      Time.utc(2024, 1, 15, 10, 30, 45.123456),
      # Time with positive offset (Tokyo +09:00)
      Time.new(2024, 1, 15, 10, 30, 45.123456, "+09:00"),
      # Time with negative offset (New York -05:00)
      Time.new(2024, 1, 15, 10, 30, 45.123456, "-05:00"),
      # Time with non-hour offset (India +05:30)
      Time.new(2024, 1, 15, 10, 30, 45.123456, "+05:30"),
    ]

    # Create test data
    rows = times.map.with_index { |t, i| [i, t] }

    # Test with UTC timezone
    schema_utc =
      Parquet::Schema.define do
        field "id", :int32
        field "timestamp", :timestamp_micros, timezone: "UTC"
      end

    # Write data
    Parquet.write_rows(rows.each, schema: schema_utc, write_to: @test_file)

    # Read data back
    read_rows = []
    Parquet.each_row(@test_file) { |row| read_rows << row }

    assert_equal times.length, read_rows.length

    # Verify each timestamp
    times.each_with_index do |original_time, i|
      read_time = read_rows[i]["timestamp"]

      # Check that the actual timestamp value is preserved
      assert_equal original_time.to_i, read_time.to_i, "Timestamp seconds should match for row #{i}"
      # Microsecond precision means we might lose some nanosecond precision
      assert_in_delta original_time.nsec, read_time.nsec, 1000, "Timestamp nanoseconds should match for row #{i} within microsecond precision"

      # When schema has UTC timezone, times should be returned in UTC
      assert read_time.utc?, "Time should be in UTC for row #{i}"
      assert_equal 0, read_time.utc_offset, "UTC offset should be 0 for row #{i}"
    end

    # Test with offset timezone
    File.delete(@test_file) if File.exist?(@test_file)

    schema_tokyo =
      Parquet::Schema.define do
        field "id", :int32
        field "timestamp", :timestamp_micros, timezone: "+09:00"
      end

    Parquet.write_rows(rows.each, schema: schema_tokyo, write_to: @test_file)

    read_rows = []
    Parquet.each_row(@test_file) { |row| read_rows << row }

    times.each_with_index do |original_time, i|
      read_time = read_rows[i]["timestamp"]

      # According to Parquet spec, timestamps with ANY timezone are normalized to UTC
      # The original timezone is lost - we only get back UTC timestamps
      assert read_time.utc?, "Time should be in UTC for row #{i} (Parquet limitation)"
      assert_equal original_time.to_i, read_time.to_i, "Timestamp seconds should be preserved for row #{i}"
    end
  end

  def test_timestamp_without_explicit_timezone_uses_default
    # Create a local time (no explicit timezone)
    local_time = Time.new(2024, 3, 15, 14, 30, 0)

    rows = [[1, local_time]]
    schema = [
      { "id" => "int32" },
      { "timestamp" => "timestamp_millis" }
    ]

    # Write and read back
    Parquet.write_rows(rows.each, schema: schema, write_to: @test_file)

    read_rows = []
    Parquet.each_row(@test_file) { |row| read_rows << row }

    read_time = read_rows[0]["timestamp"]

    # With new default, timestamps without explicit timezone parameter are stored as UTC
    assert read_time.utc?, "Timestamp without explicit timezone should use default (UTC)"
    assert_equal local_time, read_time, "Timestamp seconds should be preserved"
  end

  def test_all_timestamp_precisions_preserve_timezone
    # Test with different timestamp precisions
    test_time = Time.new(2024, 6, 15, 9, 45, 30.654321, "+10:00")

    schema =
      Parquet::Schema.define do
        field "ts_sec", :timestamp_second, timezone: "America/Los_Angeles"
        field "ts_millis", :timestamp_millis, timezone: "America/Los_Angeles"
        field "ts_micros", :timestamp_micros, timezone: "America/Los_Angeles"
        field "ts_nanos", :timestamp_nanos, timezone: "America/Los_Angeles"
      end

    rows = [[test_time, test_time, test_time, test_time]]

    Parquet.write_rows(rows.each, schema: schema, write_to: @test_file)

    read_rows = []
    Parquet.each_row(@test_file) { |row| read_rows << row }

    # Check each precision
    ["ts_sec", "ts_millis", "ts_micros", "ts_nanos"].each do |field_name|
      read_time = read_rows[0][field_name]

      # Verify the timestamp value is preserved (within precision limits)
      assert_equal test_time.to_i, read_time.to_i,
        "Timestamp seconds should be preserved for #{field_name}"

      # Check precision based on field type
      case field_name
      when "ts_sec"
        # Second precision - no subsecond data
        assert_equal 0, read_time.nsec, "Second precision should have no nanoseconds"
      when "ts_millis"
        # Millisecond precision
        assert_in_delta test_time.nsec, read_time.nsec, 1_000_000,
          "Millisecond precision should preserve up to milliseconds"
      when "ts_micros"
        # Microsecond precision
        assert_in_delta test_time.nsec, read_time.nsec, 1_000,
          "Microsecond precision should preserve up to microseconds"
      when "ts_nanos"
        # Nanosecond precision
        assert_equal test_time.nsec, read_time.nsec,
          "Nanosecond precision should preserve all nanoseconds"
      end

      # When schema has timezone, times are returned in UTC (Parquet limitation)
      assert read_time.utc?, "Read time should be in UTC for #{field_name} (Parquet limitation)"
      assert_equal 0, read_time.utc_offset, "UTC offset should be 0 for #{field_name}"
    end
  end

  def test_timestamp_string_parsing_preserves_timezone
    # Test that parsing timestamp strings with timezone in schema preserves the schema timezone
    schema = Parquet::Schema.define do
      field "id", :int32
      field "timestamp_tokyo", :timestamp_micros, timezone: "+09:00"
      field "timestamp_est", :timestamp_micros, timezone: "-05:00"
      field "timestamp_utc", :timestamp_micros, timezone: "UTC"
      field "timestamp_none", :timestamp_micros, has_timezone: false  # Explicitly no timezone
    end

    rows = [
      [1, "2024-01-01T12:00:00+09:00", "2024-01-01T12:00:00-05:00", "2024-01-01T12:00:00Z", "2024-01-01T12:00:00"],
    ]

    Parquet.write_rows(rows.each, schema: schema, write_to: @test_file)

    read_rows = []
    Parquet.each_row(@test_file) { |row| read_rows << row }

    # According to Parquet spec, ANY timestamp with timezone is normalized to UTC
    # Original timezones (+09:00, -05:00) are lost - we only get UTC back
    assert read_rows[0]["timestamp_tokyo"].utc?, "timestamp_tokyo should be in UTC (Parquet limitation)"
    assert read_rows[0]["timestamp_est"].utc?, "timestamp_est should be in UTC (Parquet limitation)"

    # UTC should be marked as UTC
    assert read_rows[0]["timestamp_utc"].utc?

    # Explicitly set has_timezone: false for local time (isAdjustedToUTC = false)
    assert_kind_of Time, read_rows[0]["timestamp_none"]
    assert !read_rows[0]["timestamp_none"].utc?, "timestamp without timezone should be local time"
  end

  def test_has_timezone_parameter
    # Test the new has_timezone parameter that makes timezone handling explicit
    schema = Parquet::Schema.define do
      field :id, :int32
      field :timestamp_with_tz, :timestamp_micros, has_timezone: true    # UTC storage (explicit)
      field :timestamp_without_tz, :timestamp_micros, has_timezone: false # Local storage
      field :timestamp_default, :timestamp_micros                        # Default: UTC storage
    end

    # Create test times
    tokyo_time = Time.new(2024, 1, 15, 10, 30, 45, "+09:00")
    ny_time = Time.new(2024, 1, 15, 10, 30, 45, "-05:00")
    local_time = Time.new(2024, 1, 15, 10, 30, 45)

    rows = [
      [1, tokyo_time, ny_time, local_time]
    ]

    Parquet.write_rows(rows.each, schema: schema, write_to: @test_file)

    read_rows = []
    Parquet.each_row(@test_file) { |row| read_rows << row }

    # Verify has_timezone: true -> UTC storage
    assert read_rows[0]["timestamp_with_tz"].utc?, "has_timezone: true should store as UTC"
    assert_equal tokyo_time.to_i, read_rows[0]["timestamp_with_tz"].to_i

    # Verify has_timezone: false -> local storage
    assert !read_rows[0]["timestamp_without_tz"].utc?, "has_timezone: false should store as local"
    assert_equal ny_time.to_i, read_rows[0]["timestamp_without_tz"].to_i

    # Verify default (no parameter) -> UTC storage (new default)
    assert read_rows[0]["timestamp_default"].utc?, "default should store as UTC"
    assert_equal local_time.to_i, read_rows[0]["timestamp_default"].to_i
  end

  def test_legacy_timezone_parameter_compatibility
    # Ensure legacy timezone parameter still works (any value -> UTC storage)
    schema = Parquet::Schema.define do
      field :id, :int32
      field :ts_utc, :timestamp_millis, timezone: "UTC"
      field :ts_tokyo, :timestamp_millis, timezone: "+09:00"      # Still stored as UTC
      field :ts_ny, :timestamp_millis, timezone: "America/New_York" # Still stored as UTC
    end

    test_time = Time.new(2024, 3, 15, 14, 30, 0, "+02:00")
    rows = [[1, test_time, test_time, test_time]]

    Parquet.write_rows(rows.each, schema: schema, write_to: @test_file)

    read_rows = []
    Parquet.each_row(@test_file) { |row| read_rows << row }

    # All should be in UTC regardless of timezone value
    assert read_rows[0]["ts_utc"].utc?, "UTC timezone should store as UTC"
    assert read_rows[0]["ts_tokyo"].utc?, "+09:00 timezone should store as UTC"
    assert read_rows[0]["ts_ny"].utc?, "America/New_York timezone should store as UTC"

    # All should have same timestamp value
    assert_equal test_time.to_i, read_rows[0]["ts_utc"].to_i
    assert_equal test_time.to_i, read_rows[0]["ts_tokyo"].to_i
    assert_equal test_time.to_i, read_rows[0]["ts_ny"].to_i
  end
end
