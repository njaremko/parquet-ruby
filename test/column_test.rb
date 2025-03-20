# frozen_string_literal: true
require "tempfile"

require "parquet"
require "minitest/autorun"

class ColumnTest < Minitest::Test
  def test_each_column
    batches = []
    Parquet.each_column("test/data.parquet", result_type: :array) { |col| batches << col }
    refute_empty batches
    assert_kind_of Array, batches.first
    assert_equal 1, batches.length # Verify expected number of record batches
    assert batches.all? { |batch| batch.is_a?(Array) }
    columns = batches.first
    assert_equal 2, columns.length # Verify expected number of columns
    assert columns.all? { |col| col.is_a?(Array) } # Verify all columns are arrays

    # Verify actual data matches what we created in setup
    assert_equal [[1, 2, 3], %w[name_1 name_2 name_3]], columns
  end

  def test_each_column_hash
    batches = []
    Parquet.each_column("test/data.parquet", result_type: :hash) { |col| batches << col }
    refute_empty batches
    assert_kind_of Hash, batches.first
    assert_equal batches.first.keys.sort, %w[id name].sort
    assert batches.all? { |batch| batch.is_a?(Hash) }

    # Verify actual data matches what we created in setup
    assert_equal [1, 2, 3], batches.first["id"]
    assert_equal %w[name_1 name_2 name_3], batches.first["name"]
  end

  def test_each_column_with_batch_size
    batches = []
    Parquet.each_column("test/data.parquet", result_type: :array, batch_size: 2) { |col| batches << col }
    refute_empty batches
    assert_kind_of Array, batches.first
    assert_equal 2, batches.length # Verify we get 2 batches with batch_size=2
    assert batches.all? { |batch| batch.is_a?(Array) }

    # First batch should have 2 rows
    assert_equal [[1, 2], %w[name_1 name_2]], batches[0]
    # Second batch should have remaining 1 row
    assert_equal [[3], %w[name_3]], batches[1]
  end

  def test_each_column_with_specific_columns
    batches = []
    Parquet.each_column("test/data.parquet", columns: ["id"], result_type: :hash) { |col| batches << col }
    refute_empty batches
    assert_kind_of Hash, batches.first
    assert_equal batches.first.keys.sort, %w[id].sort # Only id column
    assert_equal [1, 2, 3], batches.first["id"]
  end

  def test_each_column_empty_file
    File.write("test/empty.parquet", "")
    assert_raises(RuntimeError) { Parquet.each_column("test/empty.parquet") { |_| } }
  ensure
    File.delete("test/empty.parquet")
  end

  def test_each_column_nonexistent_file
    assert_raises(RuntimeError) { Parquet.each_column("test/nonexistent.parquet") { |_| } }
  end

  def test_each_column_invalid_result_type
    assert_raises(RuntimeError) { Parquet.each_column("test/data.parquet", result_type: :invalid) { |_| } }
  end

  def test_each_column_without_block
    enum = Parquet.each_column("test/data.parquet", result_type: :array)
    assert_kind_of Enumerator, enum

    batches = enum.to_a
    refute_empty batches
    assert_kind_of Array, batches.first
    assert_equal [[1, 2, 3], %w[name_1 name_2 name_3]], batches.first
  end

  def test_each_column_with_invalid_columns
    batches = []
    Parquet.each_column("test/data.parquet", columns: ["nonexistent"]) { |col| batches << col }
    refute_empty batches
    assert_kind_of Hash, batches.first
    assert_empty batches.first # Should be empty since column doesn't exist
  end

  def test_each_column_with_zero_batch_size
    assert_raises(ArgumentError) { Parquet.each_column("test/data.parquet", batch_size: 0) { |_| } }
  end

  def test_each_column_with_negative_batch_size
    assert_raises(RangeError, ArgumentError) { Parquet.each_column("test/data.parquet", batch_size: -1) { |_| } }
  end

  def test_each_column_batch_size_hash
    batches = []
    Parquet.each_column("test/data.parquet", batch_size: 2, result_type: :hash) { |col| batches << col }
    refute_empty batches
    assert_equal 2, batches.size
    assert_equal [1, 2], batches[0]["id"]
    assert_equal %w[name_1 name_2], batches[0]["name"]
    assert_equal [3], batches[1]["id"]
    assert_equal ["name_3"], batches[1]["name"]
  end

  def test_write_columns_without_schema
    temp_path = "test/no_schema_columns.parquet"
    begin
      # Write column data without providing a schema
      # Wrap the data in an additional array to represent a single batch
      data = [[[1, 2], %w[hello world]]].each
      Parquet.write_columns(data, schema: [], write_to: temp_path)

      # Read back and verify default column names were used
      rows = []
      Parquet.each_row(temp_path) { |row| rows << row }

      assert_equal 2, rows.length
      assert_equal %w[f0 f1], rows.first.keys.sort
      assert_equal "1", rows[0]["f0"]
      assert_equal "hello", rows[0]["f1"]
      assert_equal "2", rows[1]["f0"]
      assert_equal "world", rows[1]["f1"]
    ensure
      File.unlink(temp_path) if File.exist?(temp_path)
    end
  end

  def test_numeric_types_column
    columns = []
    Parquet.each_column("test/numeric.parquet", result_type: :hash) { |col| columns << col }
    assert_equal [1], columns.first["int8_col"]
    assert_equal [1000], columns.first["int16_col"]
    assert_equal [1_000_000], columns.first["int32_col"]
    assert_equal [1_000_000_000_000], columns.first["int64_col"]
    assert_in_delta 3.14, columns.first["float32_col"].first, 0.0001
    assert_in_delta 3.14159265359, columns.first["float64_col"].first, 0.0000000001
    assert_equal ["2023-01-01"], columns.first["date_col"].map(&:to_s)
    assert_equal ["2023-01-01 12:00:00 UTC"], columns.first["timestamp_col"].map(&:to_s)
    assert_equal ["2023-01-01 03:00:00 UTC"], columns.first["timestamptz_col"].map(&:to_s)
  end

  def test_empty_table_columns
    columns = []
    Parquet.each_column("test/empty_table.parquet", result_type: :hash) { |col| columns << col }
    refute_empty columns # Should still return schema info
    assert_empty columns.first["id"]
    assert_empty columns.first["name"]
  end

  def test_write_columns
    # Create batches of column data
    batches = [
      # First batch
      [
        [1, 2], # id column
        %w[Alice Bob], # name column
        [95.5, 82.3], # score column
        [Time.new(2024, 1, 1, 0, 0, 0, "UTC"), Time.new(2024, 1, 2, 0, 0, 0, "UTC")], # date column
        [Time.new(2024, 1, 1, 10, 30, 0, "UTC"), Time.new(2024, 1, 2, 14, 45, 0, "UTC")], # timestamp column
        [true, false]
      ],
      # Second batch
      [
        [3, 4], # id column
        ["Charlie", nil], # name column
        [88.7, nil], # score column
        [Time.new(2024, 1, 3, 0, 0, 0, "UTC"), nil], # date column
        [Time.new(2024, 1, 3, 9, 15, 0, "UTC"), nil], # timestamp column
        [true, nil]
      ]
    ]

    # Create an enumerator from the batches
    columns = batches.each

    # Write to a parquet file
    Parquet.write_columns(
      columns,
      schema: [
        { "id" => "int64" },
        { "name" => "string" },
        { "score" => "double" },
        { "date" => "date32" },
        { "timestamp" => "timestamp_millis" },
        { "data" => "boolean" }
      ],
      write_to: "test/students.parquet"
    )

    rows = Parquet.each_row("test/students.parquet").to_a
    assert_equal 4, rows.length

    assert_equal 1, rows[0]["id"]
    assert_equal "Alice", rows[0]["name"]
    assert_in_delta 95.5, rows[0]["score"], 0.0001
    assert_equal "2024-01-01", rows[0]["date"].to_s
    assert_equal "2024-01-01 10:30:00 UTC", rows[0]["timestamp"].to_s
    assert_equal true, rows[0]["data"]

    assert_equal 2, rows[1]["id"]
    assert_equal "Bob", rows[1]["name"]
    assert_in_delta 82.3, rows[1]["score"], 0.0001
    assert_equal "2024-01-02", rows[1]["date"].to_s
    assert_equal "2024-01-02 14:45:00 UTC", rows[1]["timestamp"].to_s
    assert_equal false, rows[1]["data"]

    assert_equal 3, rows[2]["id"]
    assert_equal "Charlie", rows[2]["name"]
    assert_in_delta 88.7, rows[2]["score"], 0.0001
    assert_equal "2024-01-03", rows[2]["date"].to_s
    assert_equal "2024-01-03 09:15:00 UTC", rows[2]["timestamp"].to_s
    assert_equal true, rows[2]["data"]

    assert_equal 4, rows[3]["id"]
    assert_nil rows[3]["name"]
    assert_nil rows[3]["score"]
    assert_nil rows[3]["date"]
    assert_nil rows[3]["timestamp"]
    assert_nil rows[3]["data"]
  ensure
    File.delete("test/students.parquet") if File.exist?("test/students.parquet")
  end

  def test_schema_with_format
    # Test writing columns with format specified in schema
    columns = [
      [
        %w[2024-01-01 2024-01-02 2024-01-03],
        ["2024-01-01 10:30:00+0000", "2024-01-02 14:45:00+0000", "2024-01-03 09:15:00+0000"]
      ]
    ].each

    Parquet.write_columns(
      columns,
      schema: [
        { "date" => { "type" => "date32", "format" => "%Y-%m-%d" } },
        { "timestamp" => { "type" => "timestamp_millis", "format" => "%Y-%m-%d %H:%M:%S%z" } }
      ],
      write_to: "test/formatted_columns.parquet"
    )

    # Verify column-based data
    rows = Parquet.each_row("test/formatted_columns.parquet").to_a
    assert_equal 3, rows.length

    assert_equal "2024-01-01", rows[0]["date"].to_s
    assert_equal "2024-01-01 10:30:00 UTC", rows[0]["timestamp"].to_s

    assert_equal "2024-01-02", rows[1]["date"].to_s
    assert_equal "2024-01-02 14:45:00 UTC", rows[1]["timestamp"].to_s

    assert_equal "2024-01-03", rows[2]["date"].to_s
    assert_equal "2024-01-03 09:15:00 UTC", rows[2]["timestamp"].to_s
  ensure
    File.delete("test/formatted_columns.parquet") if File.exist?("test/formatted_columns.parquet")
  end

  def test_write_columns_mismatched_lengths
    schema = [{ "id" => "int64" }, { "name" => "string" }]

    # First "batch" has 3 values for "id" but only 2 for "name"
    batches = [
      [
        [1, 2, 3],
        %w[Alice Bob] # Mismatch: only 2 entries here
      ]
    ]

    begin
      enumerator = batches.each
      temp_path = "test_mismatched_columns.parquet"

      error = assert_raises(RuntimeError) { Parquet.write_columns(enumerator, schema: schema, write_to: temp_path) }
      assert_match(/Failed to create record batch|mismatched.*length/i, error.message)
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end
end