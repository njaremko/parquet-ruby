# frozen_string_literal: true
require "tempfile"

require "parquet"
require "minitest/autorun"

require "csv"
require "bigdecimal"

class RowTest < Minitest::Test
  def test_asdf
    puts Parquet.each_row("/Users/njaremko/Downloads/data_0_0_0_with_types.snappy.parquet", result_type: :array).to_a
  end

  def test_each_row
    rows = []
    Parquet.each_row("test/data.parquet") { |row| rows << row }
    refute_empty rows
    assert_kind_of Hash, rows.first
    assert_equal rows.first.keys.sort, %w[id name].sort # Verify expected columns
    assert rows.all? { |row| row.is_a?(Hash) } # Verify all rows are hashes
    assert rows.all? { |row| row.key?("id") && row.key?("name") } # Verify schema consistency

    # Verify actual data matches what we created in setup
    assert_equal [1, 2, 3], rows.map { |r| r["id"] }
    assert_equal %w[name_1 name_2 name_3], rows.map { |r| r["name"] }
  end

  def test_each_row_array
    rows = []
    Parquet.each_row("test/data.parquet", result_type: :array) { |row| rows << row }
    refute_empty rows
    assert_kind_of Array, rows.first
    assert_equal 2, rows.first.length # Verify expected number of columns
    assert rows.all? { |row| row.is_a?(Array) } # Verify all rows are arrays
    assert rows.all? { |row| row.length == rows.first.length } # Verify consistent column count

    # Verify actual data matches what we created in setup
    assert_equal [[1, "name_1"], [2, "name_2"], [3, "name_3"]], rows
  end

  def test_each_row_with_file_open
    rows = []
    File.open("test/data.parquet") { |file| Parquet.each_row(file) { |row| rows << row } }
    refute_empty rows
    assert_kind_of Hash, rows.first
    assert_equal rows.first.keys.sort, %w[id name].sort # Verify expected columns
    assert rows.all? { |row| row.is_a?(Hash) } # Verify all rows are hashes
    assert rows.all? { |row| row.key?("id") && row.key?("name") } # Verify schema consistency

    # Verify actual data matches what we created in setup
    assert_equal [1, 2, 3], rows.map { |r| r["id"] }
    assert_equal %w[name_1 name_2 name_3], rows.map { |r| r["name"] }
  end

  def test_each_row_with_stringio
    rows = []
    file_content = File.binread("test/data.parquet")
    io = StringIO.new(file_content)
    Parquet.each_row(io) { |row| rows << row }
    refute_empty rows
    assert_kind_of Hash, rows.first
    assert_equal rows.first.keys.sort, %w[id name].sort # Verify expected columns
    assert rows.all? { |row| row.is_a?(Hash) } # Verify all rows are hashes
    assert rows.all? { |row| row.key?("id") && row.key?("name") } # Verify schema consistency

    # Verify actual data matches what we created in setup
    assert_equal [1, 2, 3], rows.map { |r| r["id"] }
    assert_equal %w[name_1 name_2 name_3], rows.map { |r| r["name"] }
  end

  def test_each_row_with_columns
    rows = []
    Parquet.each_row("test/data.parquet", columns: %w[id]) { |row| rows << row }
    refute_empty rows
    assert_kind_of Hash, rows.first
    assert_equal rows.first.keys.sort, %w[id].sort # Verify expected columns
    assert rows.all? { |row| row.is_a?(Hash) } # Verify all rows are hashes
    assert rows.all? { |row| row.key?("id") } # Verify schema consistency

    # Verify actual data matches what we created in setup
    assert_equal [1, 2, 3], rows.map { |r| r["id"] }
  end

  def test_write_rows_without_schema
    temp_path = "test/no_schema.parquet"
    begin
      # Write data without providing a schema
      data = [[1, "hello"], [2, "world"]].each
      Parquet.write_rows(data, schema: [], write_to: temp_path)

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

  def test_write_large_file
    skip unless ENV["RUN_SLOW_TESTS"]

    # Create a temporary file path
    temp_path = "test/large_data.parquet"
    begin
      # Generate schema for id and large string column
      schema = [{ "id" => "int64" }, { "data" => "string" }]

      # Write 100k rows using an enumerator to avoid memory allocation
      Parquet.write_rows(
        Enumerator.new do |yielder|
          5000.times do |i|
            large_string = ("a".."z").to_a[rand(26)] * 1_000_000
            yielder << [i, large_string]
          end
        end,
        schema: schema,
        write_to: temp_path
      )

      # Verify file exists and has content
      assert File.exist?(temp_path)
      assert File.size(temp_path) > 0

      # Read back first few rows to verify structure
      rows = []
      Parquet.each_row(temp_path) { |row| rows << row }

      assert_equal 5000, rows.length
      assert_equal 0, rows[0]["id"]
      assert_equal 1, rows[1]["id"]
    ensure
      File.unlink(temp_path) if File.exist?(temp_path)
    end
  end

  def test_numeric_types
    rows = []
    Parquet.each_row("test/numeric.parquet") { |row| rows << row }

    row = rows.first
    assert_equal 1, row["int8_col"]
    assert_equal 1000, row["int16_col"]
    assert_equal 1_000_000, row["int32_col"]
    assert_equal 1_000_000_000_000, row["int64_col"]
    assert_in_delta 3.14, row["float32_col"], 0.0001
    assert_in_delta 3.14159265359, row["float64_col"], 0.0000000001
    assert_equal "2023-01-01", row["date_col"].to_s
    assert_equal "2023-01-01 12:00:00 UTC", row["timestamp_col"].to_s
    assert_equal "2023-01-01 03:00:00 UTC", row["timestamptz_col"].to_s
  end

  def test_empty_table
    rows = []
    Parquet.each_row("test/empty_table.parquet") { |row| rows << row }
    assert_empty rows
  end

  def test_wide_table
    column_count = 1000
    schema = (0...column_count).map { |i| { "col#{i}" => "int64" } }
    Parquet.write_rows(
      Enumerator.new { |yielder| yielder << (0...column_count).to_a },
      schema: schema,
      write_to: "test/wide.parquet"
    )

    rows = []
    Parquet.each_row("test/wide.parquet") { |row| rows << row }
    assert_equal column_count, rows.first.keys.length
    assert_equal 0, rows.first["col0"]
    assert_equal column_count - 1, rows.first["col#{column_count - 1}"]

    # Test reading specific columns from wide table
    selected_columns = ["col0", "col#{column_count - 1}"]
    rows = []
    Parquet.each_row("test/wide.parquet", columns: selected_columns) { |row| rows << row }
    assert_equal selected_columns.sort, rows.first.keys.sort
  ensure
    File.delete("test/wide.parquet") if File.exist?("test/wide.parquet")
  end

  def test_repeated_column_names
    # The behavior with repeated column names should be consistent
    rows = []
    Parquet.each_row("test/repeated_cols.parquet") { |row| rows << row }
    assert_equal 1, rows.first["id"]
    assert_includes %w[first second], rows.first["name"]
  end

  def test_special_character_column_names
    schema = [
      { "id" => "int64" },
      { "column with spaces" => "int64" },
      { "special!@#$%^&*()_+" => "int64" },
      { "日本語" => "int64" },
      { "col.with.dots" => "int64" }
    ]
    Parquet.write_rows([[1, 2, 3, 4, 5]].each, schema: schema, write_to: "test/special_chars.parquet")

    rows = []
    Parquet.each_row("test/special_chars.parquet") { |row| rows << row }
    assert_equal 1, rows.first["id"]
    assert_equal 2, rows.first["column with spaces"]
    assert_equal 3, rows.first["special!@#$%^&*()_+"]
    assert_equal 4, rows.first["日本語"]
    assert_equal 5, rows.first["col.with.dots"]

    # Test column selection with special characters
    special_columns = ["column with spaces", "日本語"]
    rows = []
    Parquet.each_row("test/special_chars.parquet", columns: special_columns) { |row| rows << row }
    assert_equal special_columns.sort, rows.first.keys.sort
  ensure
    File.delete("test/special_chars.parquet") if File.exist?("test/special_chars.parquet")
  end

  def test_binary_data
    rows = []
    Parquet.each_row("test/binary.parquet") { |row| rows << row }
    assert_equal 1, rows.first["id"]
    expected = [0x00, 0x01, 0x02, 0x03, 0x04]
    assert_equal expected, rows.first["binary_col"].bytes
  end

  def test_decimal_types
    rows = []
    Parquet.each_row("test/decimal.parquet") { |row| rows << row }
    assert_equal BigDecimal("123.45"), rows.first["decimal_5_2"]
    assert_equal BigDecimal("123456.789"), rows.first["decimal_9_3"]
    assert_equal BigDecimal("-123.45"), rows.first["negative_decimal"]
  end

  def test_boolean_types
    Parquet.write_rows(
      [[true, false, nil]].each,
      schema: [{ "true_col" => "bool" }, { "false_col" => "bool" }, { "null_bool" => "bool" }],
      write_to: "test/boolean.parquet"
    )

    rows = []
    Parquet.each_row("test/boolean.parquet") { |row| rows << row }
    assert_equal true, rows.first["true_col"]
    assert_equal false, rows.first["false_col"]
    assert_nil rows.first["null_bool"]
  ensure
    File.delete("test/boolean.parquet") if File.exist?("test/boolean.parquet")
  end

  def test_invalid_column_specifications
    Parquet.write_rows(
      [[1, "name"]].each,
      schema: [{ "id" => "int64" }, { "name" => "string" }],
      write_to: "test/cols.parquet"
    )

    # Test with non-existent columns
    rows = []
    Parquet.each_row("test/cols.parquet", columns: ["nonexistent"]) { |row| rows << row }
    assert_empty rows.first

    # Test with mixed valid and invalid columns
    rows = []
    Parquet.each_row("test/cols.parquet", columns: %w[id nonexistent]) { |row| rows << row }
    assert_equal ["id"], rows.first.keys

    # Test with empty column list
    rows = []
    Parquet.each_row("test/cols.parquet", columns: []) { |row| rows << row }
    assert_empty rows.first
  ensure
    File.delete("test/cols.parquet") if File.exist?("test/cols.parquet")
  end

  def test_write_rows
    # Test different input formats
    data = [
      [1, "Alice", 95.5],
      [3, "Bob", 82.3],
      [3, "Charlie", 88.7],
      [4, "David", 91.2],
      [5, "Eve", 1.5e2],
      [6, nil, nil]
    ]

    # Create an enumerator from the array
    rows = data.each

    # Write to a parquet file
    Parquet.write_rows(
      rows,
      schema: [{ "id" => "int64" }, { "name" => "string" }, { "score" => "double" }],
      write_to: "test/students.parquet"
    )

    rows = Parquet.each_row("test/students.parquet").to_a
    assert_equal 6, rows.length

    # Test numeric input
    assert_equal 1, rows[0]["id"]
    assert_equal "Alice", rows[0]["name"]
    assert_in_delta 95.5, rows[0]["score"], 0.0001

    assert_equal 3, rows[1]["id"]
    assert_equal "Bob", rows[1]["name"]
    assert_in_delta 82.3, rows[1]["score"], 0.0001

    assert_equal 3, rows[2]["id"]
    assert_equal "Charlie", rows[2]["name"]
    assert_in_delta 88.7, rows[2]["score"], 0.0001

    assert_equal 4, rows[3]["id"]
    assert_equal "David", rows[3]["name"]
    assert_in_delta 91.2, rows[3]["score"], 0.0001

    assert_equal 5, rows[4]["id"]
    assert_equal "Eve", rows[4]["name"]
    assert_in_delta 150.0, rows[4]["score"], 0.0001

    assert_equal 6, rows[5]["id"]
    assert_nil rows[5]["name"]
    assert_nil rows[5]["score"]

    # Test writing to a file
    File.open("test/students_from_io.parquet", "wb") do |file|
      rows = data.each
      Parquet.write_rows(
        rows,
        schema: [{ "id" => "int64" }, { "name" => "string" }, { "score" => "double" }],
        write_to: file
      )
    end

    # Verify the data written through IO
    rows = Parquet.each_row("test/students_from_io.parquet").to_a
    assert_equal 6, rows.length

    assert_equal 1, rows[0]["id"]
    assert_equal "Alice", rows[0]["name"]
    assert_in_delta 95.5, rows[0]["score"], 0.0001

    assert_equal 5, rows[4]["id"]
    assert_equal "Eve", rows[4]["name"]
    assert_in_delta 150.0, rows[4]["score"], 0.0001
  ensure
    File.delete("test/students.parquet") if File.exist?("test/students.parquet")
    File.delete("test/students_from_io.parquet") if File.exist?("test/students_from_io.parquet")
  end

  def test_large_file_handling
    row_count = 100_000

    Parquet.write_rows(
      Enumerator.new { |yielder| row_count.times { |i| yielder << [i, "name_#{i}"] } },
      schema: [{ "id" => "int64" }, { "name" => "string" }],
      write_to: "test/large.parquet"
    )

    count = 0
    Parquet.each_row("test/large.parquet") do |row|
      assert_equal count, row["id"]
      assert_equal "name_#{count}", row["name"]
      count += 1
    end
    assert_equal row_count, count
  ensure
    File.delete("test/large.parquet") if File.exist?("test/large.parquet")
  end
end
