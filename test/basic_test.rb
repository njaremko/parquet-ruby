# frozen_string_literal: true
require "tempfile"

require "parquet"
require "minitest/autorun"

require "csv"
require "bigdecimal"

class BasicTest < Minitest::Test
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

  def test_complex_types
    # Test hash result type
    rows = []
    Parquet.each_row("test/complex.parquet") { |row| rows << row }

    assert_equal 1, rows.first["id"]
    assert_equal [1, 2, 3], rows.first["int_array"]
    assert_equal({ "key" => "value" }, rows.first["map_col"])
    assert_equal({ "nested" => { "field" => 42 } }, rows.first["struct_col"])
    assert_nil rows.first["nullable_col"]

    # Test array result type
    array_rows = []
    Parquet.each_row("test/complex.parquet", result_type: :array) { |row| array_rows << row }

    assert_equal 1, array_rows.first[0] # id
    assert_equal [1, 2, 3], array_rows.first[1] # int_array
    assert_equal({ "key" => "value" }, array_rows.first[2]) # map_col
    assert_equal({ "nested" => { "field" => 42 } }, array_rows.first[3]) # struct_col
    assert_nil array_rows.first.fetch(4) # nullable_col

    # Test each_column variant with hash result type
    columns = []
    Parquet.each_column("test/complex.parquet", result_type: :hash) { |col| columns << col }

    assert_equal [1], columns.first["id"]
    assert_equal [[1, 2, 3]], columns.first["int_array"]
    assert_equal [{ "key" => "value" }], columns.first["map_col"]
    assert_equal [{ "nested" => { "field" => 42 } }], columns.first["struct_col"]
    assert_nil columns.first["nullable_col"].first

    # Test each_column variant with array result type
    array_columns = []
    Parquet.each_column("test/complex.parquet", result_type: :array) { |col| array_columns << col }

    assert_equal [1], array_columns.first[0] # id
    assert_equal [[1, 2, 3]], array_columns.first[1] # int_array
    assert_equal [{ "key" => "value" }], array_columns.first[2] # map_col
    assert_equal [{ "nested" => { "field" => 42 } }], array_columns.first[3] # struct_col
    assert_equal [nil], array_columns.first[4] # nullable_col
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

  def test_empty_table
    rows = []
    Parquet.each_row("test/empty_table.parquet") { |row| rows << row }
    assert_empty rows

    columns = []
    Parquet.each_column("test/empty_table.parquet", result_type: :hash) { |col| columns << col }
    refute_empty columns # Should still return schema info
    assert_empty columns.first["id"]
    assert_empty columns.first["name"]
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
    # Test writing rows with format specified in schema
    rows = [
      ["2024-01-01", "2024-01-01 10:30:00+0000"],
      ["2024-01-02", "2024-01-02 14:45:00+0000"],
      ["2024-01-03", "2024-01-03 09:15:00+0000"]
    ].each

    Parquet.write_rows(
      rows,
      schema: [
        { "date" => { "type" => "date32", "format" => "%Y-%m-%d" } },
        { "timestamp" => { "type" => "timestamp_millis", "format" => "%Y-%m-%d %H:%M:%S%z" } }
      ],
      write_to: "test/formatted.parquet"
    )

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

    rows = Parquet.each_row("test/formatted.parquet").to_a
    assert_equal 3, rows.length

    assert_equal "2024-01-01", rows[0]["date"].to_s
    assert_equal "2024-01-01 10:30:00 UTC", rows[0]["timestamp"].to_s

    assert_equal "2024-01-02", rows[1]["date"].to_s
    assert_equal "2024-01-02 14:45:00 UTC", rows[1]["timestamp"].to_s

    assert_equal "2024-01-03", rows[2]["date"].to_s
    assert_equal "2024-01-03 09:15:00 UTC", rows[2]["timestamp"].to_s

    # Verify column-based data matches row-based data
    rows = Parquet.each_row("test/formatted_columns.parquet").to_a
    assert_equal 3, rows.length

    assert_equal "2024-01-01", rows[0]["date"].to_s
    assert_equal "2024-01-01 10:30:00 UTC", rows[0]["timestamp"].to_s

    assert_equal "2024-01-02", rows[1]["date"].to_s
    assert_equal "2024-01-02 14:45:00 UTC", rows[1]["timestamp"].to_s

    assert_equal "2024-01-03", rows[2]["date"].to_s
    assert_equal "2024-01-03 09:15:00 UTC", rows[2]["timestamp"].to_s
  ensure
    File.delete("test/formatted.parquet") if File.exist?("test/formatted.parquet")
    File.delete("test/formatted_columns.parquet") if File.exist?("test/formatted_columns.parquet")
  end

  def test_complex_types_write_read
    temp_path = "test/complex_types.parquet"

    begin
      # Test data with lists and maps
      data = [
        # Row 1: Various types of lists and maps
        [
          1,
          %w[apple banana cherry], # list<string>
          [10, 20, 30, 40], # list<int32>
          [1.1, 2.2, 3.3], # list<double>
          { "Alice" => 20, "Bob" => 30 }, # map<string,int32>
          { 1 => "one", 2 => "two", 3 => "three" } # map<int32,string>
        ],
        # Row 2: Empty collections and nil values
        [
          2,
          [], # empty list<string>
          [5], # list<int32> with one item
          [], # nil list<double>
          {}, # empty map<string,int32>
          { 10 => "ten" } # map<int32,string> with one item
        ],
        # Row 3: Mixed values
        [
          3,
          ["mixed", nil, "values"], # list<string> with nil
          [100, 200, 300], # list<int32>
          [5.5, 6.6, 7.7], # list<double>
          { "key1" => 1, "key2" => 2, "key3" => nil }, # map<string,int32> with nil value
          { 5 => "five", 6 => nil } # map<int32,string> with nil value
        ]
      ]

      # Create schema with list and map types
      schema = [
        { "id" => "int32" },
        { "string_list" => "list<string>" },
        { "int_list" => "list<int32>" },
        { "double_list" => "list<double>" },
        { "string_int_map" => "map<string,int32>" },
        { "int_string_map" => "map<int32,string>" }
      ]

      # Write rows to Parquet file
      Parquet.write_rows(data.each, schema: schema, write_to: temp_path)

      # Read back and verify row-based data
      rows = Parquet.each_row(temp_path).to_a
      assert_equal 3, rows.length

      # Verify Row 1
      assert_equal 1, rows[0]["id"]
      assert_equal %w[apple banana cherry], rows[0]["string_list"]
      assert_equal [10, 20, 30, 40], rows[0]["int_list"]
      assert_equal [1.1, 2.2, 3.3], rows[0]["double_list"].map { |v| v.round(1) }
      assert_equal({ "Alice" => 20, "Bob" => 30 }, rows[0]["string_int_map"])
      assert_equal({ 1 => "one", 2 => "two", 3 => "three" }, rows[0]["int_string_map"])

      # Verify Row 2
      assert_equal 2, rows[1]["id"]
      assert_equal [], rows[1]["string_list"]
      assert_equal [5], rows[1]["int_list"]
      assert_equal [], rows[1]["double_list"]
      assert_equal({}, rows[1]["string_int_map"])
      assert_equal({ 10 => "ten" }, rows[1]["int_string_map"])

      # Verify Row 3
      assert_equal 3, rows[2]["id"]
      assert_equal ["mixed", nil, "values"], rows[2]["string_list"]
      assert_equal [100, 200, 300], rows[2]["int_list"]
      assert_equal [5.5, 6.6, 7.7], rows[2]["double_list"].map { |v| v.round(1) }
      assert_equal({ "key1" => 1, "key2" => 2, "key3" => nil }, rows[2]["string_int_map"])
      assert_equal({ 5 => "five", 6 => nil }, rows[2]["int_string_map"])

      # Test column-based writing
      column_batches = [
        [
          [1, 2, 3], # id column
          [%w[apple banana cherry], [], ["mixed", nil, "values"]], # string_list column
          [[10, 20, 30, 40], [5], [100, 200, 300]], # int_list column
          [[1.1, 2.2, 3.3], nil, [5.5, 6.6, 7.7]], # double_list column
          [{ "Alice" => 20, "Bob" => 30 }, {}, { "key1" => 1, "key2" => 2, "key3" => nil }], # string_int_map column
          [{ 1 => "one", 2 => "two", 3 => "three" }, { 10 => "ten" }, { 5 => "five", 6 => nil }] # int_string_map column
        ]
      ]

      # Write columns to Parquet file
      Parquet.write_columns(column_batches.each, schema: schema, write_to: "#{temp_path}_columns")

      # Read back and verify column-based data
      column_rows = Parquet.each_row("#{temp_path}_columns").to_a
      assert_equal 3, column_rows.length

      # Spot check a few values to make sure column writing worked too
      assert_equal 1, column_rows[0]["id"]
      assert_equal %w[apple banana cherry], column_rows[0]["string_list"]
      assert_equal [5], column_rows[1]["int_list"]
      assert_equal [100, 200, 300], column_rows[2]["int_list"]
      assert_equal({ "Alice" => 20, "Bob" => 30 }, column_rows[0]["string_int_map"])
      assert_equal({ 10 => "ten" }, column_rows[1]["int_string_map"])
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
      File.delete("#{temp_path}_columns") if File.exist?("#{temp_path}_columns")
    end
  end

  def test_schema_dsl
    temp_path = "test_schema_dsl.parquet"

    # Define a complex nested schema using the DSL
    schema =
      Parquet::Schema.define do
        field "id", :int32
        field "name", :string

        # Nested struct with fields
        field "address", :struct do
          field "street", :string
          field "city", :string
          field "zip", :int32
          field "coordinates", :struct do
            field "latitude", :double
            field "longitude", :double
          end
        end

        # List of primitives
        field "tags", :list, item: :string

        # List of structs
        field "contacts", :list, item: :struct do
          field "name", :string
          field "phone", :string
          field "primary", :boolean
        end

        # Map with primitive values
        field "metadata", :map, key: :string, value: :string

        # Map with struct values
        field "scores", :map, key: :string, value: :struct do
          field "value", :double
          field "timestamp", :int64
        end
      end

    begin
      # Create test data with nested structures as arrays (not hashes)
      # to match the expected input format for write_rows
      data = [
        [
          1, # id
          "Alice", # name
          { # address struct
            "street" => "123 Main St",
            "city" => "Springfield",
            "zip" => 12_345,
            "coordinates" => {
              "latitude" => 37.7749,
              "longitude" => -122.4194
            }
          },
          %w[developer ruby], # tags
          [ # contacts
            { "name" => "Bob", "phone" => "555-1234", "primary" => true },
            { "name" => "Charlie", "phone" => "555-5678", "primary" => false }
          ],
          { # metadata
            "created" => "2023-01-01",
            "updated" => "2023-02-15"
          },
          { # scores
            "math" => {
              "value" => 95.5,
              "timestamp" => 1_672_531_200
            },
            "science" => {
              "value" => 88.0,
              "timestamp" => 1_672_617_600
            }
          }
        ],
        [
          2, # id
          "Bob", # name
          { # address struct
            "street" => "456 Oak Ave",
            "city" => "Rivertown",
            "zip" => 67_890,
            "coordinates" => {
              "latitude" => 40.7128,
              "longitude" => -74.0060
            }
          },
          ["designer"], # tags
          [{ "name" => "Alice", "phone" => "555-4321", "primary" => true }], # contacts
          { # metadata
            "created" => "2023-01-15"
          },
          { # scores
            "art" => {
              "value" => 99.0,
              "timestamp" => 1_673_740_800
            }
          }
        ]
      ]

      # Write data to Parquet file
      Parquet.write_rows(data.each, schema: schema, write_to: temp_path)

      # Read back and verify
      rows = Parquet.each_row(temp_path).to_a
      assert_equal 2, rows.length

      # Verify first row's complex nested structure
      assert_equal 1, rows[0]["id"]
      assert_equal "Alice", rows[0]["name"]

      # Verify nested struct
      assert_equal "123 Main St", rows[0]["address"]["street"]
      assert_equal "Springfield", rows[0]["address"]["city"]
      assert_equal 12_345, rows[0]["address"]["zip"]
      assert_equal 37.7749, rows[0]["address"]["coordinates"]["latitude"]
      assert_equal(-122.4194, rows[0]["address"]["coordinates"]["longitude"])

      # Verify list of primitives
      assert_equal %w[developer ruby], rows[0]["tags"]

      # Verify list of structs
      assert_equal 2, rows[0]["contacts"].length
      assert_equal "Bob", rows[0]["contacts"][0]["name"]
      assert_equal "555-1234", rows[0]["contacts"][0]["phone"]
      assert_equal true, rows[0]["contacts"][0]["primary"]

      # Verify maps
      assert_equal "2023-01-01", rows[0]["metadata"]["created"]
      assert_equal 95.5, rows[0]["scores"]["math"]["value"]
      assert_equal 1_672_531_200, rows[0]["scores"]["math"]["timestamp"]

      # Verify second row
      assert_equal 2, rows[1]["id"]
      assert_equal "Bob", rows[1]["name"]
      assert_equal "Rivertown", rows[1]["address"]["city"]
      assert_equal ["designer"], rows[1]["tags"]
      assert_equal 1, rows[1]["contacts"].length
      assert_equal "Alice", rows[1]["contacts"][0]["name"]
      assert_equal "2023-01-15", rows[1]["metadata"]["created"]
      assert_nil rows[1]["metadata"]["updated"]
      assert_equal 99.0, rows[1]["scores"]["art"]["value"]
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_strict_utf8_enforcement
    invalid_utf8_bytes = [0xC3, 0x28].pack("C*") # Example of an invalid sequence

    temp_path = "test/strict_utf8_enforcement.parquet"
    schema = [{ "payload" => "string" }]

    begin
      error =
        assert_raises(EncodingError) do
          Parquet.write_rows([[invalid_utf8_bytes]].each, schema: schema, write_to: temp_path)
        end

      assert_match(/invalid utf-8 sequence/i, error.message)
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_reading_non_parquet_file
    non_parquet_path = "test/non_parquet.txt"
    File.write(non_parquet_path, "Just some text data.")

    error = assert_raises(RuntimeError) { Parquet.each_row(non_parquet_path).to_a }

    assert_match(/Failed to open file|Invalid Parquet/, error.message)
  ensure
    File.delete(non_parquet_path) if File.exist?(non_parquet_path)
  end

  def test_corrupted_parquet_file
    temp_path = "test/a_data.parquet"
    corrupted_path = "test/corrupted_data.parquet"

    begin
      # Create a simple parquet file first
      schema = [{ "id" => "int32" }, { "name" => "string" }]
      Parquet.write_rows([[1, "test"]].each, schema: schema, write_to: temp_path)

      original_data = File.binread(temp_path)
      # Truncate the file halfway to simulate corruption
      File.binwrite(corrupted_path, original_data[0, original_data.size / 2])

      error = assert_raises(RuntimeError) { Parquet.each_row(corrupted_path).to_a }

      assert_match(/Failed to open file|EOF|Parquet error/, error.message)
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
      File.delete(corrupted_path) if File.exist?(corrupted_path)
    end
  end

  def test_mismatched_schema_write_rows
    temp_path = "test/mismatched_schema_rows.parquet"
    schema = [{ "id" => "int64" }, { "name" => "string" }]

    # Our data enumerator incorrectly yields an array of length 3
    data = [
      [1, "Alice", "ExtraColumn"] # 3 columns, but schema has only 2
    ]

    begin
      error = assert_raises(RuntimeError) { Parquet.write_rows(data.each, schema: schema, write_to: temp_path) }
      assert_match(/Row length|schema length|mismatch/, error.message)
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_enumerator_interrupt_partial_write
    temp_path = "test/partial_write.parquet"
    schema = [{ "id" => "int64" }, { "name" => "string" }]

    enumerator =
      Enumerator.new do |yielder|
        yielder << [1, "Alice"]
        yielder << [2, "Bob"]
        raise "Simulated stream failure"
      end

    begin
      error = assert_raises(RuntimeError) { Parquet.write_rows(enumerator, schema: schema, write_to: temp_path) }
      assert_equal("Simulated stream failure", error.message)

      # The file may exist but might be partially written or truncated.
      assert File.exist?(temp_path)

      # Optionally: attempt to read what is there, or confirm it's not valid.
      # We'll just check that it doesn't catastrophically break reading API:
      assert_raises(RuntimeError) { Parquet.each_row(temp_path).to_a }
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_decimal_out_of_range
    temp_path = "test/decimal_out_of_range.parquet"
    schema = [{ "small_int" => "int8" }]

    # Data enumerator yields a value out of int8 range
    data = [
      [9999] # far larger than int8's max of 127
    ]

    begin
      error =
        assert_raises(RangeError, RuntimeError) { Parquet.write_rows(data.each, schema: schema, write_to: temp_path) }
      assert_match(/fixnum too big to convert into/, error.message)
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_duplicate_columns_different_order
    temp_path = "test/repeated_different_order.parquet"
    # We'll create a file that has repeated columns or changed order.
    # For simplicity, write a small file:
    schema = [{ "col" => "int32" }, { "col" => "int32" }, { "another_col" => "string" }]

    begin
      error =
        assert_raises(ArgumentError) do
          Parquet.write_rows([[1, 2, "one-two"], [3, 4, "three-four"]].each, schema: schema, write_to: temp_path)
        end

      assert_match(/Duplicate field names in root level schema/, error.message)
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_dsl_struct_missing_subfields
    temp_path = "test/dsl_struct_missing_subfields.parquet"

    schema =
      Parquet::Schema.define do
        field "id", :int32
        field "info", :struct do
          field "x", :int32
          field "y", :int32
          field "z", :string
        end
      end

    # Notice row data's `info` only has x and y, missing z
    data = [[1, { "x" => 100, "y" => 200 }]]

    begin
      Parquet.write_rows(data.each, schema: schema, write_to: temp_path)

      rows = Parquet.each_row(temp_path).to_a
      assert_equal 1, rows.size
      assert_equal 1, rows[0]["id"]
      assert_equal 100, rows[0]["info"]["x"]
      assert_equal 200, rows[0]["info"]["y"]
      assert_nil rows[0]["info"]["z"] # The missing subfield
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_dsl_schema_writer
    temp_path = "test/dsl_schema_writer.parquet"

    schema =
      Parquet::Schema.define do
        field "id", :int32
        field "name", :string
        field "active", :boolean
        field "score", :float
        field "created_at", :timestamp_millis
        field "tags", :list, item: :string
        field "metadata", :map, key: :string, value: :string
        field "nested", :struct do
          field "x", :int32
          field "y", :int32
          field "deep", :struct do
            field "value", :string
          end
        end
        field "numbers", :list, item: :int64
        field "binary_data", :binary
      end

    data = [
      [
        1,
        "John Doe",
        true,
        95.5,
        Time.now,
        %w[ruby parquet],
        { "version" => "1.0", "env" => "test" },
        { "x" => 10, "y" => 20, "deep" => { "value" => "nested value" } },
        [100, 200, 300],
        "binary\x00data".b
      ],
      [
        2,
        "Jane Smith",
        false,
        82.3,
        Time.now - 86_400,
        %w[data processing],
        { "status" => "active" },
        { "x" => 30, "y" => 40, "deep" => { "value" => "another nested value" } },
        [400, 500],
        "more\x00binary".b
      ]
    ]

    begin
      Parquet.write_rows(data.each, schema: schema, write_to: temp_path)

      # Read back and verify
      rows = Parquet.each_row(temp_path).to_a
      assert_equal 2, rows.size

      # First row
      assert_equal 1, rows[0]["id"]
      assert_equal "John Doe", rows[0]["name"]
      assert_equal true, rows[0]["active"]
      assert_in_delta 95.5, rows[0]["score"], 0.001
      assert_instance_of Time, rows[0]["created_at"]
      assert_equal %w[ruby parquet], rows[0]["tags"]
      assert_equal({ "version" => "1.0", "env" => "test" }, rows[0]["metadata"])
      assert_equal 10, rows[0]["nested"]["x"]
      assert_equal 20, rows[0]["nested"]["y"]
      assert_equal "nested value", rows[0]["nested"]["deep"]["value"]
      assert_equal [100, 200, 300], rows[0]["numbers"]
      assert_equal "binary\x00data".b, rows[0]["binary_data"]

      # Second row
      assert_equal 2, rows[1]["id"]
      assert_equal "Jane Smith", rows[1]["name"]
      assert_equal false, rows[1]["active"]
      assert_in_delta 82.3, rows[1]["score"], 0.001
      assert_instance_of Time, rows[1]["created_at"]
      assert_equal %w[data processing], rows[1]["tags"]
      assert_equal({ "status" => "active" }, rows[1]["metadata"])
      assert_equal 30, rows[1]["nested"]["x"]
      assert_equal 40, rows[1]["nested"]["y"]
      assert_equal "another nested value", rows[1]["nested"]["deep"]["value"]
      assert_equal [400, 500], rows[1]["numbers"]
      assert_equal "more\x00binary".b, rows[1]["binary_data"]
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_schema_validation_for_non_null_fields
    schema =
      Parquet::Schema.define do
        field :id, :int64, nullable: false
        field :name, :string, nullable: false
        field :optional_field, :string, nullable: true
      end

    # Valid data with all required fields
    valid_data = [[1, "Test Name", "optional value"]]

    # Missing required field (name)
    invalid_data = [[2, nil, "optional value"]]

    # Test valid data works
    temp_path = "test_validation_valid.parquet"
    begin
      Parquet.write_rows(valid_data.each, schema: schema, write_to: temp_path)
      assert File.exist?(temp_path), "Parquet file with valid data should be created"
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end

    # Test invalid data raises error
    temp_path = "test_validation_invalid.parquet"
    begin
      error = assert_raises { Parquet.write_rows(invalid_data.each, schema: schema, write_to: temp_path) }
      assert_match(/Cannot write nil value for non-nullable field/i, error.message)
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_complex_schema_with_nested_types
    schema =
      Parquet::Schema.define do
        field :id, :int32, nullable: false
        field :name, :string
        field :age, :int16
        field :weight, :float
        field :active, :boolean
        field :last_seen, :timestamp_millis, nullable: true
        field :scores, :list, item: :int32
        field :details, :struct do
          field :name, :string
          field :score, :double
        end
        field :tags, :list, item: :string
        field :metadata, :map, key: :string, value: :string
        field :properties, :map, key: :string, value: :int32
        field :complex_map, :map, key: :string, value: :struct do
          field :count, :int32
          field :description, :string
        end
        field :nested_lists, :list, item: :list do
          field :item, :string
        end
        field :map_of_lists, :map, key: :string, value: :list do
          field :item, :int32
        end
      end

    # Create test data with all the complex types - as an array of arrays for write_rows
    test_data = [
      [
        1, # id
        "John Doe", # name
        30, # age
        75.5, # weight
        true, # active
        Time.now, # last_seen
        [85, 90, 95], # scores
        { # details struct
          "name" => "John's Details",
          "score" => 92.7
        },
        %w[ruby parquet test], # tags
        { # metadata
          "role" => "admin",
          "department" => "engineering"
        },
        { # properties
          "priority" => 1,
          "status" => 2
        },
        { # complex_map
          "feature1" => {
            "count" => 5,
            "description" => "Main feature"
          },
          "feature2" => {
            "count" => 3,
            "description" => "Secondary feature"
          }
        },
        [%w[a b], %w[c d e]], # nested_lists
        { # map_of_lists
          "group1" => [1, 2, 3],
          "group2" => [4, 5, 6]
        }
      ]
    ]

    temp_path = "test_complex_schema.parquet"
    begin
      # Write the data using write_rows
      Parquet.write_rows(test_data.each, schema: schema, write_to: temp_path)

      # Read it back and verify using each_row
      rows = Parquet.each_row(temp_path).to_a
      assert_equal 1, rows.size

      # Verify all fields
      row = rows[0]
      assert_equal 1, row["id"]
      assert_equal "John Doe", row["name"]
      assert_equal 30, row["age"]
      assert_in_delta 75.5, row["weight"], 0.001
      assert_equal true, row["active"]
      assert_instance_of Time, row["last_seen"]
      assert_equal [85, 90, 95], row["scores"]
      assert_equal "John's Details", row["details"]["name"]
      assert_in_delta 92.7, row["details"]["score"], 0.001
      assert_equal %w[ruby parquet test], row["tags"]
      assert_equal({ "role" => "admin", "department" => "engineering" }, row["metadata"])
      assert_equal({ "priority" => 1, "status" => 2 }, row["properties"])

      # Check complex map
      assert_equal 5, row["complex_map"]["feature1"]["count"]
      assert_equal "Main feature", row["complex_map"]["feature1"]["description"]
      assert_equal 3, row["complex_map"]["feature2"]["count"]
      assert_equal "Secondary feature", row["complex_map"]["feature2"]["description"]

      # Check nested lists
      assert_equal %w[a b], row["nested_lists"][0]
      assert_equal %w[c d e], row["nested_lists"][1]

      # Check map of lists
      assert_equal [1, 2, 3], row["map_of_lists"]["group1"]
      assert_equal [4, 5, 6], row["map_of_lists"]["group2"]
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end
end
