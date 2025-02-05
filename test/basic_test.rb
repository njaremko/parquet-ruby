# frozen_string_literal: true

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
    File.open("test/data.parquet", "rb") { |file| Parquet.each_row(file) { |row| rows << row } }
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

  def test_write_large_file
    # Create a temporary file path
    temp_path = "test/large_data.parquet"
    begin
      # Generate schema for id and large string column
      schema = [
        { "id" => "int64" },
        { "data" => "string" }
      ]

      # Write 100k rows using an enumerator to avoid memory allocation
      Parquet.write_rows(
        Enumerator.new do |yielder|
          5000.times do |i|
            large_string = ('a'..'z').to_a[rand(26)] * 1_000_000
            yielder << [i, large_string]
          end
        end,
        schema: schema,
        write_to: temp_path,
      )

      # Verify file exists and has content
      assert File.exist?(temp_path)
      assert File.size(temp_path) > 0

      # Read back first few rows to verify structure
      rows = []
      Parquet.each_row(temp_path) do |row|
        rows << row
      end

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
    batches = []
    Parquet.each_column("test/data.parquet", batch_size: 0) { |col| batches << col }
    assert_empty batches
  end

  def test_each_column_with_negative_batch_size
    assert_raises(RangeError) { Parquet.each_column("test/data.parquet", batch_size: -1) { |_| } }
  end

  def test_each_column_with_batch_size
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
      Enumerator.new do |yielder|
        row_count.times do |i|
          yielder << [i, "name_#{i}"]
        end
      end,
      schema: [
        { "id" => "int64" },
        { "name" => "string" }
      ],
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
      Enumerator.new do |yielder|
        yielder << (0...column_count).to_a
      end,
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
    Parquet.write_rows(
      [[1, 2, 3, 4, 5]].each,
      schema: schema,
      write_to: "test/special_chars.parquet"
    )

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
      schema: [
        { "true_col" => "bool" },
        { "false_col" => "bool" },
        { "null_bool" => "bool" }
      ],
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
      schema: [
        { "id" => "int64" },
        { "name" => "string" }
      ],
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
        [Time.new(2024, 1, 1, 0, 0, 0, 'UTC'), Time.new(2024, 1, 2, 0, 0, 0, 'UTC')], # date column
        [Time.new(2024, 1, 1, 10, 30, 0, 'UTC'), Time.new(2024, 1, 2, 14, 45, 0, 'UTC')], # timestamp column
        [true, false]
      ],
      # Second batch
      [
        [3, 4], # id column
        ["Charlie", nil], # name column
        [88.7, nil], # score column
        [Time.new(2024, 1, 3, 0, 0, 0, 'UTC'), nil], # date column
        [Time.new(2024, 1, 3, 9, 15, 0, 'UTC'), nil], # timestamp column
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
end
