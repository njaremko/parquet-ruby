# frozen_string_literal: true

require "parquet"
require "minitest/autorun"

require "duckdb"

class BasicTest < Minitest::Test
  def setup
    # Create test data using DuckDB
    DuckDB::Database.open do |db|
      db.connect do |con|
        con.query(<<~SQL)
          CREATE TABLE test_data AS
          SELECT
            range::INTEGER as id,
            'name_' || range::VARCHAR as name
          FROM range(1, 4)
        SQL
        con.query("COPY test_data TO 'test/data.parquet' (FORMAT 'parquet')")
      end
    end
  end

  def teardown
    File.delete("test/data.parquet") if File.exist?("test/data.parquet")
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
    DuckDB::Database.open do |db|
      db.connect do |con|
        con.query(<<~SQL)
          CREATE TABLE complex_data AS
          SELECT
            1 as id,
            [1, 2, 3] as int_array,
            {'key': 'value'} as map_col,
            {'nested': {'field': 42}} as struct_col,
            NULL as nullable_col
          FROM range(1)
        SQL
        con.query("COPY complex_data TO 'test/complex.parquet' (FORMAT 'parquet')")
      end
    end

    rows = []
    Parquet.each_row("test/complex.parquet") { |row| rows << row }

    assert_equal 1, rows.first["id"]
    assert_equal [1, 2, 3], rows.first["int_array"]
    assert_equal({ "key" => "value" }, rows.first["map_col"])
    assert_equal({ "nested" => { "field" => 42 } }, rows.first["struct_col"])
    assert_nil rows.first["nullable_col"]
  ensure
    File.delete("test/complex.parquet") if File.exist?("test/complex.parquet")
  end

  def test_numeric_types
    DuckDB::Database.open do |db|
      db.connect do |con|
        con.query(<<~SQL)
          CREATE TABLE numeric_data AS
          SELECT
            CAST(1 AS TINYINT) as int8_col,
            CAST(1000 AS SMALLINT) as int16_col,
            CAST(1000000 AS INTEGER) as int32_col,
            CAST(1000000000000 AS BIGINT) as int64_col,
            CAST(3.14 AS FLOAT) as float32_col,
            CAST(3.14159265359 AS DOUBLE) as float64_col,
            CAST('2023-01-01' AS DATE) as date_col,
            CAST('2023-01-01 12:00:00' AS TIMESTAMP) as timestamp_col,
            CAST('2023-01-01 12:00:00+09:00' AS TIMESTAMP WITH TIME ZONE) as timestamptz_col
          FROM range(1)
        SQL
        con.query("COPY numeric_data TO 'test/numeric.parquet' (FORMAT 'parquet')")
      end
    end

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
    assert_equal "2023-01-01T12:00:00Z", row["timestamp_col"].to_s
    assert_equal "2023-01-01T03:00:00Z", row["timestamptz_col"].to_s
  ensure
    File.delete("test/numeric.parquet") if File.exist?("test/numeric.parquet")
  end

  def test_numeric_types_column
    DuckDB::Database.open do |db|
      db.connect do |con|
        con.query(<<~SQL)
          CREATE TABLE numeric_data AS
          SELECT
            CAST(1 AS TINYINT) as int8_col,
            CAST(1000 AS SMALLINT) as int16_col,
            CAST(1000000 AS INTEGER) as int32_col,
            CAST(1000000000000 AS BIGINT) as int64_col,
            CAST(3.14 AS FLOAT) as float32_col,
            CAST(3.14159265359 AS DOUBLE) as float64_col,
            CAST('2023-01-01' AS DATE) as date_col,
            CAST('2023-01-01T12:00:00Z' AS TIMESTAMP) as timestamp_col,
            CAST('2023-01-01 12:00:00+09:00' AS TIMESTAMP WITH TIME ZONE) as timestamptz_col
          FROM range(1)
        SQL
        con.query("COPY numeric_data TO 'test/numeric.parquet' (FORMAT 'parquet')")
      end
    end

    columns = []
    Parquet.each_column("test/numeric.parquet", result_type: :hash) { |col| columns << col }

    assert_equal [1], columns.first["int8_col"]
    assert_equal [1000], columns.first["int16_col"]
    assert_equal [1_000_000], columns.first["int32_col"]
    assert_equal [1_000_000_000_000], columns.first["int64_col"]
    assert_in_delta 3.14, columns.first["float32_col"].first, 0.0001
    assert_in_delta 3.14159265359, columns.first["float64_col"].first, 0.0000000001
    assert_equal ["2023-01-01"], columns.first["date_col"].map(&:to_s)
    assert_equal ["2023-01-01T12:00:00Z"], columns.first["timestamp_col"].map(&:to_s)
    assert_equal ["2023-01-01T03:00:00Z"], columns.first["timestamptz_col"].map(&:to_s)
  ensure
    File.delete("test/numeric.parquet") if File.exist?("test/numeric.parquet")
  end

  def test_large_file_handling
    row_count = 100_000
    DuckDB::Database.open do |db|
      db.connect do |con|
        con.query(<<~SQL)
          CREATE TABLE large_data AS
          SELECT
            range as id,
            'name_' || range::VARCHAR as name
          FROM range(#{row_count})
        SQL
        con.query("COPY large_data TO 'test/large.parquet' (FORMAT 'parquet')")
      end
    end

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
