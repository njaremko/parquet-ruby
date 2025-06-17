# frozen_string_literal: true
require "tempfile"
require "parquet"
require "minitest/autorun"

class ArrowTest < Minitest::Test
  def setup
    # Create a test Arrow file
    @arrow_file = "test/test_data.arrow"
    @parquet_file = "test/data.parquet"
  end

  def test_arrow_file_detection
    skip "Arrow test file not available" unless File.exist?(@arrow_file)

    # Test reading Arrow file with each_row
    rows = []
    Parquet.each_row(@arrow_file) { |row| rows << row }
    refute_empty rows
    assert_kind_of Hash, rows.first
  end

  def test_arrow_column_reading
    skip "Arrow test file not available" unless File.exist?(@arrow_file)

    # Test reading Arrow file with each_column
    columns = []
    Parquet.each_column(@arrow_file) { |col| columns << col }
    refute_empty columns
    assert_kind_of Hash, columns.first
  end

  def test_arrow_with_column_selection
    skip "Arrow test file not available" unless File.exist?(@arrow_file)

    # Assuming the Arrow file has an 'id' column
    rows = []
    Parquet.each_row(@arrow_file, columns: ["id"]) { |row| rows << row }
    refute_empty rows
    assert_equal ["id"], rows.first.keys if rows.first.key?("id")
  end

  def test_arrow_file_from_io
    skip "Arrow test file not available" unless File.exist?(@arrow_file)

    rows = []
    File.open(@arrow_file, "rb") do |file|
      Parquet.each_row(file) { |row| rows << row }
    end
    assert_equal [{"id"=>1, "score"=>95.5, "active"=>true, "name"=>"Alice"}, {"id"=>2, "name"=>"Bob", "active"=>false, "score"=>82.3}, {"active"=>true, "name"=>"Charlie", "id"=>3, "score"=>88.7}, {"id"=>4, "active"=>true, "score"=>91.2, "name"=>"David"}, {"active"=>false, "id"=>5, "score"=>86.5, "name"=>"Eve"}], rows
  end

  def test_parquet_still_works
    # Ensure Parquet files still work as before
    rows = []
    Parquet.each_row(@parquet_file) { |row| rows << row }
    assert_equal 3, rows.length
    assert_equal [1, 2, 3], rows.map { |r| r["id"] }
    assert_equal %w[name_1 name_2 name_3], rows.map { |r| r["name"] }
  end

  def test_format_detection_with_wrong_extension
    # Test that format detection works even with wrong file extensions
    Tempfile.create(["test", ".parquet"]) do |temp|
      # Copy an Arrow file but give it a .parquet extension
      if File.exist?(@arrow_file)
        File.write(temp.path, File.binread(@arrow_file))

        rows = []
        Parquet.each_row(temp.path) { |row| rows << row }
        refute_empty rows
      end
    end
  end

  def test_arrow_array_result_type
    skip "Arrow test file not available" unless File.exist?(@arrow_file)

    rows = []
    Parquet.each_row(@arrow_file, result_type: :array) { |row| rows << row }
    refute_empty rows
    assert_kind_of Array, rows.first
  end

  def test_arrow_column_array_result_type
    skip "Arrow test file not available" unless File.exist?(@arrow_file)

    columns = []
    Parquet.each_column(@arrow_file, result_type: :array) { |col| columns << col }
    refute_empty columns
    assert_kind_of Array, columns.first
  end
end
