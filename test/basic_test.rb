# frozen_string_literal: true

require "parquet"
require "minitest/autorun"

class BasicTest < Minitest::Test
  def test_each_row
    rows = []
    Parquet.each_row("test/data.parquet") { |row| rows << row }
    refute_empty rows
    assert_kind_of Hash, rows.first
    assert_equal rows.first.keys.sort, %w[id name].sort # Verify expected columns
    assert rows.all? { |row| row.is_a?(Hash) } # Verify all rows are hashes
    assert rows.all? { |row| row.key?("id") && row.key?("name") } # Verify schema consistency
  end

  def test_each_row_array
    rows = []
    Parquet.each_row("test/data.parquet", result_type: :array) { |row| rows << row }
    refute_empty rows
    assert_kind_of Array, rows.first
    assert_equal 2, rows.first.length # Verify expected number of columns
    assert rows.all? { |row| row.is_a?(Array) } # Verify all rows are arrays
    assert rows.all? { |row| row.length == rows.first.length } # Verify consistent column count
  end

  def test_each_row_with_file_open
    rows = []
    File.open("test/data.parquet", "rb") { |file| Parquet.each_row(file) { |row| rows << row } }
    refute_empty rows
    assert_kind_of Hash, rows.first
    assert_equal rows.first.keys.sort, %w[id name].sort # Verify expected columns
    assert rows.all? { |row| row.is_a?(Hash) } # Verify all rows are hashes
    assert rows.all? { |row| row.key?("id") && row.key?("name") } # Verify schema consistency
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
  end

  def test_each_row_with_columns
    rows = []
    Parquet.each_row("test/data.parquet", columns: %w[id]) { |row| rows << row }
    refute_empty rows
    assert_kind_of Hash, rows.first
    assert_equal rows.first.keys.sort, %w[id].sort # Verify expected columns
    assert rows.all? { |row| row.is_a?(Hash) } # Verify all rows are hashes
    assert rows.all? { |row| row.key?("id") } # Verify schema consistency
  end
end
