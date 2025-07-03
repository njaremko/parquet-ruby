require_relative 'test_helper'
require 'date'

class DateFormatTest < Minitest::Test
  def setup
    @test_file = File.join(Dir.tmpdir, "test_date_format_#{Process.pid}.parquet")
  end

  def teardown
    File.delete(@test_file) if File.exist?(@test_file)
  end

  def test_custom_date_format
    # Create data with dates in custom format
    data = [
      ["2023/12/25", "Christmas"],
      ["2024/01/01", "New Year"],
      ["2024/07/04", "Independence Day"]
    ]
    
    # Schema with custom date format
    schema = {
      fields: [
        {name: 'date', type: :date32, format: '%Y/%m/%d'},
        {name: 'holiday', type: :string}
      ]
    }
    
    # Write data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)
    
    # Read data back
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }
    
    # Verify dates are correctly parsed and stored
    assert_equal 3, rows.length
    
    # Check first row
    date_value = rows[0]['date']
    assert_kind_of Date, date_value
    assert_equal 2023, date_value.year
    assert_equal 12, date_value.month
    assert_equal 25, date_value.day
    assert_equal "Christmas", rows[0]['holiday']
  end
  
  def test_multiple_date_formats
    # Test different date formats in same file
    data = [
      ["25-12-2023", "2023/12/25", "European format"],
      ["01-01-2024", "2024/01/01", "New Year"],
      ["04-07-2024", "2024/07/04", "US Independence"]
    ]
    
    schema = {
      fields: [
        {name: 'euro_date', type: :date32, format: '%d-%m-%Y'},
        {name: 'iso_date', type: :date32, format: '%Y/%m/%d'},
        {name: 'description', type: :string}
      ]
    }
    
    # Write data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)
    
    # Read data back
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }
    
    assert_equal 3, rows.length
    
    # Verify both date columns have same actual date for first row
    euro_date = rows[0]['euro_date']
    iso_date = rows[0]['iso_date']
    
    assert_equal euro_date.year, iso_date.year
    assert_equal euro_date.month, iso_date.month
    assert_equal euro_date.day, iso_date.day
    assert_equal 2023, euro_date.year
    assert_equal 12, euro_date.month
    assert_equal 25, euro_date.day
  end
  
  def test_date64_with_format
    # Test Date64 type with custom format
    data = [
      ["Jan 15, 2024", "Meeting"],
      ["Feb 28, 2024", "Deadline"],
      ["Mar 31, 2024", "Quarter End"]
    ]
    
    schema = {
      fields: [
        {name: 'date', type: :date64, format: '%b %d, %Y'},
        {name: 'event', type: :string}
      ]
    }
    
    # Write data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)
    
    # Read data back
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }
    
    assert_equal 3, rows.length
    
    # Check dates
    date1 = rows[0]['date']
    assert_equal 2024, date1.year
    assert_equal 1, date1.month
    assert_equal 15, date1.day
    
    date2 = rows[1]['date']
    assert_equal 2024, date2.year
    assert_equal 2, date2.month
    assert_equal 28, date2.day
  end
  
  def test_invalid_date_format
    # Test with invalid date according to format
    data = [
      ["not-a-date", "Invalid"]
    ]
    
    schema = {
      fields: [
        {name: 'date', type: :date32, format: '%Y-%m-%d'},
        {name: 'description', type: :string}
      ]
    }
    
    # Should raise an error
    assert_raises do
      Parquet.write_rows(data, schema: schema, write_to: @test_file)
    end
  end
  
  def test_null_dates_with_format
    # Test null dates with custom format
    data = [
      ["2024-01-15", "Valid date"],
      [nil, "Null date"],
      ["2024-02-20", "Another date"]
    ]
    
    schema = {
      fields: [
        {name: 'date', type: :date32, format: '%Y-%m-%d', nullable: true},
        {name: 'description', type: :string}
      ]
    }
    
    # Write data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)
    
    # Read data back
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }
    
    assert_equal 3, rows.length
    assert_kind_of Date, rows[0]['date']
    assert_nil rows[1]['date']
    assert_kind_of Date, rows[2]['date']
  end
  
  def test_default_date_parsing
    # Test that dates without format still parse correctly
    data = [
      ["2024-01-15", "ISO format"],
      ["15/01/2024", "European format"],
      ["Jan 15, 2024", "US format"]
    ]
    
    schema = {
      fields: [
        {name: 'date', type: :date32},  # No format specified
        {name: 'description', type: :string}
      ]
    }
    
    # Write data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)
    
    # Read data back
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }
    
    # All should parse to same date with Ruby's Date.parse
    assert_equal 3, rows.length
    rows.each do |row|
      date = row['date']
      assert_equal 2024, date.year
      assert_equal 1, date.month
      assert_equal 15, date.day
    end
  end
end