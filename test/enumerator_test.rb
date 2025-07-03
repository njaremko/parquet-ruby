require_relative 'test_helper'

class EnumeratorTest < Minitest::Test
  def setup
    @test_file = File.join(Dir.tmpdir, "test_enumerator_#{Process.pid}.parquet")
  end

  def teardown
    File.delete(@test_file) if File.exist?(@test_file)
  end

  def test_each_row_returns_enumerator_without_block
    # Create test data
    data = [
      ["Alice", 30, "Engineer"],
      ["Bob", 25, "Designer"],
      ["Charlie", 35, "Manager"]
    ]
    
    schema = {
      fields: [
        {name: 'name', type: :string},
        {name: 'age', type: :int32},
        {name: 'role', type: :string}
      ]
    }
    
    # Write data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)
    
    # Get enumerator
    enum = Parquet.each_row(@test_file)
    
    # Verify it's an Enumerator
    assert_kind_of Enumerator, enum
    
    # Test various enumerator methods
    rows = enum.to_a
    assert_equal 3, rows.length
    assert_equal "Alice", rows[0]['name']
    
    # Test lazy evaluation with take
    first_two = Parquet.each_row(@test_file).take(2)
    assert_equal 2, first_two.length
    assert_equal "Bob", first_two[1]['name']
    
    # Test with select
    adults = Parquet.each_row(@test_file).select { |row| row['age'] >= 30 }
    assert_equal 2, adults.length
    assert_equal ["Alice", "Charlie"], adults.map { |r| r['name'] }
    
    # Test map
    names = Parquet.each_row(@test_file).map { |row| row['name'] }
    assert_equal ["Alice", "Bob", "Charlie"], names
  end
  
  def test_each_column_returns_enumerator_without_block
    # Create test data
    data = [
      ["Alice", 30, true],
      ["Bob", 25, false],
      ["Charlie", 35, true],
      ["David", 28, false]
    ]
    
    schema = {
      fields: [
        {name: 'name', type: :string},
        {name: 'age', type: :int32},
        {name: 'active', type: :boolean}
      ]
    }
    
    # Write data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)
    
    # Get enumerator
    enum = Parquet.each_column(@test_file, batch_size: 2)
    
    # Verify it's an Enumerator
    assert_kind_of Enumerator, enum
    
    # Test iteration
    batches = enum.to_a
    assert batches.length > 0
    
    # First batch should have data
    first_batch = batches.first
    assert_equal ['active', 'age', 'name'], first_batch.keys.sort
    
    # Test with first
    first_batch_again = Parquet.each_column(@test_file, batch_size: 2).first
    assert_equal first_batch, first_batch_again
    
    # Test lazy evaluation with take
    first_two_batches = Parquet.each_column(@test_file, batch_size: 1).take(2)
    assert_equal 2, first_two_batches.length
  end
  
  def test_enumerator_with_options
    # Create test data
    data = [
      ["Alice", 30, "Engineer", 50000],
      ["Bob", 25, "Designer", 45000],
      ["Charlie", 35, "Manager", 60000]
    ]
    
    schema = {
      fields: [
        {name: 'name', type: :string},
        {name: 'age', type: :int32},
        {name: 'role', type: :string},
        {name: 'salary', type: :int32}
      ]
    }
    
    # Write data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)
    
    # Test with columns option
    enum = Parquet.each_row(@test_file, columns: ['name', 'salary'])
    rows = enum.to_a
    assert_equal 3, rows.length
    assert_equal ['name', 'salary'], rows[0].keys.sort
    assert_nil rows[0]['age']  # Column not selected
    
    # Test with result_type array
    enum_array = Parquet.each_row(@test_file, result_type: 'array')
    arrays = enum_array.to_a
    assert_equal 3, arrays.length
    assert_kind_of Array, arrays[0]
    assert_equal 4, arrays[0].length  # All columns
  end
  
  def test_enumerator_chaining
    # Create test data with more rows
    data = (1..10).map { |i| ["Person#{i}", 20 + i, i * 1000] }
    
    schema = {
      fields: [
        {name: 'name', type: :string},
        {name: 'age', type: :int32},
        {name: 'score', type: :int32}
      ]
    }
    
    # Write data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)
    
    # Test complex chaining
    result = Parquet.each_row(@test_file)
      .select { |row| row['age'] > 25 }
      .map { |row| { name: row['name'], score_doubled: row['score'] * 2 } }
      .take(3)
    
    assert_equal 3, result.length
    assert_equal "Person6", result[0][:name]
    assert_equal 12000, result[0][:score_doubled]
  end
  
  def test_enumerator_with_each_entry
    # Create test data
    data = [["A", 1], ["B", 2], ["C", 3]]
    
    schema = {
      fields: [
        {name: 'letter', type: :string},
        {name: 'number', type: :int32}
      ]
    }
    
    # Write data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)
    
    # Test each.with_index
    enum = Parquet.each_row(@test_file)
    indexed = enum.each.with_index.to_a
    
    assert_equal 3, indexed.length
    assert_equal ["A", 0], [indexed[0][0]['letter'], indexed[0][1]]
    assert_equal ["B", 1], [indexed[1][0]['letter'], indexed[1][1]]
  end
end