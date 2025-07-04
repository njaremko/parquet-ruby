require_relative 'test_helper'
require 'securerandom'

class UuidTest < Minitest::Test
  def setup
    @test_file = File.join(Dir.tmpdir, "test_uuid_#{Process.pid}.parquet")
  end

  def teardown
    File.delete(@test_file) if File.exist?(@test_file)
  end

  def test_uuid_binary_conversion
    # Create data with UUID strings
    data = []
    uuids = []
    10.times do
      uuid = SecureRandom.uuid
      uuids << uuid
      data << [uuid, "test_#{uuid[0..7]}"]
    end

    # Schema with UUID format
    schema = {
      fields: [
        {name: 'id', type: :binary, format: 'uuid'},
        {name: 'name', type: :string}
      ]
    }

    # Write data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)

    # Read data back
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }

    # Verify UUIDs are correctly preserved
    assert_equal 10, rows.length
    rows.each_with_index do |row, i|
      assert_equal uuids[i], row['id'], "UUID at index #{i} should match"
      assert_equal "test_#{uuids[i][0..7]}", row['name']
    end
  end

  def test_invalid_uuid_format
    # Test with invalid UUID
    data = [["not-a-valid-uuid", "test"]]

    schema = {
      fields: [
        {name: 'id', type: :binary, format: 'uuid'},
        {name: 'name', type: :string}
      ]
    }

    # Should raise an error for invalid UUID
    assert_raises do
      Parquet.write_rows(data, schema: schema, write_to: @test_file)
    end
  end

  def test_uuid_with_different_formats
    # Test various UUID formats
    uuid_formats = [
      "550e8400-e29b-41d4-a716-446655440000",  # Standard format
      "550e8400e29b41d4a716446655440000",      # Without hyphens
      "550E8400-E29B-41D4-A716-446655440000",  # Uppercase
    ]

    data = uuid_formats.map { |uuid| [uuid, "test"] }

    schema = {
      fields: [
        {name: 'id', type: "fixed_len_byte_array(16)", format: 'uuid'},
        {name: 'name', type: :string}
      ]
    }

    # Write data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)

    # Read data back
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }

    # All should be normalized to standard format
    expected_uuid = "550e8400-e29b-41d4-a716-446655440000"
    assert_equal 3, rows.length
    rows.each do |row|
      assert_equal expected_uuid, row['id']
    end
  end

  def test_null_uuid_values
    # Test with null UUIDs
    data = [
      [nil, "null uuid"],
      ["550e8400-e29b-41d4-a716-446655440000", "valid uuid"],
      [nil, "another null"]
    ]

    schema = {
      fields: [
        {name: 'id', type: "fixed_len_byte_array(16)", format: 'uuid', nullable: true},
        {name: 'description', type: :string}
      ]
    }

    # Write data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)

    # Read data back
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }

    assert_equal 3, rows.length
    assert_nil rows[0]['id']
    assert_equal "550e8400-e29b-41d4-a716-446655440000", rows[1]['id']
    assert_nil rows[2]['id']
  end

  def test_uuid_in_complex_types
    # Test UUID in struct
    id_uuid = SecureRandom.uuid
    tracking_uuid = SecureRandom.uuid

    # Data must be arrays, with nested hash for struct
    data = [
      [id_uuid, { tracking_id: tracking_uuid, name: "Test Item" }]
    ]

    schema = {
      fields: [
        {name: 'id', type: :binary, format: 'uuid'},
        {
          name: 'metadata',
          type: :struct,
          fields: [
            {name: 'tracking_id', type: :binary, format: 'uuid'},
            {name: 'name', type: :string}
          ]
        }
      ]
    }

    # Write data
    Parquet.write_rows(data, schema: schema, write_to: @test_file)

    # Read data back
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }

    assert_equal 1, rows.length
    row = rows.first

    # Verify UUIDs are preserved
    assert_equal id_uuid, row['id']
    assert_equal tracking_uuid, row['metadata']['tracking_id']
    assert_equal "Test Item", row['metadata']['name']
  end
end
