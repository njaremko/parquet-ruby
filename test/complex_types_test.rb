# frozen_string_literal: true
require "tempfile"

require "parquet"
require "minitest/autorun"

class ComplexTypesTest < Minitest::Test
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

  def test_write_rows_nested_list_of_lists_empty_sublist
    schema = [
      { "id" => "int32" },
      # This is a list<list<string>>
      { "nested_list" => "list<list<string>>" }
    ]

    data = [
      [1, [%w[a b], [], ["c"]]], # second sub-list is empty
      [2, []] # entire top-level list empty
    ]

    path = "test_nested_list_of_lists.parquet"
    begin
      Parquet.write_rows(data.each, schema: schema, write_to: path)

      # Read them back
      rows = Parquet.each_row(path).to_a
      assert_equal 2, rows.size

      assert_equal 1, rows[0]["id"]
      assert_equal [%w[a b], [], ["c"]], rows[0]["nested_list"]

      assert_equal 2, rows[1]["id"]
      assert_equal [], rows[1]["nested_list"]
    ensure
      File.delete(path) if File.exist?(path)
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

  def test_uuid_column
    rows = Parquet.each_row("test/uuid.parquet", result_type: :array).to_a
    assert_equal 1, rows.size

    assert_equal "15e22a2e-ada2-448d-adf4-4e54fc84c70c", rows[0][0]
  end
end
