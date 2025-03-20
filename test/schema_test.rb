# frozen_string_literal: true
require "tempfile"

require "parquet"
require "minitest/autorun"

class SchemaTest < Minitest::Test
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

    rows = Parquet.each_row("test/formatted.parquet").to_a
    assert_equal 3, rows.length

    assert_equal "2024-01-01", rows[0]["date"].to_s
    assert_equal "2024-01-01 10:30:00 UTC", rows[0]["timestamp"].to_s

    assert_equal "2024-01-02", rows[1]["date"].to_s
    assert_equal "2024-01-02 14:45:00 UTC", rows[1]["timestamp"].to_s

    assert_equal "2024-01-03", rows[2]["date"].to_s
    assert_equal "2024-01-03 09:15:00 UTC", rows[2]["timestamp"].to_s
  ensure
    File.delete("test/formatted.parquet") if File.exist?("test/formatted.parquet")
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

  def test_schema_dsl_list_missing_item
    error =
      assert_raises(ArgumentError) do
        schema =
          Parquet::Schema.define do
            field :id, :int32
            # Invalid: a list type is declared but no `item:` argument
            field :bad_list, :list
          end
      end
    assert_match(/list field.*requires `item:` type/, error.message)
  end

  def test_schema_dsl_unsupported_timestamp_nanos
    schema_hash = {
      type: :struct,
      fields: [
        { name: "id", type: :int64, nullable: true },
        { name: "created_at", type: :timestamp_nanos, nullable: true }
      ]
    }

    # We skip the normal `Parquet::Schema.define` DSL to show a direct hash approach
    data = [[1, Time.now]].each

    temp_path = "test_unsupported_nanos.parquet"
    begin
      error =
        assert_raises(ArgumentError, RuntimeError) do
          Parquet.write_rows(data, schema: schema_hash, write_to: temp_path)
        end
      # In arrow_data_type_to_parquet_schema_type, we raise about TimestampNanos
      assert_match(/TimestampNanos not supported|Unknown type/, error.message)
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_schema_dsl_bogus_type
    schema = {
      type: :struct,
      fields: [{ name: "id", type: :int32, nullable: true }, { name: "bogus", type: :some_bogus_type, nullable: true }]
    }

    data = [[1, "value"]].each
    temp_path = "test_bogus_type.parquet"

    begin
      error = assert_raises(ArgumentError) { Parquet.write_rows(data, schema: schema, write_to: temp_path) }
      assert_match(/Unknown type.*some_bogus_type/i, error.message)
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_schema_dsl_empty_top_level_struct
    # This attempts to define a completely empty struct, which the code disallows
    schema = { type: :struct, fields: [] }
    data = [[]].each # No columns at all

    temp_path = "test_empty_top_level_struct.parquet"
    begin
      error = assert_raises(ArgumentError) { Parquet.write_rows(data, schema: schema, write_to: temp_path) }
      assert_match(/Cannot create a struct with zero fields|Top-level schema must be a Struct/i, error.message)
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end
end