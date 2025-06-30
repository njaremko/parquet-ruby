# frozen_string_literal: true
require "tempfile"
require "bigdecimal"

require "parquet"
require "minitest/autorun"

class DecimalPrecisionTest < Minitest::Test
  # Tests for decimal precision/scale rules from schema.rb:
  # 1. When neither precision nor scale is provided, use maximum precision (38) with scale 0
  # 2. When only precision is provided, scale defaults to 0
  # 3. When only scale is provided, use maximum precision (38)
  # 4. When both are provided, use the provided values

  def test_decimal_default_values
    # Test case 1: Neither precision nor scale provided
    schema =
      Parquet::Schema.define do
        field :decimal_default, :decimal
      end

    # Verify the schema hash has expected precision/scale values
    assert_equal 38, schema[:fields][0][:precision]
    assert_equal 0, schema[:fields][0][:scale]
  end

  def test_decimal_precision_only
    # Test case 2: Only precision provided
    schema =
      Parquet::Schema.define do
        field :decimal_precision_only, :decimal, precision: 10
      end

    # Verify the schema hash has expected precision/scale values
    assert_equal 10, schema[:fields][0][:precision]
    assert_equal 0, schema[:fields][0][:scale]
  end

  def test_decimal_scale_only
    # Test case 3: Only scale provided
    schema =
      Parquet::Schema.define do
        field :decimal_scale_only, :decimal, scale: 5
      end

    # Verify the schema hash has expected precision/scale values
    assert_equal 38, schema[:fields][0][:precision]
    assert_equal 5, schema[:fields][0][:scale]
  end

  def test_decimal_both_precision_and_scale
    # Test case 4: Both precision and scale provided
    schema =
      Parquet::Schema.define do
        field :decimal_both, :decimal, precision: 15, scale: 4
      end

    # Verify the schema hash has expected precision/scale values
    assert_equal 15, schema[:fields][0][:precision]
    assert_equal 4, schema[:fields][0][:scale]
  end

  def test_decimal_in_struct
    # Test decimals in struct fields follow the same rules
    schema =
      Parquet::Schema.define do
        field :nested, :struct do
          field :decimal_default, :decimal
          field :decimal_precision_only, :decimal, precision: 10
          field :decimal_scale_only, :decimal, scale: 5
          field :decimal_both, :decimal, precision: 15, scale: 4
        end
      end

    # Get the struct fields
    struct_fields = schema[:fields][0][:fields]

    # Verify each decimal field follows the rules
    assert_equal 38, struct_fields[0][:precision]
    assert_equal 0, struct_fields[0][:scale]

    assert_equal 10, struct_fields[1][:precision]
    assert_equal 0, struct_fields[1][:scale]

    assert_equal 38, struct_fields[2][:precision]
    assert_equal 5, struct_fields[2][:scale]

    assert_equal 15, struct_fields[3][:precision]
    assert_equal 4, struct_fields[3][:scale]
  end

  def test_decimal_in_list
    # Test decimal items in lists follow the same rules
    schema =
      Parquet::Schema.define do
        field :list_default, :list, item: :decimal
        field :list_precision, :list, item: :decimal, precision: 12
        field :list_scale, :list, item: :decimal, scale: 3
        field :list_both, :list, item: :decimal, precision: 20, scale: 6
      end

    # Verify each list's item follows the decimal rules
    assert_equal 38, schema[:fields][0][:item][:precision]
    assert_equal 0, schema[:fields][0][:item][:scale]

    # This test is failing because the implementation isn't propagating precision/scale to list items
    # According to the documented behavior in schema.rb, this should be:
    assert_equal 12, schema[:fields][1][:item][:precision]
    assert_equal 0, schema[:fields][1][:item][:scale]

    assert_equal 38, schema[:fields][2][:item][:precision]
    assert_equal 3, schema[:fields][2][:item][:scale]

    assert_equal 20, schema[:fields][3][:item][:precision]
    assert_equal 6, schema[:fields][3][:item][:scale]
  end

  def test_decimal_in_map_value
    # Test decimal values in maps follow the same rules
    schema =
      Parquet::Schema.define do
        field :map_default, :map, key: :string, value: :decimal
        field :map_precision, :map, key: :string, value: :decimal, precision: 12
        field :map_scale, :map, key: :string, value: :decimal, scale: 3
        field :map_both, :map, key: :string, value: :decimal, precision: 20, scale: 6
      end

    # Maps are implemented in a specific way in Parquet
    # Verify value decimal types have proper precision/scale
    assert_equal :decimal, schema[:fields][0][:value][:type]
    assert_equal :decimal, schema[:fields][1][:value][:type]
    assert_equal :decimal, schema[:fields][2][:value][:type]
    assert_equal :decimal, schema[:fields][3][:value][:type]

    # Verify precision/scale of map value fields
    # Default values
    assert_equal 38, schema[:fields][0][:value][:precision]
    assert_equal 0, schema[:fields][0][:value][:scale]

    # Precision only
    assert_equal 12, schema[:fields][1][:value][:precision]
    assert_equal 0, schema[:fields][1][:value][:scale]

    # Scale only
    assert_equal 38, schema[:fields][2][:value][:precision]
    assert_equal 3, schema[:fields][2][:value][:scale]

    # Both precision and scale
    assert_equal 20, schema[:fields][3][:value][:precision]
    assert_equal 6, schema[:fields][3][:value][:scale]
  end

  def test_round_trip_with_different_precision_scale_combinations
    temp_path = "test/decimal_precision_combinations.parquet"
    begin
      # Define a schema with different precision/scale combinations
      schema =
        Parquet::Schema.define do
          field :default_decimal, :decimal
          field :precision_only, :decimal, precision: 10
          field :scale_only, :decimal, scale: 5
          field :both_values, :decimal, precision: 15, scale: 4
        end

      # Create test data with BigDecimal values
      test_data = [
        [
          BigDecimal("123456"), # Default (38,0) - integer value
          BigDecimal("123.45"), # Precision only (10,0) - truncated to integer
          BigDecimal("12.34567"), # Scale only (38,5) - high scale
          BigDecimal("1234.5678") # Both (15,4) - precision and scale
        ],
        [
          BigDecimal("0"),
          BigDecimal("0.5"), # Will be truncated to 0 due to scale 0
          BigDecimal("0.00001"),
          BigDecimal("9999.9999")
        ]
      ]

      # Write the data
      Parquet.write_rows(test_data.each, schema: schema, write_to: temp_path)

      # Read back and verify precision/scale was maintained
      rows = Parquet.each_row(temp_path).to_a
      assert_equal 2, rows.size

      # First row verification
      assert_equal BigDecimal("123456"), rows[0]["default_decimal"]
      assert_equal BigDecimal("123"), rows[0]["precision_only"] # Truncated to integer due to scale 0
      assert_equal BigDecimal("12.34567"), rows[0]["scale_only"]
      assert_equal BigDecimal("1234.5678"), rows[0]["both_values"]

      # Second row verification
      assert_equal BigDecimal("0"), rows[1]["default_decimal"]
      assert_equal BigDecimal("0"), rows[1]["precision_only"] # 0.5 truncated to 0 due to scale 0
      assert_equal BigDecimal("0.00001"), rows[1]["scale_only"]
      assert_equal BigDecimal("9999.9999"), rows[1]["both_values"]

      # Verify metadata precision/scale is correctly stored
      metadata = Parquet.metadata(temp_path)

      # Find each field in the schema metadata
      columns = metadata["schema"]["fields"]
      default_field = columns.find { |f| f["name"] == "default_decimal" }
      precision_only_field = columns.find { |f| f["name"] == "precision_only" }
      scale_only_field = columns.find { |f| f["name"] == "scale_only" }
      both_values_field = columns.find { |f| f["name"] == "both_values" }

      # Verify precision and scale in metadata
      # Parse from logical_type string like "Decimal { scale: 0, precision: 38 }"
      def parse_decimal_info(field)
        if field["logical_type"] =~ /Decimal\s*{\s*scale:\s*(\d+),\s*precision:\s*(\d+)\s*}/
          { precision: $2.to_i, scale: $1.to_i }
        else
          { precision: nil, scale: nil }
        end
      end

      default_info = parse_decimal_info(default_field)
      assert_equal 38, default_info[:precision]
      assert_equal 0, default_info[:scale]

      precision_only_info = parse_decimal_info(precision_only_field)
      assert_equal 10, precision_only_info[:precision]
      assert_equal 0, precision_only_info[:scale]

      scale_only_info = parse_decimal_info(scale_only_field)
      assert_equal 38, scale_only_info[:precision]
      assert_equal 5, scale_only_info[:scale]

      both_values_info = parse_decimal_info(both_values_field)
      assert_equal 15, both_values_info[:precision]
      assert_equal 4, both_values_info[:scale]
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_wrap_subtype_method_decimal_rules
    # This test verifies the wrap_subtype method follows the same rules
    # We can't test wrap_subtype directly as it's private, but we can test it indirectly
    # through nested structures that use it

    schema =
      Parquet::Schema.define do
        # Test decimal in list
        field :list_default, :list, item: :decimal
        field :list_precision, :list, item: :decimal, precision: 8
        field :list_scale, :list, item: :decimal, scale: 3
        field :list_both, :list, item: :decimal, precision: 12, scale: 6

        # Test decimal in map
        field :map_default, :map, key: :string, value: :decimal
        field :map_precision, :map, key: :string, value: :decimal, precision: 8
        field :map_scale, :map, key: :string, value: :decimal, scale: 3
        field :map_both, :map, key: :string, value: :decimal, precision: 12, scale: 6
      end

    # Check list item decimal values
    assert_equal 38, schema[:fields][0][:item][:precision]
    assert_equal 0, schema[:fields][0][:item][:scale]

    # Now with our implementation fix, these should pass
    assert_equal 8, schema[:fields][1][:item][:precision]
    assert_equal 0, schema[:fields][1][:item][:scale]

    assert_equal 38, schema[:fields][2][:item][:precision]
    assert_equal 3, schema[:fields][2][:item][:scale]

    assert_equal 12, schema[:fields][3][:item][:precision]
    assert_equal 6, schema[:fields][3][:item][:scale]

    # Check map value decimal values
    # Maps in the schema have :value property for the value type
    assert_equal 38, schema[:fields][4][:value][:precision]
    assert_equal 0, schema[:fields][4][:value][:scale]

    assert_equal 8, schema[:fields][5][:value][:precision]
    assert_equal 0, schema[:fields][5][:value][:scale]

    assert_equal 38, schema[:fields][6][:value][:precision]
    assert_equal 3, schema[:fields][6][:value][:scale]

    assert_equal 12, schema[:fields][7][:value][:precision]
    assert_equal 6, schema[:fields][7][:value][:scale]
  end
end
