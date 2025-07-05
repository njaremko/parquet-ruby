# frozen_string_literal: true

require "test_helper"

class FloatPrecisionTest < Minitest::Test
  def test_float32_conversion_precision
    test_values = [
      0.1,
      0.2,
      0.3,
      0.123456789,
      1.0,
      -1.0,
      123.456,
      1e-4,
      1e6,
      Float::INFINITY,
      -Float::INFINITY
    ]

    Tempfile.create(['float_test', '.parquet']) do |file|
      schema = [
        { "float32_col" => "float32" },
        { "float64_col" => "float64" }
      ]

      data = test_values.map do |val|
        [val, val]  # Array format for write_rows
      end

      Parquet.write_rows(data, write_to: file.path, schema: schema)
      result = []
      Parquet.each_row(file.path) { |row| result << row }

      test_values.each_with_index do |original, i|
        f32_val = result[i]['float32_col']
        f64_val = result[i]['float64_col']

        # Skip NaN comparison as NaN != NaN
        next if original.respond_to?(:nan?) && original.nan?

        # For float32, check that we don't have spurious precision
        if original.finite?
          f32_str = f32_val.to_s

          # The string representation shouldn't have excessive decimal places
          # Float32 has ~7 significant digits of precision
          if f32_str.include?('.')
            decimal_places = f32_str.split('.')[1].length
            assert decimal_places <= 8,
              "Float32 value #{original} has spurious precision: #{f32_val} (#{decimal_places} decimal places)"
          end

          # The float64 column should preserve full precision
          assert_in_delta original, f64_val, 1e-15,
            "Float64 value should preserve original: #{original} != #{f64_val}"
        end
      end
    end
  end

  def test_specific_problematic_float32_values
    # These are known problematic values that often show spurious precision
    problematic_values = {
      0.1 => 0.1,  # Often becomes 0.10000000149011612
      0.2 => 0.2,  # Often becomes 0.20000000298023224
      0.3 => 0.3,  # Often becomes 0.30000001192092896
    }

    Tempfile.create(['float_test', '.parquet']) do |file|
      schema = [{ "value" => "float32" }]

      data = problematic_values.keys.map { |val| [val] }

      Parquet.write_rows(data, write_to: file.path, schema: schema)
      result = []
      Parquet.each_row(file.path) { |row| result << row }

      problematic_values.each_with_index do |(original, expected), i|
        actual = result[i]['value']

        # Check that the value is reasonably close to expected
        assert_in_delta expected, actual, 1e-6,
          "Float32 value #{original} converted incorrectly: expected ~#{expected}, got #{actual}"

        # Check string representation doesn't have excessive precision
        str_repr = actual.to_s
        if str_repr.include?('.') && str_repr.split('.')[1].length > 8
          flunk "Float32 value #{original} has spurious precision in string form: '#{str_repr}'"
        end
      end
    end
  end
end
