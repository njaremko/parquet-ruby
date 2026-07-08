# frozen_string_literal: true

require_relative "test_helper"
require "bigdecimal"
require "fileutils"
require "tmpdir"

class RepackTest < Minitest::Test
  def setup
    @tmp_dir = Dir.mktmpdir("parquet_repack_test")
  end

  def teardown
    FileUtils.remove_entry(@tmp_dir) if @tmp_dir && Dir.exist?(@tmp_dir)
  end

  def test_repack_concatenates_inputs_without_ruby_schema_translation
    input_a = path("input_a.parquet")
    input_b = path("input_b.parquet")
    output_dir = path("output")
    output = File.join(output_dir, "batch-0.parquet")
    schema = {
      fields: [
        { name: "id", type: :int64, nullable: false },
        { name: "name", type: :string },
        { name: "amount", type: :decimal, precision: 10, scale: 2 },
        { name: "created_at", type: :timestamp_micros, has_timezone: true }
      ]
    }

    Parquet.write_rows(
      [
        [1, "a", BigDecimal("10.25"), Time.utc(2024, 1, 1, 12, 0, 0)],
        [2, "b", BigDecimal("20.50"), Time.utc(2024, 1, 2, 12, 0, 0)]
      ],
      schema: schema,
      write_to: input_a,
      compression: "zstd"
    )
    Parquet.write_rows(
      [[3, "c", BigDecimal("30.75"), Time.utc(2024, 1, 3, 12, 0, 0)]],
      schema: schema,
      write_to: input_b,
      compression: "zstd"
    )

    counts =
      Parquet.repack(
        [input_a, input_b],
        output_dir: output_dir,
        compression: "zstd",
        max_read_rows_per_chunk: 1
      )

    assert_equal [{ "path" => output, "num_rows" => 3 }], counts
    assert_equal [1, 2, 3], Parquet.each_row(output).map { |row| row["id"] }
    assert_equal schema_summary(input_a), schema_summary(output)
  end

  def test_repack_splits_outputs_on_row_boundaries_with_custom_prefix
    input = path("input.parquet")
    output_dir = path("output")
    output_a = File.join(output_dir, "part-0.parquet")
    output_b = File.join(output_dir, "part-1.parquet")
    output_c = File.join(output_dir, "part-2.parquet")
    schema = [{ "id" => "int64" }, { "name" => "string" }]
    rows = 5.times.map { |index| [index, "name_#{index}"] }

    Parquet.write_rows(rows, schema: schema, write_to: input)

    counts =
      Parquet.repack(
        input,
        output_file_prefix: "part",
        output_dir: output_dir,
        rows_per_file: 2,
        max_read_rows_per_chunk: 5,
        compression: "zstd"
      )

    assert_equal [
      { "path" => output_a, "num_rows" => 2 },
      { "path" => output_b, "num_rows" => 2 },
      { "path" => output_c, "num_rows" => 1 }
    ], counts
    assert_equal [0, 1], Parquet.each_row(output_a).map { |row| row["id"] }
    assert_equal [2, 3], Parquet.each_row(output_b).map { |row| row["id"] }
    assert_equal [4], Parquet.each_row(output_c).map { |row| row["id"] }
  end

  def test_repack_rejects_mismatched_input_schemas
    input_a = path("input_a.parquet")
    input_b = path("input_b.parquet")
    output_dir = path("output")

    Parquet.write_rows([[1]], schema: [{ "id" => "int64" }], write_to: input_a)
    Parquet.write_rows([["1"]], schema: [{ "id" => "string" }], write_to: input_b)

    error =
      assert_raises(RuntimeError) do
        Parquet.repack([input_a, input_b], output_dir: output_dir, rows_per_file: 10)
      end

    assert_match(/schema does not match/, error.message)
  end

  def test_repack_rejects_invalid_sizes
    input = path("input.parquet")
    Parquet.write_rows([[1]], schema: [{ "id" => "int64" }], write_to: input)

    assert_raises(ArgumentError) do
      Parquet.repack(input, output_dir: path("output"), rows_per_file: 0)
    end

    assert_raises(ArgumentError) do
      Parquet.repack(input, output_dir: path("output"), max_read_rows_per_chunk: 0)
    end
  end

  private

  def path(name)
    File.join(@tmp_dir, name)
  end

  def schema_summary(file)
    Parquet.metadata(file).fetch("schema").fetch("fields").map do |field|
      {
        "name" => field["name"],
        "type" => field["type"],
        "physical_type" => field["physical_type"],
        "converted_type" => field["converted_type"],
        "logical_type" => field["logical_type"],
        "precision" => field["precision"],
        "scale" => field["scale"],
        "repetition" => field["repetition"]
      }
    end
  end
end
