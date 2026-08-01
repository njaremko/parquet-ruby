# frozen_string_literal: true

# `Parquet.repack` denotes a pure function on rows:
#
#   rows    = concat(rows(input) for input in read_from)
#   outputs = partition(rows_per_file, rows)
#
# The first group of tests states that denotation as executable laws. The rest
# cover the request contract (what is rejected, and as what) and the two
# physical strategies — byte-level row-group splicing and Arrow re-encoding —
# which must be indistinguishable in the result.

require_relative "test_helper"
require "bigdecimal"
require "fileutils"
require "tmpdir"

class RepackTest < Minitest::Test
  FIXTURE_DIR = File.expand_path(__dir__)

  def setup
    @tmp_dir = Dir.mktmpdir("parquet_repack_test")
  end

  def teardown
    FileUtils.remove_entry(@tmp_dir) if @tmp_dir && Dir.exist?(@tmp_dir)
  end

  # --------------------------------------------------------------------------
  # Laws
  # --------------------------------------------------------------------------

  def test_concatenation_preserves_every_row_in_order
    inputs = 3.times.map do |file_index|
      write_rows(
        "input_#{file_index}.parquet",
        (0...4).map { |row| [file_index * 4 + row, "name_#{file_index}_#{row}"] }
      )
    end
    expected = inputs.flat_map { |path| Parquet.each_row(path).to_a }

    outputs = Parquet.repack(inputs, output_dir: output_dir)

    assert_equal [{ "path" => output_path(0), "num_rows" => 12 }], outputs
    assert_equal expected, rows_of(outputs)
  end

  def test_split_outputs_hold_exactly_rows_per_file_except_the_last
    input = write_rows("input.parquet", (0...5).map { |row| [row, "name_#{row}"] })
    expected = Parquet.each_row(input).to_a

    outputs = Parquet.repack(input, output_dir: output_dir, rows_per_file: 2)

    assert_equal [
      { "path" => output_path(0), "num_rows" => 2 },
      { "path" => output_path(1), "num_rows" => 2 },
      { "path" => output_path(2), "num_rows" => 1 }
    ], outputs
    assert_equal expected, rows_of(outputs)
  end

  def test_row_count_divisible_by_rows_per_file_produces_no_trailing_empty_file
    input = write_rows("input.parquet", (0...4).map { |row| [row, "name_#{row}"] })

    outputs = Parquet.repack(input, output_dir: output_dir, rows_per_file: 2)

    assert_equal [
      { "path" => output_path(0), "num_rows" => 2 },
      { "path" => output_path(1), "num_rows" => 2 }
    ], outputs
  end

  def test_zero_rows_still_produce_one_empty_output
    empty = write_rows("empty.parquet", [])

    outputs = Parquet.repack([empty, empty], output_dir: output_dir, rows_per_file: 5)

    assert_equal [{ "path" => output_path(0), "num_rows" => 0 }], outputs
    assert_equal [], rows_of(outputs)
  end

  # `max_read_rows_per_chunk` is a resource control, not part of the meaning, so
  # varying it must not change the result.
  #
  # "The result" is the contractual surface: the returned list, the rows, the
  # schema, and the per-column codecs. It deliberately excludes compressed byte
  # counts and page boundaries, which do shift with the chunk size because the
  # encoder checks its page budget once per batch. Those are representation, not
  # meaning, and no part of the contract depends on them.
  def test_result_is_independent_of_read_chunk_size
    input = write_rows("input.parquet", (0...5_000).map { |row| [row, "name_#{row}"] })
    expected = Parquet.each_row(input).to_a

    results =
      [1, 7, 512, 100_000].map do |chunk|
        dir = File.join(@tmp_dir, "chunk_#{chunk}")
        outputs =
          Parquet.repack(input, output_dir: dir, rows_per_file: 1_200, max_read_rows_per_chunk: chunk)
        [
          outputs.map { |output| output["num_rows"] },
          rows_of(outputs),
          outputs.map { |output| schema_of(output["path"]) },
          outputs.map { |output| codecs_of(output["path"]) }
        ]
      end

    assert_equal [results.first] * 4, results
    assert_equal [1_200, 1_200, 1_200, 1_200, 200], results.first[0]
    assert_equal expected, results.first[1]
  end

  def test_output_namespace_equals_the_returned_files
    input = write_rows("input.parquet", (0...10).map { |row| [row, "name_#{row}"] })

    outputs = Parquet.repack(input, output_dir: output_dir, rows_per_file: 2)

    assert_equal outputs.map { |output| output["path"] }.sort,
                 Dir.children(output_dir).sort.map { |name| File.join(output_dir, name) }
  end

  def test_output_parquet_schema_is_identical_to_the_first_input
    input =
      write_rows(
        "input.parquet",
        [
          [1, "a", BigDecimal("10.25"), Time.utc(2024, 1, 1, 12, 0, 0)],
          [2, "b", BigDecimal("20.50"), Time.utc(2024, 1, 2, 12, 0, 0)]
        ],
        schema: {
          fields: [
            { name: "id", type: :int64, nullable: false },
            { name: "name", type: :string },
            { name: "amount", type: :decimal, precision: 10, scale: 2 },
            { name: "created_at", type: :timestamp_micros, has_timezone: true }
          ]
        }
      )

    outputs = Parquet.repack(input, output_dir: output_dir)

    assert_equal schema_of(input), schema_of(outputs.first["path"])
  end

  # --------------------------------------------------------------------------
  # Physical strategies
  # --------------------------------------------------------------------------

  # When the codec matches, whole row groups are copied byte-for-byte rather
  # than decoded and re-encoded. Identical per-column compressed sizes are the
  # observable proof: a re-encode rebuilds dictionaries and page boundaries and
  # does not reproduce them exactly, as the contrasting case below shows.
  def test_matching_codec_splices_column_chunks_instead_of_re_encoding
    input =
      write_rows("input.parquet", (0...5_000).map { |row| [row, "name_#{row}"] }, compression: "zstd")

    spliced = Parquet.repack(input, output_dir: File.join(@tmp_dir, "spliced"))
    re_encoded = Parquet.repack(input, output_dir: File.join(@tmp_dir, "re_encoded"), compression: "snappy")

    assert_equal chunk_sizes_of(input), chunk_sizes_of(spliced.first["path"])
    refute_equal chunk_sizes_of(input), chunk_sizes_of(re_encoded.first["path"])
  end

  # A row group that does not fit the remaining budget cannot be copied whole,
  # so it falls back to the re-encode path mid-file. The rows must still come
  # back exactly, and the split must land on the requested boundary.
  def test_row_group_straddling_an_output_boundary_falls_back_to_re_encoding
    input = write_rows("input.parquet", (0...5_000).map { |row| [row, "name_#{row}"] })
    expected = Parquet.each_row(input).to_a

    outputs = Parquet.repack(input, output_dir: output_dir, rows_per_file: 3_000)

    assert_equal [3_000, 2_000], outputs.map { |output| output["num_rows"] }
    assert_equal expected, rows_of(outputs)
  end

  # An explicit codec is honoured even though it forbids copying, so the rows
  # take the re-encode path and must come back unchanged.
  def test_explicit_compression_re_encodes_and_preserves_rows
    input =
      write_rows("input.parquet", (0...20).map { |row| [row, "name_#{row}"] }, compression: "snappy")
    expected = Parquet.each_row(input).to_a

    outputs = Parquet.repack(input, output_dir: output_dir, compression: "zstd")

    assert_equal %w[ZSTD ZSTD], codecs_of(outputs.first["path"])
    assert_equal expected, rows_of(outputs)
  end

  # Nested columns are the case most likely to break when a row group is cut at
  # an output boundary, because the slice must respect repetition levels.
  def test_nested_columns_survive_slicing_across_output_boundaries
    schema = {
      fields: [
        { name: "id", type: :int64 },
        { name: "tags", type: :list, item: { type: :string } },
        { name: "meta", type: :struct,
          fields: [{ name: "k", type: :string }, { name: "v", type: :int32 }] }
      ]
    }
    rows = (0...7).map { |row| [row, ["t#{row}", "u#{row}"], { "k" => "key#{row}", "v" => row * 10 }] }
    input = write_rows("input.parquet", rows, schema: schema)
    expected = Parquet.each_row(input).to_a

    outputs =
      Parquet.repack(input, output_dir: output_dir, rows_per_file: 3, max_read_rows_per_chunk: 7)

    assert_equal [3, 3, 1], outputs.map { |output| output["num_rows"] }
    assert_equal expected, rows_of(outputs)
  end

  # A Parquet file may compress each column with a different codec. With no
  # `compression:` every column keeps its own, whichever path its row group
  # takes: a copied chunk carries its codec in its own metadata, and a
  # re-encoded one is written with the codec observed for that column.
  def test_unspecified_compression_preserves_each_columns_codec
    input = fixture("repack_mixed_codecs.parquet")

    outputs = Parquet.repack(input, output_dir: output_dir)

    assert_equal %w[ZSTD GZIP], codecs_of(input)
    assert_equal %w[ZSTD GZIP], codecs_of(outputs.first["path"])
  end

  def test_explicit_compression_overrides_every_columns_codec
    input = fixture("repack_mixed_codecs.parquet")

    outputs = Parquet.repack(input, output_dir: output_dir, compression: "zstd")

    assert_equal %w[ZSTD ZSTD], codecs_of(outputs.first["path"])
  end

  # A file's row groups must agree on whether they carry a page index — the
  # Parquet footer cannot represent a mixture, and building one used to abort
  # the call. A copied row group can only contribute the index its source had,
  # so an input without one forces the whole output to go without.
  def test_inputs_without_a_page_index_can_be_split_mid_row_group
    input = fixture("repack_no_page_index.parquet")

    outputs = Parquet.repack(input, output_dir: output_dir, rows_per_file: 2_500)

    assert_equal [2_500, 2_500], outputs.map { |output| output["num_rows"] }
    assert_equal (0...5_000).to_a, rows_of(outputs).map { |row| row["id"] }
  end

  # A row group with no rows contributes nothing, so it must not be able to
  # influence how the rows are partitioned. Deciding splice eligibility used to
  # create the output file, which published an extra empty one.
  def test_a_zero_row_row_group_does_not_add_an_output
    input = fixture("repack_zero_row_group.parquet")

    outputs = Parquet.repack(input, output_dir: output_dir, rows_per_file: 4)

    assert_equal [{ "path" => output_path(0), "num_rows" => 4 }], outputs
    assert_equal ["batch-0.parquet"], Dir.children(output_dir)
    assert_equal [0, 1, 2, 3], rows_of(outputs).map { |row| row["id"] }
  end

  # Copying row groups one-for-one would make compaction reproduce the very
  # fragmentation it is meant to remove, and would run into the Parquet limit of
  # 32767 row groups per file. Small groups go through the re-encode path and
  # merge instead.
  def test_many_small_inputs_are_merged_rather_than_copied_one_row_group_each
    inputs =
      200.times.map do |index|
        write_rows("small-#{index}.parquet", (0...5).map { |row| [index * 5 + row, "v#{row}"] })
      end

    outputs = Parquet.repack(inputs, output_dir: output_dir)

    assert_equal [{ "path" => output_path(0), "num_rows" => 1_000 }], outputs
    assert_equal 1, Parquet.metadata(outputs.first["path"])["row_groups"].size
    assert_equal (0...1_000).to_a, rows_of(outputs).map { |row| row["id"] }
  end

  # Concatenability is a property of the columns, not of who wrote the file.
  # These two fixtures hold the same `id` column but differ in file-level
  # key/value metadata, which must not make them unmergeable. They are written
  # by pyarrow because this gem cannot produce that difference; regenerate with:
  #
  #   uv run --with pyarrow --with pandas python -c "
  #   import pyarrow as pa, pyarrow.parquet as pq, pandas as pd
  #   pq.write_table(pa.table({'id': pa.array([1, 2], pa.int64())}),
  #                  'repack_no_kv_metadata.parquet', compression='snappy')
  #   pd.DataFrame({'id': [3, 4]}).astype('int64').to_parquet(
  #       'repack_pandas_kv_metadata.parquet', engine='pyarrow', compression='snappy')"
  def test_inputs_differing_only_in_key_value_metadata_concatenate
    inputs = %w[repack_no_kv_metadata.parquet repack_pandas_kv_metadata.parquet].map { |n| fixture(n) }

    outputs = Parquet.repack(inputs, output_dir: output_dir)

    assert_equal [{ "path" => output_path(0), "num_rows" => 4 }], outputs
    assert_equal [{ "id" => 1 }, { "id" => 2 }, { "id" => 3 }, { "id" => 4 }], rows_of(outputs)
  end

  # --------------------------------------------------------------------------
  # Output namespace ownership
  # --------------------------------------------------------------------------

  def test_repack_refuses_to_write_into_an_occupied_output_namespace
    input = write_rows("input.parquet", (0...10).map { |row| [row, "name_#{row}"] })
    Parquet.repack(input, output_dir: output_dir, rows_per_file: 2)

    error =
      assert_raises(ArgumentError) do
        Parquet.repack(input, output_dir: output_dir, rows_per_file: 10)
      end

    assert_match(/already contains 5 "batch"-\* file\(s\)/, error.message)
    assert_match(/overwrite: true/, error.message)
    # The refusal must leave the previous result exactly as it was.
    assert_equal 5, Dir.children(output_dir).size
  end

  def test_overwrite_replaces_the_namespace_and_removes_superseded_files
    input = write_rows("input.parquet", (0...10).map { |row| [row, "name_#{row}"] })
    expected = Parquet.each_row(input).to_a
    Parquet.repack(input, output_dir: output_dir, rows_per_file: 2)

    outputs = Parquet.repack(input, output_dir: output_dir, rows_per_file: 10, overwrite: true)

    assert_equal [{ "path" => output_path(0), "num_rows" => 10 }], outputs
    assert_equal ["batch-0.parquet"], Dir.children(output_dir).sort
    assert_equal expected, rows_of(outputs)
  end

  # `batch-00.parquet` names the same slot as `batch-0.parquet` but is not a
  # spelling repack produces, so a run that wrote fewer files than the last one
  # could leave it behind next to the file that superseded it — and a directory
  # glob would read both. Membership is decided by name, so cleanup must be too.
  def test_overwrite_removes_namespace_members_repack_would_not_have_named
    input = write_rows("input.parquet", (0...10).map { |row| [row, "name_#{row}"] })
    expected = Parquet.each_row(input).to_a
    FileUtils.mkdir_p(output_dir)
    FileUtils.cp(input, File.join(output_dir, "batch-00.parquet"))
    FileUtils.cp(input, File.join(output_dir, "batch-9.parquet"))

    outputs = Parquet.repack(input, output_dir: output_dir, overwrite: true)

    assert_equal [{ "path" => output_path(0), "num_rows" => 10 }], outputs
    assert_equal ["batch-0.parquet"], Dir.children(output_dir).sort
    assert_equal expected, rows_of(outputs)
  end

  def test_overwrite_leaves_files_outside_the_namespace_alone
    input = write_rows("input.parquet", [[1, "a"]])
    FileUtils.mkdir_p(output_dir)
    bystanders = ["notes.txt", "other-0.parquet", "batch-0.parquet.bak"]
    bystanders.each { |name| File.write(File.join(output_dir, name), "keep me") }

    Parquet.repack(input, output_dir: output_dir, overwrite: true)

    assert_equal (bystanders + ["batch-0.parquet"]).sort, Dir.children(output_dir).sort
    assert_equal ["keep me"] * 3, bystanders.map { |name| File.read(File.join(output_dir, name)) }
  end

  # --------------------------------------------------------------------------
  # Request contract
  # --------------------------------------------------------------------------

  def test_repack_rejects_mismatched_input_schemas
    input_a = write_rows("input_a.parquet", [[1]], schema: [{ "id" => "int64" }])
    input_b = write_rows("input_b.parquet", [["1"]], schema: [{ "id" => "string" }])

    error =
      assert_raises(ArgumentError) do
        Parquet.repack([input_a, input_b], output_dir: output_dir, rows_per_file: 10)
      end

    assert_match(/schema does not match/, error.message)
    assert_match(/column "id" is BYTE_ARRAY \(String\), expected INT64/, error.message)
    # A rejected request must not create the output directory.
    refute Dir.exist?(output_dir)
  end

  def test_repack_rejects_invalid_sizes
    input = write_rows("input.parquet", [[1]], schema: [{ "id" => "int64" }])

    assert_raises(ArgumentError) { Parquet.repack(input, output_dir: output_dir, rows_per_file: 0) }
    assert_raises(ArgumentError) do
      Parquet.repack(input, output_dir: output_dir, max_read_rows_per_chunk: 0)
    end
  end

  def test_repack_rejects_output_file_prefixes_that_escape_the_output_directory
    input = write_rows("input.parquet", [[1]], schema: [{ "id" => "int64" }])

    ["../escaped", "foo/../bar", "..", ".", "a/b", "/tmp/escaped", "/etc/passwd", ""].each do |prefix|
      error =
        assert_raises(ArgumentError, "expected #{prefix.inspect} to be rejected") do
          Parquet.repack(input, output_dir: output_dir, output_file_prefix: prefix)
        end
      assert_match(/output_file_prefix/, error.message)
    end

    refute Dir.exist?(output_dir)
  end

  def test_repack_rejects_an_empty_output_dir
    input = write_rows("input.parquet", [[1]], schema: [{ "id" => "int64" }])

    error = assert_raises(ArgumentError) { Parquet.repack(input, output_dir: "") }

    assert_match(/output_dir must not be empty/, error.message)
  end

  def test_repack_rejects_invalid_compression
    input = write_rows("input.parquet", [[1]], schema: [{ "id" => "int64" }])

    error =
      assert_raises(ArgumentError) do
        Parquet.repack(input, output_dir: output_dir, compression: "invalid")
      end

    assert_match(/Invalid compression option/, error.message)
  end

  def test_repack_raises_io_error_for_a_missing_input
    error =
      assert_raises(IOError) do
        Parquet.repack(File.join(@tmp_dir, "absent.parquet"), output_dir: output_dir)
      end

    assert_match(/absent\.parquet/, error.message)
  end

  def test_repack_rejects_non_string_paths
    assert_raises(TypeError) { Parquet.repack([1], output_dir: output_dir) }
    assert_raises(TypeError) { Parquet.repack(42, output_dir: output_dir) }
  end

  private

  DEFAULT_SCHEMA = [{ "id" => "int64" }, { "name" => "string" }].freeze

  def write_rows(name, rows, schema: DEFAULT_SCHEMA, compression: "zstd")
    path = File.join(@tmp_dir, name)
    Parquet.write_rows(rows, schema: schema, write_to: path, compression: compression)
    path
  end

  def output_dir
    File.join(@tmp_dir, "output")
  end

  def output_path(index, prefix: "batch")
    File.join(output_dir, "#{prefix}-#{index}.parquet")
  end

  def fixture(name)
    File.join(FIXTURE_DIR, name)
  end

  def rows_of(outputs)
    outputs.flat_map { |output| Parquet.each_row(output["path"]).to_a }
  end

  # Codec names only; the level is a writer-side knob Parquet never records.
  def codecs_of(path)
    Parquet.metadata(path)["row_groups"]
           .flat_map { |group| group["columns"].map { |column| column["compression"] } }
           .map { |codec| codec.split("(").first }
  end

  # Per-column compressed byte counts, in row-group order.
  def chunk_sizes_of(path)
    Parquet.metadata(path)["row_groups"]
           .flat_map { |group| group["columns"].map { |column| column["total_compressed_size"] } }
  end

  def schema_of(path)
    Parquet.metadata(path).fetch("schema").fetch("fields").map do |field|
      field.slice(
        "name", "type", "physical_type", "converted_type", "logical_type",
        "precision", "scale", "repetition"
      )
    end
  end
end
