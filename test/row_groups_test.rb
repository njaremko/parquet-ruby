# frozen_string_literal: true

require "tempfile"
require "fileutils"
require "parquet"
require "minitest/autorun"

class RowGroupsTest < Minitest::Test
  ROW_SCHEMA = [{ "id" => "int64" }, { "name" => "string" }].freeze
  TOTAL_ROWS = 20_000

  def setup
    @paths = []
  end

  def teardown
    @paths.each { |dir| FileUtils.rm_rf(dir) }
  end

  def test_each_row_reads_only_the_requested_row_group
    path = multi_row_group_path

    rows = []
    Parquet.each_row(path, row_groups: [1]) { |row| rows << row }

    assert_equal group_bounds[1], rows.length
    expected_ids = (group_starts[1]...group_starts[2]).to_a
    assert_equal expected_ids, rows.map { |row| row["id"] }
  end

  def test_each_row_reads_multiple_row_groups_in_request_order
    path = multi_row_group_path

    rows = []
    Parquet.each_row(path, row_groups: [2, 0]) { |row| rows << row }

    expected_ids = (group_starts[2]...TOTAL_ROWS).to_a + (0...group_starts[1]).to_a
    assert_equal expected_ids, rows.map { |row| row["id"] }
  end

  def test_each_row_row_groups_with_column_projection
    path = multi_row_group_path

    rows = []
    Parquet.each_row(path, columns: ["name"], row_groups: [0]) { |row| rows << row }

    assert_equal group_bounds[0], rows.length
    assert rows.first.key?("name")
    refute rows.first.key?("id")
  end

  def test_each_row_row_groups_without_block_returns_enumerator
    path = multi_row_group_path

    enum = Parquet.each_row(path, row_groups: [1], result_type: :array)

    assert_kind_of Enumerator, enum
    assert_equal group_starts[1], enum.first[0]
    assert_equal group_bounds[1], enum.count
  end

  def test_each_column_reads_only_the_requested_row_group
    path = multi_row_group_path

    batches = []
    Parquet.each_column(path, row_groups: [1], batch_size: 500) { |batch| batches << batch }

    ids = batches.flat_map { |batch| batch["id"] }
    assert_equal group_bounds[1], ids.length
    assert_equal group_starts[1], ids.first
  end

  def test_each_column_row_groups_without_block_returns_enumerator
    path = multi_row_group_path

    enum = Parquet.each_column(path, row_groups: [2])

    assert_kind_of Enumerator, enum
    assert_equal group_bounds[2], enum.to_a.flat_map { |batch| batch["id"] }.length
  end

  # A block-less call only opens the file when the enumerator is iterated, so
  # validation errors surface on first pull.
  def test_row_groups_empty_array_raises
    path = multi_row_group_path

    error = assert_raises(ArgumentError) do
      Parquet.each_row(path, row_groups: []).to_a
    end
    assert_match(/at least one row group index/, error.message)
  end

  def test_row_groups_out_of_range_index_raises
    path = multi_row_group_path
    num_groups = group_bounds.length

    error = assert_raises(ArgumentError) do
      Parquet.each_row(path, row_groups: [num_groups]).to_a
    end
    assert_match(/out of range/, error.message)

    assert_raises(ArgumentError) do
      Parquet.each_column(path, row_groups: [0, num_groups]).to_a
    end
  end

  private

  # Write a file whose rows split across several row groups, and return its
  # path. Row-group boundaries come from `flush_threshold:` (floored at 8MB) so
  # the payload must be large enough for the in-progress row group to cross the
  # threshold; the exact split is read back from metadata rather than assumed.
  def multi_row_group_path
    @multi_row_group_path ||= begin
      path = output_path("row_groups")
      rows = (0...TOTAL_ROWS).lazy.map { |index| [index, ("payload-%06d" % index).ljust(1024, "x")] }
      Parquet.write_rows(
        rows,
        schema: ROW_SCHEMA,
        write_to: path,
        batch_size: 100,
        compression: "none",
        flush_threshold: 8 * 1024 * 1024
      )
      assert_operator Parquet.metadata(path).fetch("row_groups").length, :>=, 2,
        "fixture must produce multiple row groups"
      path
    end
  end

  def group_bounds
    @group_bounds ||= Parquet.metadata(multi_row_group_path).fetch("row_groups").map { |g| g["num_rows"] }
  end

  # Start row (in file order) of each row group, plus a final entry of TOTAL_ROWS.
  def group_starts
    @group_starts ||= group_bounds.each_with_object([0]) { |size, starts| starts << starts.last + size }
  end

  def output_path(name)
    dir = Dir.mktmpdir
    @paths << dir
    File.join(dir, "#{name}.parquet")
  end
end
