require_relative "test_helper"

class WriteStreamingTest < Minitest::Test
  class ToATrapEnumerable
    include Enumerable

    def initialize(&source)
      @source = source
    end

    def each(&block)
      return enum_for(:each) unless block

      Enumerator.new(&@source).each(&block)
    end

    def to_a
      raise "write input must not be materialized"
    end
  end

  ROW_SCHEMA = [{ "id" => "int64" }, { "name" => "string" }].freeze

  def setup
    @paths = []
  end

  def teardown
    @paths.each { |path| File.delete(path) if File.exist?(path) }
  end

  def test_write_rows_pulls_from_each_without_materializing_input
    path = output_path("lazy_rows")
    rows = ToATrapEnumerable.new do |yielder|
      2_500.times { |index| yielder << [index, "row-#{index}"] }
    end

    Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: path, batch_size: 37, flush_threshold: 4_096)

    written = Parquet.each_row(path).to_a
    assert_equal 2_500, written.length
    assert_equal({ "id" => 0, "name" => "row-0" }, written.first)
    assert_equal({ "id" => 2_499, "name" => "row-2499" }, written.last)
  end

  def test_write_columns_pulls_batches_without_materializing_or_accumulating_input
    path = output_path("lazy_columns")
    batches = ToATrapEnumerable.new do |yielder|
      25.times do |batch_index|
        first = batch_index * 100
        ids = (first...(first + 100)).to_a
        names = ids.map { |id| "row-#{id}" }
        yielder << [ids, names]
      end
    end

    Parquet.write_columns(batches, schema: ROW_SCHEMA, write_to: path, flush_threshold: 4_096)

    written = Parquet.each_row(path).to_a
    assert_equal 2_500, written.length
    assert_equal({ "id" => 0, "name" => "row-0" }, written.first)
    assert_equal({ "id" => 2_499, "name" => "row-2499" }, written.last)
  end

  def test_write_columns_rejects_each_malformed_batch_before_later_batches_can_cancel_it
    path = output_path("mismatched_columns")
    batches = [
      [[1, 2], ["one"]],
      [[3], ["two", "three"]]
    ]

    error = assert_raises(RuntimeError) do
      Parquet.write_columns(batches.each, schema: ROW_SCHEMA, write_to: path)
    end

    assert_match(/batch 0.*column.*1.*1 values.*expected 2/i, error.message)
    refute_path_exists(path)
  end

  def test_tiny_memory_quantum_crosses_the_previous_row_group_limit
    path = output_path("tiny_quantum")
    row_count = 32_769
    rows = (0...row_count).lazy.map { |index| [index] }

    Parquet.write_rows(
      rows,
      schema: [{ "id" => "int64" }],
      write_to: path,
      batch_size: 500_000,
      flush_threshold: 1
    )

    assert_equal 1, Parquet.metadata(path).fetch("row_groups").length
    assert_equal row_count, Parquet.each_row(path).count
  end

  def test_late_row_stream_failure_preserves_existing_destination
    path = output_path("existing_rows")
    File.binwrite(path, "existing destination")
    rows = Enumerator.new do |yielder|
      yielder << [1, "one"]
      yielder << [2, "two"]
      raise "late row failure"
    end

    error = assert_raises(RuntimeError) do
      Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: path, batch_size: 1)
    end

    assert_equal "late row failure", error.message
    assert_equal "existing destination", File.binread(path)
  end

  def test_late_column_stream_failure_preserves_existing_destination
    path = output_path("existing_columns")
    File.binwrite(path, "existing destination")
    batches = Enumerator.new do |yielder|
      yielder << [[1, 2], ["one", "two"]]
      raise "late column failure"
    end

    error = assert_raises(RuntimeError) do
      Parquet.write_columns(batches, schema: ROW_SCHEMA, write_to: path)
    end

    assert_equal "late column failure", error.message
    assert_equal "existing destination", File.binread(path)
  end

  def test_atomic_path_publication_preserves_creation_and_existing_file_permissions
    skip "Unix permission modes are not available" if Gem.win_platform?

    reference = output_path("permission_reference")
    new_path = output_path("new_permissions")
    existing_paths = {
      0o666 => output_path("existing_read_write_permissions"),
      0o777 => output_path("existing_executable_permissions"),
      0o444 => output_path("existing_read_only_permissions")
    }
    File.binwrite(reference, "reference")
    existing_paths.each do |mode, path|
      File.binwrite(path, "existing")
      File.chmod(mode, path)
    end

    Parquet.write_rows([[1, "one"]], schema: ROW_SCHEMA, write_to: new_path)
    existing_paths.each_value do |path|
      Parquet.write_rows([[2, "two"]], schema: ROW_SCHEMA, write_to: path)
    end

    assert_equal File.stat(reference).mode & 0o777, File.stat(new_path).mode & 0o777
    existing_paths.each do |mode, path|
      assert_equal mode, File.stat(path).mode & 0o777
      assert_equal({ "id" => 2, "name" => "two" }, Parquet.each_row(path).first)
    end
  end

  private

  def output_path(label)
    path = File.join(Dir.tmpdir, "parquet-ruby-#{label}-#{Process.pid}.parquet")
    @paths << path
    path
  end

  def refute_path_exists(path)
    refute File.exist?(path), "failed writes must not publish #{path}"
  end
end
