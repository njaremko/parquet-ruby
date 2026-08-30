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

  class PullTrapEnumerable
    include Enumerable

    attr_reader :pulls

    def initialize
      @pulls = 0
    end

    def each
      return enum_for(:each) unless block_given?

      @pulls += 1
      raise "input was pulled"
    end
  end

  ROW_SCHEMA = [{ "id" => "int64" }, { "name" => "string" }].freeze

  def setup
    @paths = []
  end

  def teardown
    @paths.each { |path| File.delete(path) if File.exist?(path) || File.symlink?(path) }
  end

  def test_write_rows_pulls_from_each_without_materializing_input
    path = output_path("lazy_rows")
    rows = ToATrapEnumerable.new do |yielder|
      2_500.times { |index| yielder << [index, "row-#{index}"] }
    end

    Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: path, batch_size: 37, flush_threshold: 4_096)

    written = Parquet.each_row(path).to_a
    expected = (0...2_500).map { |index| { "id" => index, "name" => "row-#{index}" } }
    assert_equal expected, written
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
    expected = (0...2_500).map { |index| { "id" => index, "name" => "row-#{index}" } }
    assert_equal expected, written
  end

  def test_empty_row_and_column_streams_produce_the_same_complete_observation
    row_path = output_path("empty_rows")
    column_path = output_path("empty_columns")

    Parquet.write_rows([].each, schema: ROW_SCHEMA, write_to: row_path)
    Parquet.write_columns([].each, schema: ROW_SCHEMA, write_to: column_path)

    assert_equal [[], []], [Parquet.each_row(row_path).to_a, Parquet.each_row(column_path).to_a]
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

  def test_interrupt_preserves_existing_destination_and_cleans_the_stage
    Dir.mktmpdir("parquet-ruby-existing-interrupt") do |directory|
      destination = File.join(directory, "destination.parquet")
      File.binwrite(destination, "existing destination")
      interruption = Interrupt.new("write interrupted")
      rows = Enumerator.new do |yielder|
        yielder << [1, "one"]
        raise interruption
      end

      error = assert_raises(Interrupt) do
        Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: destination)
      end

      assert_same interruption, error
      assert_equal "existing destination", File.binread(destination)
      assert_equal ["destination.parquet"], Dir.children(directory)
    end
  end

  def test_interrupt_leaves_an_absent_destination_absent_and_cleans_the_stage
    Dir.mktmpdir("parquet-ruby-absent-interrupt") do |directory|
      destination = File.join(directory, "destination.parquet")
      interruption = Interrupt.new("write interrupted")
      rows = Enumerator.new do |yielder|
        yielder << [1, "one"]
        raise interruption
      end

      error = assert_raises(Interrupt) do
        Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: destination)
      end

      assert_same interruption, error
      refute File.exist?(destination)
      assert_empty Dir.children(directory)
    end
  end

  def test_atomic_path_publication_preserves_creation_and_writable_file_permissions
    skip "Unix permission modes are not available" if Gem.win_platform?

    reference = output_path("permission_reference")
    new_path = output_path("new_permissions")
    existing_paths = {
      0o666 => output_path("existing_read_write_permissions"),
      0o777 => output_path("existing_executable_permissions"),
      0o4666 => output_path("existing_setuid_permissions"),
      0o2777 => output_path("existing_setgid_permissions")
    }
    File.binwrite(reference, "reference")
    existing_paths.each do |mode, path|
      File.binwrite(path, "existing")
      File.chmod(mode, path)
    end
    existing_metadata = existing_paths.to_h do |mode, path|
      metadata = File.stat(path)
      [path, [metadata.uid, metadata.gid, mode]]
    end

    Parquet.write_rows([[1, "one"]], schema: ROW_SCHEMA, write_to: new_path)
    existing_paths.each_value do |path|
      Parquet.write_rows([[2, "two"]], schema: ROW_SCHEMA, write_to: path)
    end

    assert_equal File.stat(reference).mode & 0o777, File.stat(new_path).mode & 0o777
    existing_paths.each do |mode, path|
      metadata = File.stat(path)
      actual_metadata = [metadata.uid, metadata.gid, metadata.mode & 0o7777]
      assert_equal existing_metadata.fetch(path), actual_metadata
      assert_equal({ "id" => 2, "name" => "two" }, Parquet.each_row(path).first)
    end
  end

  def test_atomic_path_publication_updates_a_symlink_target_without_replacing_the_link
    skip "Unix symlinks are not available" if Gem.win_platform?

    target = output_path("symlink_target")
    link = output_path("symlink")
    File.binwrite(target, "existing target")
    File.symlink(target, link)

    Parquet.write_rows([[7, "seven"]], schema: ROW_SCHEMA, write_to: link)

    assert_equal(
      [true, target, { "id" => 7, "name" => "seven" }],
      [File.symlink?(link), File.readlink(link), Parquet.each_row(target).first]
    )
  end

  def test_atomic_path_publication_rejects_hard_links_before_pulling_input
    skip "Unix hard links are not available" if Gem.win_platform?

    destination = output_path("hard_link_destination")
    alias_path = output_path("hard_link_alias")
    File.binwrite(destination, "existing destination")
    File.link(destination, alias_path)
    rows = PullTrapEnumerable.new

    error = assert_raises(RuntimeError) do
      Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: destination)
    end

    assert_equal(
      [
        RuntimeError,
        "Failed to create staging file: refusing to atomically replace #{destination}: destination has 2 hard links",
        0,
        true,
        "existing destination",
        "existing destination"
      ],
      [
        error.class,
        error.message,
        rows.pulls,
        File.identical?(destination, alias_path),
        File.binread(destination),
        File.binread(alias_path)
      ]
    )
  end

  def test_read_only_destination_is_rejected_before_pulling_input
    skip "Unix permission modes are not available" if Gem.win_platform?
    skip "root bypasses Unix mode write checks" if Process.euid.zero?

    destination = output_path("read_only_destination")
    File.binwrite(destination, "existing destination")
    File.chmod(0o444, destination)
    batches = PullTrapEnumerable.new

    error = assert_raises(RuntimeError) do
      Parquet.write_columns(batches, schema: ROW_SCHEMA, write_to: destination)
    end

    assert_equal(
      [
        RuntimeError,
        "Failed to create staging file: destination is not writable: #{destination}",
        0,
        0o444,
        "existing destination"
      ],
      [
        error.class,
        error.message,
        batches.pulls,
        File.stat(destination).mode & 0o777,
        File.binread(destination)
      ]
    )
  ensure
    File.chmod(0o644, destination) if destination && File.exist?(destination)
  end

  def test_existing_destination_publication_uses_atomic_last_committer_wins
    skip "Unix file identities are not available" if Gem.win_platform?

    destination = output_path("raced_destination")
    displaced = output_path("raced_displaced")
    File.binwrite(destination, "original destination")
    rows = Enumerator.new do |yielder|
      yielder << [1, "one"]
      File.rename(destination, displaced)
      File.binwrite(destination, "concurrent replacement")
    end

    Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: destination)

    assert_equal(
      [
        { "id" => 1, "name" => "one" },
        "original destination"
      ],
      [Parquet.each_row(destination).first, File.binread(displaced)]
    )
  end

  def test_publication_does_not_clobber_a_destination_created_during_encoding
    skip "Unix no-clobber publication is not available" if Gem.win_platform?

    destination = output_path("concurrently_created_destination")
    rows = Enumerator.new do |yielder|
      yielder << [1, "one"]
      File.binwrite(destination, "concurrent creation")
    end

    error = assert_raises(RuntimeError) do
      Parquet.write_rows(rows, schema: ROW_SCHEMA, write_to: destination)
    end

    assert_equal(
      [
        RuntimeError,
        "Failed to publish staging file to #{destination}: destination changed while writing: #{destination}",
        "concurrent creation"
      ],
      [error.class, error.message, File.binread(destination)]
    )
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
