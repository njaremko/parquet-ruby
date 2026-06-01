# frozen_string_literal: true
require "tempfile"
require "parquet"
require "minitest/autorun"

# Tests for the `string_storage:` reader option, which controls how Rust string
# values are materialized as Ruby strings: :copy (default), :intern, :shared.
class StringStorageTest < Minitest::Test
  # File column order is deliberately a, b, c so projection-order tests can
  # request columns out of file order.
  SCHEMA = [{ "a" => "int64" }, { "b" => "string" }, { "c" => "string" }].freeze

  def setup
    @path = Tempfile.new(["string_storage", ".parquet"]).path
    # "label" repeats across every row so dedup strategies can collapse it.
    rows = (0...50).map { |i| [i, "label", "row-#{i}"] }
    Parquet.write_rows(rows, schema: SCHEMA, write_to: @path)
  end

  def teardown
    File.unlink(@path) if @path && File.exist?(@path)
  end

  def read_rows(**opts)
    rows = []
    Parquet.each_row(@path, **opts) { |row| rows << row }
    rows
  end

  def test_default_is_copy_with_mutable_distinct_strings
    rows = read_rows
    first = rows[0]["b"]
    second = rows[1]["b"]

    assert_equal "label", first
    refute first.frozen?, "default :copy strings should be mutable"
    refute_same first, second, "default :copy must not share objects"
  end

  def test_intern_returns_frozen_shared_objects_for_equal_values
    rows = read_rows(string_storage: :intern)

    assert_equal(%w[label] * 50, rows.map { |r| r["b"] })
    assert rows[0]["b"].frozen?, ":intern strings must be frozen"
    # Every repeat of "label" is the same interned object.
    assert_same rows[0]["b"], rows[1]["b"]
    assert_same rows[0]["b"], rows[49]["b"]
    # Distinct values remain distinct.
    refute_equal rows[0]["c"], rows[1]["c"]
  end

  def test_shared_returns_frozen_zero_copy_strings
    rows = read_rows(string_storage: :shared)

    assert_equal(%w[label] * 50, rows.map { |r| r["b"] })
    assert rows[0]["b"].frozen?, ":shared strings must be frozen"
    assert_equal "row-7", rows[7]["c"]
  end

  def test_all_modes_agree_on_values
    copy = read_rows
    intern = read_rows(string_storage: :intern)
    shared = read_rows(string_storage: :shared)

    assert_equal copy, intern
    assert_equal copy, shared
  end

  # The :shared leak budget can be set via the hash form. Values stay correct and
  # frozen regardless of the budget (small budgets just leak fewer of them).
  def test_shared_accepts_a_custom_budget_hash
    rows = read_rows(string_storage: { mode: :shared, max_entries: 4, max_value_bytes: 64 })

    assert_equal(%w[label] * 50, rows.map { |r| r['b'] })
    assert rows[0]['b'].frozen?
    assert_equal copy = read_rows, rows
  end

  # A block-less call returns an Enumerator; the budget must survive the round
  # trip so iterating it behaves identically to the block form.
  def test_shared_budget_survives_enumerator_round_trip
    enum = Parquet.each_row(@path, string_storage: { mode: :shared, max_entries: 4, max_value_bytes: 64 })
    rows = enum.to_a

    assert_equal 50, rows.length
    assert_equal 'label', rows[0]['b']
    assert rows[0]['b'].frozen?
  end

  def test_string_storage_hash_requires_mode
    assert_raises(ArgumentError) { read_rows(string_storage: { max_entries: 8 }) }
  end

  def test_string_storage_hash_rejects_unknown_keys
    assert_raises(ArgumentError) do
      read_rows(string_storage: { mode: :shared, max_entry: 8 })
    end
  end

  def test_string_storage_hash_rejects_nonpositive_budget
    assert_raises(ArgumentError) { read_rows(string_storage: { mode: :shared, max_entries: 0 }) }
  end

  def test_string_storage_rejects_budget_for_non_shared_mode
    assert_raises(ArgumentError) { read_rows(string_storage: { mode: :intern, max_entries: 8 }) }
    assert_raises(ArgumentError) { read_rows(string_storage: { mode: :copy, max_value_bytes: 64 }) }
  end

  # Nested struct field-name keys are always interned (frozen + reused), even in
  # the default :copy mode where string VALUES are mutable copies.
  def test_nested_struct_field_keys_are_always_interned
    nested_path = Tempfile.new(["string_storage_nested", ".parquet"]).path
    schema =
      Parquet::Schema.define do
        field :profile, :struct do
          field :name, :string
        end
      end
    Parquet.write_rows([[{ "name" => "Ada" }], [{ "name" => "Ada" }]], schema: schema, write_to: nested_path)

    %i[copy intern shared].each do |mode|
      rows = []
      Parquet.each_row(nested_path, string_storage: mode) { |row| rows << row }

      assert_equal "Ada", rows[0]["profile"]["name"]
      key0 = rows[0]["profile"].keys.first
      key1 = rows[1]["profile"].keys.first
      assert_equal "name", key0
      assert key0.frozen?, "field-name key must be frozen (interned) under #{mode}"
      assert_same key0, key1, "field-name key must be the same interned object across rows under #{mode}"
    end
  ensure
    File.unlink(nested_path) if nested_path && File.exist?(nested_path)
  end

  def test_invalid_string_storage_raises
    assert_raises(ArgumentError) { read_rows(string_storage: :nonsense) }
  end

  # Regression: projected rows are returned in FILE order, so the hash keys must
  # be file-ordered too. Requesting columns out of file order must still pair
  # each value with the right key.
  def test_projection_hash_keys_follow_file_order
    rows = read_rows(columns: %w[c a])

    assert_equal [1, 2], [rows[1]["a"], rows[2]["a"]]
    assert_equal %w[row-1 row-2], [rows[1]["c"], rows[2]["c"]]
    assert_equal %w[a c].sort, rows.first.keys.sort
  end

  def test_projection_with_intern_pairs_correctly
    rows = read_rows(columns: %w[c b], string_storage: :intern)

    assert_equal "label", rows[3]["b"]
    assert_equal "row-3", rows[3]["c"]
    assert rows[3]["b"].frozen?
  end

  # Past the internal leak bound :shared returns frozen owned copies rather than
  # leaking, so every :shared value is frozen regardless of cardinality, and
  # contents stay exact.
  def test_shared_is_uniformly_frozen_beyond_the_leak_bound
    path = Tempfile.new(["string_storage_highcard", ".parquet"]).path
    count = 9000 # exceeds the internal SHARED_LEAK_ENTRY_COUNT_MAX
    rows = (0...count).map { |i| [i, "v#{i}", "w#{i}"] }
    Parquet.write_rows(rows, schema: SCHEMA, write_to: path)

    seen = 0
    Parquet.each_row(path, string_storage: :shared) do |row|
      assert row["b"].frozen?, "every :shared value must be frozen"
      assert_equal "v#{seen}", row["b"]
      seen += 1
    end
    assert_equal count, seen
  ensure
    File.unlink(path) if path && File.exist?(path)
  end

  def test_intern_falls_back_to_frozen_copies_beyond_value_cache
    tempfile = Tempfile.new(["string_storage_intern_highcard", ".parquet"])
    rows = (0...8192).map { |i| [i, "v#{i}", "w#{i}"] }
    rows << [8192, "overflow", "first"]
    rows << [8193, "overflow", "second"]
    Parquet.write_rows(rows, schema: SCHEMA, write_to: tempfile.path)

    overflow_values = []
    Parquet.each_row(tempfile.path, string_storage: :intern) do |row|
      overflow_values << row["b"] if row["b"] == "overflow"
    end

    assert_equal 2, overflow_values.length
    assert overflow_values.all?(&:frozen?)
    refute_same overflow_values[0], overflow_values[1]
  ensure
    tempfile&.close!
  end

  # The interned key cache holds Ruby strings across yields; moving the heap
  # under it (GC.compact) must not corrupt the cached field-name keys.
  def test_interned_keys_survive_compaction
    skip "GC.compact unavailable" unless GC.respond_to?(:compact)

    # Hold the Tempfile object: the test forces GC.compact, which would finalize
    # an unreferenced Tempfile and delete the file out from under the reader.
    tempfile = Tempfile.new(["string_storage_compact", ".parquet"])
    schema =
      Parquet::Schema.define do
        field :profile, :struct do
          field :name, :string
          field :city, :string
        end
      end
    rows = (0...1500).map { |i| [{ "name" => "n#{i % 25}", "city" => "c#{i % 25}" }] }
    Parquet.write_rows(rows, schema: schema, write_to: tempfile.path)

    %i[copy intern shared].each do |mode|
      i = 0
      Parquet.each_row(tempfile.path, string_storage: mode) do |row|
        GC.compact if (i % 100).zero?
        profile = row["profile"]
        assert_equal %w[name city], profile.keys
        assert profile.keys.all?(&:frozen?)
        assert_equal "n#{i % 25}", profile["name"]
        i += 1
      end
      assert_equal rows.length, i
    end
  ensure
    tempfile&.close!
  end

  def test_each_column_honors_string_storage
    batches = []
    Parquet.each_column(@path, string_storage: :intern, batch_size: 10) { |batch| batches << batch }

    sample = batches.first["b"].first
    assert_equal "label", sample
    assert sample.frozen?, ":intern must produce frozen column values"
  end

  def test_each_column_hash_keys_are_interned
    batches = []
    Parquet.each_column(@path, result_type: :hash, batch_size: 10) { |batch| batches << batch }

    first_key = batches[0].keys.find { |key| key == "b" }
    second_key = batches[1].keys.find { |key| key == "b" }

    assert_equal "b", first_key
    assert first_key.frozen?, "top-level column-name keys must be frozen"
    assert_same first_key, second_key, "top-level column-name keys must be reused across batches"
  end
end
