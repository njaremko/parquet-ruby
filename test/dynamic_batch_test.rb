require_relative 'test_helper'

# Batch sizing is owned by the core writer; these tests verify that the
# `batch_size:`, `flush_threshold:`, and `sample_size:` write options are
# accepted and that data round-trips correctly regardless of how it is batched.
# The batching algorithm itself is covered by the parquet-core Rust tests.
class DynamicBatchTest < Minitest::Test
  SCHEMA = { fields: [{ name: 'id', type: :int32 }, { name: 'value', type: :string }] }.freeze

  def setup
    @test_file = File.join(Dir.tmpdir, "test_batch_#{Process.pid}.parquet")
  end

  def teardown
    File.delete(@test_file) if File.exist?(@test_file)
  end

  def write_and_read(data, **opts)
    Parquet.write_rows(data, schema: SCHEMA, write_to: @test_file, **opts)
    rows = []
    Parquet.each_row(@test_file) { |row| rows << row }
    rows
  end

  def test_fixed_batch_size_round_trips
    data = (0...250).map { |i| [i, "string #{i}"] }
    rows = write_and_read(data, batch_size: 50)

    assert_equal 250, rows.length
    assert_equal 0, rows.first['id']
    assert_equal 'string 249', rows.last['value']
  end

  def test_small_fixed_batch_size_round_trips
    data = (0...5).map { |i| [i, "string #{i}"] }
    rows = write_and_read(data, batch_size: 1)

    assert_equal 5, rows.length
    assert_equal((0...5).to_a, rows.map { |row| row['id'] })
  end

  def test_batch_size_rejects_zero
    data = [[1, 'one']]

    assert_raises(ArgumentError) do
      Parquet.write_rows(data, schema: SCHEMA, write_to: @test_file, batch_size: 0)
    end
  end

  def test_batch_size_rejects_excessive_value
    data = [[1, 'one']]

    assert_raises(ArgumentError) do
      Parquet.write_rows(data, schema: SCHEMA, write_to: @test_file, batch_size: 1_000_001)
    end
  end

  def test_memory_threshold_round_trips_large_rows
    data = (0...100).map { |i| [i, 'x' * 10_000] }
    rows = write_and_read(data, flush_threshold: 1024 * 1024)

    assert_equal 100, rows.length
    assert_equal 10_000, rows.last['value'].length
  end

  def test_sample_size_option_round_trips
    data = (0...20).map { |i| [i, 'small'] } + (0...80).map { |i| [i + 20, 'x' * 5000] }
    rows = write_and_read(data, sample_size: 10, flush_threshold: 512 * 1024)

    assert_equal 100, rows.length
    assert_equal 'small', rows[0]['value']
    assert_equal 5000, rows.last['value'].length
  end

  def test_sample_size_rejects_zero
    data = [[1, 'one']]

    assert_raises(ArgumentError) do
      Parquet.write_rows(data, schema: SCHEMA, write_to: @test_file, sample_size: 0)
    end
  end

  def test_sample_size_rejects_excessive_value
    data = [[1, 'one']]

    assert_raises(ArgumentError) do
      Parquet.write_rows(data, schema: SCHEMA, write_to: @test_file, sample_size: 10_001)
    end
  end

  def test_default_batch_sizing_round_trips
    data = (0...1500).map { |i| [i, "string #{i}"] }
    rows = write_and_read(data)

    assert_equal 1500, rows.length
    assert_equal((0...1500).to_a, rows.map { |r| r['id'] })
  end

  def test_mixed_row_sizes_round_trip
    data = (0...200).map { |i| i < 50 ? [i, 'small'] : [i, 'x' * 1000] }
    rows = write_and_read(data, flush_threshold: 100_000, sample_size: 30)

    assert_equal 200, rows.length
    assert_equal 'small', rows[10]['value']
    assert_equal 1000, rows[100]['value'].length
  end
end
