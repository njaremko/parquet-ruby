# frozen_string_literal: true

require "test_helper"
require "bigdecimal"
require "fileutils"
require "stringio"
require "tmpdir"

# The extension's methods may be called from any Ractor on Ruby 4.0 and later.
# Earlier Rubies limit them to the main Ractor.
class RactorTest < Minitest::Test
  RACTOR_SAFE = Gem::Version.new(RUBY_VERSION) >= Gem::Version.new("4.0")

  SCHEMA = Ractor.make_shareable(
    Parquet::Schema.define do
      field :id, :int64, nullable: false
      field :name, :string
      field :ts, :timestamp_micros, has_timezone: true
      field :day, :date32
      field :amount, :decimal, precision: 10, scale: 2
    end
  )

  def setup
    @experimental = Warning[:experimental]
    Warning[:experimental] = false
    @dir = Dir.mktmpdir("parquet-ractor")
    @path = File.join(@dir, "rows.parquet")
    rows = [
      [1, "a", Time.utc(2024, 1, 2, 3, 4, 5), "2024-01-02", BigDecimal("1.25")],
      [2, "b", Time.utc(1969, 12, 31, 12), "1969-12-31", BigDecimal("-2.50")]
    ]
    Parquet.write_rows(rows.each, schema: SCHEMA, write_to: @path)
  end

  def teardown
    FileUtils.remove_entry(@dir) if @dir
    Warning[:experimental] = @experimental
  end

  def test_version_is_shareable
    assert Ractor.shareable?(Parquet::VERSION)
  end

  def test_main_ractor_only_before_ruby_4
    skip "methods are Ractor safe on Ruby #{RUBY_VERSION}" if RACTOR_SAFE

    outcome = ractor_value(
      Ractor.new(@path) do |path|
        Parquet.metadata(path)
        :allowed
      rescue Ractor::UnsafeError
        :unsafe
      end
    )
    assert_equal :unsafe, outcome
  end

  def test_reads_in_a_ractor
    skip "methods are limited to the main Ractor on Ruby #{RUBY_VERSION}" unless RACTOR_SAFE

    expected = Parquet.each_row(@path).to_a
    assert_equal expected, ractor_value(Ractor.new(@path) { |path| Parquet.each_row(path).to_a })
    %i[intern shared].each do |mode|
      rows = ractor_value(Ractor.new(@path, mode) { |path, m| Parquet.each_row(path, string_storage: m).to_a })
      assert_equal expected, rows, "string_storage: #{mode}"
    end

    columns = ractor_value(Ractor.new(@path) { |path| Parquet.each_column(path, batch_size: 10).to_a })
    assert_equal Parquet.each_column(@path, batch_size: 10).to_a, columns

    bytes = File.binread(@path)
    from_io = ractor_value(Ractor.new(bytes) { |b| Parquet.each_row(StringIO.new(b), result_type: :array).to_a })
    assert_equal Parquet.each_row(@path, result_type: :array).to_a, from_io

    assert_equal Parquet.metadata(@path), ractor_value(Ractor.new(@path) { |path| Parquet.metadata(path) })
  end

  def test_writes_in_a_ractor
    skip "methods are limited to the main Ractor on Ruby #{RUBY_VERSION}" unless RACTOR_SAFE

    path = File.join(@dir, "from-ractor.parquet")
    ractor_value(
      Ractor.new(path, SCHEMA) do |p, schema|
        rows = [[3, "c", "2024-05-06T07:08:09Z", "2024-05-06", "3.10"]]
        Parquet.write_rows(rows.each, schema: schema, write_to: p)
      end
    )
    assert_equal(
      [[3, "c", Time.utc(2024, 5, 6, 7, 8, 9), Date.new(2024, 5, 6), BigDecimal("3.10")]],
      Parquet.each_row(path, result_type: :array).to_a
    )

    bytes = ractor_value(
      Ractor.new(SCHEMA) do |schema|
        io = StringIO.new(+"")
        batch = [[4], ["d"], [Time.utc(2024, 1, 1)], ["2024-01-01"], [BigDecimal("4.00")]]
        Parquet.write_columns([batch].each, schema: schema, write_to: io)
        io.string
      end
    )
    assert_equal [[4, "d"]], Parquet.each_row(StringIO.new(bytes), result_type: :array).map { |row| row.first(2) }
  end

  def test_repack_in_a_ractor
    skip "methods are limited to the main Ractor on Ruby #{RUBY_VERSION}" unless RACTOR_SAFE

    output_dir = File.join(@dir, "repacked")
    outputs = ractor_value(
      Ractor.new(@path, output_dir) do |path, dir|
        Parquet.repack(path, output_dir: dir, rows_per_file: 1)
      end
    )
    assert_equal 2, outputs.size
    assert_equal Parquet.each_row(@path).to_a, outputs.flat_map { |output| Parquet.each_row(output["path"]).to_a }
  end

  # Reads in parallel Ractors while the main Ractor compacts the heap. Each read
  # keeps its own cached hash keys and :intern strings, which must stay correct
  # when compaction moves them.
  def test_parallel_reads_while_compacting
    skip "methods are limited to the main Ractor on Ruby #{RUBY_VERSION}" unless RACTOR_SAFE

    path = File.join(@dir, "wide.parquet")
    names = Array.new(32) { |i| "col_#{i}" }
    schema = Parquet::Schema.define { names.each { |name| field name, :string } }
    rows = Array.new(40) { |r| Array.new(names.size) { |c| "v#{(r + c) % 5}" } }
    Parquet.write_rows(rows.each, schema: schema, write_to: path)

    %i[copy intern shared].each do |mode|
      expected = Parquet.each_row(path, string_storage: mode).to_a
      results = with_compaction do
        Array.new(4) do
          Ractor.new(path, mode) do |p, m|
            Array.new(20) do
              Parquet.each_column(p, batch_size: 7, string_storage: m).to_a
              Parquet.each_row(p, string_storage: m).to_a
            end.last
          end
        end.map { |ractor| ractor_value(ractor) }
      end
      results.each { |result| assert_equal expected, result, "string_storage: #{mode}" }
    end
  end

  private

  def ractor_value(ractor)
    ractor.respond_to?(:value) ? ractor.value : ractor.take
  end

  def with_compaction
    compactor = Thread.new do
      loop do
        GC.start
        begin
          GC.compact
        rescue NotImplementedError
          nil
        end
        sleep 0.001
      end
    end
    yield
  ensure
    compactor&.kill
    compactor&.join
  end
end
