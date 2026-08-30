require_relative "test_helper"
require "rbconfig"

class WriteMemoryUsageTest < Minitest::Test
  FRESH_PROCESS_ENV = "PARQUET_RUBY_WRITE_MEMORY_CHILD"
  ROW_COUNT = 80_000
  EARLY_SAMPLE_ROW = 20_000
  MAX_PLATEAU_GROWTH_KIB = 32 * 1_024
  SCHEMA = [{ "id" => "int64" }, { "payload" => "string" }].freeze

  def setup
    skip unless ENV["RUN_SLOW_TESTS"]
    @paths = []
  end

  def teardown
    @paths&.each { |path| File.delete(path) if File.exist?(path) }
  end

  def test_row_write_peak_memory_reaches_a_plateau
    return assert_plateau_in_fresh_process unless fresh_process?

    samples = {}
    rows = Enumerator.new do |yielder|
      ROW_COUNT.times do |index|
        yielder << [index, "#{index}-#{"x" * 1_024}"]
        sample_rss(samples, index + 1)
      end
    end

    Parquet.write_rows(
      rows,
      schema: SCHEMA,
      write_to: output_path("rows"),
      batch_size: 500_000,
      flush_threshold: 1
    )

    assert_memory_plateau(samples)
  end

  def test_column_write_peak_memory_reaches_a_plateau
    return assert_plateau_in_fresh_process unless fresh_process?

    samples = {}
    batches = Enumerator.new do |yielder|
      first = 0
      while first < ROW_COUNT
        last = [first + 1_000, ROW_COUNT].min
        ids = (first...last).to_a
        payloads = ids.map { |index| "#{index}-#{"x" * 1_024}" }
        yielder << [ids, payloads]
        first = last
        sample_rss(samples, first)
      end
    end

    Parquet.write_columns(
      batches,
      schema: SCHEMA,
      write_to: output_path("columns"),
      flush_threshold: 1
    )

    assert_memory_plateau(samples)
  end

  private

  def fresh_process?
    ENV[FRESH_PROCESS_ENV] == "1"
  end

  def assert_plateau_in_fresh_process
    root = File.expand_path("..", __dir__)
    passed = system(
      { FRESH_PROCESS_ENV => "1" },
      RbConfig.ruby,
      "-I#{File.join(root, "lib")}",
      "-I#{__dir__}",
      File.expand_path(__FILE__),
      "--name=#{name}"
    )

    assert passed, "fresh-process memory plateau check failed"
  end

  def sample_rss(samples, row_count)
    return unless [EARLY_SAMPLE_ROW, ROW_COUNT].include?(row_count)

    GC.start(full_mark: true, immediate_sweep: true)
    samples[row_count] = `ps -o rss= -p #{Process.pid}`.to_i
  end

  def assert_memory_plateau(samples)
    early_rss = samples.fetch(EARLY_SAMPLE_ROW)
    final_rss = samples.fetch(ROW_COUNT)
    growth = final_rss - early_rss
    assert_operator(
      growth,
      :<,
      MAX_PLATEAU_GROWTH_KIB,
      "RSS grew by #{growth / 1_024.0} MiB while total rows grew 4x"
    )
  end

  def output_path(label)
    path = File.join(Dir.tmpdir, "parquet-ruby-memory-#{label}-#{Process.pid}.parquet")
    @paths << path
    path
  end
end
