#!/usr/bin/env ruby
# frozen_string_literal: true

require "benchmark/ips"
require "polars-df"
require "parquet"
require "fileutils"
require "duckdb"

RubyVM::YJIT.enable

# Generate test data using DuckDB
DuckDB::Database.open do |db|
  db.connect do |con|
    # Create a table with id, name, and age columns
    con.query(
      'CREATE TABLE test_data AS
      SELECT
        row_number() OVER () as id,
        chr((ascii(\'A\') + (random() * 26)::INTEGER)::INTEGER) || \' \' ||
        chr((ascii(\'A\') + (random() * 26)::INTEGER)::INTEGER) as name,
        (random() * 100)::INTEGER as age
      FROM range(1000000)'
    )

    # Export to parquet
    con.query('COPY test_data TO \'benchmark/test.parquet\' (FORMAT PARQUET)')
  end
end

# Calculate total age for validation
age =
  DuckDB::Database.open do |db|
    db.connect { |con| con.query('SELECT sum(age) FROM read_parquet(\'benchmark/test.parquet\')').first[0] }
  end

TEST_FILES = %w[benchmark/test.parquet].freeze

begin
  Benchmark.ips do |x|
    x.config(time: 30, warmup: 5)

    x.report("Polars") do
      count = 0
      df = Polars.read_parquet("benchmark/test.parquet")
      age_col = df["age"].to_a
      age_col.each { |val| count += val.to_i }
      raise "Age mismatch: #{age} != #{count}" if age != count
    end

    x.report("Parquet rows") do
      count = 0
      Parquet.each_row("benchmark/test.parquet", columns: %w[age]) { |row| count += row["age"] }
      raise "Age mismatch: #{age} != #{count}" if age != count
    end

    x.report("Parquet columns") do
      count = 0
      Parquet.each_column("benchmark/test.parquet", columns: %w[age]) do |batch|
        batch["age"].each { |val| count += val.to_i }
      end
      raise "Age mismatch: #{age} != #{count}" if age != count
    end

    x.compare!
  end
ensure
  # Cleanup test files even if the script fails or is interrupted
  FileUtils.rm_f(TEST_FILES)
end
