#!/usr/bin/env ruby
# frozen_string_literal: true

require "parquet"
require "duckdb"
require "fileutils"

RubyVM::YJIT.enable

# Generate test data using DuckDB
DuckDB::Database.open do |db|
  db.connect do |con|
    # Create a table with sample data
    con.query(
      'CREATE TABLE test_data AS
      SELECT
        row_number() OVER () as id,
        chr((ascii(\'A\') + (random() * 26)::INTEGER)::INTEGER) || \' \' ||
        chr((ascii(\'A\') + (random() * 26)::INTEGER)::INTEGER) as name,
        (random() * 80 + 18)::INTEGER as age,
        \'person\' || row_number() OVER () || \'@example.com\' as email,
        \'City\' || (random() * 100)::INTEGER as city,
        \'Country\' || (random() * 50)::INTEGER as country,
        (random() * 170000 + 30000)::INTEGER as salary,
        CASE (random() * 5)::INTEGER
          WHEN 0 THEN \'Engineering\'
          WHEN 1 THEN \'Sales\'
          WHEN 2 THEN \'Marketing\'
          WHEN 3 THEN \'HR\'
          ELSE \'Finance\'
        END as department,
        date_add(\'2020-01-01\'::DATE, INTERVAL ((random() * 365)::INTEGER) DAY) as hire_date,
        (random() * 1000)::INTEGER as manager_id,
        round(random() * 4 + 1, 1) as performance_score,
        (random() * 10)::INTEGER as project_count,
        random() > 0.5 as active,
        \'\' as notes,
        \'\' as last_login
      FROM range(1000000)'
    )

    # Export to parquet
    con.query('COPY test_data TO \'benchmark_test.parquet\' (FORMAT PARQUET)')
  end
end

# Calculate total age for validation
total_age =
  DuckDB::Database.open do |db|
    db.connect { |con| con.query('SELECT sum(age) FROM read_parquet(\'benchmark_test.parquet\')').first[0] }
  end

# Process the file in a loop for 10 seconds
end_time = Time.now + 10
iterations = 0

while Time.now < end_time
  count = 0
  Parquet.each_row("benchmark_test.parquet", columns: %w[age]) { |row| count += row["age"] }
  raise "Age mismatch: #{total_age} != #{count}" if total_age != count
  iterations += 1
end

puts "Completed #{iterations} iterations in 10 seconds"

# Cleanup
FileUtils.rm_f("benchmark_test.parquet")
