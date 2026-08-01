#!/usr/bin/env ruby
# frozen_string_literal: true

# Measures what `Parquet.repack` costs per strategy.
#
# Repack has two physical paths that produce the same rows:
#
#   splice    whole input row groups are copied into the output byte-for-byte
#   re-encode rows are decoded to Arrow and encoded again
#
# A row group is spliced when the request keeps the inputs' codec and the group
# fits the output's remaining row budget. The cases below isolate each path,
# show what happens when a split lands mid-row-group, and contrast all of them
# with moving the same rows through Ruby — the cost `repack` exists to avoid.
#
#   ROWS=4000000 CODEC=snappy bundle exec ruby benchmark/benchmark_repack.rb

require "bundler/setup"
require "parquet"
require "fileutils"
require "tmpdir"

ROWS = Integer(ENV.fetch("ROWS", 1_000_000))
INPUT_FILES = Integer(ENV.fetch("INPUT_FILES", 4))
CODEC = ENV.fetch("CODEC", "zstd")
ROWS_PER_INPUT = ROWS / INPUT_FILES

SCHEMA = {
  fields: [
    { name: "id", type: :int64 },
    { name: "name", type: :string },
    { name: "category", type: :string },
    { name: "value", type: :float64 },
    { name: "created_at", type: :timestamp_micros }
  ]
}.freeze

def elapsed
  started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  result = yield
  [Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, result]
end

def report(label, bytes)
  seconds, outputs = elapsed { yield }
  rows = outputs.sum { |output| output["num_rows"] }
  printf("  %-36s %7.3fs  %8.1f MB/s  %11d rows/s  -> %d file(s), %d rows\n",
         label, seconds, bytes / seconds / 1024.0 / 1024.0, rows / seconds, outputs.size, rows)
  seconds
end

Dir.mktmpdir("parquet_repack_bench") do |dir|
  inputs =
    INPUT_FILES.times.map do |file_index|
      path = File.join(dir, "input-#{file_index}.parquet")
      rows = Enumerator.new do |yielder|
        base = Time.utc(2024, 1, 1)
        offset = file_index * ROWS_PER_INPUT
        ROWS_PER_INPUT.times do |i|
          id = offset + i
          yielder << [id, "name_#{id}", "category_#{id % 50}", id * 1.5, base + id]
        end
      end
      Parquet.write_rows(rows, schema: SCHEMA, write_to: path, compression: CODEC)
      path
    end

  bytes = inputs.sum { |path| File.size(path) }
  puts "inputs: #{INPUT_FILES} files, #{ROWS} rows, #{CODEC}, #{(bytes / 1024.0 / 1024.0).round(1)} MB total"
  puts

  puts "concatenate into one file"
  spliced = report("splice (keeps input codec)", bytes) do
    Parquet.repack(inputs, output_dir: File.join(dir, "splice"))
  end
  re_encoded = report("re-encode (different codec)", bytes) do
    Parquet.repack(inputs, output_dir: File.join(dir, "recode"),
                   compression: CODEC == "zstd" ? "snappy" : "zstd")
  end
  puts

  puts "split"
  report("rows_per_file aligned to inputs", bytes) do
    Parquet.repack(inputs, output_dir: File.join(dir, "aligned"), rows_per_file: ROWS_PER_INPUT)
  end
  report("rows_per_file mid-row-group", bytes) do
    Parquet.repack(inputs, output_dir: File.join(dir, "straddle"),
                   rows_per_file: (ROWS_PER_INPUT * 0.7).to_i)
  end
  puts

  puts "for contrast: the same rows through Ruby"
  via_ruby, = elapsed do
    Parquet.write_rows(
      Enumerator.new { |y| inputs.each { |p| Parquet.each_row(p, result_type: :array).each { |r| y << r } } },
      schema: SCHEMA, write_to: File.join(dir, "via_ruby.parquet"), compression: CODEC
    )
  end
  printf("  %-36s %7.3fs  %8.1f MB/s  %11d rows/s\n",
         "each_row -> write_rows", via_ruby, bytes / via_ruby / 1024.0 / 1024.0, ROWS / via_ruby)
  puts

  printf("splice is %.1fx faster than re-encode, %.0fx faster than round-tripping through Ruby\n",
         re_encoded / spliced, via_ruby / spliced)
end
