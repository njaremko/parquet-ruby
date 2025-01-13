# parquet-ruby

[![Gem Version](https://badge.fury.io/rb/parquet.svg)](https://badge.fury.io/rb/parquet)

This project is a Ruby library wrapping the [parquet-rs](https://github.com/apache/parquet-rs) rust crate.

## Usage

This library provides high-level bindings to parquet-rs with two primary APIs for reading Parquet files: row-wise and column-wise iteration. The column-wise API generally offers better performance, especially when working with subset of columns.

### Row-wise Iteration

The `each_row` method provides sequential access to individual rows:

```ruby
require "parquet"

# Basic usage with default hash output
Parquet.each_row("data.parquet") do |row|
  puts row.inspect  # {"id"=>1, "name"=>"name_1"}
end

# Array output for more efficient memory usage
Parquet.each_row("data.parquet", result_type: :array) do |row|
  puts row.inspect  # [1, "name_1"]
end

# Select specific columns to reduce I/O
Parquet.each_row("data.parquet", columns: ["id", "name"]) do |row|
  puts row.inspect
end

# Reading from IO objects
File.open("data.parquet", "rb") do |file|
  Parquet.each_row(file) do |row|
    puts row.inspect
  end
end
```

### Column-wise Iteration

The `each_column` method reads data in column-oriented batches, which is typically more efficient for analytical queries:

```ruby
require "parquet"

# Process columns in batches of 1024 rows
Parquet.each_column("data.parquet", batch_size: 1024) do |batch|
  # With result_type: :hash (default)
  puts batch.inspect
  # {
  #   "id" => [1, 2, ..., 1024],
  #   "name" => ["name_1", "name_2", ..., "name_1024"]
  # }
end

# Array output with specific columns
Parquet.each_column("data.parquet",
                    columns: ["id", "name"],
                    result_type: :array,
                    batch_size: 1024) do |batch|
  puts batch.inspect
  # [
  #   [1, 2, ..., 1024],           # id column
  #   ["name_1", "name_2", ...]    # name column
  # ]
end
```

### Arguments

Both methods accept these common arguments:

- `input`: Path string or IO-like object containing Parquet data
- `result_type`: Output format (`:hash` or `:array`, defaults to `:hash`)
- `columns`: Optional array of column names to read (improves performance)

Additional arguments for `each_column`:

- `batch_size`: Number of rows per batch (defaults to implementation-defined value)

When no block is given, both methods return an Enumerator.

### Writing Row-wise Data

The `write_rows` method allows you to write data row by row:

```ruby
require "parquet"

# Define the schema for your data
schema = [
  { "id" => "int64" },
  { "name" => "string" },
  { "score" => "double" }
]

# Create an enumerator that yields arrays of row values
rows = [
  [1, "Alice", 95.5],
  [2, "Bob", 82.3],
  [3, "Charlie", 88.7]
].each

# Write to a file
Parquet.write_rows(rows, schema: schema, write_to: "data.parquet")

# Write to an IO object
File.open("data.parquet", "wb") do |file|
  Parquet.write_rows(rows, schema: schema, write_to: file)
end

# Optionally specify batch size (default is 1000)
Parquet.write_rows(rows,
  schema: schema,
  write_to: "data.parquet",
  batch_size: 500
)
```

### Writing Column-wise Data

The `write_columns` method provides a more efficient way to write data in column-oriented batches:

```ruby
require "parquet"

# Define the schema
schema = [
  { "id" => "int64" },
  { "name" => "string" },
  { "score" => "double" }
]

# Create batches of column data
batches = [
  # First batch
  [
    [1, 2],          # id column
    ["Alice", "Bob"], # name column
    [95.5, 82.3]     # score column
  ],
  # Second batch
  [
    [3],             # id column
    ["Charlie"],     # name column
    [88.7]           # score column
  ]
]

# Create an enumerator from the batches
columns = batches.each

# Write to a parquet file
Parquet.write_columns(columns, schema: schema, write_to: "data.parquet")

# Write to an IO object
File.open("data.parquet", "wb") do |file|
  Parquet.write_columns(columns, schema: schema, write_to: file)
end
```

The following data types are supported in the schema:

- `int8`, `int16`, `int32`, `int64`
- `uint8`, `uint16`, `uint32`, `uint64`
- `float`, `double`
- `string`
- `binary`
- `boolean`
- `date32`
- `timestamp_millis`, `timestamp_micros`

Note: List and Map types are currently not supported.
