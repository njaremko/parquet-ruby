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

# Optionally specify memory threshold for flushing (default is 64MB)
Parquet.write_rows(rows,
  schema: schema,
  write_to: "data.parquet",
  flush_threshold: 32 * 1024 * 1024  # 32MB
)

# Optionally specify sample size for row size estimation (default is 100)
Parquet.write_rows(rows,
  schema: schema,
  write_to: "data.parquet",
  sample_size: 200  # Sample 200 rows for size estimation
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

# Write to a parquet file with default ZSTD compression
Parquet.write_columns(columns, schema: schema, write_to: "data.parquet")

# Write to a parquet file with specific compression and memory threshold
Parquet.write_columns(columns,
  schema: schema,
  write_to: "data.parquet",
  compression: "snappy",  # Supported: "none", "uncompressed", "snappy", "gzip", "lz4", "zstd"
  flush_threshold: 32 * 1024 * 1024  # 32MB
)

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

### Schema DSL for Complex Data Types

In addition to the hash-based schema definition shown above, this library provides a more expressive DSL for defining complex schemas with nested structures:

```ruby
require "parquet"

# Define a complex schema using the Schema DSL
schema = Parquet::Schema.define do
  field :id, :int64, nullable: false  # Required field
  field :name, :string                # Optional field (nullable: true is default)

  # Nested struct
  field :address, :struct do
    field :street, :string
    field :city, :string
    field :zip, :string
    field :coordinates, :struct do
      field :latitude, :double
      field :longitude, :double
    end
  end

  # List of primitives
  field :scores, :list, item: :float

  # List of structs
  field :contacts, :list, item: :struct do
    field :name, :string
    field :phone, :string
    field :primary, :boolean
  end

  # Map with string values
  field :metadata, :map, key: :string, value: :string

  # Map with struct values
  field :properties, :map, key: :string, value: :struct do
    field :count, :int32
    field :description, :string
  end

  # Nested lists
  field :nested_lists, :list, item: :list do
    field :item, :string  # For nested lists, inner item must be named 'item'
  end

  # Map of lists
  field :map_of_lists, :map, key: :string, value: :list do
    field :item, :int32  # For list items in maps, item must be named 'item'
  end
end

# Sample data with nested structures
data = [
  [
    1,                            # id
    "John Doe",                   # name
    {                             # address (struct)
      "street" => "123 Main St",
      "city" => "Springfield",
      "zip" => "12345",
      "coordinates" => {
        "latitude" => 37.7749,
        "longitude" => -122.4194
      }
    },
    [85.5, 92.0, 78.5],          # scores (list of floats)
    [                             # contacts (list of structs)
      { "name" => "Contact 1", "phone" => "555-1234", "primary" => true },
      { "name" => "Contact 2", "phone" => "555-5678", "primary" => false }
    ],
    { "created" => "2023-01-01", "status" => "active" },  # metadata (map)
    {                             # properties (map of structs)
      "feature1" => { "count" => 5, "description" => "Main feature" },
      "feature2" => { "count" => 3, "description" => "Secondary feature" }
    },
    [["a", "b"], ["c", "d", "e"]],  # nested_lists
    {                                # map_of_lists
      "group1" => [1, 2, 3],
      "group2" => [4, 5, 6]
    }
  ]
]

# Write to a parquet file using the schema
Parquet.write_rows(data.each, schema: schema, write_to: "complex_data.parquet")

# Read back the data
Parquet.each_row("complex_data.parquet") do |row|
  puts row.inspect
end
```

The Schema DSL supports:

- **Primitive types**: All standard Parquet types (`int32`, `string`, etc.)
- **Complex types**: Structs, lists, and maps with arbitrary nesting
- **Nullability control**: Specify which fields can contain null values with `nullable: false/true`
- **List item nullability**: Control whether list items can be null with `item_nullable: false/true`
- **Map key/value nullability**: Control whether map keys or values can be null with `value_nullable: false/true`

Note: When using List and Map types, you need to provide at least:
- For lists: The `item:` parameter specifying the item type
- For maps: Both `key:` and `value:` parameters specifying key and value types
