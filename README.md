# parquet-ruby

[![Gem Version](https://badge.fury.io/rb/parquet.svg)](https://badge.fury.io/rb/parquet)

This project is a Ruby library wrapping the [parquet-rs](https://github.com/apache/parquet-rs) rust crate.

## Usage

This library provides high-level bindings to parquet-rs with two primary APIs for reading Parquet files: row-wise and column-wise iteration. The column-wise API generally offers better performance, especially when working with subset of columns.

### Metadata

The `metadata` method provides detailed information about a Parquet file's structure and contents:

```ruby
require "parquet"

# Get metadata from a file path
metadata = Parquet.metadata("data.parquet")

# Or from an IO object
File.open("data.parquet", "rb") do |file|
  metadata = Parquet.metadata(file)
end

# Example metadata output:
# {
#   "num_rows" => 3,
#   "created_by" => "parquet-rs version 54.2.0",
#   "key_value_metadata" => [
#     {
#       "key" => "ARROW:schema",
#       "value" => "base64_encoded_schema"
#     }
#   ],
#   "schema" => {
#     "name" => "arrow_schema",
#     "fields" => [
#       {
#         "name" => "id",
#         "type" => "primitive",
#         "physical_type" => "INT64",
#         "repetition" => "OPTIONAL",
#         "converted_type" => "NONE"
#       },
#       # ... other fields
#     ]
#   },
#   "row_groups" => [
#     {
#       "num_columns" => 5,
#       "num_rows" => 3,
#       "total_byte_size" => 379,
#       "columns" => [
#         {
#           "column_path" => "id",
#           "num_values" => 3,
#           "compression" => "UNCOMPRESSED",
#           "total_compressed_size" => 91,
#           "encodings" => ["PLAIN", "RLE", "RLE_DICTIONARY"],
#           "statistics" => {
#             "min_is_exact" => true,
#             "max_is_exact" => true
#           }
#         },
#         # ... other columns
#       ]
#     }
#   ]
# }
```

The metadata includes:
- Total number of rows
- File creation information
- Key-value metadata (including Arrow schema)
- Detailed schema information for each column
- Row group information including:
  - Number of columns and rows
  - Total byte size
  - Column-level details (compression, encodings, statistics)

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
- `time_millis`, `time_micros`

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

  # Nested lists (list of lists of strings)
  field :nested_lists, :list, item: :list do
    field :item, :string  # REQUIRED: Inner item field MUST be named 'item' for nested lists
  end

  # Map of lists
  field :map_of_lists, :map, key: :string, value: :list do
    field :item, :int32  # REQUIRED: List items in maps MUST be named 'item'
  end
end

### Nested Lists

When working with nested lists (a list of lists), there are specific requirements:

1. Using the Schema DSL:
```ruby
# A list of lists of strings
field :nested_lists, :list, item: :list do
  field :item, :string  # For nested lists, inner item MUST be named 'item'
end
```

2. Using hash-based schema format:
```ruby
# A list of lists of integers
{ "nested_numbers" => "list<list<int32>>" }
```

The data for nested lists is structured as an array of arrays:
```ruby
# Data for the nested_lists field
[["a", "b"], ["c", "d", "e"], []]  # Last one is an empty inner list
```

### Decimal Data Type

Parquet supports decimal numbers with configurable precision and scale, which is essential for financial applications where exact decimal representation is critical. The library seamlessly converts between Ruby's `BigDecimal` and Parquet's decimal type.

#### Decimal Precision and Scale

When working with decimal fields, you need to understand two key parameters:

- **Precision**: The total number of significant digits (both before and after the decimal point)
- **Scale**: The number of digits after the decimal point

The rules for defining decimals are:

```ruby
# No precision/scale specified - uses maximum precision (38) with scale 0
field :amount1, :decimal  # Equivalent to INTEGER with 38 digits

# Only precision specified - scale defaults to 0
field :amount2, :decimal, precision: 10  # 10 digits, no decimal places

# Only scale specified - uses maximum precision (38)
field :amount3, :decimal, scale: 2  # 38 digits with 2 decimal places

# Both precision and scale specified
field :amount4, :decimal, precision: 10, scale: 2  # 10 digits with 2 decimal places
```

#### Financial Data Example

Here's a practical example for a financial application:

```ruby
require "parquet"
require "bigdecimal"

# Schema for financial transactions
schema = Parquet::Schema.define do
  field :transaction_id, :string, nullable: false
  field :timestamp, :timestamp_millis, nullable: false
  field :amount, :decimal, precision: 12, scale: 2  # Supports up to 10^10 with 2 decimal places
  field :balance, :decimal, precision: 16, scale: 2  # Larger precision for running balances
  field :currency, :string
  field :exchange_rate, :decimal, precision: 10, scale: 6  # 6 decimal places for forex rates
  field :fee, :decimal, precision: 8, scale: 2, nullable: true  # Optional fee
  field :category, :string
end

# Sample financial data
transactions = [
  [
    "T-12345",
    Time.now,
    BigDecimal("1256.99"),       # amount (directly using BigDecimal)
    BigDecimal("10250.25"),      # balance
    "USD",
    BigDecimal("1.0"),           # exchange_rate
    BigDecimal("2.50"),          # fee
    "Groceries"
  ],
  [
    "T-12346",
    Time.now - 86400,            # yesterday
    BigDecimal("-89.50"),        # negative amount for withdrawal
    BigDecimal("10160.75"),      # updated balance
    "USD",
    BigDecimal("1.0"),           # exchange_rate
    nil,                         # no fee
    "Transportation"
  ],
  [
    "T-12347",
    Time.now - 172800,           # two days ago
    BigDecimal("250.00"),        # amount
    BigDecimal("10410.75"),      # balance
    "EUR",                       # different currency
    BigDecimal("1.05463"),       # exchange_rate
    BigDecimal("1.75"),          # fee
    "Entertainment"
  ]
]

# Write financial data to Parquet file
Parquet.write_rows(transactions.each, schema: schema, write_to: "financial_data.parquet")

# Read back transactions
Parquet.each_row("financial_data.parquet") do |transaction|
  # Access decimal fields as BigDecimal objects
  puts "Transaction: #{transaction['transaction_id']}"
  puts "  Amount: #{transaction['currency']} #{transaction['amount']}"
  puts "  Balance: $#{transaction['balance']}"
  puts "  Fee: #{transaction['fee'] || 'No fee'}"

  # You can perform precise decimal calculations
  if transaction['currency'] != 'USD'
    usd_amount = transaction['amount'] * transaction['exchange_rate']
    puts "  USD Equivalent: $#{usd_amount.round(2)}"
  end
end
```

#### Decimal Type Storage Considerations

Parquet optimizes storage based on the precision:
- For precision ≤ 9: Uses 4-byte INT32
- For precision ≤ 18: Uses 8-byte INT64
- For precision ≤ 38: Uses 16-byte BYTE_ARRAY

Choose appropriate precision and scale for your data to optimize storage while ensuring adequate range:

```ruby
# Banking examples
field :account_balance, :decimal, precision: 16, scale: 2   # Up to 14 digits before decimal point
field :interest_rate, :decimal, precision: 8, scale: 6      # Rate with 6 decimal places (e.g., 0.015625)

# E-commerce examples
field :product_price, :decimal, precision: 10, scale: 2     # Product price
field :shipping_weight, :decimal, precision: 6, scale: 3    # Weight in kg with 3 decimal places

# Analytics examples
field :conversion_rate, :decimal, precision: 5, scale: 4    # Rate like 0.0123
field :daily_revenue, :decimal, precision: 14, scale: 2     # Daily revenue with 2 decimal places
```

### Sample Data with Nested Structures

Here's an example showing how to use the schema defined earlier with sample data:

```ruby
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
    [["a", "b"], ["c", "d", "e"]],  # nested_lists (a list of lists of strings)
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
