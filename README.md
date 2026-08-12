# parquet-ruby

[![Gem Version](https://badge.fury.io/rb/parquet.svg)](https://badge.fury.io/rb/parquet)

Read and write [Apache Parquet](https://parquet.apache.org/) files from Ruby. This gem wraps the official Apache [`parquet`](https://github.com/apache/arrow-rs/tree/main/parquet) rust crate, providing:

- **High performance** columnar data storage and retrieval
- **Memory-efficient** streaming APIs for large datasets
- **Full compatibility** with the Apache Parquet specification
- **Simple, Ruby-native** APIs that feel natural

## Why Use This Library?

Apache Parquet is the de facto standard for analytical data storage, offering:
- **Efficient compression** - typically 2-10x smaller than CSV
- **Fast columnar access** - read only the columns you need
- **Rich type system** - preserves data types, including nested structures
- **Wide ecosystem support** - works with Spark, Pandas, DuckDB, and more

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'parquet'
```

Then execute:

```bash
$ bundle install
```

Or install it directly:

```bash
$ gem install parquet
```

## Quick Start

### Reading Data

```ruby
require "parquet"

# Read Parquet files row by row
Parquet.each_row("data.parquet") do |row|
  puts row  # => {"id" => 1, "name" => "Alice", "score" => 95.5}
end

# Or column by column for better performance
Parquet.each_column("data.parquet", batch_size: 1000) do |batch|
  puts batch  # => {"id" => [1, 2, ...], "name" => ["Alice", "Bob", ...]}
end
```

### Writing Data

```ruby
# Define your schema
schema = [
  { "id" => "int64" },
  { "name" => "string" },
  { "score" => "double" }
]

# Write row by row
rows = [
  [1, "Alice", 95.5],
  [2, "Bob", 82.3]
]

Parquet.write_rows(rows.each, schema: schema, write_to: "output.parquet")
```

## Reading Parquet Files

The library provides two APIs for reading data, each optimized for different use cases:

### Row-wise Reading (Sequential Access)

Best for: Processing records one at a time, data transformations, ETL pipelines

```ruby
# Basic usage - returns hashes
Parquet.each_row("data.parquet") do |row|
  puts row  # => {"id" => 1, "name" => "Alice"}
end

# Memory-efficient array format
Parquet.each_row("data.parquet", result_type: :array) do |row|
  puts row  # => [1, "Alice"]
end

# Read specific columns only
Parquet.each_row("data.parquet", columns: ["id", "name"]) do |row|
  # Only requested columns are loaded from disk
end

# Works with IO objects
File.open("data.parquet", "rb") do |file|
  Parquet.each_row(file) do |row|
    # Process row
  end
end
```

### Column-wise Reading (Analytical Access)

Best for: Analytics, aggregations, when you need few columns from wide tables

```ruby
# Process data in column batches
Parquet.each_column("data.parquet", batch_size: 1000) do |batch|
  # batch is a hash of column_name => array_of_values
  ids = batch["id"]      # => [1, 2, 3, ..., 1000]
  names = batch["name"]  # => ["Alice", "Bob", ...]

  # Perform columnar operations
  avg_id = ids.sum.to_f / ids.length
end

# Array format for more control
Parquet.each_column("data.parquet",
                    result_type: :array,
                    columns: ["id", "name"]) do |batch|
  # batch is an array of arrays
  # [[1, 2, ...], ["Alice", "Bob", ...]]
end
```

### File Metadata

Inspect file structure without reading data:

```ruby
metadata = Parquet.metadata("data.parquet")

puts metadata["num_rows"]           # Total row count
puts metadata["created_by"]         # Writer identification
puts metadata["schema"]["fields"]   # Column definitions
puts metadata["row_groups"].size    # Number of row groups
```

## Writing Parquet Files

### Row-wise Writing

Best for: Streaming data, converting from other formats, memory-constrained environments

```ruby
# Basic schema definition
schema = [
  { "id" => "int64" },
  { "name" => "string" },
  { "active" => "boolean" },
  { "balance" => "double" }
]

# Stream data from any enumerable
rows = CSV.foreach("input.csv").lazy.map do |row|
  [row[0].to_i, row[1], row[2] == "true", row[3].to_f]
end

Parquet.write_rows(rows,
  schema: schema,
  write_to: "output.parquet",
  batch_size: 5000  # Positive rows per batch (default: 1000)
)
```

### Repacking Existing Parquet Files

Concatenate Parquet files and re-split them into differently sized files without
translating rows through Ruby.

```ruby
Parquet.repack(
  ["input-0.parquet", "input-1.parquet"],
  output_dir: "repacked",
  rows_per_file: 100_000
)
# => [{ "path" => "repacked/batch-0.parquet", "num_rows" => 100_000 },
#     { "path" => "repacked/batch-1.parquet", "num_rows" => 42_137 }]
```

The outputs hold exactly the input rows, in input order. Every output but the
last holds `rows_per_file` rows, and there is always at least one output even
when the inputs are empty. Omit `rows_per_file:` to concatenate everything into
a single file.

Each output's Parquet schema is identical to the first input's, and that input's
file-level key/value metadata (`ARROW:schema`, `pandas`, and so on) is carried
over. Inputs must agree on leaf column shape — path, physical and logical type,
nesting — but may differ in key/value metadata and Parquet field ids.

#### Compression and copying

With no `compression:`, each column keeps its own codec — Parquet records one
per column, and a file may legitimately use several. Naming a codec applies it
to every column instead:

```ruby
Parquet.repack("input.parquet", output_dir: "out", compression: "zstd")
```

Keeping the inputs' codecs also lets repack copy whole row groups into the
output byte-for-byte, skipping decompression and re-encoding entirely. A row
group is copied when it fits the output's remaining row budget, is large enough
to be worth copying, and the request did not ask for a different codec;
otherwise its rows are decoded and re-encoded. Both routes produce the same
rows, so which one runs is not something callers need to reason about — but the
copy route is dramatically faster, so a plain concatenation is close to an
I/O-bound copy.

Small row groups are deliberately merged rather than copied: copying them
one-for-one would make a compaction of many small files reproduce exactly the
fragmentation it was meant to remove.

#### Output directory ownership

`repack` owns the `{output_file_prefix}-{n}.parquet` names in `output_dir`. If
any already exist it raises `ArgumentError` rather than mixing two runs' files
in one directory:

```ruby
Parquet.repack("input.parquet", output_dir: "out", rows_per_file: 1000, overwrite: true)
```

`overwrite: true` replaces that set and deletes members left over from a longer
earlier run, so the returned list always equals what a reader finds in the
directory. Files outside the set are never touched.

#### Bounds

`max_read_rows_per_chunk:` (default 8192, reduced for wide schemas) bounds rows
buffered while reading; output row groups are bounded in rows by the same slot
budget. Both are resource controls: varying them cannot change the returned
list, the rows, the schema, or the codecs. They do shift compressed byte counts
and page boundaries, which are representation rather than contract.

Input metadata is read one file at a time, so peak memory is set by the widest
single file and its row-group size, not by how many files you pass or how many
rows they hold in total. Note that the row-group bound is in rows, not bytes: a
schema with very large values still buffers one row group's worth of encoded
data.

Reading and writing run with the GVL released, so other Ruby threads keep
running and `Interrupt` / `Timeout` are honoured. An interrupted call leaves no
output behind.

A Parquet file cannot hold more than 32767 row groups. Merging small groups
keeps that limit out of reach in practice; if a single output ever did reach it,
repack raises rather than writing an unreadable file, and `rows_per_file:` is
the way out.

#### Page indexes and other optional structures

Every row group in a Parquet file must agree on whether it carries a page index,
and a copied row group can only contribute the index its source had. So an
output carries one exactly when every contributing input does. Bloom filters are
not carried over on either route.

### Column-wise Writing

Best for: Pre-columnar data, better compression, higher performance

```ruby
# Prepare columnar data
ids = [1, 2, 3, 4, 5]
names = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
scores = [95.5, 82.3, 88.7, 91.2, 79.8]

# Create batches
batches = [[
  ids,    # First column
  names,  # Second column
  scores  # Third column
]]

schema = [
  { "id" => "int64" },
  { "name" => "string" },
  { "score" => "double" }
]

Parquet.write_columns(batches.each,
  schema: schema,
  write_to: "output.parquet",
  compression: "snappy"  # Options: none, snappy, gzip, lz4, zstd
)
```

`write_columns` also accepts `logger:` with the same Ruby logger interface as
row writes. The outer enumerable is pulled one batch at a time; each batch must
contain one array per schema field, and every array in that batch must have the
same length.

## Data Types

### Basic Types

```ruby
schema = [
  # Integers
  { "tiny" => "int8" },         # -128 to 127
  { "small" => "int16" },       # -32,768 to 32,767
  { "medium" => "int32" },      # ±2 billion
  { "large" => "int64" },       # ±9 quintillion

  # Unsigned integers
  { "ubyte" => "uint8" },       # 0 to 255
  { "ushort" => "uint16" },     # 0 to 65,535
  { "uint" => "uint32" },       # 0 to 4 billion
  { "ulong" => "uint64" },      # 0 to 18 quintillion

  # Floating point
  { "price" => "float" },       # 32-bit precision
  { "amount" => "double" },     # 64-bit precision

  # Other basics
  { "name" => "string" },
  { "data" => "binary" },
  { "active" => "boolean" }
]
```

### Date and Time Types

```ruby
schema = [
  # Date (days since Unix epoch)
  { "date" => "date32" },

  # Timestamps (with different precisions)
  { "created_sec" => "timestamp_second" },
  { "created_ms" => "timestamp_millis" },    # Most common
  { "created_us" => "timestamp_micros" },
  { "created_ns" => "timestamp_nanos" },

  # Time of day (without date)
  { "time_ms" => "time_millis" },    # Milliseconds since midnight
  { "time_us" => "time_micros" }     # Microseconds since midnight
]
```

### Decimal Type (Financial Data)

For exact decimal arithmetic (no floating-point errors):

```ruby
require "bigdecimal"

schema = [
  # Financial amounts with 2 decimal places
  { "price" => "decimal", "precision" => 10, "scale" => 2 },  # Up to 99,999,999.99
  { "balance" => "decimal", "precision" => 15, "scale" => 2 }, # Larger amounts

  # High-precision calculations
  { "rate" => "decimal", "precision" => 10, "scale" => 8 }     # 8 decimal places
]

# Use BigDecimal for exact values
data = [[
  BigDecimal("19.99"),
  BigDecimal("1234567.89"),
  BigDecimal("0.00000123")
]]
```

## Complex Data Structures

The library includes a powerful Schema DSL for defining nested data:

### Using the Schema DSL

```ruby
schema = Parquet::Schema.define do
  # Simple fields
  field :id, :int64, nullable: false      # Required field
  field :name, :string                    # Optional by default

  # Nested structure
  field :address, :struct do
    field :street, :string
    field :city, :string
    field :location, :struct do
      field :lat, :double
      field :lng, :double
    end
  end

  # Lists
  field :tags, :list, item: :string
  field :scores, :list, item: :int32

  # Maps (dictionaries)
  field :metadata, :map, key: :string, value: :string

  # Complex combinations
  field :contacts, :list, item: :struct do
    field :name, :string
    field :email, :string
    field :primary, :boolean
  end
end
```

### Writing Complex Data

```ruby
data = [[
  1,                              # id
  "Alice Johnson",                # name
  {                               # address
    "street" => "123 Main St",
    "city" => "Springfield",
    "location" => {
      "lat" => 40.7128,
      "lng" => -74.0060
    }
  },
  ["ruby", "parquet", "data"],    # tags
  [85, 92, 88],                   # scores
  { "dept" => "Engineering" },    # metadata
  [                               # contacts
    { "name" => "Bob", "email" => "bob@example.com", "primary" => true },
    { "name" => "Carol", "email" => "carol@example.com", "primary" => false }
  ]
]]

Parquet.write_rows(data.each, schema: schema, write_to: "complex.parquet")
```

## ⚠️ Important Limitations

### Timezone Handling in Parquet

The Parquet specification has a fundamental limitation with timezone storage:

1. **UTC-normalized**: Any timestamp with timezone info (including "+09:00" or "America/New_York") is converted to UTC
2. **Local/unzoned**: Timestamps without timezone info are stored as-is

**The original timezone information is permanently lost.** This is not a limitation of this library but of the Parquet format itself.

```ruby
schema = Parquet::Schema.define do
  # These BOTH store in UTC - timezone info is lost!
  field :timestamp_utc, :timestamp_millis, timezone: "UTC"
  field :timestamp_tokyo, :timestamp_millis, timezone: "+09:00"

  # This stores as local time (no timezone)
  field :timestamp_local, :timestamp_millis
end

# If you need timezone preservation, store it separately:
schema = Parquet::Schema.define do
  field :timestamp, :timestamp_millis, has_timezone: true  # UTC storage
  field :original_tz, :string                              # "America/New_York"
end
```

## Performance Tips

1. **Use column-wise reading** when you need only a few columns from wide tables
2. **Specify columns parameter** to avoid reading unnecessary data
3. **Choose appropriate batch sizes**:
   - Larger batches = better throughput but more memory
   - Smaller batches = less memory but more overhead
4. **Pre-sort data** by commonly filtered columns for better compression


## Memory Management

Control memory usage with flush thresholds:

```ruby
Parquet.write_rows(huge_dataset.each,
  schema: schema,
  write_to: "output.parquet",
  batch_size: 1000,              # Maximum converted rows per native batch
  flush_threshold: 32 * 1024**2  # Converted-value memory quantum
)
```

Both write APIs pull from `each`; they never call `to_a` on the outer input.
Converted values are flushed before admitting a row that would cross
`flush_threshold` (a single larger row is written alone). Encoded Parquet row
groups use `max(flush_threshold, 8 MiB)` so tiny memory quanta cannot create one
row group per row and exhaust the locked backend's row-group ordinal. Completed
footer metadata is serialized immediately to a disk spool instead of
accumulating in memory. Peak writer-owned native heap use is therefore
independent of total file length: it is a constant multiple of the row-group
quantum, plus schema state, bounded caches/samples, conversion scratch, one
oversized logical row, and the caller-owned row or column batch currently
yielded. Memory intentionally owned by a caller-provided in-memory output object
is outside that bound.

`batch_size` is also capped by the schema width, and no full batch is reserved
up front. Column batches are validated and consumed independently, so earlier
batches are released before the next one is requested. Path outputs are staged
beside the destination and atomically published only after the complete stream
and Parquet footer succeed.

## Architecture

This gem uses a modular architecture:

- **parquet-core**: Language-agnostic Rust core for Parquet operations
- **parquet-ruby-adapter**: Ruby-specific FFI adapter layer
- **parquet gem**: High-level Ruby API

Take a look at [ARCH.md](./ARCH.md)

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/njaremko/parquet-ruby.

## License

The gem is available as open source under the terms of the MIT License.
