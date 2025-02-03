# Changelog

## 0.2.12

- Improvements to error handling throughout the library
- Improvements to the header cache used when reading in `:hash` mode
- Optional UTF-8 validation when reading strings with `strict: true`

## 0.2.9

- Added `sample_size` option to `write_rows` for customizing row size estimation:
  - Controls how many rows are sampled to estimate optimal batch sizes
  - Defaults to 100 rows if not specified
  - Example: `Parquet.write_rows(data, schema: schema, write_to: path, sample_size: 200)`

## 0.2.8

- Added support for writing Parquet files with compression:
  - Supports common compression codecs: gzip, snappy, lz4, zstd
  - Configurable via `compression` option when writing files
  - Example: `Parquet.write_rows(data, schema: schema, write_to: path, compression: "gzip")`
  - Default is uncompressed if no compression specified

## 0.2.7

- Added support for specifying `format` in schema for parsing time strings in the iterators when writing to Parquet
  - Allows parsing date strings with `format` option in schema (e.g. `"%Y-%m-%d"` for dates)
  - Allows parsing timestamp strings with `format` option in schema (e.g. `"%Y-%m-%d %H:%M:%S%z"` for timestamps)
  - Works with both `write_rows` and `write_columns` methods

## 0.2.6

- Fix handling of explicit `nil` for optional arguments

## 0.2.5

- Arbitrarily bumping the verison a bit imply that the gem isn't alpha quality.
- Add support for writing all types except for structs and arrays

## 0.0.5

- Remove unused rust dependencies

## 0.0.4

- Fix the "Homepage" field in the gemspec

## 0.0.3

- Added `each_column` method for efficient column-oriented reading of Parquet files
  - Reads data in batches for better performance compared to row-wise iteration
  - Supports both hash and array output formats via `result_type` option
  - Accepts optional `columns` parameter to read only specific columns
  - Configurable `batch_size` parameter to control memory usage
  - Works with file paths and IO objects
  - Returns Enumerator when no block given
  - Handles complex types like arrays, maps, and nested structs
  - Preserves type information for numeric, date, and timestamp columns

## 0.0.2

- Added `columns` option to `each_row` method. Allows us to take advantage of the column projection feature of the parquet crate.
- General refactoring to improve readability and maintainability.

## 0.0.1

Initial release.

Supports reading each row as a hash or an array from a file or an IO object.
