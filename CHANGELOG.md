# Changelog

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
