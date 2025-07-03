# Changelog

## 0.6.2
- Only create header strings once on the rust side
- Re-add nanosecond time support
- Fix regression with UUID parsing
- Fix regression with complex type parsing

## 0.6.1
- Fix regression, handle symbol keys in schema definition when constructing writer

## 0.6.0
- Complete rewrite of the underlying rust code
  - Refactored to separate the parquet writing / reading code separate from the Ruby interop
- External API remains the same
- Timezone handling for timestamps is now spec compliant
- Dates are returned as `Date` objects

## 0.5.13
- Get rid of assertion preventing reading maps with complex key types

## 0.5.12
- Add support for TIME_MILLIS and TIME_MICROS

## 0.5.11
- Revert arrow reading changes.

## 0.5.10

- Ability to write decimal256 values (stored correctly in Parquet files)
- Limited ability to read decimal256 values (currently truncated due to arrow-rs limitation)
- Added ability to read arrow IPC files

## 0.5.9

- Fix handling of all possible byte array lengths used for decimal value representation

## 0.5.8

- Upstreamed another fix, but it can't release until July, so pinning to my feature branch again
- Fix parsing of TIME millis and TIME micros.

## 0.5.7

- Merged a fix into upstream, so updating pinned arrow commit to `apache/arrow-rs#main` for now
- Match `SecureRandom.uuid` formatting when returning UUIDs
  - We used to return byte strings

## 0.5.6

- Upgrade rust arrow and parquet libraries to 55.1.0
- Improve support for byte array encoding of decimal values
  - Only supported 128 bit integer encoding, now we support 32, 64, and 128 bit encoding.

## 0.5.5

- More improvements to decimal support

## 0.5.4

- Fix an inconsistency in how precision and scale defaults were treated when writing parquet

## 0.5.3

- _Actually_ fix support for Ruby 3.1
- Add support for decimal type
- Add support for reading metadata from parquet files

## 0.5.2

- Fix support for Ruby 3.1

## 0.5.1

- Revert a change Arc usage, it was pointless.

## 0.5.0

### New Features & Enhancements

1. **Schema DSL for Complex Data Types**

   - Introduced a **new DSL** (Domain-Specific Language) for defining Parquet schemas in Ruby.
   - You can now describe **structs**, **lists**, and **maps** in a more expressive way:
     ```ruby
      schema = Parquet::Schema.define do
        field :id, :int32, nullable: false
        field :name, :string
        field :age, :int16
        field :weight, :float
        field :active, :boolean
        field :last_seen, :timestamp_millis, nullable: true
        field :scores, :list, item: :int32
        field :details, :struct do
          field :name, :string
          field :score, :double
        end
        field :tags, :list, item: :string
        field :metadata, :map, key: :string, value: :string
        field :properties, :map, key: :string, value: :int32
        field :complex_map, :map, key: :string, value: :struct do
          field :count, :int32
          field :description, :string
        end
        field :nested_lists, :list, item: :list do
          field :item, :string
        end
        field :map_of_lists, :map, key: :string, value: :list do
          field :item, :int32
        end
      end
     ```
   - This DSL supports nested (`struct` within `struct`, `list` of `struct`, etc.) and required/optional fields (`nullable: true`), making it easier to handle complex Parquet schemas.

2. **Writing Maps, Structs, and Lists**

   - The gem now supports **nested data** writes for:
     - **Maps** (`map<keyType, valueType>`), including map of primitives or map of nested types.
     - **Lists** (`list<T>`), including lists of structs or lists of primitives.
     - **Structs** (nested structures), letting you store records that have sub-records.
   - This feature is integrated into both the row-based and column-based writing APIs.
   - Reading these complex types (`each_row`, `each_column`) has been updated as well, so lists and maps yield the expected Ruby arrays and hashes.

3. **Logger Integration**

   - Added support for passing in a **Ruby logger** object for the gem’s internal logging.
   - The gem checks for an optional `logger:` keyword argument in methods and will use it if provided.
   - We'll respect the level returned by `logger.level` otherwise you can also override the log level with the environment variable **`PARQUET_GEM_LOG_LEVEL`** (e.g., `export PARQUET_GEM_LOG_LEVEL=debug`).
   - When no logger is provided, important warnings will print to `stderr`.

4. **Optional Slow Tests in CI**
   - In the GitHub Actions workflow (`ruby.yml`), we now set the environment variable `RUN_SLOW_TESTS=true`.
   - This allows the test suite to include (or skip) certain slow tests. In your local development, you can unset or override this if you want to skip longer-running tests.

### Other Changes

- **Internal Refactoring**:
  - Moved some reading/writing logic into `common.rs` and `logger.rs` for clearer code organization.
  - Introduced a new `RubyLogger` wrapper in Rust for bridging Ruby’s logger with Rust logging methods.
  - Streamlined the enumerator creation code to handle `logger` and `strict` mode more consistently.
  - Improved error-handling wrappers (`MagnusErrorWrapper`) around Rust’s `ParquetError` to raise clearer Ruby exceptions.

### Breaking Changes or Important Notes

- **Non-Null Fields in DSL**: In the new schema DSL, you can mark fields as `nullable: false`. Attempting to write a `nil` value into a non-nullable field will now raise an exception. Previously, the gem did not strictly enforce non-null constraints.
- **Strict UTF-8 Checks**: Writing invalid UTF-8 strings (e.g., corrupted byte sequences) will now raise an `EncodingError` rather than silently truncating or converting them.
- **Complex Nested Fields**: If you attempt to write nested lists/maps/structs but pass incompatible Ruby data (like an array for a map or a simple string instead of a struct-hash), you’ll get a clearer runtime error. The gem enforces that your data matches the declared schema shape.

### Migration Tips

- If you already used the older “legacy” schema format (an array of `{"name" => "type"}` pairs), it will continue to work. However, you can now opt into the **DSL** approach for more nested/complex use cases.

## 0.4.2

- When no schema is provided. Default to `f0`, `f1`, `f2`, etc.
- Improve string conversion when writing parquet.
- Improve error message when writing columns with a bad payload.

## 0.4.1

- Add validations that seekable IO objects are actually seekable.

## 0.4.0

- Default to strict parsing of strings
- Instead of returning strings from the reader in `ASCII-8BIT` format when `strict: false` (the default prior to this change), we now return the string encoded as lossy UTF-8.

## 0.3.3

- Re-add seek-able IO optimizations.

## 0.3.2

- Determining whether we've received a StringIO is difficult to do safely, so just treat it like an IO.

## 0.3.1

- Start estimating batch size before we have filled the sampling buffer to prevent OOMs on huge rows.

## 0.3.0

Got rid of surprising behaviour that bypassed ruby if the provided IO had a file descriptor. It led to confusing bugs where people would write a custom read method that was ignored because we read the file descriptor directly.

## 0.2.13

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
