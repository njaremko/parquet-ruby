# parquet-ruby

[![Gem Version](https://badge.fury.io/rb/parquet.svg)](https://badge.fury.io/rb/parquet)

This project is a Ruby library wrapping the [parquet-rs](https://github.com/apache/parquet-rs) rust crate.

At the moment, it only supports iterating rows as either a hash or an array.

## Usage

```ruby
require "parquet"

# Read each row as a hash
Parquet.each_row("test/data.parquet") { |row| puts row.inspect }

# Read each row as an array
Parquet.each_row("test/data.parquet", result_type: :array) { |row| puts row.inspect }

# Read from an IO object (like File or StringIO)
File.open("test/data.parquet", "rb") do |file|
  Parquet.each_row(file) { |row| puts row.inspect }
end

# Or with StringIO
io = StringIO.new(File.binread("test/data.parquet"))
Parquet.each_row(io) { |row| puts row.inspect }

```
