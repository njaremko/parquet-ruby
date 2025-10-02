require_relative "parquet/version"
require_relative "parquet/schema"

begin
  require "parquet/#{RUBY_VERSION.to_f}/parquet"
rescue LoadError
  require "parquet/parquet"
end

module Parquet
  class << self
    alias_method :__original_each_row__, :each_row
    alias_method :__original_each_column__, :each_column

    def each_row(source, result_type: :hash, columns: nil, row_groups: nil, strict: false, logger: nil, &block)
      return enum_for(:each_row, source, result_type: result_type, columns: columns, row_groups: row_groups, strict: strict, logger: logger) unless block_given?

      columns = Array(columns).map!(&:to_s) if columns
      row_groups = Array(row_groups).map!(&:to_i) if row_groups

      __original_each_row__(source, result_type: result_type, columns: columns, row_groups: row_groups, strict: strict, logger: logger, &block)
    end

    def each_column(source, result_type: :hash, columns: nil, row_groups: nil, batch_size: nil, strict: false, logger: nil, &block)
      return enum_for(:each_column, source, result_type: result_type, columns: columns, row_groups: row_groups, batch_size: batch_size, strict: strict, logger: logger) unless block_given?

      columns = Array(columns).map!(&:to_s) if columns
      row_groups = Array(row_groups).map!(&:to_i) if row_groups

      __original_each_column__(source, result_type: result_type, columns: columns, row_groups: row_groups, batch_size: batch_size, strict: strict, logger: logger, &block)
    end
  end

  class FileWriter
    def initialize(schema:, write_to:, compression: nil, row_group_target_bytes: nil, sample_size: nil, string_cache: false, logger: nil)
      @id = Parquet._fw_create(schema, write_to,
        compression: compression,
        row_group_target_bytes: row_group_target_bytes,
        sample_size: sample_size,
        string_cache: string_cache,
        logger: logger)
    end

    def write_rows(rows)
      Parquet._fw_write_rows(@id, rows)
      self
    end

    def flush_row_group
      Parquet._fw_flush_row_group(@id)
      self
    end

    def close
      Parquet._fw_close(@id)
      self
    end
  end
end
