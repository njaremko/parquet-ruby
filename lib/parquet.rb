require_relative "parquet/version"
require_relative "parquet/schema"

begin
  require "parquet/#{RUBY_VERSION.to_f}/parquet"
rescue LoadError
  require "parquet/parquet"
end

module Parquet
end
