require_relative "parquet/version"

begin
  require "parquet/#{RUBY_VERSION.to_f}/parquet"
rescue LoadError
  require "parquet/parquet"
end

module Parquet
end
