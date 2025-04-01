# frozen_string_literal: true

require "minitest/autorun"
require "parquet"
require "csv"

module Minitest
  class Test
    alias_method :original_run, :run
    
    def run
      puts "Running test: #{self.class}##{name}"
      original_run
    end
  end
end