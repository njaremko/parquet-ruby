source "https://rubygems.org"

gem "rb_sys", "~> 0.9.56"
gem "rake"
gem "bigdecimal"
gem "logger"

# Use local version of parquet
gemspec

group :development do
  # gem "benchmark-ips", "~> 2.12"
  # gem "polars-df"
  # gem "duckdb"
  gem "benchmark-memory"
end

group :test do
  gem "csv"
  gem "minitest", "~> 5.0"
end
