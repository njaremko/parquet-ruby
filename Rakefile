# frozen_string_literal: true

require "rake/testtask"
require "rb_sys/extensiontask"

task default: :test

GEMSPEC = Gem::Specification.load("parquet.gemspec")

RbSys::ExtensionTask.new("parquet", GEMSPEC) do |ext|
  ext.lib_dir = "lib/parquet"
  ext.ext_dir = "ext/parquet"
end

Rake::TestTask.new do |t|
  t.deps << :compile
  t.test_files = FileList[File.expand_path("test/*_test.rb", __dir__)]
  t.libs << "lib"
  t.libs << "test"
end

task :release do
  sh "bundle exec rake test"
  sh "mkdir -p pkg"
  sh "gem build parquet.gemspec -o pkg/parquet-#{Parquet::VERSION}.gem"
  sh "gem push pkg/parquet-#{Parquet::VERSION}.gem"
end
