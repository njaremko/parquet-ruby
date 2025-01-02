require_relative "lib/parquet/version"

Gem::Specification.new do |spec|
  spec.name = "parquet"
  spec.version = Parquet::VERSION
  spec.authors = ["Nathan Jaremko"]
  spec.email = ["nathan@jaremko.ca"]

  spec.summary = "Parquet library for Ruby, written in Rust"
  spec.description = <<-EOF
    Parquet is a high-performance Parquet library for Ruby, written in Rust.
    It wraps the official Apache Rust implementation to provide fast, correct Parquet parsing.
  EOF
  spec.homepage = "https://github.com/njaremko/parquet"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/njaremko/parquet-ruby"
  spec.metadata["readme_uri"] = "https://github.com/njaremko/parquet-ruby/blob/main/README.md"
  spec.metadata["changelog_uri"] = "https://github.com/njaremko/parquet-ruby/blob/main/CHANGELOG.md"
  spec.metadata["documentation_uri"] = "https://www.rubydoc.info/gems/parquet"
  spec.metadata["funding_uri"] = "https://github.com/sponsors/njaremko"

  spec.files =
    Dir[
      "lib/**/*.rb",
      "lib/**/*.rbi",
      "ext/**/*",
      "LICENSE",
      "README.md",
      "Cargo.toml",
      "Cargo.lock",
      "Gemfile",
      "Rakefile"
    ]
  spec.require_paths = ["lib"]

  spec.extensions = ["ext/parquet/extconf.rb"]

  # needed until rubygems supports Rust support is out of beta
  spec.add_dependency "rb_sys", "~> 0.9.39"

  # only needed when developing or packaging your gem
  spec.add_development_dependency "rake-compiler", "~> 1.2.0"
end
