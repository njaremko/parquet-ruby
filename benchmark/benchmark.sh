#!/usr/bin/env bash
set -euo pipefail

export RB_SYS_CARGO_PROFILE=profiling

# echo "🧹 Cleaning previous build..."
# cargo clean

echo "📦 Installing Ruby dependencies..."
bundle install

echo "🔨 Compiling Rust extension..."
bundle exec rake compile

echo "🏃 Running benchmarks..."
bundle exec benchmark/comparison_benchmark.rb
