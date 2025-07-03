#!/bin/bash

# Memory testing script for macOS
# This script runs memory tests while monitoring system memory usage

echo "=== Parquet Memory Testing Suite (macOS) ==="
echo "Starting at: $(date)"
echo

# Function to get current memory usage for a PID
get_memory_mb() {
    local pid=$1
    ps -o rss= -p $pid 2>/dev/null | awk '{print $1/1024}'
}

# Function to monitor memory usage during test execution
monitor_memory() {
    local test_name=$1
    local log_file="memory_log_${test_name}_$(date +%s).txt"

    echo "Running test: $test_name"
    echo "Logging memory usage to: $log_file"

    # Start the test in background
    direnv exec . ruby -Ilib:test test/memory_usage_test.rb -n $test_name &
    local test_pid=$!

    # Monitor memory usage
    echo "Time,Memory_MB" > $log_file
    local start_time=$(date +%s)

    while kill -0 $test_pid 2>/dev/null; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        local memory=$(get_memory_mb $test_pid)

        if [ ! -z "$memory" ]; then
            echo "$elapsed,$memory" >> $log_file
            printf "\r  Memory: %6.2f MB  Time: %3ds" $memory $elapsed
        fi

        sleep 0.5
    done

    wait $test_pid
    local exit_code=$?

    echo  # New line after progress

    # Analyze results
    if [ -f $log_file ]; then
        local max_memory=$(awk -F, 'NR>1 {if($2>max) max=$2} END {print max}' $log_file)
        local min_memory=$(awk -F, 'NR>1 {if(min=="" || $2<min) min=$2} END {print min}' $log_file)
        local growth=$(echo "$max_memory - $min_memory" | bc)

        echo "  Memory range: ${min_memory}MB - ${max_memory}MB"
        echo "  Peak growth: ${growth}MB"
        echo
    fi

    return $exit_code
}

# Function to generate memory usage graph (requires gnuplot)
generate_graph() {
    if command -v gnuplot >/dev/null 2>&1; then
        echo "Generating memory usage graphs..."

        for log_file in memory_log_*.txt; do
            [ -f "$log_file" ] || continue

            local graph_file="${log_file%.txt}.png"

            gnuplot <<EOF
set terminal png size 800,600
set output '$graph_file'
set title 'Memory Usage Over Time'
set xlabel 'Time (seconds)'
set ylabel 'Memory (MB)'
set grid
set datafile separator ','
plot '$log_file' using 1:2 with lines title 'Memory Usage' lw 2
EOF
            echo "  Generated: $graph_file"
        done
    else
        echo "Install gnuplot to generate memory usage graphs"
    fi
}

# Main execution
echo "1. Running individual memory tests..."
echo

# Run each test method
for test in test_reading_large_file_constant_memory \
           test_writing_large_rows_constant_memory \
           test_concurrent_reading_memory_efficiency \
           test_memory_with_complex_types; do
    monitor_memory $test
done

echo
echo "2. Running memory benchmark..."
echo

# Run the benchmark
direnv exec . ruby benchmark/benchmark_memory.rb

echo
echo "3. Running streaming benchmark with memory profiling..."
echo

# Set up memory profiling for streaming benchmark
export MEMORY_PROFILE=1
direnv exec . ruby benchmark/benchmark_streaming.rb 100

# Generate graphs if possible
echo
generate_graph

# Summary
echo
echo "=== Test Summary ==="
echo "Completed at: $(date)"
echo
echo "Log files generated:"
ls -la memory_log_*.txt 2>/dev/null || echo "  No log files found"
echo
echo "To run with Ruby memory profiler:"
echo "  gem install memory_profiler"
echo "  MEMORY_PROFILE=1 direnv exec . ruby -Ilib:test test/memory_usage_test.rb"
echo
echo "To run with larger test files:"
echo "  RUN_LARGE_TESTS=1 direnv exec . rake test"
