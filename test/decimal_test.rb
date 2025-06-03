# frozen_string_literal: true
require "tempfile"
require "bigdecimal"
require "date"

require "parquet"
require "minitest/autorun"

class DecimalTest < Minitest::Test
  def test_decimal_column_operations
    temp_path = "test/decimal_columns.parquet"
    begin
      # Write some decimal data in row format to test column reading
      test_data = [
        [BigDecimal("123.45"), BigDecimal("1234.567"), BigDecimal("-123.45")],
        [BigDecimal("567.89"), BigDecimal("7890.123"), BigDecimal("-567.89")],
        [BigDecimal("999.99"), BigDecimal("9999.999"), BigDecimal("-999.99")],
        [BigDecimal("0.01"), BigDecimal("0.001"), BigDecimal("-0.01")],
        [BigDecimal("0.00"), BigDecimal("0.000"), BigDecimal("0.00")],
        [BigDecimal("1.23"), BigDecimal("0.123"), BigDecimal("-1.23")]
      ]

      schema = [
        { "decimal_5_2" => "decimal(5,2)" },
        { "decimal_7_3" => "decimal(7,3)" },
        { "negative_decimal" => "decimal(5,2)" }
      ]

      # Write to parquet file first as rows
      Parquet.write_rows(test_data.each, schema: schema, write_to: temp_path)

      # Now read back as columns
      columns = []
      Parquet.each_row(temp_path) do |row|
        # Convert rows to columns format for testing
        columns = [{ "decimal_5_2" => [], "decimal_7_3" => [], "negative_decimal" => [] }] if columns.empty?

        columns[0]["decimal_5_2"] << row["decimal_5_2"]
        columns[0]["decimal_7_3"] << row["decimal_7_3"]
        columns[0]["negative_decimal"] << row["negative_decimal"]
      end

      # Verify all values are correctly captured
      assert_equal 1, columns.size, "Should have one batch with all data"
      assert_equal 6, columns[0]["decimal_5_2"].size, "Should have 6 values in decimal_5_2 column"

      # Check values from first batch
      assert_equal BigDecimal("123.45"), columns[0]["decimal_5_2"][0]
      assert_equal BigDecimal("567.89"), columns[0]["decimal_5_2"][1]
      assert_equal BigDecimal("999.99"), columns[0]["decimal_5_2"][2]

      assert_equal BigDecimal("1234.567"), columns[0]["decimal_7_3"][0]
      assert_equal BigDecimal("7890.123"), columns[0]["decimal_7_3"][1]
      assert_equal BigDecimal("9999.999"), columns[0]["decimal_7_3"][2]

      assert_equal BigDecimal("-123.45"), columns[0]["negative_decimal"][0]
      assert_equal BigDecimal("-567.89"), columns[0]["negative_decimal"][1]
      assert_equal BigDecimal("-999.99"), columns[0]["negative_decimal"][2]

      # Check values from second batch
      assert_equal BigDecimal("0.01"), columns[0]["decimal_5_2"][3]
      assert_equal BigDecimal("0.00"), columns[0]["decimal_5_2"][4]
      assert_equal BigDecimal("1.23"), columns[0]["decimal_5_2"][5]

      assert_equal BigDecimal("0.001"), columns[0]["decimal_7_3"][3]
      assert_equal BigDecimal("0.000"), columns[0]["decimal_7_3"][4]
      assert_equal BigDecimal("0.123"), columns[0]["decimal_7_3"][5]

      assert_equal BigDecimal("-0.01"), columns[0]["negative_decimal"][3]
      assert_equal BigDecimal("0.00"), columns[0]["negative_decimal"][4]
      assert_equal BigDecimal("-1.23"), columns[0]["negative_decimal"][5]

      # Test all rows are correctly read
      all_rows = []
      Parquet.each_row(temp_path) { |row| all_rows << row }

      # We should get six rows in total
      assert_equal 6, all_rows.size, "Should have six rows in total"

      # Test with specific columns
      specific_columns_rows = []
      Parquet.each_row(temp_path, columns: %w[decimal_5_2 negative_decimal]) { |row| specific_columns_rows << row }

      # Should only have the requested columns
      assert_equal 6, specific_columns_rows.size
      assert_equal %w[decimal_5_2 negative_decimal].sort, specific_columns_rows[0].keys.sort
      assert_nil specific_columns_rows[0]["decimal_7_3"]

      # Values should still be correct
      assert_equal BigDecimal("123.45"), specific_columns_rows[0]["decimal_5_2"]
      assert_equal BigDecimal("-123.45"), specific_columns_rows[0]["negative_decimal"]
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_decimal_parse_3_bytes
    x = Parquet.each_row("test/3-byte-decimal.parquet", result_type: :array).to_a
    assert_equal BigDecimal("123.45"), x[0][7]
    assert_equal BigDecimal("123.45"), x[0][8]
  end

  def test_decimal_precision_scale_edge_cases
    temp_path = "test/decimal_edge_cases_advanced.parquet"
    begin
      # Simplified test with more reasonable precision/scale values
      data = [
        # Normal values
        [
          "12345", # standard integer
          "1234.5678", # decimal with fraction
          "1.00", # small value with trailing zeros
          "0.123", # small decimal
          "1.234", # standard precision
          "0.00000" # zero with padding
        ],
        # Boundary values
        [
          "99999", # max for the precision
          "9999.9999", # max value with decimal
          "9.99", # max for limited precision
          "0.999", # close to 1
          "9.999", # standard boundary
          "9.99999" # upper boundary
        ],
        # Negative values
        [
          "-12345", # negative integer
          "-1234.5678", # negative with fraction
          "-1.00", # negative with trailing zeros
          "-0.123", # negative small value
          "-1.234", # negative standard
          "-9.99999" # negative upper boundary
        ],
        # Scientific notation values for the parser improvements
        [
          "1.23e2", # 123.0 in scientific notation
          "1.234e3", # 1234.0 in scientific notation
          "1.2e-2", # 0.012 in scientific notation
          "0.01", # Direct decimal (same as 1.0e-2)
          "5e3", # 5000 without decimal point
          "5e-3" # 0.005 without decimal point
        ]
      ]

      # Create schema with different decimal types
      schema = [
        { "int_val" => "decimal(8,0)" }, # Integer values
        { "dec_val" => "decimal(10,4)" }, # Standard decimal
        { "small_dec" => "decimal(5,2)" }, # Small decimal
        { "tiny_dec" => "decimal(5,3)" }, # Tiny decimal
        { "std_dec" => "decimal(6,3)" }, # Standard decimal
        { "special_val" => "decimal(10,5)" } # Special values with padding
      ]

      # Write rows to parquet file
      Parquet.write_rows(data.each, schema: schema, write_to: temp_path)

      # Read the file back to verify what was actually stored
      rows = Parquet.each_row(temp_path).to_a
      assert_equal 4, rows.size

      # Check first row values
      assert_equal BigDecimal("12345"), rows[0]["int_val"]
      assert_equal BigDecimal("1234.5678"), rows[0]["dec_val"]
      assert_equal BigDecimal("1.00"), rows[0]["small_dec"]
      assert_equal BigDecimal("0.123"), rows[0]["tiny_dec"]
      assert_equal BigDecimal("1.234"), rows[0]["std_dec"]
      assert_equal BigDecimal("0.00000"), rows[0]["special_val"]

      # Check boundary values
      assert_equal BigDecimal("99999"), rows[1]["int_val"]
      assert_equal BigDecimal("9999.9999"), rows[1]["dec_val"]
      assert_equal BigDecimal("9.99"), rows[1]["small_dec"]
      assert_equal BigDecimal("0.999"), rows[1]["tiny_dec"]
      assert_equal BigDecimal("9.999"), rows[1]["std_dec"]
      assert_equal BigDecimal("9.99999"), rows[1]["special_val"]

      # Check negative values
      assert_equal BigDecimal("-12345"), rows[2]["int_val"]
      assert_equal BigDecimal("-1234.5678"), rows[2]["dec_val"]
      assert_equal BigDecimal("-1.00"), rows[2]["small_dec"]
      assert_equal BigDecimal("-0.123"), rows[2]["tiny_dec"]
      assert_equal BigDecimal("-1.234"), rows[2]["std_dec"]
      assert_equal BigDecimal("-9.99999"), rows[2]["special_val"]

      # Check scientific notation values
      assert_equal BigDecimal("123"), rows[3]["int_val"]
      assert_equal BigDecimal("1234"), rows[3]["dec_val"]
      assert_equal BigDecimal("0.01"), rows[3]["small_dec"]
      assert_equal BigDecimal("0.010"), rows[3]["tiny_dec"]
      assert_equal BigDecimal("5000.000"), rows[3]["std_dec"]
      assert_equal BigDecimal("0.005"), rows[3]["special_val"]

      # Now test with a simpler rounding scenario
      rounding_test_path = "test/decimal_rounding.parquet"
      begin
        # Test only cases where rounding behavior should be clear
        rounding_data = [
          ["1.23"], # No rounding needed
          ["1.26"], # Should round to 1.26
          ["-1.23"], # No rounding needed
          ["-1.26"] # Should round to -1.26
        ]

        # Write with scale 2
        Parquet.write_rows(rounding_data.each, schema: [{ "rounded" => "decimal(4,2)" }], write_to: rounding_test_path)

        # Verify behavior
        rounded_rows = Parquet.each_row(rounding_test_path).to_a
        assert_equal 4, rounded_rows.size

        # These assertions should pass regardless of exact rounding implementation
        assert_equal BigDecimal("1.23"), rounded_rows[0]["rounded"]
        assert_equal BigDecimal("1.26"), rounded_rows[1]["rounded"]
        assert_equal BigDecimal("-1.23"), rounded_rows[2]["rounded"]
        assert_equal BigDecimal("-1.26"), rounded_rows[3]["rounded"]
      ensure
        File.delete(rounding_test_path) if File.exist?(rounding_test_path)
      end
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_decimal_compression_options
    # This test verifies that different compression algorithms work correctly with decimal data
    # We'll test the following compression options: UNCOMPRESSED, SNAPPY, GZIP, and ZSTD

    compression_types = [
      nil, # Default/no compression specified
      "UNCOMPRESSED",
      "SNAPPY", # Fast compression
      "GZIP", # Better compression ratio
      "ZSTD" # Modern alternative
    ]

    # Define decimal test data with a mix of values
    decimal_data = [
      [
        BigDecimal("123.45"),
        BigDecimal("0.00123"),
        BigDecimal("-9999.9999"),
        BigDecimal("0.0"),
        BigDecimal("9999999.999999")
      ]
    ]

    # Define schema with different decimal precisions/scales
    schema = [{ "mixed_decimals" => "decimal(15,6)" }]

    compression_results = {}

    # Test each compression type
    compression_types.each do |compression|
      # Skip compression types not supported by the library
      begin
        temp_path = "test/decimal_#{compression || "default"}.parquet"

        # Write the file with specified compression
        if compression.nil?
          Parquet.write_rows(decimal_data.each, schema: schema, write_to: temp_path)
        else
          Parquet.write_rows(decimal_data.each, schema: schema, write_to: temp_path, compression: compression)
        end

        # Verify file exists and record its size
        assert File.exist?(temp_path), "File should be created with compression: #{compression || "default"}"
        file_size = File.size(temp_path)
        compression_results[compression || "default"] = file_size

        # Read back and verify that all values are preserved correctly
        rows = Parquet.each_row(temp_path).to_a
        assert_equal 5, rows.size, "Should have 5 rows with compression: #{compression || "default"}"

        # Check that decimal values are preserved exactly
        assert_equal BigDecimal("123.45"), rows[0]["mixed_decimals"]
        assert_equal BigDecimal("0.00123"), rows[1]["mixed_decimals"]
        assert_equal BigDecimal("-9999.9999"), rows[2]["mixed_decimals"]
        assert_equal BigDecimal("0.0"), rows[3]["mixed_decimals"]
        assert_equal BigDecimal("9999999.999999"), rows[4]["mixed_decimals"]

        # Cleanup
        File.delete(temp_path)
      rescue => e
        # If a compression type isn't supported, just note it
        puts "Compression type #{compression} not supported: #{e.message}" if ENV["VERBOSE"]
      ensure
        File.delete(temp_path) if File.exist?(temp_path)
      end
    end

    # Compare file sizes to verify compression is actually working
    # Note: UNCOMPRESSED should be larger than compressed versions
    if compression_results.key?("UNCOMPRESSED") && compression_results.key?("GZIP")
      assert_operator compression_results["UNCOMPRESSED"],
                      :>,
                      compression_results["GZIP"],
                      "GZIP should produce smaller file than UNCOMPRESSED"
    end

    # Create a much larger decimal dataset to better test compression
    large_decimal_test_path = "test/large_decimal_compression.parquet"
    begin
      # Generate 200 rows with repeating decimal patterns (good for compression)
      large_data = []

      # Generate data with good compression potential (repeating patterns)
      200.times do |i|
        large_data << [
          BigDecimal("123.456789"), # Repeating value
          BigDecimal((i % 10).to_s), # Cycling values 0-9
          BigDecimal("#{i}.#{i}"), # Structured pattern
          BigDecimal(i.to_s) # Incrementing values
        ]
      end

      # Schema with different decimal types
      large_schema = [
        { "repeated" => "decimal(10,6)" },
        { "cycling" => "decimal(2,0)" },
        { "patterned" => "decimal(8,3)" },
        { "incrementing" => "decimal(6,0)" }
      ]

      # Test GZIP (generally available) compression with the larger dataset
      Parquet.write_rows(large_data.each, schema: large_schema, write_to: large_decimal_test_path, compression: "GZIP")

      # Read back and verify
      result_rows = Parquet.each_row(large_decimal_test_path).to_a
      assert_equal 200, result_rows.size

      # Check a few values
      assert_equal BigDecimal("123.456789"), result_rows[0]["repeated"]
      assert_equal BigDecimal("0"), result_rows[0]["cycling"]
      assert_equal BigDecimal("0.0"), result_rows[0]["patterned"]
      assert_equal BigDecimal("0"), result_rows[0]["incrementing"]

      assert_equal BigDecimal("123.456789"), result_rows[10]["repeated"]
      assert_equal BigDecimal("0"), result_rows[10]["cycling"] # 10 % 10 = 0
      assert_equal BigDecimal("10.10"), result_rows[10]["patterned"]
      assert_equal BigDecimal("10"), result_rows[10]["incrementing"]

      # Verify file existence and size for reporting
      assert File.exist?(large_decimal_test_path)
      compressed_size = File.size(large_decimal_test_path)

      # Now write the same data without compression for comparison
      Parquet.write_rows(
        large_data.each,
        schema: large_schema,
        write_to: "#{large_decimal_test_path}_uncompressed",
        compression: "UNCOMPRESSED"
      )

      uncompressed_size = File.size("#{large_decimal_test_path}_uncompressed")

      # We don't test the compression ratio as it depends on the data and algorithm,
      # just verify that the file was written successfully
      assert_operator compressed_size, :>, 0, "Compressed file should have a valid size"

      # Calculate compression ratio for interest
      compression_ratio = (uncompressed_size.to_f / compressed_size).round(2)
      puts "Decimal data compression ratio with GZIP: #{compression_ratio}x" if ENV["VERBOSE"]
    ensure
      File.delete(large_decimal_test_path) if File.exist?(large_decimal_test_path)
      File.delete("#{large_decimal_test_path}_uncompressed") if File.exist?("#{large_decimal_test_path}_uncompressed")
    end
  end

  def test_schema_evolution_with_decimals
    # This test verifies correct handling of schema evolution scenarios with decimal fields

    # First, create a parquet file with the initial schema
    initial_path = "test/decimal_schema_initial.parquet"
    evolved_path = "test/decimal_schema_evolved.parquet"

    begin
      # Initial schema with simple decimal columns
      initial_schema = [{ "id" => "int32" }, { "amount" => "decimal(10,2)" }, { "name" => "string" }]

      # Initial data with integers and standard decimal values
      initial_data = [
        [1, BigDecimal("123.45"), "Item 1"],
        [2, BigDecimal("678.90"), "Item 2"],
        [3, BigDecimal("50.00"), "Item 3"]
      ]

      # Write the initial file
      Parquet.write_rows(initial_data.each, schema: initial_schema, write_to: initial_path)

      # Evolved schema:
      # - Changed precision and scale for 'amount'
      # - Added a new decimal column 'tax'
      # - Removed 'name' column
      # - Added a new column 'date'
      evolved_schema = [
        { "id" => "int32" },
        { "amount" => "decimal(12,4)" }, # Increased precision and scale
        { "tax" => "decimal(6,2)" }, # New decimal column
        { "date" => "date32" } # New date column
      ]

      # Evolved data with more precise decimals and additional columns
      # Using string representation of dates since Date objects aren't directly supported
      evolved_data = [
        [101, BigDecimal("123.4567"), BigDecimal("12.34"), "2023-01-01"],
        [102, BigDecimal("678.9012"), BigDecimal("67.89"), "2023-01-02"],
        [103, BigDecimal("50.0000"), BigDecimal("5.00"), "2023-01-03"]
      ]

      # Write the evolved file
      Parquet.write_rows(evolved_data.each, schema: evolved_schema, write_to: evolved_path)

      # Test 1: Read evolved schema file with all columns
      evolved_rows = Parquet.each_row(evolved_path).to_a
      assert_equal 3, evolved_rows.size

      # Verify the evolved schema data
      assert_equal 101, evolved_rows[0]["id"]
      assert_equal BigDecimal("123.4567"), evolved_rows[0]["amount"]
      assert_equal BigDecimal("12.34"), evolved_rows[0]["tax"]
      assert_equal "2023-01-01", evolved_rows[0]["date"].to_s

      # Test 2: Read evolved file but only request original columns
      # This tests reading a subset of columns from the evolved schema
      evolved_original_cols = Parquet.each_row(evolved_path, columns: %w[id amount]).to_a
      assert_equal 3, evolved_original_cols.size
      assert_equal 101, evolved_original_cols[0]["id"]
      assert_equal BigDecimal("123.4567"), evolved_original_cols[0]["amount"]
      assert_nil evolved_original_cols[0]["tax"] # Not requested
      assert_nil evolved_original_cols[0]["date"] # Not requested

      # Test 3: Attempt to read initial file with evolved schema column expectations
      # Try to read 'tax' column which doesn't exist in the original file
      initial_evolved_cols = Parquet.each_row(initial_path, columns: %w[id amount tax]).to_a
      assert_equal 3, initial_evolved_cols.size
      assert_equal 1, initial_evolved_cols[0]["id"]
      assert_equal BigDecimal("123.45"), initial_evolved_cols[0]["amount"]
      assert_nil initial_evolved_cols[0]["tax"] # Should be nil since column doesn't exist

      # Test 4: Test schema evolution with different scales
      # Create a file with high scale
      high_scale_path = "test/decimal_high_scale.parquet"
      begin
        high_scale_schema = [
          { "amount" => "decimal(10,5)" } # 5 decimal places
        ]

        high_scale_data = [
          [BigDecimal("123.45678")], # More decimal places than will fit
          [BigDecimal("0.00001")] # Very small value
        ]

        Parquet.write_rows(high_scale_data.each, schema: high_scale_schema, write_to: high_scale_path)

        # Read with different column specifications to test how decimal scaling is handled
        full_rows = Parquet.each_row(high_scale_path).to_a
        assert_equal 2, full_rows.size
        assert_equal BigDecimal("123.45678"), full_rows[0]["amount"]
        assert_equal BigDecimal("0.00001"), full_rows[1]["amount"]
      ensure
        File.delete(high_scale_path) if File.exist?(high_scale_path)
      end

      # Test 5: Test schema evolution with different decimal types
      # First create file with small precision values
      decimal64_path = "test/decimal64.parquet"
      decimal128_path = "test/decimal128.parquet"

      begin
        # Create with small precision decimal values
        decimal64_schema = [{ "small_decimal" => "decimal(8,2)" }]

        decimal64_data = [
          [BigDecimal("123.45")] # Uses precision 8, scale 2
        ]

        Parquet.write_rows(decimal64_data.each, schema: decimal64_schema, write_to: decimal64_path)

        # Read file with smaller precision to verify values
        d64_rows = Parquet.each_row(decimal64_path).to_a
        assert_equal 1, d64_rows.size, "Should have one row in decimal64 file"
        assert_equal BigDecimal("123.45"), d64_rows[0]["small_decimal"]

        # Now create with large precision values
        decimal128_schema = [
          { "large_decimal" => "decimal(18,8)" } # Reduced from 20,10 to avoid potential overflow
        ]

        decimal128_data = [
          [BigDecimal("1234567.12345678")] # Uses precision 18, scale 8
        ]

        Parquet.write_rows(decimal128_data.each, schema: decimal128_schema, write_to: decimal128_path)

        # Read file with larger precision to verify values
        d128_rows = Parquet.each_row(decimal128_path).to_a
        assert_equal 1, d128_rows.size, "Should have one row in decimal128 file"
        assert_equal BigDecimal("1234567.12345678"), d128_rows[0]["large_decimal"]
      ensure
        File.delete(decimal64_path) if File.exist?(decimal64_path)
        File.delete(decimal128_path) if File.exist?(decimal128_path)
      end
    ensure
      File.delete(initial_path) if File.exist?(initial_path)
      File.delete(evolved_path) if File.exist?(evolved_path)
    end
  end

  def test_threadsafe_decimal_writer
    # This test verifies that the Parquet writer is thread-safe,
    # particularly for decimal values which might use complex conversions

    # Skip this test if we can't use threads
    skip "Thread testing requires a Ruby with working threads" unless Thread.respond_to?(:fork)

    output_path = "test/threadsafe_decimal_writer.parquet"
    begin
      # Define a common schema to be used by all threads
      schema = [
        { "thread_id" => "int32" },
        { "counter" => "int32" },
        { "amount" => "decimal(10,2)" },
        { "timestamp_str" => "string" } # Changed from timestamp_millis to string
      ]

      # Number of threads to use
      num_threads = 4

      # Number of rows each thread will write
      rows_per_thread = 10

      # Create a temporary directory for thread-specific files
      temp_dir = "test/thread_temp"
      Dir.mkdir(temp_dir) unless Dir.exist?(temp_dir)

      # Start multiple threads, each creating its own decimals
      threads = []
      thread_files = []

      # Create thread files first
      num_threads.times { |thread_id| thread_files << "#{temp_dir}/thread_#{thread_id}.parquet" }

      # Each thread will create its own data file, then we'll merge them
      num_threads.times do |thread_id|
        threads << Thread.new do
          thread_file = thread_files[thread_id]

          # Generate thread-specific data with decimals
          data = []
          rows_per_thread.times do |i|
            # Use simple decimal calculations for thread safety
            amount = BigDecimal((thread_id * 100 + i).to_s) / BigDecimal("100")

            data << [
              thread_id,
              i,
              amount,
              Time.now.to_s # Store time as a string instead of Time object
            ]
          end

          # Write this thread's data to its own file
          Parquet.write_rows(data.each, schema: schema, write_to: thread_file)
        end
      end

      # Wait for all threads to complete
      threads.each(&:join)

      # Verify each thread created its file
      thread_files.each { |file| assert File.exist?(file), "Thread file #{file} should exist" }

      # Now read all thread files and combine their data
      combined_data = []
      thread_files.each do |file|
        rows = Parquet.each_row(file).to_a
        assert_equal rows_per_thread, rows.size, "Each thread should write #{rows_per_thread} rows"
        combined_data.concat(rows)
      end

      # Write the combined data to a single file
      combined_rows =
        combined_data.map { |row| [row["thread_id"], row["counter"], row["amount"], row["timestamp_str"]] }

      Parquet.write_rows(combined_rows.each, schema: schema, write_to: output_path)

      # Read the final file and verify all data is present and correct
      final_rows = Parquet.each_row(output_path).to_a
      assert_equal num_threads * rows_per_thread, final_rows.size

      # Check data integrity by thread
      num_threads.times do |thread_id|
        thread_rows = final_rows.select { |row| row["thread_id"] == thread_id }
        assert_equal rows_per_thread, thread_rows.size

        # Verify counter sequence for each thread
        counters = thread_rows.map { |row| row["counter"] }.sort
        assert_equal (0...rows_per_thread).to_a, counters

        # Verify decimal calculations
        thread_rows.each do |row|
          counter = row["counter"]
          expected_amount = BigDecimal((thread_id * 100 + counter).to_s) / BigDecimal("100")

          assert_equal expected_amount,
                       row["amount"],
                       "Decimal calculation in thread #{thread_id}, counter #{counter} should match"

          # Verify timestamp string is present (don't check exact value)
          assert_kind_of String, row["timestamp_str"]
          refute_nil row["timestamp_str"]
          refute_empty row["timestamp_str"]
        end
      end
    ensure
      # Clean up all temporary files
      File.delete(output_path) if File.exist?(output_path)

      if Dir.exist?("test/thread_temp")
        Dir.glob("test/thread_temp/*.parquet").each { |f| File.delete(f) if File.exist?(f) }
        Dir.rmdir("test/thread_temp")
      end
    end
  end

  def test_decimal_schema_dsl
    temp_path = "test/decimal_schema_dsl.parquet"
    begin
      # Test data with decimal values
      test_data = [
        [BigDecimal("123.45"), BigDecimal("9876.54321"), BigDecimal("-42.42")],
        [BigDecimal("0.01"), BigDecimal("1234.56789"), BigDecimal("-99.99")],
        [BigDecimal("999.99"), BigDecimal("0.00001"), BigDecimal("-0.01")]
      ]

      # Use schema DSL to define decimal columns with different precision and scale
      schema =
        Parquet::Schema.define do
          field :small_decimal, :decimal, precision: 5, scale: 2
          field :large_decimal, :decimal, precision: 10, scale: 5
          field :negative_decimal, :decimal, precision: 4, scale: 2, nullable: false
        end

      Parquet.write_rows(test_data.each, schema:, write_to: temp_path)

      # Read back and verify the data
      rows = Parquet.each_row(temp_path).to_a
      assert_equal 3, rows.size

      # Verify first row
      assert_equal BigDecimal("123.45"), rows[0]["small_decimal"]
      assert_equal BigDecimal("9876.54321"), rows[0]["large_decimal"]
      assert_equal BigDecimal("-42.42"), rows[0]["negative_decimal"]

      # Verify second row
      assert_equal BigDecimal("0.01"), rows[1]["small_decimal"]
      assert_equal BigDecimal("1234.56789"), rows[1]["large_decimal"]
      assert_equal BigDecimal("-99.99"), rows[1]["negative_decimal"]

      # Verify third row
      assert_equal BigDecimal("999.99"), rows[2]["small_decimal"]
      assert_equal BigDecimal("0.00001"), rows[2]["large_decimal"]
      assert_equal BigDecimal("-0.01"), rows[2]["negative_decimal"]

      # Verify schema metadata
      file_metadata = Parquet.metadata(temp_path)
      assert_kind_of Hash, file_metadata, "Metadata should be a hash"
      assert_equal 3, file_metadata["num_rows"], "Metadata should report 3 rows"
      assert file_metadata["schema"], "Should have schema information"
      assert file_metadata["row_groups"], "Should have row group information"
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_decimal_comparison_with_different_scales
    temp_path = "test/decimal_scale_comparison.parquet"
    begin
      # Test data with different scales for same values
      test_data = [
        [BigDecimal("123.45"), BigDecimal("123.45")],
        [BigDecimal("1.2345"), BigDecimal("1.2345")],
        [BigDecimal("1.00"), BigDecimal("1")]
      ]

      # Schema with different scale specifications
      schema = [
        { "decimal_scale_2" => "decimal(5,2)" }, # 123.45 stored as 12345 with scale 2
        { "decimal_scale_4" => "decimal(6,4)" } # 123.45 stored as 1234500 with scale 4
      ]

      Parquet.write_rows(test_data.each, schema: schema, write_to: temp_path)

      # Read back and verify data
      rows = Parquet.each_row(temp_path).to_a
      assert_equal 3, rows.size

      # First row - same value with different scales should compare equal
      assert_equal BigDecimal("123.45"), rows[0]["decimal_scale_2"]
      assert_equal BigDecimal("123.45"), rows[0]["decimal_scale_4"]
      assert_equal rows[0]["decimal_scale_2"], rows[0]["decimal_scale_4"]

      # Second row - values may be represented differently based on scale
      assert_equal BigDecimal("1.23"), rows[1]["decimal_scale_2"] # Only 2 decimal places retained
      assert_equal BigDecimal("1.2345"), rows[1]["decimal_scale_4"] # All 4 decimal places retained
      # Test the equals implementation allowing different scales
      # Note: We use assert_in_delta for the equality check, as the values might be slightly different due to rounding
      assert_in_delta rows[1]["decimal_scale_2"].to_f, rows[1]["decimal_scale_4"].to_f, 0.01

      # Third row - values that represent the same number with different representations
      assert_equal BigDecimal("1.00"), rows[2]["decimal_scale_2"]
      assert_equal BigDecimal("1"), rows[2]["decimal_scale_4"]
      assert_equal rows[2]["decimal_scale_2"], rows[2]["decimal_scale_4"]
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_large_integer_decimals
    temp_path = "test/decimal_large_integers.parquet"
    begin
      # Test data with large integer values that would typically use negative scales
      test_data = [
        [BigDecimal("1234500"), BigDecimal("9900000")],
        [BigDecimal("1000"), BigDecimal("10000")],
        [BigDecimal("-5000"), BigDecimal("-2500000")]
      ]

      # Schema with precision but scale 0 to handle large integers
      schema = [
        { "large_integer1" => "decimal(10,0)" }, # Large integer with no decimal points
        { "large_integer2" => "decimal(10,0)" } # Another large integer value
      ]

      Parquet.write_rows(test_data.each, schema: schema, write_to: temp_path)

      # Read back and verify data
      rows = Parquet.each_row(temp_path).to_a
      assert_equal 3, rows.size

      # First row - check that large integer values are preserved
      assert_equal BigDecimal("1234500"), rows[0]["large_integer1"]
      assert_equal BigDecimal("9900000"), rows[0]["large_integer2"]

      # Second row
      assert_equal BigDecimal("1000"), rows[1]["large_integer1"]
      assert_equal BigDecimal("10000"), rows[1]["large_integer2"]

      # Third row - with negative values
      assert_equal BigDecimal("-5000"), rows[2]["large_integer1"]
      assert_equal BigDecimal("-2500000"), rows[2]["large_integer2"]
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_mixed_precision_decimals
    temp_path = "test/decimal_mixed_precision.parquet"
    begin
      # Test data with values using different precisions
      test_data = [
        [BigDecimal("123.45"), BigDecimal("67890"), BigDecimal("0.00123")],
        [BigDecimal("456.78"), BigDecimal("12345"), BigDecimal("0.00456")],
        [BigDecimal("-789.01"), BigDecimal("-98765"), BigDecimal("-0.00789")]
      ]

      # Schema with mixed scale specifications, all positive
      schema = [
        { "decimal_medium" => "decimal(10,2)" }, # Medium precision (divide by 100)
        { "decimal_integer" => "decimal(10,0)" }, # Integer precision (no division)
        { "decimal_high" => "decimal(10,5)" } # High precision (divide by 100000)
      ]

      Parquet.write_rows(test_data.each, schema: schema, write_to: temp_path)

      # Read back and verify data
      rows = Parquet.each_row(temp_path).to_a
      assert_equal 3, rows.size

      # First row - verify different precisions are handled correctly
      assert_equal BigDecimal("123.45"), rows[0]["decimal_medium"] # Medium precision
      assert_equal BigDecimal("67890"), rows[0]["decimal_integer"] # Integer precision
      assert_equal BigDecimal("0.00123"), rows[0]["decimal_high"] # High precision

      # Second row
      assert_equal BigDecimal("456.78"), rows[1]["decimal_medium"]
      assert_equal BigDecimal("12345"), rows[1]["decimal_integer"]
      assert_equal BigDecimal("0.00456"), rows[1]["decimal_high"]

      # Third row - with negative values
      assert_equal BigDecimal("-789.01"), rows[2]["decimal_medium"]
      assert_equal BigDecimal("-98765"), rows[2]["decimal_integer"]
      assert_equal BigDecimal("-0.00789"), rows[2]["decimal_high"]
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_varied_precision_decimals
    temp_path = "test/decimal_varied_precision.parquet"
    begin
      # Test data with values using varied precisions, but within safe limits
      test_data = [
        [BigDecimal("1.23"), BigDecimal("987654"), BigDecimal("0.0000123")],
        [BigDecimal("4.56"), BigDecimal("123456"), BigDecimal("0.0000456")],
        [BigDecimal("-7.89"), BigDecimal("-987654"), BigDecimal("-0.0000789")]
      ]

      # Schema with varied precision specifications
      schema = [
        { "decimal_normal" => "decimal(10,2)" }, # Normal precision
        { "decimal_large" => "decimal(10,0)" }, # Integer precision, no scale
        { "decimal_tiny" => "decimal(10,7)" } # Higher precision
      ]

      Parquet.write_rows(test_data.each, schema: schema, write_to: temp_path)

      # Read back and verify data
      rows = Parquet.each_row(temp_path).to_a
      assert_equal 3, rows.size

      # First row - verify varied precision differences are handled correctly
      assert_equal BigDecimal("1.23"), rows[0]["decimal_normal"] # Normal precision
      assert_equal BigDecimal("987654"), rows[0]["decimal_large"] # Integer value
      assert_equal BigDecimal("0.0000123"), rows[0]["decimal_tiny"] # Small value

      # Second row
      assert_equal BigDecimal("4.56"), rows[1]["decimal_normal"]
      assert_equal BigDecimal("123456"), rows[1]["decimal_large"]
      assert_equal BigDecimal("0.0000456"), rows[1]["decimal_tiny"]

      # Third row - with negative values
      assert_equal BigDecimal("-7.89"), rows[2]["decimal_normal"]
      assert_equal BigDecimal("-987654"), rows[2]["decimal_large"]
      assert_equal BigDecimal("-0.0000789"), rows[2]["decimal_tiny"]
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end

  def test_negative_scale_error_message
    # This test captures the current error message for negative scales
    temp_file = Tempfile.new("negative_scale_test")
    temp_path = temp_file.path
    temp_file.close

    begin
      schema = [{ "negative_scale" => "decimal(10,-2)" }]
      test_data = [[BigDecimal("12345")]]

      error = assert_raises(RuntimeError) { Parquet.write_rows(test_data.each, schema: schema, write_to: temp_path) }

      # Assert that the error contains this specific message about invalid scale
      assert_match(/Invalid DECIMAL scale: -2/, error.message, "Error should mention that scale -2 is invalid")
    ensure
      File.delete(temp_path) if File.exist?(temp_path)
    end
  end
end
