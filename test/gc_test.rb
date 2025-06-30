# frozen_string_literal: true

require "test_helper"
require "tempfile"
require "stringio"

# Tests focusing on Ruby GC interactions with the Parquet extension
class GCTest < Minitest::Test
  def setup
    # Create a simple parquet file for testing
    @csv_data = "id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35\n"
    @csv_file = Tempfile.new(["test", ".csv"])
    @csv_file.write(@csv_data)
    @csv_file.close

    # Create parquet file
    @parquet_file = Tempfile.new(["test", ".parquet"])
    @parquet_path = @parquet_file.path
    @parquet_file.close

    # Convert CSV to parquet for testing
    data = [
      [1, "Alice", 25],
      [2, "Bob", 30],
      [3, "Charlie", 35]
    ]

    Parquet.write_rows(
      data.each,
      schema: [{ "id" => "int64" }, { "name" => "string" }, { "age" => "int32" }],
      write_to: @parquet_path
    )
  end

  def teardown
    @csv_file.unlink if @csv_file
    File.unlink(@parquet_path) if File.exist?(@parquet_path)
  end

  # Test GC during row parsing
  def test_gc_during_row_parsing
    # Allocate many objects to increase GC pressure
    objects = []
    1000.times { objects << "x" * 10000 }

    # Attempt to parse while forcing GC
    begin
      reader_thread = Thread.new do
        100.times do
          Parquet.each_row(@parquet_path) do |row|
            # Force object allocation inside the block
            100.times { "x" * 1000 }
            # Explicitly trigger GC during parsing
            GC.start(full_mark: true, immediate_sweep: true)
          end
        end
      end

      gc_thread = Thread.new do
        100.times do
          GC.start(full_mark: true, immediate_sweep: true)
          sleep 0.001
        end
      end

      reader_thread.join
      gc_thread.join
    rescue => e
      flunk("GC during row parsing caused error: #{e.message}")
    end

    # To prevent GC from collecting objects before test ends
    assert(objects.size > 0)
  end

  # Test GC with StringIO parsing
  def test_gc_with_stringio
    # Read file into memory
    file_content = File.binread(@parquet_path)

    100.times do
      # Create a new StringIO for each iteration
      io = StringIO.new(file_content)

      # Force GC while processing
      begin
        Parquet.each_row(io) do |row|
          GC.start(full_mark: true, immediate_sweep: true)
        end
      rescue => e
        flunk("GC during StringIO parsing caused error: #{e.message}")
      end

      # Create pressure between iterations
      1000.times { "x" * 1000 }
      GC.start
    end
  end

  # Test concurrent GC and parsing with multiple threads
  def test_concurrent_threads_with_gc
    threads = []

    # Create 5 threads that all read and force GC
    5.times do
      threads << Thread.new do
        20.times do
          # Create objects to increase GC pressure
          1000.times { "x" * 1000 }

          begin
            Parquet.each_row(@parquet_path) do |row|
              # Randomly trigger GC during iterations
              GC.start if rand < 0.3
            end
          rescue => e
            flunk("Concurrent GC with multiple threads caused error: #{e.message}")
          end

          # Force GC between parquet reads
          GC.start
        end
      end
    end

    threads.each(&:join)
  end

  # Test when the reader object is collected during iteration
  def test_reader_object_collection
    # Create a function that returns an enumerator but doesn't keep a reference
    def create_enumerator(path)
      Parquet.each_row(path)
    end

    enum = create_enumerator(@parquet_path)

    # Try to force collection of any temporary objects
    GC.start(full_mark: true, immediate_sweep: true)

    # Now try to use the enumerator
    begin
      enum.each do |row|
        # Force more GC during iteration
        GC.start if rand < 0.5
      end
    rescue => e
      flunk("GC after creating enumerator caused error: #{e.message}")
    end
  end
end
