# frozen_string_literal: true

require "test_helper"
require "tempfile"
require "stringio"
require "thread"

# Tests specifically targeting the ThreadSafeRubyReader implementation
class ThreadSafeReaderTest < Minitest::Test
  def setup
    # Create a parquet file for testing
    @data_size = 5000
    @parquet_file = Tempfile.new(["threadsafe_test", ".parquet"])
    @parquet_path = @parquet_file.path
    @parquet_file.close
    
    # Create test data
    data = []
    @data_size.times do |i|
      data << [i, "name_#{i}", rand(100)]
    end
    
    # Write test data to parquet file
    Parquet.write_rows(
      data.each,
      schema: [{ "id" => "int64" }, { "name" => "string" }, { "value" => "int32" }],
      write_to: @parquet_path
    )
  end
  
  def teardown
    File.unlink(@parquet_path) if File.exist?(@parquet_path)
  end
  
  # Test simultaneous column and row reading, which uses ThreadSafeRubyReader
  def test_simultaneous_column_and_row_reading
    100.times do
      # Create threads that read both rows and columns at the same time
      row_thread = Thread.new do
        Parquet.each_row(@parquet_path) do |row|
          # Access each value to force potential issues
          row.each_value { |v| v.to_s }
          
          # Occasionally force GC
          GC.start if rand < 0.1
        end
      end
      
      column_thread = Thread.new do
        Parquet.each_column(@parquet_path) do |column|
          # Access each value to force potential issues
          column.each_value { |values| values.map(&:to_s) }
          
          # Occasionally force GC
          GC.start if rand < 0.1
        end
      end
      
      # Force GC while threads are running
      gc_thread = Thread.new do
        5.times do
          GC.start(full_mark: true, immediate_sweep: true)
          sleep 0.01
        end
      end
      
      # Wait for all threads to complete
      [row_thread, column_thread, gc_thread].each(&:join)
    end
  end
  
  # Test interleaved reading from the same file
  def test_interleaved_reading
    50.times do
      # Start with a row reader
      row_enum = Parquet.each_row(@parquet_path)
      
      # Read some rows
      10.times { row_enum.next rescue nil }
      
      # Now create a column reader for the same file
      column_enum = Parquet.each_column(@parquet_path)
      
      # Read some columns
      column_batch = column_enum.next rescue nil
      
      # Force GC
      GC.start(full_mark: true, immediate_sweep: true)
      
      # Continue reading rows
      10.times { row_enum.next rescue nil }
      
      # Force GC again
      GC.start(full_mark: true, immediate_sweep: true)
      
      # Read more columns
      column_batch = column_enum.next rescue nil
      
      # Create another row reader
      row_enum2 = Parquet.each_row(@parquet_path)
      
      # Read from all three in alternating pattern with GC
      20.times do
        row_enum.next rescue nil
        GC.start if rand < 0.3
        column_enum.next rescue nil
        GC.start if rand < 0.3
        row_enum2.next rescue nil
        GC.start if rand < 0.3
      end
    end
  end
  
  # Test with aggressive thread switching during read operations
  def test_aggressive_thread_switching
    50.times do
      # Create a large number of threads all accessing the same file
      threads = []
      mutex = Mutex.new
      file_content = File.binread(@parquet_path)
      
      20.times do |i|
        threads << Thread.new do
          # Each thread gets its own StringIO from the same content
          io = StringIO.new(file_content)
          
          # Half the threads read rows, half read columns
          if i % 2 == 0
            Parquet.each_row(io) do |row|
              # Very short sleep to force thread switching
              sleep 0.0001
              
              # Occasionally force GC
              mutex.synchronize { GC.start } if rand < 0.05
            end
          else
            Parquet.each_column(io) do |column|
              # Very short sleep to force thread switching
              sleep 0.0001
              
              # Occasionally force GC
              mutex.synchronize { GC.start } if rand < 0.05
            end
          end
        end
      end
      
      # Add a thread that just forces GC aggressively
      threads << Thread.new do
        10.times do
          mutex.synchronize { GC.start(full_mark: true, immediate_sweep: true) }
          sleep 0.01
        end
      end
      
      # Wait for all threads
      threads.each(&:join)
    end
  end
  
  # Test with a custom IO implementation that intentionally forces locking conflicts
  class ContentiousIO
    def initialize(path)
      @file = File.open(path, "rb")
      @mutex = Mutex.new
      @sleep_factor = 0.0001
    end
    
    def read(bytes)
      @mutex.synchronize do
        # Intentionally sleep during reads to force contention
        sleep(@sleep_factor) if rand < 0.1
        @file.read(bytes)
      end
    end
    
    def seek(offset, whence = IO::SEEK_SET)
      @mutex.synchronize do
        # Intentionally sleep during seeks to force contention
        sleep(@sleep_factor) if rand < 0.1
        @file.seek(offset, whence)
      end
    end
    
    def pos
      @mutex.synchronize do
        # Intentionally sleep during pos to force contention
        sleep(@sleep_factor) if rand < 0.1
        @file.pos
      end
    end
    
    def close
      @mutex.synchronize do
        @file.close
      end
    end
  end
  
  # Test with an IO that intentionally introduces contention
  def test_contentious_io_with_gc
    30.times do
      io = ContentiousIO.new(@parquet_path)
      
      begin
        # Use multiple threads to read with our contentious IO
        threads = []
        
        3.times do
          threads << Thread.new do
            # Each thread tries to read all rows
            Parquet.each_row(io) do |row|
              # Access some data
              row["id"]
              
              # Occasionally force GC
              GC.start if rand < 0.1
            end
          end
        end
        
        # Add a thread that just forces GC
        threads << Thread.new do
          5.times do
            GC.start(full_mark: true, immediate_sweep: true)
            sleep 0.01
          end
        end
        
        # Wait for all threads
        threads.each(&:join)
      ensure
        io.close
      end
    end
  end
  
  # Test with an IO object that gets garbage collected during operations
  def test_gc_during_io_operations
    30.times do
      # This test intentionally leaks the IO object and lets GC claim it
      # We expect Rust to handle this gracefully rather than segfault
      
      enum = nil
      
      # Create an IO and wrap it in a lambda that can be GC'd
      io_lambda = -> {
        StringIO.new(File.binread(@parquet_path))
      }
      
      # Get the IO and create an enumerator
      io = io_lambda.call
      enum = Parquet.each_row(io)
      
      # Read a few rows
      5.times { enum.next rescue nil }
      
      # Clear the reference and force GC
      io = nil
      io_lambda = nil
      GC.start(full_mark: true, immediate_sweep: true)
      
      # Try to read more rows after IO reference is gone
      # This might cause issues if the code isn't properly handling
      # the case where Ruby GC'd the IO object
      begin
        5.times { enum.next rescue nil }
      rescue => e
        # We accept either an error or success here, just not a segfault
      end
      
      # Force more GC
      enum = nil
      GC.start(full_mark: true, immediate_sweep: true)
    end
  end
end