# frozen_string_literal: true

require "test_helper"
require "tempfile"
require "stringio"
require "thread"

# Tests specifically targeting the RubyReader implementation
class ReaderGCTest < Minitest::Test
  def setup
    # Create a parquet file for testing
    @data_size = 10000
    @parquet_file = Tempfile.new(["test_reader", ".parquet"])
    @parquet_path = @parquet_file.path
    @parquet_file.close
    
    # Create test data with large strings to increase memory pressure
    data = []
    @data_size.times do |i|
      # Create large strings that will require more memory management
      data << [i, "name_#{i}_" + "x" * 1000, rand(100)]
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
  
  # Test with StringIO which uses RubyReader.read implementation
  def test_stringio_with_gc_stress
    # Read file content
    file_content = File.binread(@parquet_path)
    
    50.times do
      # Create a new StringIO for each iteration
      io = StringIO.new(file_content)
      
      # Create an enumerator
      enum = Parquet.each_row(io)
      
      # Force GC between creation and usage
      GC.start(full_mark: true, immediate_sweep: true)
      
      count = 0
      
      # Process in batches with GC in between
      while count < @data_size
        # Process a small batch
        10.times do
          begin
            row = enum.next
            count += 1
          rescue StopIteration
            break
          end
        end
        
        # Force GC between batches
        GC.start(full_mark: true, immediate_sweep: true)
      end
      
      assert_equal @data_size, count, "Should have read all rows"
    end
  end
  
  # Test with a custom IO-like object that forces all Ruby callbacks
  class SlowIO
    def initialize(path)
      @file = File.open(path, "rb")
      @pos = 0
    end
    
    def read(bytes)
      # Small reads to force many callbacks
      bytes = [bytes, 16].min
      result = @file.read(bytes)
      if result
        @pos += result.bytesize
      end
      result
    end
    
    def seek(offset, whence = IO::SEEK_SET)
      @pos = @file.seek(offset, whence)
    end
    
    def pos
      @pos
    end
    
    def close
      @file.close
    end
  end
  
  # Test with a custom IO that forces many small reads and callbacks to RubyReader
  def test_custom_io_with_gc_stress
    50.times do
      File.open(@parquet_path, "rb") do |file|
        # Use our custom IO to force many read callbacks
        io = SlowIO.new(@parquet_path)
        
        begin
          # Set GC stress mode to force aggressive GC
          GC.stress = true
          
          count = 0
          Parquet.each_row(io) do |_row|
            count += 1
            
            # Force even more GC during reads
            GC.start if count % 10 == 0
          end
          
          assert_equal @data_size, count, "Should have read all rows"
        ensure
          GC.stress = false
          io.close
        end
      end
    end
  end
  
  # Test concurrent readers using the same parquet file
  def test_concurrent_readers
    # Launch multiple threads all reading the same file
    threads = []
    errors = Queue.new
    
    # Create a shared atomic counter for GC forcing
    gc_counter = Mutex.new
    gc_count = 0
    
    10.times do |thread_id|
      threads << Thread.new do
        begin
          # Each thread reads the file fully
          count = 0
          
          Parquet.each_row(@parquet_path) do |_row|
            count += 1
            
            # Coordinated GC between threads to maximize contention
            if count % 100 == 0
              gc_counter.synchronize do
                gc_count += 1
                if gc_count % 10 == 0
                  GC.start(full_mark: true, immediate_sweep: true)
                end
              end
            end
          end
          
          assert_equal @data_size, count, "Thread #{thread_id} should read all rows"
        rescue => e
          errors << "Thread #{thread_id} error: #{e.message}\n#{e.backtrace.join("\n")}"
        end
      end
    end
    
    # Wait for all threads to finish
    threads.each(&:join)
    
    # Check for errors
    unless errors.empty?
      error_messages = []
      until errors.empty?
        error_messages << errors.pop
      end
      flunk("Errors during concurrent reading:\n#{error_messages.join("\n")}")
    end
  end
  
  # Test enumerator usage with early collection
  def test_early_enumerator_collection
    50.times do
      # This test creates enumerators and tries to collect them before they're done
      
      enumerators = []
      10.times do
        # Create enumerators but don't fully consume them
        enumerators << Parquet.each_row(@parquet_path)
        
        # Start reading but don't finish
        enum = enumerators.last
        10.times { enum.next rescue nil }
      end
      
      # Force GC to potentially collect uncompleted enumerators
      GC.start(full_mark: true, immediate_sweep: true)
      
      # Pick a random enumerator and try to use it after GC
      if enumerators.size > 0
        enum = enumerators.sample
        begin
          # May or may not work depending on if it was collected
          10.times { enum.next rescue nil }
        rescue => e
          # Just ensure we don't segfault
        end
      end
      
      # Clear and force GC again
      enumerators = nil
      GC.start(full_mark: true, immediate_sweep: true)
    end
  end
  
  # Test with reading and modifying during parsing which might trigger Ruby finalizers
  def test_gc_during_stringio_read_reuse
    file_content = File.binread(@parquet_path)
    
    50.times do
      io = StringIO.new(file_content)
      
      # Create a set of rows from the file
      rows = []
      
      # Process in small batches with GC and StringIO manipulation between
      enum = Parquet.each_row(io)
      
      # Use batched reading to force specific GC timing
      while rows.size < @data_size
        # Read a small batch
        batch_size = 50
        batch = []
        
        batch_size.times do
          begin
            batch << enum.next
          rescue StopIteration
            break
          end
        end
        
        # Store references to the batch rows
        batch.each { |row| rows << row }
        
        # Force major GC
        GC.start(full_mark: true, immediate_sweep: true)
        
        # Now try to access previously read rows after GC
        # This can trigger segfault if the Ruby objects were incorrectly handled
        rows.sample(10).each do |row|
          # Just accessing the row may cause a segfault if there's an issue
          row["id"]
          row["name"]
        end
      end
      
      assert_equal @data_size, rows.size, "Should have read all rows"
    end
  end
end