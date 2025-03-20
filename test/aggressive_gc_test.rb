# frozen_string_literal: true

require "test_helper"
require "tempfile"
require "stringio"
require "objspace"

# Tests focused on reproducing segmentation faults with GC during Parquet operations
class AggressiveGCTest < Minitest::Test
  def setup
    # Create a simple parquet file for testing
    @parquet_file = Tempfile.new(["test", ".parquet"])
    @parquet_path = @parquet_file.path
    @parquet_file.close
    
    # Create test data
    data = [
      [1, "Alice", 25],
      [2, "Bob", 30],
      [3, "Charlie", 35]
    ]
    
    # Write test data to parquet file
    Parquet.write_rows(
      data.each,
      schema: [{ "id" => "int64" }, { "name" => "string" }, { "age" => "int32" }],
      write_to: @parquet_path
    )
  end
  
  def teardown
    File.unlink(@parquet_path) if File.exist?(@parquet_path)
  end
  
  # Force GC during object read to potentially create a segfault
  def test_aggressive_gc_during_parquet_read
    iterations = 200
    
    iterations.times do |i|
      # Create many objects to increase memory pressure
      objects = []
      500.times { objects << "x" * (1000 + rand(10000)) }
      
      # Create an enumerator
      enum = Parquet.each_row(@parquet_path)
      
      # Force memory pressure and GC between getting the enumerator and using it
      GC.stress = true
      50.times { GC.start(full_mark: true, immediate_sweep: true) }
      
      begin
        # Try to use the enumerator after intense GC pressure
        enum.each do |row|
          # More GC pressure during iteration
          GC.start if rand < 0.3
          # Create temporary objects during iteration
          50.times { "x" * (100 + rand(1000)) }
        end
      rescue => e
        flunk("Iteration #{i}: GC-related error during Parquet read: #{e.message}")
      ensure
        GC.stress = false
      end
    end
  end
  
  # Test concurrent readers with GC stress
  def test_concurrent_readers_with_gc_stress
    threads = []
    errors = Queue.new
    running = true
    
    # Start a dedicated GC thread
    gc_thread = Thread.new do
      while running
        GC.start(full_mark: true, immediate_sweep: true)
        sleep 0.001
      end
    end
    
    begin
      # Create 8 reader threads
      8.times do |thread_id|
        threads << Thread.new do
          begin
            20.times do |iter|
              # Create many temporary objects
              1000.times { "x" * (1000 + rand(5000)) }
              
              # Read file during GC pressure
              Parquet.each_row(@parquet_path) do |row|
                # Create temporary objects while reading
                10.times { "x" * (100 + rand(1000)) }
                # Force GC sometimes during read
                GC.start if rand < 0.2
              end
            end
          rescue => e
            errors << "Thread #{thread_id}: #{e.message}\n#{e.backtrace.join("\n")}"
          end
        end
      end
      
      # Wait for all threads to complete
      threads.each(&:join)
      
      # Check for errors
      unless errors.empty?
        error_messages = []
        until errors.empty?
          error_messages << errors.pop
        end
        flunk("Encountered errors during concurrent reading:\n#{error_messages.join("\n")}")
      end
    ensure
      running = false
      gc_thread.join
    end
  end
  
  # Test with objects being deallocated during parsing
  def test_deallocated_objects_during_parsing
    # Enable object space tracking to manually force object collection
    ObjectSpace.trace_object_allocations_start
    
    begin
      100.times do
        # Create a reference to reader that will get garbage collected
        reader_ref = nil
        
        # Create a proc that reads the file
        read_proc = proc do
          # This is a common problematic case - when a reference is reassigned
          # while still being used internally
          reader_ref = Parquet.each_row(@parquet_path)
          
          # Use the reader and create some objects during use
          reader_ref.each do |_row|
            # Create some objects to increase memory pressure
            10.times { "x" * 1000 }
          end
          
          # Force garbage collection
          GC.start(full_mark: true, immediate_sweep: true)
          
          # The reference goes out of scope here
        end
        
        # Run the proc
        read_proc.call
        
        # Force more GC to try to trigger the segfault
        reader_ref = nil
        GC.start(full_mark: true, immediate_sweep: true)
      end
    ensure
      ObjectSpace.trace_object_allocations_stop
    end
  end
  
  # Test forcing major GC during specific points in parsing
  def test_major_gc_during_specific_parsing_points
    # This test attempts to force GC at specific potential problem points
    100.times do
      row_count = 0
      
      # Use a StringIO to make the reader use a different code path
      file_content = File.binread(@parquet_path)
      io = StringIO.new(file_content)
      
      # Get reader but don't use it yet
      reader = Parquet.each_row(io)
      
      # Force GC between reader creation and enumeration
      GC.start(full_mark: true, immediate_sweep: true)
      
      # Use the reader - call .next directly to have more control
      begin
        loop do
          # Force GC right before .next call
          GC.start if rand < 0.5
          
          row = reader.next
          row_count += 1
          
          # Force GC right after .next call
          GC.start if rand < 0.5
        end
      rescue StopIteration
        # Expected - end of enumeration
      end
      
      # There should be 3 rows
      assert_equal 3, row_count
    end
  end
  
  # Test with lots of string copies to potentially catch string sharing issues
  def test_string_references_with_gc
    # This test tries to force issues with string copying/references and GC
    100.times do
      strings = []
      
      Parquet.each_row(@parquet_path) do |row|
        # Force GC before we store string references
        GC.start(full_mark: true, immediate_sweep: true)
        
        # Store the string reference
        name = row["name"]
        strings << name
        
        # Create string copies which might interfere with memory refs
        100.times do 
          strings << String.new(name)
        end
        
        # Force GC again
        GC.start(full_mark: true, immediate_sweep: true)
        
        # Access the strings to verify they haven't been corrupted
        strings.each do |s|
          # Just accessing the string may cause segfault if there's a reference issue
          s.length
        end
      end
    end
  end
  
  # Test with allocation tracking to catch any memory leaks
  def test_memory_exhaustion
    return if ENV["SKIP_MEMORY_TEST"]
    
    allocated_strings = []
    
    # Try to consume enough memory to force aggressive GC
    begin
      while true
        # Allocate a large string
        allocated_strings << "x" * 1_000_000
        
        # Read the parquet file with an existing memory pressure
        Parquet.each_row(@parquet_path).to_a
        
        # Force aggressive GC
        GC.start(full_mark: true, immediate_sweep: true)
      end
    rescue NoMemoryError
      # This is expected - we want to reach memory limits
      # to test behavior under extreme conditions
      allocated_strings = nil
      GC.start
      pass
    end
  end
end