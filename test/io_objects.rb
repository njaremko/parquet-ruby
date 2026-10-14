# IO-like objects that tests write Parquet files to, each with a behavior of
# its own, such as raising from `write`.
module IoObjects
  # Records every string written to it.
  class RecordingIO
    attr_reader :writes, :flushes

    def initialize
      @writes = []
      @flushes = 0
    end

    def write(data)
      @writes << data.b
      data.bytesize
    end

    def flush
      @flushes += 1
      self
    end

    def bytes
      @writes.join.b
    end

    def bytesize
      @writes.sum(&:bytesize)
    end
  end

  # Accepts at most 1,000 bytes from each call to `write`, as a non-blocking
  # writer can.
  class ShortWriteIO
    attr_reader :bytes

    def initialize
      @bytes = String.new(encoding: Encoding::BINARY)
    end

    def write(data)
      chunk = data.byteslice(0, 1_000)
      @bytes << chunk
      chunk.bytesize
    end
  end

  # Has no `flush`, and returns nil from `write`.
  class WriteOnlyIO
    attr_reader :bytes

    def initialize
      @bytes = String.new(encoding: Encoding::BINARY)
    end

    def write(data)
      @bytes << data
      nil
    end
  end

  class FailingIO
    class Failure < StandardError; end

    attr_reader :calls, :raised

    def initialize
      @calls = 0
    end

    def write(_data)
      @calls += 1
      @raised = Failure.new("disk full")
      raise @raised
    end
  end

  class ThrowingIO
    def write(_data)
      throw :stop_writing, :thrown
    end
  end
end
