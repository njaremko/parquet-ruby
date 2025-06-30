# Parquet Ruby Gem Architecture Documentation

This document provides a comprehensive overview of the Parquet Ruby gem's architecture, including detailed diagrams and descriptions of how data flows through the system.

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Component Architecture](#component-architecture)
3. [Data Flow Diagrams](#data-flow-diagrams)
4. [Type System and Conversions](#type-system-and-conversions)
5. [Memory Management](#memory-management)
6. [Crate Descriptions](#crate-descriptions)
7. [Key Design Patterns](#key-design-patterns)

## Architecture Overview

The Parquet Ruby gem provides high-performance Parquet file reading and writing capabilities by wrapping the official Apache parquet-rs Rust crate through FFI. The architecture consists of three main layers:

### Timestamp and Timezone Handling

**PARQUET SPECIFICATION LIMITATION**: The Apache Parquet format specification defines only two types of timestamp storage:
1. **UTC-normalized timestamps** (`isAdjustedToUTC = true`) - When ANY timezone is present in the schema
2. **Local/unzoned timestamps** (`isAdjustedToUTC = false`) - When NO timezone is present in the schema

This means Parquet CANNOT preserve specific timezone information like "+09:00" or "America/New_York". This is defined in the Parquet specification, not a limitation of this Ruby implementation.

#### When Writing
- Schema WITH timezone (e.g., "UTC", "+09:00", "America/New_York"):
  - ALL timestamps are converted to UTC
  - Stored with `isAdjustedToUTC = true`
  - Original timezone offset is LOST (e.g., "+09:00" becomes "UTC")
- Schema WITHOUT timezone:
  - Timestamps stored as local/unzoned time (no conversion)
  - Stored with `isAdjustedToUTC = false`
  - Represents "wall clock" time without timezone context

#### When Reading
- Files with `isAdjustedToUTC = true` (had ANY timezone when written):
  - Time objects returned in UTC
  - Original timezone is NOT recoverable (was lost during write)
- Files with `isAdjustedToUTC = false` (had NO timezone):
  - Time objects returned as local time in system timezone
  - These are "wall clock" times without timezone information
- Date32 fields are returned as Ruby Date objects (timezone-agnostic)

**Workaround**: If you need to preserve timezone information, store it separately as a string column alongside the timestamp.

```mermaid
graph TB
    subgraph "Ruby Layer"
        A[parquet.rb<br/>Main Entry Point]
        B[schema.rb<br/>Schema DSL]
        C[Ruby API<br/>Public Methods]
    end

    subgraph "FFI Bridge Layer"
        D[Magnus FFI<br/>Ruby-Rust Bridge]
        E[adapter_ffi.rs<br/>FFI Handlers]
        F[Type Converters<br/>Ruby ↔ Parquet]
    end

    subgraph "Core Rust Layer"
        G[parquet-core<br/>Core Logic]
        H[parquet-ruby-adapter<br/>Ruby Adapters]
        I[Apache Arrow<br/>Columnar Processing]
    end

    A --> D
    B --> D
    C --> D
    D --> E
    E --> F
    F --> G
    F --> H
    G --> I
    H --> I

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style B fill:#f9f,stroke:#333,stroke-width:2px
    style C fill:#f9f,stroke:#333,stroke-width:2px
    style D fill:#9ff,stroke:#333,stroke-width:2px
    style E fill:#9ff,stroke:#333,stroke-width:2px
    style F fill:#9ff,stroke:#333,stroke-width:2px
    style G fill:#ff9,stroke:#333,stroke-width:2px
    style H fill:#ff9,stroke:#333,stroke-width:2px
    style I fill:#ff9,stroke:#333,stroke-width:2px
```

## Component Architecture

### Workspace Structure

The project uses a Rust workspace with three crates:

```mermaid
graph LR
    subgraph "Rust Workspace"
        A[ext/parquet<br/>FFI Extension]
        B[parquet-ruby-adapter<br/>Ruby Integration]
        C[parquet-core<br/>Core Library]
    end

    A --> B
    B --> C

    style A fill:#9cf,stroke:#333,stroke-width:2px
    style B fill:#fc9,stroke:#333,stroke-width:2px
    style C fill:#cfc,stroke:#333,stroke-width:2px
```

### Module Hierarchy

```mermaid
graph TD
    subgraph "Ruby Module Structure"
        A[Parquet Module]
        A --> B[Schema Class]
        A --> C[SchemaBuilder<br/>Internal]
        A --> D[VERSION Constant]
        A --> E[Native Methods]

        E --> F[metadata]
        E --> G[each_row]
        E --> H[each_column]
        E --> I[write_rows]
        E --> J[write_columns]
    end

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style B fill:#fcf,stroke:#333,stroke-width:2px
    style C fill:#fcf,stroke:#333,stroke-width:2px
    style E fill:#cff,stroke:#333,stroke-width:2px
```

## Data Flow Diagrams

### Read Operation Flow

#### Row-wise Reading

```mermaid
sequenceDiagram
    participant Ruby as Ruby Code
    participant FFI as FFI Bridge
    participant Core as Core Reader
    participant Arrow as Apache Arrow
    participant File as Parquet File

    Ruby->>FFI: Parquet.each_row(file, options)
    FFI->>FFI: Parse arguments
    FFI->>Core: Create Reader
    Core->>Arrow: Build RecordBatchReader

    loop For each batch
        Arrow->>File: Read batch (1024 rows)
        File-->>Arrow: RecordBatch data
        Arrow-->>Core: Arrow arrays
        Core->>Core: Extract row values
        Core-->>FFI: Vec<ParquetValue>
        FFI->>FFI: Convert to Ruby
        FFI-->>Ruby: Yield Ruby Hash/Array
    end
```

#### Column-wise Reading

```mermaid
sequenceDiagram
    participant Ruby as Ruby Code
    participant FFI as FFI Bridge
    participant Core as Core Reader
    participant Arrow as Apache Arrow
    participant File as Parquet File

    Ruby->>FFI: Parquet.each_column(file, batch_size: 1000)
    FFI->>FFI: Parse arguments
    FFI->>Core: Create Reader
    Core->>Arrow: Build RecordBatchReader

    loop For each batch
        Arrow->>File: Read columnar batch
        File-->>Arrow: RecordBatch data
        Arrow-->>Core: Column arrays
        Core->>Core: Process columns
        Core-->>FFI: ColumnBatch
        FFI->>FFI: Convert arrays to Ruby
        FFI-->>Ruby: Yield column arrays
    end
```

### Write Operation Flow

#### Row-wise Writing

```mermaid
flowchart TD
    A[Ruby: write_rows] --> B[Parse Schema]
    B --> C[Create BatchSizeManager]
    C --> D[Initialize Writer]

    D --> E{For each row}
    E --> F[Convert to ParquetValue]
    F --> G[Add to batch]
    G --> H{Batch full?}

    H -->|Yes| I[Write batch to Arrow]
    H -->|No| J{Memory threshold?}

    J -->|Yes| I
    J -->|No| E

    I --> K[Compress & Write]
    K --> E

    E -->|Done| L[Flush remaining]
    L --> M[Close file]

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style M fill:#9f9,stroke:#333,stroke-width:2px
```

#### Dynamic Batch Size Management (Reservoir Sampling)

```mermaid
graph TD
    A[Start Writing] --> B[Sample First 100 Rows<br/>Using Reservoir Sampling]
    B --> C[Calculate Average Row Size]
    C --> D[Set Initial Batch Size<br/>target_memory_usage / avg_row_size]

    D --> E{Writing Rows}
    E --> F[Track Recent Row Sizes<br/>in samples array]
    F --> G{Samples >= 10?}

    G -->|Yes| H[Recalculate Average<br/>from Recent Samples]
    G -->|No| I[Continue with Current Size]

    H --> J[Adjust Batch Size<br/>min(10, calculated_size)]
    J --> I
    I --> K{Memory Threshold?<br/>batch_memory_usage > threshold}

    K -->|Yes| L[Flush Batch]
    K -->|No| E

    L --> E

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style L fill:#ff9,stroke:#333,stroke-width:2px
```

## Type System and Conversions

### Type Conversion Flow

```mermaid
graph LR
    subgraph "Ruby to Parquet"
        A1[Ruby Integer] --> B1[Int8-64/UInt8-64]
        A2[Ruby Float] --> B2[Float32/Float64]
        A3[Ruby String] --> B3[String/Binary/UUID]
        A4[Ruby Time] --> B4[Timestamp<br/>Second/Millis/Micros/Nanos]
        A5[Ruby BigDecimal] --> B5[Decimal128/256]
        A6[Ruby Array] --> B6[List]
        A7[Ruby Hash] --> B7[Map/Record]
        A8[Ruby Date] --> B8[Date32/Date64]
        A9[NilClass] --> B9[Null]
    end

    subgraph "Parquet to Ruby"
        C1[Int8-64/UInt8-64] --> D1[Ruby Integer]
        C2[Float32/Float64] --> D2[Ruby Float]
        C3[String/Binary] --> D3[Ruby String]
        C4[Timestamp] --> D4[Ruby Time]
        C5[Decimal128/256] --> D5[Ruby BigDecimal]
        C6[List] --> D6[Ruby Array]
        C7[Map/Record] --> D7[Ruby Hash]
        C8[Date32/Date64] --> D8[Ruby Date]
        C9[Float16] --> D9[Ruby Float<br/>(stored as f32)]
    end
```

### ParquetValue Enum

The core type system uses a comprehensive enum with all Parquet types:
- Numeric: Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64
- Float: Float16 (as f32), Float32, Float64 (using OrderedFloat)
- Temporal: Date32, Date64, TimeMillis, TimeMicros, TimestampSecond/Millis/Micros/Nanos
- Decimal: Decimal128 (i128), Decimal256 (BigInt)
- Complex: List, Map, Record
- Basic: Boolean, String, Bytes, Null

### Schema Definition System

```mermaid
graph TD
    A[Schema Definition] --> B{Format?}

    B -->|DSL| C[Schema.define block]
    B -->|Hash| D[Hash format]
    B -->|Array| E[Legacy array format]

    C --> F[SchemaBuilder]
    D --> G[Direct parsing]
    E --> H[Legacy parser]

    F --> I[Parquet Schema]
    G --> I
    H --> I

    I --> J[Arrow Schema]

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style I fill:#9f9,stroke:#333,stroke-width:2px
    style J fill:#9ff,stroke:#333,stroke-width:2px
```

## Memory Management

### Memory Architecture

```mermaid
graph TD
    subgraph "Platform-specific Allocators"
        A[Linux: jemalloc<br/>with disable_initial_exec_tls]
        B[macOS/Unix: mimalloc]
        C[Windows: system allocator]
    end

    subgraph "Memory Management Features"
        D[BatchSizeManager<br/>Reservoir sampling]
        E[StringCache<br/>Statistics tracking]
        F[Streaming IO<br/>Low memory usage]
    end

    subgraph "Safety Features"
        G[Magnus GC Integration]
        H[Rust Ownership]
        I[Arc/Mutex for threads]
    end

    A --> D
    B --> D
    C --> D

    D --> G
    E --> G
    F --> H

    style D fill:#9ff,stroke:#333,stroke-width:2px
    style G fill:#9f9,stroke:#333,stroke-width:2px
    style H fill:#9f9,stroke:#333,stroke-width:2px
```

### IO Handling Architecture

```mermaid
graph TD
    A[IO Input] --> B{Type?}

    B -->|File Path| C[Direct File Access]
    B -->|Ruby String| D[In-memory Buffer]
    B -->|IO Object| E[RubyIOReader]

    E --> F{Seekable?}
    F -->|Yes| G[Direct wrapper]
    F -->|No| H[Copy to temp file]

    C --> I[CloneableChunkReader]
    D --> I
    G --> I
    H --> I

    I --> J[Thread-safe wrapper]
    J --> K[Parquet Reader]

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style I fill:#9ff,stroke:#333,stroke-width:2px
    style K fill:#9f9,stroke:#333,stroke-width:2px
```

## Crate Descriptions

### 1. ext/parquet - Main FFI Extension

**Purpose**: Provides the FFI bridge between Ruby and Rust, exposing Parquet functionality to Ruby code.

**Key Components**:
- **lib.rs**: Entry point that registers Ruby module functions using Magnus
- **adapter_ffi.rs**: Thin FFI wrapper that parses Ruby arguments and delegates all logic to parquet-ruby-adapter modules
- **allocator.rs**: Platform-specific memory allocator configuration

**Responsibilities**:
- Ruby method registration and binding
- Argument parsing using Magnus scan_args
- Delegating all business logic to parquet-ruby-adapter modules
- Error type conversion from Rust to Ruby exceptions

**Key Features**:
- Minimal FFI surface area - contains only thin wrapper functions
- Complete separation between FFI layer and business logic
- All implementation details moved to parquet-ruby-adapter
- Clean, maintainable code structure

### 2. parquet-core - Core Library

**Purpose**: Language-agnostic core Parquet functionality that can be reused across different language bindings.

**Key Components**:
- **Reader**: High-performance file reader with row/column iteration
- **Writer**: Efficient file writer supporting various compression formats with dynamic batch sizing
- **Schema**: Type-safe schema representation with builder API
- **ParquetValue**: Comprehensive enum representing all Parquet data types
- **ParquetError**: Comprehensive error handling with context support
- **arrow_conversion**: Bidirectional conversion between Arrow arrays and ParquetValue
- **test_utils.rs**: Testing utilities (test builds only)

**arrow_conversion Module**:
- **Purpose**: Consolidates all type conversion logic between Arrow arrays and ParquetValues
- **Key Functions**:
  - `arrow_to_parquet_value`: Converts a single value from an Arrow array to ParquetValue
  - `parquet_values_to_arrow_array`: Converts a vector of ParquetValues to Arrow arrays
  - `append_parquet_value_to_builder`: Incremental building for complex scenarios
- **Features**:
  - Handles all primitive types with automatic upcasting (e.g., Int8->Int32)
  - Complex type support (structs, lists, maps)
  - Proper null bitmap handling for collections
  - Decimal256 conversion using BigInt
  - Efficient batch conversions
- **Benefits**:
  - Eliminates 900+ lines of code duplication between reader and writer
  - Single source of truth for type conversions
  - Easier to maintain and test
  - Consistent behavior across read/write operations

**Traits**:
- `SchemaInspector`: Provides methods for examining and querying schemas

**Note**: The crate uses concrete types rather than abstract I/O traits:
- Reader uses `parquet::file::reader::ChunkReader` directly
- Writer uses `std::io::Write + Send` directly
- Schema building is implemented directly in language adapters

**Design Philosophy**:
- Zero-cost abstractions using Rust's trait system
- Separation of concerns between core logic and language bindings
- Efficient memory usage with streaming operations
- Type safety enforced at compile time
- DRY principle - no duplicate conversion logic

### 3. parquet-ruby-adapter - Ruby Integration

**Purpose**: Bridges Ruby-specific functionality with the core library, implementing Ruby-specific type conversions and IO handling.

**Module Structure**:

**converter.rs - RubyValueConverter**:
- Handles Ruby to Parquet value conversions
- Complex type support (BigDecimal, Time, DateTime, UUID)
- Optional string caching for statistics
- Schema-guided type conversions
- Note: Not thread-safe due to Ruby's GIL requirements

**io.rs - RubyIOReader/Writer**:
- Implements Rust's Read/Write/Seek traits for Ruby objects
- Thread-safe wrappers with GIL management
- Support for non-seekable IO through temporary files

**chunk_reader.rs - CloneableChunkReader**:
- Enables parallel reading from various sources
- Critical for Parquet's multi-threaded architecture
- Abstracts over files and IO objects

**schema.rs - Schema Conversion**:
- Converts Ruby schema formats to Parquet schemas
- Supports multiple formats (DSL, hash, legacy array)
- Handles nested types (structs, lists, maps)
- Complex type parsing (e.g., "list<string>", "decimal(5,2)")
- Note: Cannot use SchemaBuilder trait due to Ruby Value not being Send/Sync

**string_cache.rs - StringCache**:
- Interns strings in Ruby VM using RString::to_interned_str()
- Thread-safe global cache with Arc<Mutex<HashMap>>
- Tracks cache hits/misses for performance statistics
- Returns interned strings as &'static str valid for Ruby VM lifetime
- Note: Primary benefit is Ruby VM memory savings, not Rust-side savings

**logger.rs - RubyLogger**:
- Integration with Ruby's logging system
- Validates logger methods and provides lazy evaluation

**batch_manager.rs - BatchSizeManager**:
- Dynamic batch size management using reservoir sampling
- Samples first 100 rows to estimate average size
- Adjusts batch size to stay within memory threshold (80% target)
- Minimum batch size of 10 rows

**metadata.rs - Metadata Handling**:
- RubyParquetMetaData wrapper for IntoValue trait
- parse_metadata function for reading file metadata
- Converts Parquet metadata to Ruby-friendly format

**types.rs - Type Definitions**:
- ParserResultType enum (Hash/Array output formats)
- WriterOutput enum for file/tempfile handling
- Argument structs (ParquetWriteArgs, RowEnumeratorArgs, ColumnEnumeratorArgs)
- Common type definitions used across modules

**utils.rs - Utility Functions**:
- Argument parsing helpers (parse_parquet_write_args)
- Memory estimation functions (estimate_parquet_value_size, estimate_row_size)
- Compression parsing (parse_compression)
- Enumerator creation helpers

**reader.rs - Reading Operations**:
- each_row function for row-wise iteration
- each_column function for column-wise iteration
- Handles file and IO object inputs
- Manages streaming readers and chunk readers
- Supports projection (column selection)

**writer.rs - Writing Operations**:
- create_writer function for file/IO initialization
- finalize_writer function for closing and cleanup
- write_rows function for row-oriented data writing
- write_columns function for column-oriented data writing
- Handles temporary file creation for IO objects
- Manages batch writing and memory thresholds

## Key Design Patterns

### 1. FFI Bridge Pattern

```mermaid
graph LR
    A[Ruby Method Call] --> B[Magnus Wrapper]
    B --> C[Rust Function]
    C --> D[Core Logic]
    D --> E[Result]
    E --> F[Magnus Conversion]
    F --> G[Ruby Return Value]

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style G fill:#f9f,stroke:#333,stroke-width:2px
    style B fill:#9ff,stroke:#333,stroke-width:2px
    style F fill:#9ff,stroke:#333,stroke-width:2px
```

### 2. Iterator Pattern

```mermaid
stateDiagram-v2
    [*] --> Created: new()
    Created --> Ready: initialize
    Ready --> HasData: next()
    HasData --> Ready: yield item
    HasData --> NeedsBatch: batch empty
    NeedsBatch --> HasData: fetch batch
    NeedsBatch --> Done: no more data
    Done --> [*]
```

```mermaid
graph TD
    A[Ruby Value] --> B{Schema Check}
    B -->|Valid| C[Type Conversion]
    B -->|Invalid| D[Type Error]
    C --> E[Parquet Value]
    D --> F[Ruby Exception]

    style D fill:#f99,stroke:#333,stroke-width:2px
    style F fill:#f99,stroke:#333,stroke-width:2px
    style E fill:#9f9,stroke:#333,stroke-width:2px
```

```mermaid
graph TD
    A[Rust Error] --> B{Error Type}
    B -->|IO Error| C[Ruby IOError]
    B -->|Type Error| D[Ruby TypeError]
    B -->|Encoding Error| E[Ruby EncodingError]
    B -->|General Error| F[Ruby RuntimeError]

    C --> G[With context]
    D --> G
    E --> G
    F --> G

    style A fill:#f99,stroke:#333,stroke-width:2px
    style G fill:#9f9,stroke:#333,stroke-width:2px
```
