# SimpleDB Implementation

A basic database management system implementation that supports transactions, concurrency control, and basic query processing.

## Project Structure

```
simpledb/
├── src/java/simpledb/        # Core implementation
│   ├── Database.java         # Main database class
│   ├── BufferPool.java       # Buffer pool management
│   ├── LockManager.java      # Concurrency control
│   ├── HeapFile.java         # File storage
│   └── ...
├── test/                     # Test cases
└── lib/                      # Dependencies
```

## Core Components

### Storage Engine
- **HeapFile**: Physical storage of tables on disk
- **HeapPage**: Fixed-size page format for storing tuples
- **BufferPool**: Memory management and page caching
  - LRU eviction policy
  - Transaction isolation enforcement
  - Write-ahead logging support

### Query Processing
- **Operators**: Basic query execution operators
  - `SeqScan`: Table scanning
  - `Join`: Various join implementations
  - `Filter`: Predicate evaluation
  - `Aggregate`: GROUP BY and aggregation
- **Query Optimizer**: Cost-based optimization
  - `JoinOptimizer`: Join order optimization
  - `TableStats`: Statistics for cost estimation

### Transaction Management
- **Transaction**: ACID transaction support
  - Strict 2PL (Two-Phase Locking)
  - Deadlock detection
  - Recovery via write-ahead logging

### Concurrency Control
- **LockManager**: Lock-based concurrency control
  - Shared (S) and Exclusive (X) locks
  - Deadlock detection using wait-for graph
  - Lock upgrade/downgrade support

### Catalog Management
- **Catalog**: Database metadata management
  - Table schemas
  - Statistics
  - File management

## Key Features

### Transaction Support
- ACID properties
- Two-phase locking protocol
- Deadlock detection and resolution
- Write-ahead logging

### Query Processing
- Sequential scans
- Predicate pushdown
- Join optimization
- Aggregation support

### Storage Management
- Page-level granularity
- Buffer pool caching
- LRU eviction
- Write-ahead logging

## Building and Testing

### Prerequisites
- Java 8 or higher
- Apache Ant

### Build Commands
```bash
# Build the project
ant

# Run all tests
ant test

# Run specific test
ant runtest -Dtest=LockingTest
```

### Key Test Suites
- `LockingTest`: Concurrency control
- `TransactionTest`: ACID properties
- `JoinTest`: Query processing
- `BufferPoolWriteTest`: Storage management

## Implementation Details

### Locking Protocol
- Page-level locking
- Lock types:
  - `READ_ONLY`: Shared locks
  - `READ_WRITE`: Exclusive locks
- Deadlock detection via cycle detection in wait-for graph

### Buffer Pool Management
- Fixed size buffer pool (default 50 pages)
- LRU eviction policy
- Dirty page tracking
- Transaction-consistent page eviction

### Query Processing
- Iterator-based query execution
- Cost-based join optimization
- Support for:
  - Nested loop joins
  - Sequential scans
  - Predicate filtering
  - Basic aggregations

## File Descriptions

### Core Classes
- `Database.java`: Main database singleton
- `BufferPool.java`: Memory management
- `LockManager.java`: Concurrency control
- `HeapFile.java`: Table storage
- `Transaction.java`: Transaction management

### Query Processing
- `Operator.java`: Base class for operators
- `SeqScan.java`: Table scanning
- `Join.java`: Join implementation
- `Aggregate.java`: GROUP BY support

### Storage
- `Page.java`: Page interface
- `HeapPage.java`: Page implementation
- `TupleDesc.java`: Schema management

## Contributing
1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Submit pull request
