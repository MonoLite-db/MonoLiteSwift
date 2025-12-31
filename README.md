# MonoLite

MonoLite is a **single-file, embeddable document database** for Swift, compatible with MongoDB Wire Protocol. A pure Swift implementation with Actor-based concurrency model.

<div align="center">

![Swift Version](https://img.shields.io/badge/Swift-5.9+-F05138?style=flat&logo=swift)
![Platform](https://img.shields.io/badge/Platform-macOS%2013%2B%20%7C%20iOS%2016%2B-blue?style=flat)
![MongoDB Compatible](https://img.shields.io/badge/MongoDB-Wire%20Protocol-47A248?style=flat&logo=mongodb)
![License](https://img.shields.io/badge/License-MIT-blue?style=flat)

**[README (EN)](README.md)** · **[README (中文)](README_CN.md)**

</div>

## Project Vision

> **Simple as SQLite, yet think and work the MongoDB way.**

- **Single-File Storage** — One `.monodb` file is the complete database
- **Zero Dependencies** — Pure Swift implementation, no external libraries
- **Actor-Based Concurrency** — Thread-safe by design using Swift's actor model
- **Embedded-First** — Library-first design, embed directly into your Swift application
- **MongoDB Driver Compatible** — Use familiar APIs and tools via Wire Protocol

## Why MonoLite? The Pain Points We Solve

### The SQLite Dilemma

SQLite is an excellent embedded database, but when your Swift application deals with **document-oriented data**, you'll encounter these frustrations:

| Pain Point | SQLite Reality | MonoLite Solution |
|------------|----------------|------------------------|
| **Rigid Schema** | Must define tables upfront with `CREATE TABLE`, schema changes require `ALTER TABLE` migrations | Schema-free — documents can have different fields, evolve naturally |
| **Nested Data** | Requires JSON1 extension or serialization, clunky to query | Native nested documents with dot notation (`address.city`) |
| **Array Operations** | No native array type, must serialize or use junction tables | Native arrays with operators like `$push`, `$pull`, `$elemMatch` |
| **Object-Relational Mismatch** | Swift structs ↔ relational tables require mapping layer | BSON documents map directly to Swift types |
| **Query Complexity** | Complex JOINs for hierarchical data, verbose SQL | Intuitive query operators (`$gt`, `$in`, `$or`) and aggregation pipelines |
| **Concurrency** | Manual thread safety with locks | Actor-based concurrency — thread-safe by design |

### When to Choose MonoLite over SQLite

✅ **Choose MonoLite when:**
- Your data is naturally hierarchical or document-shaped (JSON-like)
- Documents have varying structures (optional fields, evolving schemas)
- You need powerful array operations
- You want native Swift async/await with Actor isolation
- Your team already knows MongoDB
- You want to prototype with MongoDB compatibility

✅ **Stick with SQLite when:**
- Your data is highly relational with many-to-many relationships
- You need complex multi-table JOINs
- You require strict schema enforcement
- You're working with existing Core Data or GRDB tooling

### MonoLite vs SQLite: Feature Comparison

| Feature | MonoLite | SQLite |
|---------|---------------|--------|
| **Data Model** | Document (BSON) | Relational (Tables) |
| **Schema** | Flexible, schema-free | Fixed, requires migrations |
| **Nested Data** | Native support | JSON1 extension |
| **Arrays** | Native with operators | Serialization required |
| **Query Language** | MongoDB Query Language | SQL |
| **Swift Concurrency** | Actor-based async/await | Manual thread safety |
| **Transactions** | ✅ Multi-document ACID | ✅ ACID |
| **Indexes** | B+Tree (single, compound, unique) | B-Tree (various types) |
| **File Format** | Single `.monodb` file | Single `.db` file |
| **Crash Recovery** | WAL | WAL/Rollback Journal |
| **Maturity** | New | 20+ years battle-tested |

## Quick Start

### Installation

Add MonoLiteSwift to your `Package.swift`:

```swift
dependencies: [
    .package(path: "../MonoLiteSwift")  // Local path
    // Or use git URL when available:
    // .package(url: "https://github.com/user/MonoLiteSwift.git", from: "1.0.0")
]
```

### Basic Usage (Library API)

```swift
import MonoLiteSwift

// Open database
let db = try await Database.open(path: "data.monodb")

// Get collection
let users = try await db.collection("users")

// Insert documents
let doc: BSONDocument = [
    "name": "Alice",
    "age": .int32(25),
    "email": "alice@example.com"
]
let insertResult = try await users.insertOne(doc)

// Insert multiple documents
let docs: [BSONDocument] = [
    ["name": "Bob", "age": .int32(30), "tags": .array([.string("dev"), .string("swift")])],
    ["name": "Carol", "age": .int32(28), "address": .document(["city": "Beijing"])]
]
let insertManyResult = try await users.insertMany(docs)

// Query documents
let results = try await users.find(["age": ["$gt": .int32(20)]])
for doc in results {
    print(doc)
}

// Find one document
if let alice = try await users.findOne(["name": "Alice"]) {
    print("Found: \(alice)")
}

// Update documents
let updateResult = try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$set": ["age": .int32(26)]]
)

// Delete documents
let deleteResult = try await users.deleteOne(["name": "Alice"])

// Close database
try await db.close()
```

### Wire Protocol Server

```swift
import MonoLiteSwift

// Start MongoDB-compatible server
let db = try await Database.open(path: "data.monodb")
let server = try MongoWireTCPServer(database: db, port: 27017)
try await server.start()

// Now connect with mongosh:
// mongosh mongodb://localhost:27017
```

### Using Transactions

```swift
// Start a transaction
let session = try await db.startSession()
try await session.startTransaction()

do {
    let users = try await db.collection("users")
    let accounts = try await db.collection("accounts")

    // Transfer operation
    try await users.updateOne(
        filter: ["name": "Alice"],
        update: ["$inc": ["balance": .int32(-100)]],
        session: session
    )
    try await users.updateOne(
        filter: ["name": "Bob"],
        update: ["$inc": ["balance": .int32(100)]],
        session: session
    )

    try await session.commitTransaction()
} catch {
    try await session.abortTransaction()
    throw error
}
```

### Aggregation Pipeline

```swift
let orders = try await db.collection("orders")

let pipeline: [BSONDocument] = [
    ["$match": ["status": "completed"]],
    ["$group": [
        "_id": "$customerId",
        "total": ["$sum": "$amount"]
    ]],
    ["$sort": ["total": .int32(-1)]],
    ["$limit": .int32(10)]
]

let results = try await orders.aggregate(pipeline)
```

### Index Management

```swift
let users = try await db.collection("users")

// Create unique index
try await users.createIndex(
    keys: ["email": .int32(1)],
    options: ["unique": true]
)

// Create compound index
try await users.createIndex(
    keys: ["name": .int32(1), "age": .int32(-1)]
)

// List indexes
let indexes = try await users.listIndexes()

// Drop index
try await users.dropIndex("email_1")
```

## Core Features

### Actor-Based Concurrency

- **Thread Safety** — All database operations are actor-isolated
- **Async/Await** — Native Swift concurrency support
- **No Data Races** — Compiler-enforced thread safety

### Crash Consistency (WAL)

- **Write-Ahead Logging** — All writes are logged to WAL before being written to data files
- **Automatic Crash Recovery** — WAL replay on startup restores to a consistent state
- **Checkpoint Mechanism** — Periodic checkpoints accelerate recovery and control WAL size
- **Atomic Writes** — Guarantees atomicity of individual write operations

### Full Transaction Support

- **Multi-Document Transactions** — Support for transactions spanning multiple collections
- **Transaction API** — startTransaction / commitTransaction / abortTransaction
- **Lock Management** — Document-level and collection-level lock granularity
- **Deadlock Detection** — Wait-graph based deadlock detection with automatic transaction abort
- **Transaction Rollback** — Complete Undo Log support for transaction rollback

### B+Tree Indexes

- **Efficient Lookup** — O(log n) lookup complexity
- **Multiple Index Types** — Single-field, compound, and unique indexes
- **Dot Notation Support** — Support for nested field indexes (e.g., `address.city`)
- **Leaf Node Linked List** — Efficient range queries and sorting

### Resource Limits & Security

| Limit | Value |
|-------|-------|
| Maximum document size | 16 MB |
| Maximum nesting depth | 100 levels |
| Maximum indexes per collection | 64 |
| Maximum batch write | 100,000 documents |
| Maximum field name length | 1,024 characters |

## Feature Support Status

### Supported Core Features

| Category | Supported |
|----------|-----------|
| **CRUD** | insert, find, update, delete, findAndModify, replaceOne, distinct |
| **Query Operators** | $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $and, $or, $not, $nor, $exists, $type, $all, $elemMatch, $size, $regex |
| **Update Operators** | $set, $unset, $inc, $min, $max, $mul, $rename, $push, $pop, $pull, $pullAll, $addToSet, $setOnInsert |
| **Aggregation Stages** | $match, $project, $sort, $limit, $skip, $group, $count, $unwind, $addFields, $set, $unset, $lookup, $replaceRoot |
| **$group Accumulators** | $sum, $avg, $min, $max, $count, $push, $addToSet, $first, $last |
| **Indexes** | Single-field, compound, unique indexes, dot notation (nested fields) |
| **Cursors** | getMore, killCursors, batchSize |
| **Commands** | dbStats, collStats, listCollections, listIndexes, serverStatus, validate, explain |
| **Transactions** | startTransaction, commitTransaction, abortTransaction |

### Query Operators Details

| Category | Operators |
|----------|-----------|
| Comparison | `$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$in` `$nin` |
| Logical | `$and` `$or` `$not` `$nor` |
| Element | `$exists` `$type` |
| Array | `$all` `$elemMatch` `$size` |
| Evaluation | `$regex` |

### Update Operators Details

| Category | Operators |
|----------|-----------|
| Field | `$set` `$unset` `$inc` `$min` `$max` `$mul` `$rename` `$setOnInsert` |
| Array | `$push` `$pop` `$pull` `$pullAll` `$addToSet` |

### Aggregation Pipeline Stages Details

| Stage | Description |
|-------|-------------|
| `$match` | Document filtering (supports all query operators) |
| `$project` | Field projection (include/exclude mode) |
| `$sort` | Sorting (supports compound sorting) |
| `$limit` | Limit result count |
| `$skip` | Skip specified count |
| `$group` | Group aggregation (supports 9 accumulators) |
| `$count` | Document count |
| `$unwind` | Array expansion (supports preserveNullAndEmptyArrays) |
| `$addFields` / `$set` | Add/set fields |
| `$unset` | Remove fields |
| `$lookup` | Collection join (left outer join) |
| `$replaceRoot` | Replace root document |

### Unsupported Features (Non-Goals)

- Replica Sets / Sharding (distributed)
- Authentication & Authorization
- Change Streams
- Geospatial Features
- Full-Text Search
- GridFS

## Storage Engine Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                      Wire Protocol                              │
│              (OP_MSG / OP_QUERY / OP_REPLY)                    │
├────────────────────────────────────────────────────────────────┤
│                      Query Engine                               │
│        ┌─────────────┬─────────────┬─────────────┐             │
│        │   Parser    │  Executor   │  Optimizer  │             │
│        │  (BSON)     │  (Pipeline) │  (Index)    │             │
│        └─────────────┴─────────────┴─────────────┘             │
├────────────────────────────────────────────────────────────────┤
│                   Transaction Manager                           │
│        ┌─────────────┬─────────────┬─────────────┐             │
│        │    Lock     │  Deadlock   │    Undo     │             │
│        │   Manager   │  Detector   │    Log      │             │
│        └─────────────┴─────────────┴─────────────┘             │
├────────────────────────────────────────────────────────────────┤
│                     Storage Engine                              │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│   │   B+Tree     │  │    Pager     │  │     WAL      │        │
│   │   Index      │  │    Cache     │  │   Recovery   │        │
│   └──────────────┘  └──────────────┘  └──────────────┘        │
├────────────────────────────────────────────────────────────────┤
│                       Single File                               │
│                     (.monodb file)                              │
└────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
MonoLiteSwift/
├── Package.swift              # Swift Package Manager configuration
├── Sources/MonoLiteSwift/
│   ├── BSON/                  # BSON encoding/decoding
│   │   ├── BSONValue.swift    # BSON value types
│   │   ├── BSONDocument.swift # Document type
│   │   ├── BSONEncoder.swift  # BSON serialization
│   │   ├── BSONDecoder.swift  # BSON deserialization
│   │   └── BSONCompare.swift  # Value comparison (MongoDB standard)
│   │
│   ├── Engine/                # Database engine
│   │   ├── Database.swift     # Database core (actor)
│   │   ├── MonoCollection.swift # Collection operations (actor)
│   │   ├── DatabaseCommands.swift # Command handlers
│   │   ├── Index.swift        # Index management
│   │   ├── UpdateOperator.swift # Update operators
│   │   ├── Cursor.swift       # Cursor management
│   │   └── Explain.swift      # Query plan explanation
│   │
│   ├── Aggregate/             # Aggregation pipeline
│   │   └── Pipeline.swift     # Pipeline stages implementation
│   │
│   ├── Transaction/           # Transaction management
│   │   ├── Transaction.swift  # Transaction state
│   │   ├── TransactionManager.swift # Transaction coordination
│   │   ├── LockManager.swift  # Lock management & deadlock detection
│   │   └── SessionManager.swift # Session management
│   │
│   ├── Storage/               # Storage engine
│   │   ├── Pager.swift        # Page manager (caching, read/write)
│   │   ├── Page.swift         # Page structure
│   │   ├── SlottedPage.swift  # Slotted page for documents
│   │   ├── BTree.swift        # B+Tree implementation
│   │   ├── BTreeNode.swift    # B+Tree node
│   │   ├── WAL.swift          # Write-Ahead Log
│   │   ├── KeyString.swift    # Index key encoding
│   │   └── FileHeader.swift   # File header structure
│   │
│   ├── Protocol/              # MongoDB Wire Protocol
│   │   ├── TCPServer.swift    # TCP server
│   │   ├── ProtocolServer.swift # Protocol handler
│   │   ├── WireMessage.swift  # Message parsing
│   │   ├── OpMsg.swift        # OP_MSG handling
│   │   └── OpQuery.swift      # OP_QUERY handling
│   │
│   └── Core/                  # Core utilities
│       ├── MonoError.swift    # Error types
│       ├── ErrorCodes.swift   # MongoDB error codes
│       ├── Limits.swift       # Resource limits
│       ├── Validation.swift   # Document validation
│       └── Logger.swift       # Structured logging
│
└── Tests/MonoLiteSwiftTests/  # Unit tests
```

## Technical Specifications

| Item | Specification |
|------|---------------|
| Maximum document size | 16 MB |
| Maximum nesting depth | 100 levels |
| Maximum indexes per collection | 64 |
| Maximum batch write | 100,000 documents |
| Page size | 4 KB |
| Default cursor batch size | 101 documents |
| Cursor timeout | 10 minutes |
| Transaction lock timeout | 30 seconds |
| WAL format version | 1 |
| File format version | 1 |
| Wire Protocol version | 13 (MongoDB 5.0) |

## Cross-Language Compatibility

MonoLiteSwift is part of the MonoLite family, with identical implementations in:

| Language | Repository | Status |
|----------|------------|--------|
| Go | MonoLite | Reference Implementation |
| Swift | MonoLiteSwift | Actor-based Swift Port |
| TypeScript | MonoLiteTS | Node.js/Bun Implementation |

All three implementations:
- Share the same `.monodb` file format
- Pass identical consistency tests (33/33 tests, 100%)
- Support the same query/update operators
- Compatible with MongoDB Wire Protocol

## Requirements

- Swift 5.9+
- macOS 13+ or iOS 16+

## Development

```bash
# Build
swift build

# Run tests
swift test

# Build release
swift build -c release
```

## License

MIT License

---

<div align="center">

**[README (EN)](README.md)** · **[README (中文)](README_CN.md)**

</div>
