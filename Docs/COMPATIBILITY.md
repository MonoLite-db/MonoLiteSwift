# MonoLite Swift - MongoDB Compatibility

Created by Yanjunhui

This document describes **MonoLite Swift** API compatibility with MongoDB semantics.

- **中文版本**：[`docs/COMPATIBILITY_CN.md`](COMPATIBILITY_CN.md)
- **Back to README**：[`README.md`](../README.md)

---

## Overview

MonoLite Swift is an **embedded document database library** that provides MongoDB-compatible APIs for Swift applications. It is designed for:

- Native Swift integration with async/await
- Actor-based concurrency model for thread safety
- Single-file storage with BSON format
- Local/embedded scenarios without network overhead

**Note**: MonoLite Swift is a library, not a server. It does not implement MongoDB Wire Protocol. For protocol-level compatibility, see the Go version.

---

## API Compatibility

MonoLite Swift provides MongoDB-style APIs through its `Database` and `Collection` actors.

### Database Operations

| Operation | Status | Swift API |
|-----------|--------|-----------|
| Open database | ✅ | `Database.open(path:)` |
| Close database | ✅ | `database.close()` |
| Flush to disk | ✅ | `database.flush()` |
| Get collection | ✅ | `database.collection(_:)` |
| Create collection | ✅ | `database.createCollection(_:)` |
| Drop collection | ✅ | `database.dropCollection(_:)` |
| List collections | ✅ | `database.listCollections()` |
| Database stats | ✅ | `database.stats()` |

### Collection Operations

| Operation | Status | Swift API |
|-----------|--------|-----------|
| Insert one | ✅ | `collection.insertOne(_:)` |
| Insert many | ✅ | `collection.insertMany(_:)` |
| Find | ✅ | `collection.find(filter:options:)` |
| Find one | ✅ | `collection.findOne(filter:)` |
| Update one | ✅ | `collection.updateOne(filter:update:upsert:)` |
| Update many | ✅ | `collection.updateMany(filter:update:)` |
| Delete one | ✅ | `collection.deleteOne(filter:)` |
| Delete many | ✅ | `collection.deleteMany(filter:)` |
| Replace one | ✅ | `collection.replaceOne(filter:replacement:)` |
| Count documents | ✅ | `collection.countDocuments(filter:)` |
| Distinct | ✅ | `collection.distinct(field:filter:)` |
| Aggregate | ✅ | `collection.aggregate(pipeline:)` |
| Find and modify | ✅ | `collection.findOneAndUpdate(...)` |
| Create index | ✅ | `collection.createIndex(keys:options:)` |
| Drop index | ✅ | `collection.dropIndex(name:)` |
| List indexes | ✅ | `collection.listIndexes()` |

---

## Query Filter Operators

Filters are specified using `BSONDocument` with MongoDB-style operators.

### Comparison Operators

| Operator | Status | Example |
|----------|--------|---------|
| `$eq` | ✅ | `["age": ["$eq": 25]]` |
| `$ne` | ✅ | `["status": ["$ne": "inactive"]]` |
| `$gt` | ✅ | `["age": ["$gt": 18]]` |
| `$gte` | ✅ | `["age": ["$gte": 21]]` |
| `$lt` | ✅ | `["price": ["$lt": 100]]` |
| `$lte` | ✅ | `["score": ["$lte": 60]]` |
| `$in` | ✅ | `["status": ["$in": ["active", "pending"]]]` |
| `$nin` | ✅ | `["role": ["$nin": ["admin", "root"]]]` |

### Logical Operators

| Operator | Status | Example |
|----------|--------|---------|
| `$and` | ✅ | `["$and": [["age": ["$gte": 18]], ["status": "active"]]]` |
| `$or` | ✅ | `["$or": [["status": "active"], ["premium": true]]]` |
| `$not` | ✅ | `["age": ["$not": ["$lt": 18]]]` |
| `$nor` | ✅ | `["$nor": [["deleted": true], ["banned": true]]]` |

### Element Operators

| Operator | Status | Example |
|----------|--------|---------|
| `$exists` | ✅ | `["email": ["$exists": true]]` |
| `$type` | ✅ | `["age": ["$type": "int"]]` |

### Array Operators

| Operator | Status | Example |
|----------|--------|---------|
| `$all` | ✅ | `["tags": ["$all": ["swift", "ios"]]]` |
| `$size` | ✅ | `["items": ["$size": 3]]` |
| `$elemMatch` | ✅ | `["scores": ["$elemMatch": ["$gte": 80]]]` |

### Other Operators

| Operator | Status | Example |
|----------|--------|---------|
| `$regex` | ✅ | `["email": ["$regex": "@gmail\\.com$"]]` |
| `$mod` | ✅ | `["num": ["$mod": [5, 0]]]` |

---

## Update Operators

### Field Operators

| Operator | Status | Example |
|----------|--------|---------|
| `$set` | ✅ | `["$set": ["name": "Alice", "age": 26]]` |
| `$unset` | ✅ | `["$unset": ["tempField": ""]]` |
| `$inc` | ✅ | `["$inc": ["count": 1, "score": 10]]` |
| `$mul` | ✅ | `["$mul": ["price": 1.1]]` |
| `$min` | ✅ | `["$min": ["lowScore": 50]]` |
| `$max` | ✅ | `["$max": ["highScore": 100]]` |
| `$rename` | ✅ | `["$rename": ["oldName": "newName"]]` |
| `$currentDate` | ✅ | `["$currentDate": ["lastModified": true]]` |
| `$setOnInsert` | ✅ | `["$setOnInsert": ["createdAt": Date()]]` |

### Array Operators

| Operator | Status | Example |
|----------|--------|---------|
| `$push` | ✅ | `["$push": ["tags": "newTag"]]` |
| `$push` + `$each` | ✅ | `["$push": ["tags": ["$each": ["a", "b"]]]]` |
| `$pop` | ✅ | `["$pop": ["items": 1]]` |
| `$pull` | ✅ | `["$pull": ["tags": "oldTag"]]` |
| `$pullAll` | ✅ | `["$pullAll": ["tags": ["a", "b"]]]` |
| `$addToSet` | ✅ | `["$addToSet": ["tags": "unique"]]` |
| `$addToSet` + `$each` | ✅ | `["$addToSet": ["tags": ["$each": ["a", "b"]]]]` |

---

## Indexes

| Feature | Status | Notes |
|---------|--------|-------|
| B+Tree index | ✅ | Default index structure |
| Single field index | ✅ | `["email": 1]` |
| Compound index | ✅ | `["lastName": 1, "firstName": 1]` |
| Unique index | ✅ | `options: ["unique": true]` |
| Descending index | ✅ | `["createdAt": -1]` |
| Sparse index | ❌ | Not implemented |
| TTL index | ❌ | Not implemented |
| Text index | ❌ | Not implemented |
| Geospatial index | ❌ | Not implemented |

---

## Aggregation Pipeline

MonoLite Swift supports aggregation through `collection.aggregate(pipeline:)`.

### Supported Stages

| Stage | Status | Description |
|-------|--------|-------------|
| `$match` | ✅ | Filter documents |
| `$project` | ✅ | Reshape documents |
| `$sort` | ✅ | Sort documents |
| `$limit` | ✅ | Limit results |
| `$skip` | ✅ | Skip documents |
| `$group` | ✅ | Group and aggregate |
| `$count` | ✅ | Count documents |
| `$unwind` | ✅ | Deconstruct array |
| `$addFields` / `$set` | ✅ | Add new fields |
| `$unset` | ✅ | Remove fields |
| `$replaceRoot` | ✅ | Replace root document |
| `$lookup` | ✅ | Left outer join |

### Group Accumulators

| Accumulator | Status |
|-------------|--------|
| `$sum` | ✅ |
| `$avg` | ✅ |
| `$min` | ✅ |
| `$max` | ✅ |
| `$first` | ✅ |
| `$last` | ✅ |
| `$push` | ✅ |
| `$addToSet` | ✅ |

### Not Implemented

| Stage | Status |
|-------|--------|
| `$out` | ❌ |
| `$merge` | ❌ |
| `$facet` | ❌ |
| `$bucket` | ❌ |
| `$graphLookup` | ❌ |
| `$geoNear` | ❌ |

---

## Transactions

MonoLite Swift supports single-node transactions:

| Feature | Status | Notes |
|---------|--------|-------|
| Start transaction | ✅ | `database.startTransaction()` |
| Commit transaction | ✅ | `transaction.commit()` |
| Abort transaction | ✅ | `transaction.abort()` |
| Lock manager | ✅ | Read/write locks |
| Deadlock detection | ✅ | Wait graph analysis |
| Rollback on abort | ✅ | Undo log support |

Limitations:
- Single-node only (no distributed transactions)
- No causal consistency

---

## BSON Types

| Type | Status | Swift Type |
|------|--------|------------|
| Double | ✅ | `Double` |
| String | ✅ | `String` |
| Document | ✅ | `BSONDocument` |
| Array | ✅ | `[BSONValue]` |
| Binary | ✅ | `BSONBinary` |
| ObjectId | ✅ | `ObjectId` |
| Boolean | ✅ | `Bool` |
| Date | ✅ | `Date` |
| Null | ✅ | `BSONNull` |
| Int32 | ✅ | `Int32` |
| Int64 | ✅ | `Int64` |
| Timestamp | ✅ | `BSONTimestamp` |
| Decimal128 | ❌ | Not supported |
| MinKey/MaxKey | ❌ | Not supported |
| JavaScript | ❌ | Not supported |

---

## Concurrency Model

MonoLite Swift uses Swift's **Actor model** for thread safety:

```swift
// Database and Collection are actors
actor Database {
    func collection(_ name: String) async throws -> Collection
}

actor Collection {
    func insertOne(_ document: BSONDocument) async throws -> InsertOneResult
}
```

All operations are:
- **Thread-safe**: Actor isolation prevents data races
- **Async/await**: Native Swift concurrency support
- **Non-blocking**: Operations don't block threads

---

## Feature Comparison with MongoDB

| Feature | MongoDB | MonoLite Swift |
|---------|---------|----------------|
| Network server | ✅ | ❌ (embedded) |
| Replica sets | ✅ | ❌ |
| Sharding | ✅ | ❌ |
| Authentication | ✅ | ❌ |
| Wire protocol | ✅ | ❌ |
| Single-file storage | ❌ | ✅ |
| Zero configuration | ❌ | ✅ |
| iOS/macOS native | ❌ | ✅ |
| Actor-based concurrency | ❌ | ✅ |

---

## Platform Support

| Platform | Status |
|----------|--------|
| macOS 13+ | ✅ |
| iOS 16+ | ✅ |
| watchOS 9+ | ✅ |
| tvOS 16+ | ✅ |
| Linux | 🚧 |
| Windows | ❌ |

---

## Reporting Issues

When reporting compatibility issues, include:

- Swift version and platform
- Code snippet that reproduces the issue
- Expected behavior (MongoDB) vs actual behavior (MonoLite)
- Stack trace if applicable
