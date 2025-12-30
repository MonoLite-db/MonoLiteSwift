// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

/// Helper actor for counting in async context
private actor Counter {
    var commits = 0
    var aborts = 0
    func incrementCommit() { commits += 1 }
    func incrementAbort() { aborts += 1 }
    func get() -> (Int, Int) { (commits, aborts) }
}

/// Concurrency stress tests (Go parity: engine/fuzz_test.go TestConcurrentOperations)
final class ConcurrencyStressTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    /// Test sequential batch inserts to multiple collections
    func testSequentialBatchInserts() async throws {
        try await withTestTimeout(seconds: 30, mode: .hard) {
            let dir = try self.makeWorkDir("batch-insert-\(UUID().uuidString)")
            let path = dir.appendingPathComponent("data.monodb").path
            let db = try await Database(path: path, enableWAL: true)

            let collectionCount = 5
            let docsPerCollection = 50

            // Sequential batch inserts to different collections
            for t in 0..<collectionCount {
                let col = try await db.collection("batch_\(t)")
                var docs: [BSONDocument] = []
                for i in 0..<docsPerCollection {
                    docs.append(BSONDocument([
                        ("task", .int32(Int32(t))),
                        ("idx", .int32(Int32(i))),
                        ("data", .string("payload-\(t)-\(i)")),
                    ]))
                }
                _ = try await col.insert(docs)
            }

            // Verify each collection has correct count
            for t in 0..<collectionCount {
                if let col = await db.getCollection("batch_\(t)") {
                    let count = await col.count
                    XCTAssertEqual(count, Int64(docsPerCollection), "expected \(docsPerCollection) documents in batch_\(t)")
                }
            }

            // Validate database integrity
            let result = await db.validate()
            XCTAssertTrue(result.valid, "database validation failed after batch inserts: \(result.errors)")

            try await db.close()
        }
    }

    /// Test concurrent inserts to the SAME collection with SerialWriteQueue protection
    func testConcurrentInsertsToSameCollection() async throws {
        try await withTestTimeout(seconds: 30, mode: .hard) {
            let dir = try self.makeWorkDir("concurrent-same-col-\(UUID().uuidString)")
            let path = dir.appendingPathComponent("data.monodb").path
            let db = try await Database(path: path, enableWAL: true)

            let col = try await db.collection("concurrent")
            let taskCount = 10
            let docsPerTask = 20

            // Concurrent inserts to the SAME collection
            await withTaskGroup(of: Int.self) { group in
                for t in 0..<taskCount {
                    group.addTask {
                        var inserted = 0
                        for i in 0..<docsPerTask {
                            do {
                                _ = try await col.insertOne(BSONDocument([
                                    ("task", .int32(Int32(t))),
                                    ("idx", .int32(Int32(i))),
                                    ("data", .string("payload-\(t)-\(i)")),
                                ]))
                                inserted += 1
                            } catch {
                                // Record failure but continue
                            }
                        }
                        return inserted
                    }
                }

                var totalInserted = 0
                for await inserted in group {
                    totalInserted += inserted
                }
                print("Concurrent inserts to same collection: \(totalInserted)/\(taskCount * docsPerTask)")
            }

            // Verify count
            let count = await col.count
            XCTAssertEqual(count, Int64(taskCount * docsPerTask), "expected \(taskCount * docsPerTask) documents")

            // Validate database integrity
            let result = await db.validate()
            XCTAssertTrue(result.valid, "database validation failed after concurrent inserts: \(result.errors)")

            try await db.close()
        }
    }

    /// Test concurrent reads and writes
    func testConcurrentReadWrite() async throws {
        try await withTestTimeout(seconds: 30, mode: .hard) {
            let dir = try self.makeWorkDir("concurrent-rw-\(UUID().uuidString)")
            let path = dir.appendingPathComponent("data.monodb").path
            let db = try await Database(path: path, enableWAL: true)

            let col = try await db.collection("rw_test")

            // Insert initial data
            for i in 0..<100 {
                _ = try await col.insertOne(BSONDocument([
                    ("x", .int32(Int32(i))),
                    ("value", .string("initial")),
                ]))
            }

            let writerCount = 5
            let readerCount = 10
            let operationsPerTask = 20

            await withTaskGroup(of: Void.self) { group in
                // Writers
                for _ in 0..<writerCount {
                    group.addTask {
                        for _ in 0..<operationsPerTask {
                            let idx = Int32.random(in: 0..<100)
                            do {
                                _ = try await col.updateOne(
                                    BSONDocument([("x", .int32(idx))]),
                                    update: BSONDocument([("$set", .document(BSONDocument([("value", .string("updated-\(Date().timeIntervalSince1970)"))])))]),
                                    upsert: false
                                )
                            } catch {
                                // Ignore contention errors
                            }
                        }
                    }
                }

                // Readers
                for _ in 0..<readerCount {
                    group.addTask {
                        for _ in 0..<operationsPerTask {
                            let idx = Int32.random(in: 0..<100)
                            do {
                                _ = try await col.findOne(BSONDocument([("x", .int32(idx))]))
                            } catch {
                                // Ignore
                            }
                        }
                    }
                }
            }

            // Validate database integrity
            let result = await db.validate()
            XCTAssertTrue(result.valid, "database validation failed after concurrent read/write: \(result.errors)")

            try await db.close()
        }
    }

    /// Test concurrent transactions (commit and abort)
    func testConcurrentTransactions() async throws {
        try await withTestTimeout(seconds: 30, mode: .hard) {
            let dir = try self.makeWorkDir("concurrent-txn-\(UUID().uuidString)")
            let path = dir.appendingPathComponent("data.monodb").path
            let db = try await Database(path: path, enableWAL: true)

            let _ = try await db.collection("txn_test")

            let txnCount = 20
            let counter = Counter()

            await withTaskGroup(of: Bool.self) { group in
                for i in 0..<txnCount {
                    group.addTask {
                        // Create a unique session ID for each transaction
                        let sessionId = BSONDocument([("id", .binary(BSONBinary(subtype: .uuid, data: UUID().uuidString.data(using: .utf8)!)))])
                        let txnNumber = Int64(i + 1)

                        do {
                            // Start transaction
                            _ = try await db.runCommand(BSONDocument([
                                ("startTransaction", .int32(1)),
                                ("lsid", .document(sessionId)),
                                ("txnNumber", .int64(txnNumber)),
                            ]))

                            // Insert within transaction
                            _ = try await db.runCommand(BSONDocument([
                                ("insert", .string("txn_test")),
                                ("documents", .array(BSONArray([
                                    .document(BSONDocument([("txn", .int64(txnNumber)), ("data", .string("txn-data"))]))
                                ]))),
                                ("lsid", .document(sessionId)),
                                ("txnNumber", .int64(txnNumber)),
                                ("autocommit", .bool(false)),
                            ]))

                            // Randomly commit or abort
                            if Bool.random() {
                                _ = try await db.runCommand(BSONDocument([
                                    ("commitTransaction", .int32(1)),
                                    ("lsid", .document(sessionId)),
                                    ("txnNumber", .int64(txnNumber)),
                                ]))
                                return true // committed
                            } else {
                                _ = try await db.runCommand(BSONDocument([
                                    ("abortTransaction", .int32(1)),
                                    ("lsid", .document(sessionId)),
                                    ("txnNumber", .int64(txnNumber)),
                                ]))
                                return false // aborted
                            }
                        } catch {
                            // Transaction failed, treat as aborted
                            return false
                        }
                    }
                }

                for await committed in group {
                    if committed {
                        await counter.incrementCommit()
                    } else {
                        await counter.incrementAbort()
                    }
                }
            }
            
            let (commitCount, abortCount) = await counter.get()

            // Validate database integrity
            let result = await db.validate()
            XCTAssertTrue(result.valid, "database validation failed after concurrent transactions: \(result.errors)")

            // Log statistics
            print("Concurrent transactions: committed=\(commitCount), aborted=\(abortCount)")

            try await db.close()
        }
    }

    /// Bulk insert performance test
    func testBulkInsertPerformance() async throws {
        try await withTestTimeout(seconds: 60, mode: .hard) {
            let dir = try self.makeWorkDir("bulk-insert-\(UUID().uuidString)")
            let path = dir.appendingPathComponent("data.monodb").path
            let db = try await Database(path: path, enableWAL: true)

            let col = try await db.collection("bulk")
            let docCount = 1000

            let start = Date()

            // Bulk insert
            var docs: [BSONDocument] = []
            for i in 0..<docCount {
                docs.append(BSONDocument([
                    ("idx", .int32(Int32(i))),
                    ("name", .string("document-\(i)")),
                    ("timestamp", .dateTime(Date())),
                ]))
            }
            _ = try await col.insert(docs)

            let elapsed = Date().timeIntervalSince(start)
            let docsPerSecond = Double(docCount) / elapsed

            print("Bulk insert: \(docCount) docs in \(String(format: "%.3f", elapsed))s (\(String(format: "%.0f", docsPerSecond)) docs/s)")

            // Verify
            let count = await col.count
            XCTAssertEqual(count, Int64(docCount))

            // Validate
            let result = await db.validate()
            XCTAssertTrue(result.valid, "database validation failed after bulk insert: \(result.errors)")

            try await db.close()
        }
    }
}

