// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

/// Tests for non-unique index behavior (Go parity: engine/index_nonunique_test.go)
final class IndexNonUniqueTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testNonUniqueIndexAllowsDuplicateKeys() async throws {
        let dir = try makeWorkDir("nonunique-idx-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)

        let col = try await db.collection("t")

        // Create non-unique index on field "a"
        let indexName = try await col.createIndex(
            keys: BSONDocument([("a", .int32(1))]),
            options: BSONDocument([("name", .string("a_1")), ("unique", .bool(false))])
        )
        XCTAssertFalse(indexName.isEmpty, "index name should not be empty")

        // Insert two documents with the same indexed key (a=1)
        _ = try await col.insertOne(BSONDocument([("a", .int32(1))]))
        _ = try await col.insertOne(BSONDocument([("a", .int32(1))]))

        // Flush to persist pages
        try await db.flush()

        // Validate must pass
        let result = await db.validate()
        XCTAssertTrue(result.valid, "database validation failed: errors=\(result.errors) warnings=\(result.warnings)")

        // Count should be 2
        let count = await col.count
        XCTAssertEqual(count, 2, "expected 2 documents")

        try await db.close()
    }

    func testNonUniqueIndexQueryByIndexedField() async throws {
        let dir = try makeWorkDir("nonunique-query-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)

        let col = try await db.collection("items")

        // Create non-unique index on field "category"
        _ = try await col.createIndex(
            keys: BSONDocument([("category", .int32(1))]),
            options: BSONDocument([("name", .string("category_1")), ("unique", .bool(false))])
        )

        // Insert documents with duplicate category values
        _ = try await col.insertOne(BSONDocument([("category", .string("A")), ("name", .string("item1"))]))
        _ = try await col.insertOne(BSONDocument([("category", .string("A")), ("name", .string("item2"))]))
        _ = try await col.insertOne(BSONDocument([("category", .string("B")), ("name", .string("item3"))]))

        // Query by category
        let results = try await col.find(BSONDocument([("category", .string("A"))]))
        XCTAssertEqual(results.count, 2, "expected 2 documents with category=A")

        try await db.close()
    }
}

