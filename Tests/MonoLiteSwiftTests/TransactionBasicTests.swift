// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class TransactionBasicTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testTransactionAbortRollsBackInsert() async throws {
        let dir = try makeWorkDir("txn-abort-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path

        let db = try await Database(path: path, enableWAL: true)
        let col = try await db.collection("foo")

        let txn = await db.txnManager.begin()
        _ = try await col.insertOne(BSONDocument([("a", .int32(1))]), in: txn)

        // 可见性：本实现是 undo log，abort 需要回滚删除
        try await db.txnManager.abort(txn)

        let docs = try await col.find(BSONDocument())
        XCTAssertEqual(docs.count, 0)
        try await db.close()
    }

    func testTransactionCommitPersistsInsert() async throws {
        let dir = try makeWorkDir("txn-commit-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path

        do {
            let db = try await Database(path: path, enableWAL: true)
            let col = try await db.collection("foo")
            let txn = await db.txnManager.begin()
            _ = try await col.insertOne(BSONDocument([("a", .int32(1))]), in: txn)
            try await db.txnManager.commit(txn)
            try await db.close()
        }

        do {
            let db = try await Database(path: path, enableWAL: true)
            let col = try await db.collection("foo")
            let docs = try await col.find(BSONDocument())
            XCTAssertEqual(docs.count, 1)
            try await db.close()
        }
    }
}


