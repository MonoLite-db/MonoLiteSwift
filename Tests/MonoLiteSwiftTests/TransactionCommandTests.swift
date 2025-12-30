// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class TransactionCommandTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    private func makeLSID() -> BSONDocument {
        let uuid = UUID()
        let bin = BSONBinary(uuid: uuid)
        return BSONDocument([("id", .binary(bin))])
    }

    func testRunCommandTransactionAbortRollsBackInsert() async throws {
        let dir = try makeWorkDir("txn-cmd-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path

        let db = try await Database(path: path, enableWAL: true)
        let lsid = makeLSID()
        let txnNumber: Int64 = 1

        // startTransaction
        _ = try await db.runCommand(BSONDocument([
            ("startTransaction", .int32(1)),
            ("lsid", .document(lsid)),
            ("txnNumber", .int64(txnNumber)),
        ]))

        // insert within txn (autocommit=false)
        let docs = BSONArray([.document(BSONDocument([("x", .int32(1))]))])
        _ = try await db.runCommand(BSONDocument([
            ("insert", .string("foo")),
            ("documents", .array(docs)),
            ("lsid", .document(lsid)),
            ("txnNumber", .int64(txnNumber)),
            ("autocommit", .bool(false)),
        ]))

        // abort
        _ = try await db.runCommand(BSONDocument([
            ("abortTransaction", .int32(1)),
            ("lsid", .document(lsid)),
            ("txnNumber", .int64(txnNumber)),
        ]))

        // find should be empty
        let res = try await db.runCommand(BSONDocument([
            ("find", .string("foo")),
            ("filter", .document(BSONDocument())),
        ]))
        guard case .document(let cursor)? = res["cursor"],
              case .array(let firstBatch)? = cursor["firstBatch"] else {
            return XCTFail("missing cursor.firstBatch")
        }
        XCTAssertEqual(firstBatch.count, 0)

        try await db.close()
    }

    func testRunCommandTransactionCommitPersistsInsert() async throws {
        let dir = try makeWorkDir("txn-cmd-commit-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path

        let db = try await Database(path: path, enableWAL: true)
        let lsid = makeLSID()
        let txnNumber: Int64 = 1

        _ = try await db.runCommand(BSONDocument([
            ("startTransaction", .int32(1)),
            ("lsid", .document(lsid)),
            ("txnNumber", .int64(txnNumber)),
        ]))

        let docs = BSONArray([.document(BSONDocument([("x", .int32(7))]))])
        _ = try await db.runCommand(BSONDocument([
            ("insert", .string("foo")),
            ("documents", .array(docs)),
            ("lsid", .document(lsid)),
            ("txnNumber", .int64(txnNumber)),
            ("autocommit", .bool(false)),
        ]))

        _ = try await db.runCommand(BSONDocument([
            ("commitTransaction", .int32(1)),
            ("lsid", .document(lsid)),
            ("txnNumber", .int64(txnNumber)),
        ]))

        let res = try await db.runCommand(BSONDocument([
            ("find", .string("foo")),
            ("filter", .document(BSONDocument())),
        ]))
        guard case .document(let cursor)? = res["cursor"],
              case .array(let firstBatch)? = cursor["firstBatch"] else {
            return XCTFail("missing cursor.firstBatch")
        }
        XCTAssertEqual(firstBatch.count, 1)

        try await db.close()
    }

    func testCommitAndAbortAreIdempotentAndStateTransitionsMatchGo() async throws {
        let dir = try makeWorkDir("txn-cmd-idem-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path

        let db = try await Database(path: path, enableWAL: true)
        defer { try? FileManager.default.removeItem(at: dir) }

        let lsid = makeLSID()
        do {
            // txnNumber=1 -> commit twice OK
            _ = try await db.runCommand(BSONDocument([
                ("startTransaction", .int32(1)),
                ("lsid", .document(lsid)),
                ("txnNumber", .int64(1)),
            ]))
            _ = try await db.runCommand(BSONDocument([
                ("commitTransaction", .int32(1)),
                ("lsid", .document(lsid)),
                ("txnNumber", .int64(1)),
            ]))
            _ = try await db.runCommand(BSONDocument([
                ("commitTransaction", .int32(1)),
                ("lsid", .document(lsid)),
                ("txnNumber", .int64(1)),
            ]))

            // txnNumber=2 -> abort twice OK
            _ = try await db.runCommand(BSONDocument([
                ("startTransaction", .int32(1)),
                ("lsid", .document(lsid)),
                ("txnNumber", .int64(2)),
            ]))
            _ = try await db.runCommand(BSONDocument([
                ("abortTransaction", .int32(1)),
                ("lsid", .document(lsid)),
                ("txnNumber", .int64(2)),
            ]))
            _ = try await db.runCommand(BSONDocument([
                ("abortTransaction", .int32(1)),
                ("lsid", .document(lsid)),
                ("txnNumber", .int64(2)),
            ]))

            // Committing an aborted txn should error with TransactionAborted (Go parity).
            do {
                _ = try await db.runCommand(BSONDocument([
                    ("commitTransaction", .int32(1)),
                    ("lsid", .document(lsid)),
                    ("txnNumber", .int64(2)),
                ]))
                XCTFail("expected commitTransaction on aborted txn to fail")
            } catch let e as MonoError {
                XCTAssertEqual(e.code, .transactionAborted)
            }

            try await db.close()
        } catch {
            try? await db.close()
            throw error
        }
    }
}


