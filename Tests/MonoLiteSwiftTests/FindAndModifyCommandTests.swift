// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class FindAndModifyCommandTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testFindAndModifyUsesSortAndReturnsNewDoc() async throws {
        let dir = try makeWorkDir("fam-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)

        let docs = BSONArray([
            .document(BSONDocument([("x", .int32(1)), ("y", .int32(2))])),
            .document(BSONDocument([("x", .int32(1)), ("y", .int32(1))])),
        ])
        _ = try await db.runCommand(BSONDocument([
            ("insert", .string("foo")),
            ("documents", .array(docs)),
        ]))

        let res = try await db.runCommand(BSONDocument([
            ("findAndModify", .string("foo")),
            ("query", .document(BSONDocument([("x", .int32(1))]))),
            ("sort", .document(BSONDocument([("y", .int32(1))]))),
            ("update", .document(BSONDocument([
                ("$set", .document(BSONDocument([("z", .int32(9))]))),
            ]))),
            ("new", .bool(true)),
        ]))

        XCTAssertEqual(res["ok"]?.intValue, 1)
        guard case .document(let valueDoc)? = res["value"] else {
            return XCTFail("missing value")
        }
        XCTAssertEqual(valueDoc["y"]?.intValue, 1)
        XCTAssertEqual(valueDoc["z"]?.intValue, 9)

        try await db.close()
    }

    func testFindAndModifyRemoveReturnsOldDoc() async throws {
        let dir = try makeWorkDir("fam-remove-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)

        let docs = BSONArray([
            .document(BSONDocument([("x", .int32(1)), ("y", .int32(1))])),
        ])
        _ = try await db.runCommand(BSONDocument([
            ("insert", .string("foo")),
            ("documents", .array(docs)),
        ]))

        let res = try await db.runCommand(BSONDocument([
            ("findAndModify", .string("foo")),
            ("query", .document(BSONDocument([("x", .int32(1))]))),
            ("remove", .bool(true)),
        ]))
        XCTAssertEqual(res["ok"]?.intValue, 1)
        guard case .document(let valueDoc)? = res["value"] else {
            return XCTFail("missing value")
        }
        XCTAssertEqual(valueDoc["x"]?.intValue, 1)

        let after = try await db.runCommand(BSONDocument([
            ("find", .string("foo")),
            ("filter", .document(BSONDocument())),
        ]))
        guard case .document(let cursor)? = after["cursor"],
              case .array(let batch)? = cursor["firstBatch"] else {
            return XCTFail("missing firstBatch")
        }
        XCTAssertEqual(batch.count, 0)

        try await db.close()
    }
}


