// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class AggregateCommandTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testAggregateMatchSortLimitProject() async throws {
        let dir = try makeWorkDir("agg-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)

        // insert docs
        let docs = BSONArray([
            .document(BSONDocument([("x", .int32(1)), ("y", .int32(2))])),
            .document(BSONDocument([("x", .int32(1)), ("y", .int32(1))])),
            .document(BSONDocument([("x", .int32(2)), ("y", .int32(0))])),
        ])
        _ = try await db.runCommand(BSONDocument([
            ("insert", .string("foo")),
            ("documents", .array(docs)),
            ("$db", .string("test")),
        ]))

        let pipeline = BSONArray([
            .document(BSONDocument([("$match", .document(BSONDocument([("x", .int32(1))])))])),
            .document(BSONDocument([("$sort", .document(BSONDocument([("y", .int32(1))])))])),
            .document(BSONDocument([("$limit", .int32(1))])),
            .document(BSONDocument([("$project", .document(BSONDocument([("y", .int32(1)), ("_id", .int32(0))])))])),
        ])

        let res = try await db.runCommand(BSONDocument([
            ("aggregate", .string("foo")),
            ("pipeline", .array(pipeline)),
            ("$db", .string("test")),
        ]))

        guard case .document(let cursor)? = res["cursor"],
              case .array(let firstBatch)? = cursor["firstBatch"] else {
            return XCTFail("missing cursor.firstBatch")
        }
        XCTAssertEqual(firstBatch.count, 1)
        guard case .document(let out0) = firstBatch[0] else {
            return XCTFail("firstBatch[0] not a document")
        }
        XCTAssertEqual(out0["y"]?.intValue, 1)
        XCTAssertNil(out0["_id"])

        try await db.close()
    }

    func testAggregateCountStage() async throws {
        let dir = try makeWorkDir("agg-count-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)

        let docs = BSONArray([
            .document(BSONDocument([("x", .int32(1))])),
            .document(BSONDocument([("x", .int32(2))])),
        ])
        _ = try await db.runCommand(BSONDocument([
            ("insert", .string("foo")),
            ("documents", .array(docs)),
        ]))

        let pipeline = BSONArray([
            .document(BSONDocument([("$match", .document(BSONDocument([("x", .int32(1))])))])),
            .document(BSONDocument([("$count", .string("n"))])),
        ])

        let res = try await db.runCommand(BSONDocument([
            ("aggregate", .string("foo")),
            ("pipeline", .array(pipeline)),
        ]))
        guard case .document(let cursor)? = res["cursor"],
              case .array(let firstBatch)? = cursor["firstBatch"],
              case .document(let out0) = firstBatch.first else {
            return XCTFail("missing firstBatch doc")
        }
        XCTAssertEqual(out0["n"]?.intValue, 1)

        try await db.close()
    }
}


