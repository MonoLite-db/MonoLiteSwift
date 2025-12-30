// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class AggregateExtraStageTests: XCTestCase {
    func testAggregateGroupSumAndCount() async throws {
        let dir = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
            .appendingPathComponent(".build/monolite_test_tmp/agg-group-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let path = dir.appendingPathComponent("data.monodb").path

        let db = try await Database(path: path, enableWAL: true)

        do {
            _ = try await db.runCommand(BSONDocument([
                ("insert", .string("t")),
                ("documents", .array(BSONArray([
                    .document(BSONDocument([("_id", .int32(1)), ("cat", .string("a")), ("x", .int32(10))])),
                    .document(BSONDocument([("_id", .int32(2)), ("cat", .string("a")), ("x", .int32(20))])),
                    .document(BSONDocument([("_id", .int32(3)), ("cat", .string("b")), ("x", .int32(7))])),
                ]))),
                ("$db", .string("test")),
            ]))

            let resp = try await db.runCommand(BSONDocument([
                ("aggregate", .string("t")),
                ("pipeline", .array(BSONArray([
                    .document(BSONDocument([("$group", .document(BSONDocument([
                        ("_id", .string("$cat")),
                        ("total", .document(BSONDocument([("$sum", .string("$x"))]))),
                        ("cnt", .document(BSONDocument([("$count", .int32(1))]))),
                    ])))])),
                    .document(BSONDocument([("$sort", .document(BSONDocument([("_id", .int32(1))])))])),
                ]))),
                ("$db", .string("test")),
            ]))

            guard case .document(let cursor)? = resp["cursor"],
                  case .array(let firstBatch)? = cursor["firstBatch"] else {
                XCTFail("missing cursor.firstBatch")
                return
            }
            XCTAssertEqual(firstBatch.count, 2)
            guard case .document(let a0) = firstBatch[0],
                  case .document(let a1) = firstBatch[1] else {
                XCTFail("unexpected batch doc types")
                return
            }
            XCTAssertEqual(a0["_id"]?.stringValue, "a")
            XCTAssertEqual(a0["total"]?.doubleValue, 30.0)
            XCTAssertEqual(a0["cnt"]?.int64Value, 2)
            XCTAssertEqual(a1["_id"]?.stringValue, "b")
            XCTAssertEqual(a1["total"]?.doubleValue, 7.0)
            XCTAssertEqual(a1["cnt"]?.int64Value, 1)
        } catch {
            try? await db.close()
            throw error
        }

        try await db.close()
    }

    func testAggregateUnwindAddFieldsUnsetReplaceRootLookup() async throws {
        let dir = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
            .appendingPathComponent(".build/monolite_test_tmp/agg-extra-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let path = dir.appendingPathComponent("data.monodb").path

        let db = try await Database(path: path, enableWAL: true)

        do {
            _ = try await db.runCommand(BSONDocument([
                ("insert", .string("local")),
                ("documents", .array(BSONArray([
                    .document(BSONDocument([("_id", .int32(1)), ("k", .int32(1)), ("arr", .array(BSONArray([.int32(1), .int32(2)]))), ("x", .int32(5))])),
                ]))),
                ("$db", .string("test")),
            ]))
            _ = try await db.runCommand(BSONDocument([
                ("insert", .string("foreign")),
                ("documents", .array(BSONArray([
                    .document(BSONDocument([("_id", .int32(10)), ("k", .int32(1)), ("v", .string("hit"))])),
                    .document(BSONDocument([("_id", .int32(11)), ("k", .int32(2)), ("v", .string("miss"))])),
                ]))),
                ("$db", .string("test")),
            ]))

            let resp = try await db.runCommand(BSONDocument([
                ("aggregate", .string("local")),
                ("pipeline", .array(BSONArray([
                    .document(BSONDocument([("$unwind", .string("$arr"))])),
                    .document(BSONDocument([("$addFields", .document(BSONDocument([
                        ("y", .string("$x")),
                    ])))])),
                    .document(BSONDocument([("$unset", .string("x"))])),
                    .document(BSONDocument([("$lookup", .document(BSONDocument([
                        ("from", .string("foreign")),
                        ("localField", .string("k")),
                        ("foreignField", .string("k")),
                        ("as", .string("matches")),
                    ])))])),
                    .document(BSONDocument([("$replaceRoot", .document(BSONDocument([
                        ("newRoot", .document(BSONDocument([
                            ("a", .string("$arr")),
                            ("y", .string("$y")),
                            ("m", .string("$matches")),
                        ]))),
                    ])))])),
                    .document(BSONDocument([("$sort", .document(BSONDocument([("a", .int32(1))])))])),
                ]))),
                ("$db", .string("test")),
            ]))

            guard case .document(let cursor)? = resp["cursor"],
                  case .array(let firstBatch)? = cursor["firstBatch"] else {
                XCTFail("missing cursor.firstBatch")
                return
            }
            XCTAssertEqual(firstBatch.count, 2)
            guard case .document(let d0) = firstBatch[0],
                  case .document(let d1) = firstBatch[1] else {
                XCTFail("unexpected batch doc types")
                return
            }
            XCTAssertEqual(d0["a"]?.intValue, 1)
            XCTAssertEqual(d0["y"]?.intValue, 5)
            XCTAssertNotNil(d0["m"]?.arrayValue)
            XCTAssertEqual(d1["a"]?.intValue, 2)
            XCTAssertEqual(d1["y"]?.intValue, 5)
        } catch {
            try? await db.close()
            throw error
        }

        try await db.close()
    }
}


