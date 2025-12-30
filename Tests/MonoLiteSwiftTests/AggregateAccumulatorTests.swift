// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

/// Tests for aggregate $group accumulators (Go parity: engine/aggregate_test.go)
final class AggregateAccumulatorTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testGroupStageAvg() async throws {
        let dir = try makeWorkDir("agg-avg-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)

        let col = try await db.collection("scores")
        _ = try await col.insert([
            BSONDocument([("subject", .string("math")), ("score", .int32(80))]),
            BSONDocument([("subject", .string("math")), ("score", .int32(90))]),
            BSONDocument([("subject", .string("english")), ("score", .int32(70))]),
            BSONDocument([("subject", .string("english")), ("score", .int32(80))]),
        ])

        let pipeline = BSONArray([
            .document(BSONDocument([
                ("$group", .document(BSONDocument([
                    ("_id", .string("$subject")),
                    ("avgScore", .document(BSONDocument([("$avg", .string("$score"))]))),
                ]))),
            ])),
            .document(BSONDocument([("$sort", .document(BSONDocument([("_id", .int32(1))])))]))
        ])

        let results = try await db.runCommand(BSONDocument([
            ("aggregate", .string("scores")),
            ("pipeline", .array(pipeline)),
            ("cursor", .document(BSONDocument())),
        ]))

        guard case .document(let cursor)? = results["cursor"],
              case .array(let docs)? = cursor["firstBatch"] else {
            return XCTFail("missing cursor.firstBatch")
        }

        XCTAssertEqual(docs.count, 2)

        // english avg = 75, math avg = 85
        if case .document(let d0) = docs[0] {
            XCTAssertEqual(d0["_id"]?.stringValue, "english")
            XCTAssertEqual(d0["avgScore"]?.doubleValue ?? 0, 75.0, accuracy: 0.01)
        }
        if case .document(let d1) = docs[1] {
            XCTAssertEqual(d1["_id"]?.stringValue, "math")
            XCTAssertEqual(d1["avgScore"]?.doubleValue ?? 0, 85.0, accuracy: 0.01)
        }

        try await db.close()
    }

    func testGroupStageMinMax() async throws {
        let dir = try makeWorkDir("agg-minmax-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)

        let col = try await db.collection("temps")
        _ = try await col.insert([
            BSONDocument([("city", .string("Tokyo")), ("temp", .int32(25))]),
            BSONDocument([("city", .string("Tokyo")), ("temp", .int32(30))]),
            BSONDocument([("city", .string("Tokyo")), ("temp", .int32(20))]),
            BSONDocument([("city", .string("London")), ("temp", .int32(15))]),
            BSONDocument([("city", .string("London")), ("temp", .int32(18))]),
        ])

        let pipeline = BSONArray([
            .document(BSONDocument([
                ("$group", .document(BSONDocument([
                    ("_id", .string("$city")),
                    ("minTemp", .document(BSONDocument([("$min", .string("$temp"))]))),
                    ("maxTemp", .document(BSONDocument([("$max", .string("$temp"))]))),
                ]))),
            ])),
            .document(BSONDocument([("$sort", .document(BSONDocument([("_id", .int32(1))])))]))
        ])

        let results = try await db.runCommand(BSONDocument([
            ("aggregate", .string("temps")),
            ("pipeline", .array(pipeline)),
            ("cursor", .document(BSONDocument())),
        ]))

        guard case .document(let cursor)? = results["cursor"],
              case .array(let docs)? = cursor["firstBatch"] else {
            return XCTFail("missing cursor.firstBatch")
        }

        XCTAssertEqual(docs.count, 2)

        // London: min=15, max=18
        if case .document(let d0) = docs[0] {
            XCTAssertEqual(d0["_id"]?.stringValue, "London")
            XCTAssertEqual(d0["minTemp"]?.intValue, 15)
            XCTAssertEqual(d0["maxTemp"]?.intValue, 18)
        }

        // Tokyo: min=20, max=30
        if case .document(let d1) = docs[1] {
            XCTAssertEqual(d1["_id"]?.stringValue, "Tokyo")
            XCTAssertEqual(d1["minTemp"]?.intValue, 20)
            XCTAssertEqual(d1["maxTemp"]?.intValue, 30)
        }

        try await db.close()
    }

    func testGroupStagePush() async throws {
        let dir = try makeWorkDir("agg-push-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)

        let col = try await db.collection("orders")
        _ = try await col.insert([
            BSONDocument([("customer", .string("Alice")), ("item", .string("apple"))]),
            BSONDocument([("customer", .string("Alice")), ("item", .string("banana"))]),
            BSONDocument([("customer", .string("Bob")), ("item", .string("cherry"))]),
        ])

        let pipeline = BSONArray([
            .document(BSONDocument([
                ("$group", .document(BSONDocument([
                    ("_id", .string("$customer")),
                    ("items", .document(BSONDocument([("$push", .string("$item"))]))),
                ]))),
            ])),
            .document(BSONDocument([("$sort", .document(BSONDocument([("_id", .int32(1))])))]))
        ])

        let results = try await db.runCommand(BSONDocument([
            ("aggregate", .string("orders")),
            ("pipeline", .array(pipeline)),
            ("cursor", .document(BSONDocument())),
        ]))

        guard case .document(let cursor)? = results["cursor"],
              case .array(let docs)? = cursor["firstBatch"] else {
            return XCTFail("missing cursor.firstBatch")
        }

        XCTAssertEqual(docs.count, 2)

        // Alice: items = [apple, banana]
        if case .document(let d0) = docs[0] {
            XCTAssertEqual(d0["_id"]?.stringValue, "Alice")
            if case .array(let items) = d0["items"] {
                XCTAssertEqual(items.count, 2)
            } else {
                XCTFail("expected items array")
            }
        }

        // Bob: items = [cherry]
        if case .document(let d1) = docs[1] {
            XCTAssertEqual(d1["_id"]?.stringValue, "Bob")
            if case .array(let items) = d1["items"] {
                XCTAssertEqual(items.count, 1)
            } else {
                XCTFail("expected items array")
            }
        }

        try await db.close()
    }

    func testGroupStageAddToSet() async throws {
        let dir = try makeWorkDir("agg-addtoset-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)

        let col = try await db.collection("tags")
        _ = try await col.insert([
            BSONDocument([("category", .string("fruit")), ("tag", .string("red"))]),
            BSONDocument([("category", .string("fruit")), ("tag", .string("red"))]),  // duplicate
            BSONDocument([("category", .string("fruit")), ("tag", .string("green"))]),
        ])

        let pipeline = BSONArray([
            .document(BSONDocument([
                ("$group", .document(BSONDocument([
                    ("_id", .string("$category")),
                    ("uniqueTags", .document(BSONDocument([("$addToSet", .string("$tag"))]))),
                ]))),
            ])),
        ])

        let results = try await db.runCommand(BSONDocument([
            ("aggregate", .string("tags")),
            ("pipeline", .array(pipeline)),
            ("cursor", .document(BSONDocument())),
        ]))

        guard case .document(let cursor)? = results["cursor"],
              case .array(let docs)? = cursor["firstBatch"] else {
            return XCTFail("missing cursor.firstBatch")
        }

        XCTAssertEqual(docs.count, 1)

        if case .document(let d) = docs[0] {
            XCTAssertEqual(d["_id"]?.stringValue, "fruit")
            if case .array(let tags) = d["uniqueTags"] {
                XCTAssertEqual(tags.count, 2, "expected 2 unique tags (red, green)")
            } else {
                XCTFail("expected uniqueTags array")
            }
        }

        try await db.close()
    }
}

