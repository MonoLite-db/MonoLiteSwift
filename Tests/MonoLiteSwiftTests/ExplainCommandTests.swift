// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class ExplainCommandTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testExplainFindReturnsCollScan() async throws {
        let dir = try makeWorkDir("explain-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)

        // Insert some documents
        let docs = BSONArray([
            .document(BSONDocument([("x", .int32(1))])),
            .document(BSONDocument([("x", .int32(2))])),
            .document(BSONDocument([("x", .int32(3))])),
        ])
        _ = try await db.runCommand(BSONDocument([
            ("insert", .string("foo")),
            ("documents", .array(docs)),
        ]))

        // Explain find command
        let explainCmd = BSONDocument([
            ("find", .string("foo")),
            ("filter", .document(BSONDocument([("x", .int32(1))]))),
        ])
        let res = try await db.runCommand(BSONDocument([
            ("explain", .document(explainCmd)),
        ]))

        XCTAssertEqual(res["ok"]?.doubleValue, 1.0)

        guard case .document(let queryPlanner)? = res["queryPlanner"] else {
            return XCTFail("missing queryPlanner")
        }
        XCTAssertEqual(queryPlanner["namespace"]?.stringValue, "foo")

        guard case .document(let winningPlan)? = queryPlanner["winningPlan"] else {
            return XCTFail("missing winningPlan")
        }
        XCTAssertEqual(winningPlan["stage"]?.stringValue, "COLLSCAN")

        guard case .document(let execStats)? = res["executionStats"] else {
            return XCTFail("missing executionStats")
        }
        XCTAssertEqual(execStats["totalDocsExamined"]?.int64Value, 3)
        XCTAssertEqual(execStats["totalKeysExamined"]?.int64Value, 0)

        try await db.close()
    }

    func testExplainNonExistentCollectionReturnsEOF() async throws {
        let dir = try makeWorkDir("explain-eof-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)

        // Explain on non-existent collection
        let explainCmd = BSONDocument([
            ("find", .string("nonexistent")),
            ("filter", .document(BSONDocument())),
        ])
        let res = try await db.runCommand(BSONDocument([
            ("explain", .document(explainCmd)),
        ]))

        XCTAssertEqual(res["ok"]?.doubleValue, 1.0)

        guard case .document(let queryPlanner)? = res["queryPlanner"] else {
            return XCTFail("missing queryPlanner")
        }
        guard case .document(let winningPlan)? = queryPlanner["winningPlan"] else {
            return XCTFail("missing winningPlan")
        }
        XCTAssertEqual(winningPlan["stage"]?.stringValue, "EOF")

        guard case .document(let execStats)? = res["executionStats"] else {
            return XCTFail("missing executionStats")
        }
        XCTAssertEqual(execStats["totalDocsExamined"]?.int64Value, 0)

        try await db.close()
    }
}

