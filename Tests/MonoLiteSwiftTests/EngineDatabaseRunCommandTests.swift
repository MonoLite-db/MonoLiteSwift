// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class EngineDatabaseRunCommandTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testRunCommandInsertFindGetMoreKillCursors() async throws {
        let dir = try makeWorkDir("db-runcmd-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path

        let db = try await Database(path: path, enableWAL: true)

        // insert 200 docs
        var docsArr = BSONArray()
        for i in 0..<200 {
            docsArr.append(.document(BSONDocument([("x", .int32(Int32(i)))])))
        }
        let insertCmd = BSONDocument([
            ("insert", .string("foo")),
            ("documents", .array(docsArr)),
            ("$db", .string("test")),
        ])
        let insertRes = try await db.runCommand(insertCmd)
        XCTAssertEqual(insertRes["ok"]?.intValue, 1)
        XCTAssertEqual(insertRes["n"]?.intValue, 200)

        // find with batchSize=50
        let findCmd = BSONDocument([
            ("find", .string("foo")),
            ("filter", .document(BSONDocument())),
            ("batchSize", .int32(50)),
            ("$db", .string("test")),
        ])
        let findRes = try await db.runCommand(findCmd)
        guard case .document(let cursorDoc)? = findRes["cursor"] else {
            return XCTFail("missing cursor")
        }
        let cursorId = cursorDoc["id"]?.int64Value ?? 0
        XCTAssertGreaterThan(cursorId, 0)
        guard case .array(let firstBatch)? = cursorDoc["firstBatch"] else {
            return XCTFail("missing firstBatch")
        }
        XCTAssertEqual(firstBatch.count, 50)

        // getMore
        let getMore1 = try await db.runCommand(BSONDocument([
            ("getMore", .int64(cursorId)),
            ("collection", .string("foo")),
            ("batchSize", .int32(50)),
            ("$db", .string("test")),
        ]))
        guard case .document(let cur1)? = getMore1["cursor"],
              case .array(let next1)? = cur1["nextBatch"] else {
            return XCTFail("missing nextBatch")
        }
        XCTAssertEqual(next1.count, 50)

        // killCursors
        let kill = try await db.runCommand(BSONDocument([
            ("killCursors", .string("foo")),
            ("cursors", .array(BSONArray([.int64(cursorId)]))),
            ("$db", .string("test")),
        ]))
        XCTAssertEqual(kill["ok"]?.intValue, 1)
        XCTAssertNotNil(kill["cursorsKilled"])
        XCTAssertNotNil(kill["cursorsNotFound"])

        try await db.close()
    }
}


