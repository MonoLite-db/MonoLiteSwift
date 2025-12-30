// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class DistinctCommandTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testDistinctReturnsUniqueValues() async throws {
        let dir = try makeWorkDir("distinct-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)

        let docs = BSONArray([
            .document(BSONDocument([("x", .int32(1))])),
            .document(BSONDocument([("x", .int32(2))])),
            .document(BSONDocument([("x", .int32(1))])),
        ])
        _ = try await db.runCommand(BSONDocument([
            ("insert", .string("foo")),
            ("documents", .array(docs)),
        ]))

        let res = try await db.runCommand(BSONDocument([
            ("distinct", .string("foo")),
            ("key", .string("x")),
            ("query", .document(BSONDocument())),
        ]))

        XCTAssertEqual(res["ok"]?.intValue, 1)
        guard case .array(let arr)? = res["values"] else {
            return XCTFail("missing values array")
        }
        let ints = arr.compactMap { $0.intValue }.sorted()
        XCTAssertEqual(ints, [1, 2])

        try await db.close()
    }
}


