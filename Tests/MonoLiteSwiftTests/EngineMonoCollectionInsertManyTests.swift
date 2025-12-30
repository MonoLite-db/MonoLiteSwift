// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class EngineMonoCollectionInsertManyTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testInsertManyDocsDirectCollectionAPIDoesNotTrap() async throws {
        let dir = try makeWorkDir("col-insert-many-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path

        let db = try await Database(path: path, enableWAL: true)
        let col = try await db.collection("foo")

        for i in 0..<20 {
            _ = try await col.insertOne(BSONDocument([("x", .int32(Int32(i)))]))
        }

        let docs = try await col.find(BSONDocument())
        XCTAssertEqual(docs.count, 20)
        try await db.close()
    }
}


