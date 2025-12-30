// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class EngineMonoCollectionInsertManyNoIndexTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testInsertManyWithoutAnyIndexesDoesNotTrap() async throws {
        let dir = try makeWorkDir("col-insert-noindex-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path

        let pager = try await Pager(path: path, enableWAL: true)
        let info = CollectionInfo(name: "foo", firstPageId: 0, lastPageId: 0, documentCount: 0, indexes: [])
        let col = await MonoCollection(info: info, pager: pager)

        for i in 0..<20 {
            _ = try await col.insertOne(BSONDocument([("x", .int32(Int32(i)))]))
        }

        let docs = try await col.find(BSONDocument())
        XCTAssertEqual(docs.count, 20)
        try await pager.close()
    }
}


