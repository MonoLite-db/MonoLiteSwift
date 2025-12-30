// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class EngineDatabaseCatalogTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testCatalogPersistsCollectionsAndIdIndexIsUnique() async throws {
        let dir = try makeWorkDir("db-catalog-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path

        // create + insert
        do {
            let db = try await Database(path: path, enableWAL: true)
            let col = try await db.collection("foo")
            let _ = try await col.insertOne(BSONDocument([("a", .int32(1))]))

            // 强制写盘，确保下次打开可恢复
            // 通过 close() 触发 Pager.flush
            try await db.close()
        }

        // reopen + verify
        do {
            let db = try await Database(path: path, enableWAL: true)
            let names = await db.listCollections()
            XCTAssertEqual(names, ["foo"])

            let col = try await db.collection("foo")
            let docs = try await col.find(BSONDocument())
            XCTAssertEqual(docs.count, 1)

            // _id_ 唯一性：插入同一 _id 必须报 duplicate key
            let existingId = docs[0]["_id"]!
            let doc = BSONDocument([("_id", existingId), ("a", .int32(2))])
            do {
                _ = try await col.insertOne(doc)
                XCTFail("expected duplicate key")
            } catch let e as MonoError {
                XCTAssertEqual(e.code, .duplicateKey)
            }

            try await db.close()
        }
    }
}


