// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class EngineDatabaseValidateTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testValidateOnFreshDbIsValid() async throws {
        let dir = try makeWorkDir("db-validate-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path

        let db = try await Database(path: path, enableWAL: true)
        let _ = try await db.collection("foo")
        let res = await db.validate()
        XCTAssertTrue(res.valid)
        XCTAssertGreaterThanOrEqual(res.stats.totalPages, 1)
        try await db.close()
    }
}


