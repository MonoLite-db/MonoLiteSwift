// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class StorageBTreeStressTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    private func makeKey(_ i: Int) -> Data {
        var d = Data(count: 4)
        DataEndian.writeUInt32LE(UInt32(i), to: &d, at: 0)
        return d
    }

    func testInsertManyKeysDoesNotCrashAndSearchWorks() async throws {
        try await withTestTimeout(seconds: 15, mode: .hard) { [self] in
            let dir = try self.makeWorkDir("btree-stress-\(UUID().uuidString)")
            let path = dir.appendingPathComponent("data.monodb").path

            let pager = try await Pager(path: path, enableWAL: true)
            let tree = try await BTree(pager: pager, name: "stress", unique: true)

            let n = Int(ProcessInfo.processInfo.environment["MONOLITE_STRESS_N"].flatMap(Int.init) ?? 2000)
            for i in 0..<n {
                let key = self.makeKey(i)
                let value = Data([UInt8(i & 0xFF)])
                try await tree.insert(key, value)
            }

            let c = try await tree.count()
            XCTAssertEqual(c, n)

            for i in stride(from: 0, to: n, by: 97) {
                let got = try await tree.search(self.makeKey(i))
                XCTAssertEqual(got, Data([UInt8(i & 0xFF)]))
            }

            try await pager.close()
        }
    }
}


