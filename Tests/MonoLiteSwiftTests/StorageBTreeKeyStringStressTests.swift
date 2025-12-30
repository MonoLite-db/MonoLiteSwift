// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class StorageBTreeKeyStringStressTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testInsertManyKeyStringObjectIdKeysRandomOrder() async throws {
        try await withTestTimeout(seconds: 20, mode: .hard) { [self] in
            let dir = try self.makeWorkDir("btree-keystring-\(UUID().uuidString)")
            let path = dir.appendingPathComponent("data.monodb").path

            let pager = try await Pager(path: path, enableWAL: true)
            let tree = try await BTree(pager: pager, name: "_id_", unique: true)

            let n = Int(ProcessInfo.processInfo.environment["MONOLITE_STRESS_N"].flatMap(Int.init) ?? 2000)
            var pairs: [(key: Data, value: Data)] = []
            pairs.reserveCapacity(n)

            for _ in 0..<n {
                let oid = ObjectID.generate()
                // Key: KeyString({ _id: oid })
                var ks = KeyStringBuilder()
                ks.appendValue(.objectId(oid), direction: 1)
                let key = ks.bytes()

                // Value: BSON encode { _id: oid } (Go 对齐：索引 value 存 _id 的 BSON 编码)
                let valDoc = BSONDocument([("_id", .objectId(oid))])
                let value = try BSONEncoder().encode(valDoc)

                pairs.append((key: key, value: value))
            }

            // 随机插入顺序（更接近真实写入分布）
            pairs.shuffle()

            for (k, v) in pairs {
                try await tree.insert(k, v)
            }

            let c = try await tree.count()
            XCTAssertEqual(c, n)

            // 抽样验证 search
            for i in stride(from: 0, to: n, by: 97) {
                let (k, v) = pairs[i]
                let got = try await tree.search(k)
                XCTAssertEqual(got, v)
            }

            try await pager.close()
        }
    }
}


