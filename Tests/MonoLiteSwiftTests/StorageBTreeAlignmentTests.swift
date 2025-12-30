// Created by Yanjunhui

import XCTest
@testable import MonoLiteSwift

final class StorageBTreeAlignmentTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        // 在包目录的 .build 下创建测试目录，避免写出工作区
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testMinKeysMatchesGo() {
        XCTAssertEqual(BTreeNode.minKeys, (StorageConstants.btreeOrder - 1) / 2)
    }

    func testSplitTriggeredByKeyCountAndLeafChainUpdated() async throws {
        let dir = try makeWorkDir("btree-split-\(UUID().uuidString)")
        let dbPath = dir.appendingPathComponent("data.monodb").path

        let pager = try await Pager(path: dbPath, enableWAL: true)
        let tree = try await BTree(pager: pager, name: "idx", unique: false)

        // 先插入 order-1 个键，根仍应为叶子；插入第 order 个键触发根分裂（Go 语义）
        let order = StorageConstants.btreeOrder
        for i in 0..<order {
            var key = Data(count: 4)
            DataEndian.writeUInt32LE(UInt32(i), to: &key, at: 0)
            let rid = RecordID(pageId: UInt32(i), slotIndex: 0).marshal()
            try await tree.insert(key, rid)
        }

        let rootPage = await tree.rootPage
        let rootNode = try BTreeNode.unmarshal(try await pager.readPage(rootPage).data, pageId: rootPage)

        XCTAssertFalse(rootNode.isLeaf, "root should become internal node after split")
        XCTAssertEqual(rootNode.children.count, rootNode.keyCount + 1)
        XCTAssertEqual(rootNode.keyCount, 1, "splitting a leaf root should promote exactly one separator key")

        // 验证叶子链表：left.next == right.pageId && right.prev == left.pageId
        let leftId = rootNode.children[0]
        let rightId = rootNode.children[1]
        let left = try BTreeNode.unmarshal(try await pager.readPage(leftId).data, pageId: leftId)
        let right = try BTreeNode.unmarshal(try await pager.readPage(rightId).data, pageId: rightId)

        XCTAssertTrue(left.isLeaf && right.isLeaf)
        XCTAssertEqual(left.next, right.pageId)
        XCTAssertEqual(right.prev, left.pageId)

        try await pager.close()
    }

    func testNodePageDataTailIsZeroed() async throws {
        let dir = try makeWorkDir("btree-zero-tail-\(UUID().uuidString)")
        let dbPath = dir.appendingPathComponent("data.monodb").path

        let pager = try await Pager(path: dbPath, enableWAL: true)
        let tree = try await BTree(pager: pager, name: "idx", unique: false)

        // 插入少量键，让根保持叶子并写入一次节点
        for i in 0..<5 {
            let key = Data([UInt8(i)])
            let rid = RecordID(pageId: UInt32(i), slotIndex: 0).marshal()
            try await tree.insert(key, rid)
        }

        let rootPage = await tree.rootPage
        let page = try await pager.readPage(rootPage)
        let node = try BTreeNode.unmarshal(page.data, pageId: rootPage)
        let encoded = node.marshal()

        XCTAssertLessThanOrEqual(encoded.count, StorageConstants.maxPageData)
        XCTAssertEqual(page.data.count, StorageConstants.maxPageData)

        // Go 语义：writeNode 写满 maxPageData，未使用尾部必须为 0
        if encoded.count < StorageConstants.maxPageData {
            let tail = page.data[encoded.count..<StorageConstants.maxPageData]
            XCTAssertTrue(tail.allSatisfy { $0 == 0 }, "tail bytes should be zeroed to avoid stale data")
        }

        try await pager.close()
    }

    func testDeleteRebalancesAndLeafChainStaysConsistent() async throws {
        let dir = try makeWorkDir("btree-delete-\(UUID().uuidString)")
        let dbPath = dir.appendingPathComponent("data.monodb").path

        let pager = try await Pager(path: dbPath, enableWAL: true)
        let tree = try await BTree(pager: pager, name: "idx", unique: false)

        func makeKey(_ i: Int) -> Data {
            var d = Data(count: 4)
            DataEndian.writeUInt32LE(UInt32(i), to: &d, at: 0)
            return d
        }

        // 构造 3 层左右的树
        let n = 300
        for i in 0..<n {
            let key = makeKey(i)
            let rid = RecordID(pageId: UInt32(i), slotIndex: 0).marshal()
            try await tree.insert(key, rid)
        }

        // 删除一大段，触发借键/合并
        for i in 0..<200 {
            try await tree.delete(makeKey(i))
        }

        // 验证剩余键可查到，已删键查不到
        for i in 0..<200 {
            let r = try await tree.search(makeKey(i))
            XCTAssertNil(r)
        }
        for i in 200..<n {
            let r = try await tree.search(makeKey(i))
            XCTAssertNotNil(r)
        }

        // 叶子链表一致性检查：prev/next 双向一致、跨节点严格递增
        let rootPage = await tree.rootPage
        var node = try BTreeNode.unmarshal(try await pager.readPage(rootPage).data, pageId: rootPage)
        while !node.isLeaf {
            node = try BTreeNode.unmarshal(try await pager.readPage(node.children[0]).data, pageId: node.children[0])
        }

        var prevId: PageID = StorageConstants.invalidPageId
        var lastKey: Data? = nil

        while true {
            XCTAssertEqual(node.prev, prevId)
            for k in node.keys {
                if let lastKey {
                    XCTAssertLessThan(compareKeys(lastKey, k), 0, "leaf chain must be strictly increasing by key bytes")
                }
                lastKey = k
            }

            if node.next == StorageConstants.invalidPageId {
                break
            }
            prevId = node.pageId
            node = try BTreeNode.unmarshal(try await pager.readPage(node.next).data, pageId: node.next)
        }

        try await pager.close()
    }
}


