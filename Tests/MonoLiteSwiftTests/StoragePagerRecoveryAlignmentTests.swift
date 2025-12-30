// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class StoragePagerRecoveryAlignmentTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        // 在包目录的 .build 下创建测试目录，避免写出工作区
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    /// 对齐 Go 测试语义：
    /// - 先 free 一个已落盘页面
    /// - 再从 free list 复用该 PageId 分配为 Index page
    /// - “崩溃”后恢复必须保证：该 PageId 的 pageType 已被正确初始化为 Index，而不是遗留 Free
    func testRecoverAllocateFromFreeListCrashInitializesPageType() async throws {
        let dir = try makeWorkDir("pager-recover-alloc-from-free-\(UUID().uuidString)")
        let dbPath = dir.appendingPathComponent("data.monodb").path

        // 1) 创建数据库并分配一个可复用页，确保物理页已落盘
        do {
            let p1 = try await Pager(path: dbPath, enableWAL: true)
            _ = try await p1.allocatePage(type: .data)
            try await p1.flush()
            try await p1.close()
        }

        // 2) 重新打开，释放该页，flush 确保 free page 与 freelistHead 均已落盘
        let freeId: PageID
        do {
            let p2 = try await Pager(path: dbPath, enableWAL: true)
            // 第一个用户页必然是 pageId=1（meta=0），直接释放它用于测试
            freeId = 1
            try await p2.freePage(freeId)
            try await p2.flush()
            try await p2.close()
        }

        // 3) 从 free list 复用该页分配为 Index，然后模拟“页内容未被覆盖”的崩溃窗口
        do {
            let p3 = try await Pager(path: dbPath, enableWAL: true)
            let page = try await p3.allocatePage(type: .index)
            XCTAssertEqual(page.id, freeId, "allocation should reuse the free list head (Go semantics)")

            // 关键：强制把数据文件里该页写回为 Free（模拟“allocPage 已持久化，但 init pageWrite 丢失/未发生”）
            let freePage = Page(id: freeId, type: .free)
            freePage.nextPageId = StorageConstants.invalidPageId
            freePage.prevPageId = StorageConstants.invalidPageId
            let raw = freePage.marshal()

            let fh = try FileHandle(forUpdating: URL(fileURLWithPath: dbPath))
            defer { try? fh.close() }
            let offset = UInt64(StorageConstants.fileHeaderSize) + UInt64(freeId) * UInt64(StorageConstants.pageSize)
            try fh.seek(toOffset: offset)
            try fh.write(contentsOf: raw)
            try fh.synchronize()

            // 模拟崩溃：不 flush、不正常 close
            await p3.simulateCrashCloseForTesting()
        }

        // 4) 恢复后，该页必须被初始化为 Index，并且不应仍出现在 free list 中
        do {
            let p4 = try await Pager(path: dbPath, enableWAL: true)
            let recovered = try await p4.readPage(freeId)
            XCTAssertEqual(recovered.pageType, .index, "recovery must initialize allocated-from-free page type from WAL alloc records")

            // 再分配一个页，确保不会再次复用 freeId
            let newPage = try await p4.allocatePage(type: .data)
            XCTAssertNotEqual(newPage.id, freeId)

            try await p4.close()
        }
    }
}


