// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class StoragePagerSlottedPageBSONInsertReproTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testLoadSlotsReadsSlotDirectoryUsingAbsoluteOffsets() async throws {
        let dir = try makeWorkDir("pager-slotted-bson-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path

        let pager = try await Pager(path: path, enableWAL: true)
        let page = try await pager.allocatePage(type: .data)

        for i in 0..<3 {
            let data = Data(repeating: UInt8(i), count: 32)
            let sp = SlottedPage(page: page)
            _ = try sp.insertRecord(data)
            await pager.markDirty(page.id)
            XCTAssertEqual(page.itemCount, UInt16(i + 1))
        }

        try await pager.close()
    }
}


