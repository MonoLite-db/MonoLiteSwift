// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class StorageSlottedPageInsertTests: XCTestCase {
    func testInsertThreeSmallRecordsDoesNotTrap() throws {
        let page = Page(id: 1, type: .data)
        let sp = SlottedPage(page: page)

        let rec = Data([0x01, 0x02, 0x03, 0x04])
        _ = try sp.insertRecord(rec)
        _ = try sp.insertRecord(rec)
        _ = try sp.insertRecord(rec)

        XCTAssertEqual(sp.slotCount, 3)
        XCTAssertEqual(sp.liveCount, 3)
    }
}


