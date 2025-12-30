// Created by Yanjunhui

import XCTest
@testable import MonoLiteSwift

final class StorageKeyStringAlignmentTests: XCTestCase {
    func testBoolEncodingMatchesGo() {
        var b = KeyStringBuilder()
        b.appendValue(.bool(true), direction: 1)
        XCTAssertEqual(b.bytes(), Data([KeyStringType.bool.rawValue, 0x02, KeyStringType.end]))

        b.reset()
        b.appendValue(.bool(false), direction: 1)
        XCTAssertEqual(b.bytes(), Data([KeyStringType.bool.rawValue, 0x01, KeyStringType.end]))
    }

    func testBoolEncodingDescendingMatchesGoRule() {
        var b = KeyStringBuilder()
        b.appendValue(.bool(true), direction: -1)

        let expected = Data([~KeyStringType.bool.rawValue, ~UInt8(0x02), ~KeyStringType.end])
        XCTAssertEqual(b.bytes(), expected)
    }

    func testBinaryEncodingNoSubtypeAndBigEndianLength() {
        let bin = BSONBinary(subtype: .generic, data: Data([0xAA, 0xBB, 0xCC]))
        var b = KeyStringBuilder()
        b.appendValue(.binary(bin), direction: 1)

        // type(0x20) + len(be u32=3) + data + end(0x04)
        let expected = Data([
            KeyStringType.binData.rawValue,
            0x00, 0x00, 0x00, 0x03,
            0xAA, 0xBB, 0xCC,
            KeyStringType.end
        ])
        XCTAssertEqual(b.bytes(), expected)
    }

    func testStringBodyEscapingAndTerminatorMatchesGo() {
        // 仅测试 0x00 的转义与 0x00 0x00 终止符（0xFF 在 UTF-8 字符串中不可出现，但实现保持与 Go 一致的 escape 分支）
        // "a\0b" -> a 00 FF b 00 00
        let s = "a\u{0000}b"
        var b = KeyStringBuilder()
        b.appendValue(.string(s), direction: 1)

        let expected = Data([
            KeyStringType.string.rawValue,
            0x61, 0x00, 0xFF, 0x62, 0x00, 0x00,
            KeyStringType.end
        ])
        XCTAssertEqual(b.bytes(), expected)
    }
}


