// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class GoldstandardNumericAlignmentTests: XCTestCase {
    // B-GOLD-NUM-001 (Go): int64 comparison beyond 2^53 must be correct.
    func testCompareNumericInt64Beyond2Pow53() {
        let pow53: Int64 = 9_007_199_254_740_992
        let pow53Plus1: Int64 = 9_007_199_254_740_993

        XCTAssertLessThan(BSONCompare.compare(.int64(pow53), .int64(pow53Plus1)), 0)
        XCTAssertGreaterThan(BSONCompare.compare(.int64(pow53Plus1), .int64(pow53)), 0)
        XCTAssertEqual(BSONCompare.compare(.int64(pow53), .int64(pow53)), 0)
        XCTAssertEqual(BSONCompare.compare(.int64(pow53Plus1), .int64(pow53Plus1)), 0)
        XCTAssertGreaterThan(BSONCompare.compare(.int64(-pow53), .int64(-pow53 - 1)), 0)
        XCTAssertLessThan(BSONCompare.compare(.int64(-pow53 - 1), .int64(-pow53)), 0)
        XCTAssertLessThan(BSONCompare.compare(.int64(9_007_199_254_740_999), .int64(9_007_199_254_741_000)), 0)
        XCTAssertLessThan(BSONCompare.compare(.int64((1 << 62) - 1), .int64(1 << 62)), 0)
    }

    func testCompareNumericInt64VsDouble() {
        XCTAssertEqual(BSONCompare.compare(.int64(10), .double(10.0)), 0)
        XCTAssertLessThan(BSONCompare.compare(.int64(10), .double(10.5)), 0)
        XCTAssertGreaterThan(BSONCompare.compare(.double(10.5), .int64(10)), 0)
        XCTAssertEqual(BSONCompare.compare(.int32(10), .int64(10)), 0)
    }

    // B-GOLD-DEC-001 (Go): Decimal128 comparison must preserve scale and precision.
    func testCompareNumericDecimal128() {
        // Constants generated from go.mongodb.org/mongo-driver v1.17.6 primitive.ParseDecimal128 (see Go goldstandard numeric_test.go).
        let d110 = Decimal128(high: 0x303c000000000000, low: 0x000000000000006e) // "1.10"
        let d12 = Decimal128(high: 0x303e000000000000, low: 0x000000000000000c)  // "1.2"
        let d01 = Decimal128(high: 0x303e000000000000, low: 0x0000000000000001)  // "0.1"
        let d010 = Decimal128(high: 0x303c000000000000, low: 0x000000000000000a) // "0.10"
        let dLarge789 = Decimal128(high: 0x303a000000000000, low: 0x112210f47de98115) // "1234567890123456.789"
        let dLarge788 = Decimal128(high: 0x303a000000000000, low: 0x112210f47de98114) // "1234567890123456.788"
        let dNeg15 = Decimal128(high: 0xb03e000000000000, low: 0x000000000000000f) // "-1.5"
        let dNeg14 = Decimal128(high: 0xb03e000000000000, low: 0x000000000000000e) // "-1.4"
        let d1000 = Decimal128(high: 0x303e000000000000, low: 0x00000000000003e8) // "100.0"
        let d1005 = Decimal128(high: 0x303e000000000000, low: 0x00000000000003ed) // "100.5"

        XCTAssertLessThan(BSONCompare.compare(.decimal128(d110), .decimal128(d12)), 0)
        XCTAssertGreaterThan(BSONCompare.compare(.decimal128(d12), .decimal128(d110)), 0)
        XCTAssertEqual(BSONCompare.compare(.decimal128(d01), .decimal128(d010)), 0)
        XCTAssertGreaterThan(BSONCompare.compare(.decimal128(dLarge789), .decimal128(dLarge788)), 0)
        XCTAssertLessThan(BSONCompare.compare(.decimal128(dNeg15), .decimal128(dNeg14)), 0)

        // Cross-type comparisons: decimal vs int/double (Go behavior approximates via float in many cases).
        XCTAssertEqual(BSONCompare.compare(.decimal128(d1000), .int64(100)), 0)
        XCTAssertEqual(BSONCompare.compare(.decimal128(d1005), .double(100.5)), 0)
    }

    func testCompareTypeOrderingCoreSet() {
        // MongoDB type ordering (core subset):
        // MinKey < Null < Number < String < Object < Array < BinData < ObjectId < Boolean < Date < Timestamp < RegEx < MaxKey
        XCTAssertLessThan(BSONCompare.compare(.minKey, .null), 0)
        XCTAssertLessThan(BSONCompare.compare(.null, .int32(1)), 0)
        XCTAssertLessThan(BSONCompare.compare(.int32(1), .string("x")), 0)
        XCTAssertLessThan(BSONCompare.compare(.string("x"), .document(BSONDocument([("a", .int32(1))]))), 0)
        XCTAssertLessThan(BSONCompare.compare(.document(BSONDocument([("a", .int32(1))])), .array(BSONArray([.int32(1)]))), 0)
        XCTAssertLessThan(BSONCompare.compare(.array(BSONArray([.int32(1)])), .binary(BSONBinary(subtype: .generic, data: Data([1])))), 0)
        XCTAssertLessThan(BSONCompare.compare(.binary(BSONBinary(subtype: .generic, data: Data([1]))), .objectId(ObjectID.generate())), 0)
        XCTAssertLessThan(BSONCompare.compare(.objectId(ObjectID.generate()), .bool(false)), 0)
        XCTAssertLessThan(BSONCompare.compare(.bool(false), .dateTime(Date(timeIntervalSince1970: 0))), 0)
        XCTAssertLessThan(BSONCompare.compare(.dateTime(Date(timeIntervalSince1970: 0)), .timestamp(BSONTimestamp(t: 1, i: 0))), 0)
        XCTAssertLessThan(BSONCompare.compare(.timestamp(BSONTimestamp(t: 1, i: 0)), .regex(BSONRegex(pattern: "a"))), 0)
        XCTAssertLessThan(BSONCompare.compare(.regex(BSONRegex(pattern: "a")), .maxKey), 0)
    }
}


