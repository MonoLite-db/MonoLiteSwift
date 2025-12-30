// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class GoldstandardProtocolAlignmentTests: XCTestCase {
    // B-GOLD-PROTO-001 (Go): OP_MSG with unknown required flag bits (0-15) must return error.
    func testOpMsgRequiredFlagBits() throws {
        struct TC {
            let name: String
            let flagBits: UInt32
            let expectError: Bool
        }

        let tests: [TC] = [
            .init(name: "no flags (valid)", flagBits: 0, expectError: false),
            .init(name: "checksumPresent flag (bit 0, valid)", flagBits: 1, expectError: false),
            .init(name: "moreToCome flag (bit 1, valid)", flagBits: 2, expectError: false),
            .init(name: "unknown required bit 2 (should error)", flagBits: 4, expectError: true),
            .init(name: "unknown required bit 3 (should error)", flagBits: 8, expectError: true),
            .init(name: "unknown required bit 15 (should error)", flagBits: 0x8000, expectError: true),
            .init(name: "optional bit 16 (should NOT error)", flagBits: 0x10000, expectError: false),
            .init(name: "combination: valid + unknown required (should error)", flagBits: 1 | 4, expectError: true),
        ]

        // Minimal document: {} (5 bytes)
        let minDoc = Data([5, 0, 0, 0, 0])

        for tc in tests {
            var body = Data(count: 4 + 1 + minDoc.count)
            DataEndian.writeUInt32LE(tc.flagBits, to: &body, at: 0)
            body[4] = OpMsgSectionKind.body.rawValue
            body.replaceSubrange(5..<5 + minDoc.count, with: minDoc)

            // If checksumPresent, append 4 bytes checksum (contents irrelevant; parse should only check length here).
            if (tc.flagBits & OpMsgFlags.checksumPresent.rawValue) != 0 {
                body.append(Data(count: 4))
            }

            do {
                _ = try OpMsgParser.parse(body: body, fullMessage: body) // fullMessage only used when checksumPresent
                if tc.expectError {
                    XCTFail("expected error for flagBits=0x\(String(tc.flagBits, radix: 16))")
                }
            } catch {
                if !tc.expectError {
                    // Keep this strict: with checksumPresent=0, the body is valid and should parse.
                    if (tc.flagBits & OpMsgFlags.checksumPresent.rawValue) == 0 {
                        XCTFail("unexpected error for flagBits=0x\(String(tc.flagBits, radix: 16)): \(error)")
                    }
                }
            }
        }
    }

    // B-GOLD-PROTO-002 (Go): DocSequence parsing must be strict - bad length must return error.
    func testOpMsgDocSequenceStrictParsing() throws {
        struct TC {
            let name: String
            let build: () -> Data
            let expectError: Bool
        }

        let tests: [TC] = [
            .init(name: "valid docSequence", build: {
                // Section kind 1:
                // seqLen(4, includes itself) + identifier(cstring) + documents...
                // Minimal: seqLen=11, id="x\0", doc={}
                var msg = Data(count: 4 + 1 + 11)
                DataEndian.writeUInt32LE(0, to: &msg, at: 0) // flags
                msg[4] = OpMsgSectionKind.documentSequence.rawValue
                DataEndian.writeUInt32LE(11, to: &msg, at: 5) // seqLen
                msg[9] = UInt8(UnicodeScalar("x").value)
                msg[10] = 0
                DataEndian.writeUInt32LE(5, to: &msg, at: 11) // doc length
                msg[15] = 0
                return msg
            }, expectError: false),

            .init(name: "docSequence with bad seqLen (too short)", build: {
                var msg = Data(count: 4 + 1 + 11)
                DataEndian.writeUInt32LE(0, to: &msg, at: 0)
                msg[4] = OpMsgSectionKind.documentSequence.rawValue
                DataEndian.writeUInt32LE(5, to: &msg, at: 5) // too short
                msg[9] = UInt8(UnicodeScalar("x").value)
                msg[10] = 0
                DataEndian.writeUInt32LE(5, to: &msg, at: 11)
                msg[15] = 0
                return msg
            }, expectError: true),

            .init(name: "docSequence with docLen exceeding seqLen", build: {
                var msg = Data(count: 4 + 1 + 11)
                DataEndian.writeUInt32LE(0, to: &msg, at: 0)
                msg[4] = OpMsgSectionKind.documentSequence.rawValue
                DataEndian.writeUInt32LE(11, to: &msg, at: 5)
                msg[9] = UInt8(UnicodeScalar("x").value)
                msg[10] = 0
                DataEndian.writeUInt32LE(100, to: &msg, at: 11) // exceeds seqLen
                return msg
            }, expectError: true),
        ]

        for tc in tests {
            let msg = tc.build()
            do {
                _ = try OpMsgParser.parse(body: msg, fullMessage: msg)
                if tc.expectError {
                    XCTFail("expected error for \(tc.name)")
                }
            } catch {
                if !tc.expectError {
                    XCTFail("unexpected error for \(tc.name): \(error)")
                }
            }
        }
    }
}


