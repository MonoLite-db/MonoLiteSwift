// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class ProtocolOpMsgTests: XCTestCase {
    func testWireMessageRoundtrip() throws {
        let body = Data([1, 2, 3])
        let msg = WireMessage(
            header: WireMessageHeader(messageLength: Int32(16 + body.count), requestId: 123, responseTo: 0, opCode: WireOpCode.msg.rawValue),
            body: body
        )
        let parsed = try WireMessage.parse(msg.bytes)
        XCTAssertEqual(parsed.header.requestId, 123)
        XCTAssertEqual(parsed.header.opCode, WireOpCode.msg.rawValue)
        XCTAssertEqual(parsed.body, body)
    }

    func testOpMsgParseBodySection() async throws {
        // command: { ping: 1, $db: "test" }
        let cmd = BSONDocument([("ping", .int32(1)), ("$db", .string("test"))])
        let enc = BSONEncoder()
        let cmdBytes = try enc.encode(cmd)

        var body = Data(count: 4 + 1 + cmdBytes.count)
        DataEndian.writeUInt32LE(0, to: &body, at: 0)
        body[4] = OpMsgSectionKind.body.rawValue
        body.replaceSubrange(5..<5 + cmdBytes.count, with: cmdBytes)

        let header = WireMessageHeader(messageLength: Int32(16 + body.count), requestId: 7, responseTo: 0, opCode: WireOpCode.msg.rawValue)
        let wire = WireMessage(header: header, body: body)

        let parsed = try OpMsgParser.parse(body: wire.body, fullMessage: wire.bytes)
        XCTAssertEqual(parsed.body?["ping"]?.intValue, 1)
    }

    func testOpMsgChecksumVerification() throws {
        let cmd = BSONDocument([("ping", .int32(1)), ("$db", .string("test"))])
        let cmdBytes = try BSONEncoder().encode(cmd)

        // OP_MSG with checksum: flags + kind0 + doc + crc32c
        var opBody = Data()
        let flags: UInt32 = OpMsgFlags.checksumPresent.rawValue
        var flagsBuf = Data(count: 4)
        DataEndian.writeUInt32LE(flags, to: &flagsBuf, at: 0)
        opBody.append(flagsBuf)
        opBody.append(OpMsgSectionKind.body.rawValue)
        opBody.append(cmdBytes)
        // append placeholder checksum (4 bytes)
        opBody.append(Data(count: 4))

        let header = WireMessageHeader(messageLength: Int32(16 + opBody.count), requestId: 9, responseTo: 0, opCode: WireOpCode.msg.rawValue)
        let wire = WireMessage(header: header, body: opBody)

        // write checksum into last 4 bytes of full message
        var full = wire.bytes
        let crc = CRC32C.checksum(full.prefix(full.count - 4))
        DataEndian.writeUInt32LE(crc, to: &full, at: full.count - 4)

        // re-parse from full
        let reparsed = try WireMessage.parse(full)
        _ = try OpMsgParser.parse(body: reparsed.body, fullMessage: full) // should not throw
    }

    func testOpQueryHelloHandshakeReturnsOpReply() async throws {
        let dir = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
            .appendingPathComponent(".build/monolite_test_tmp/opquery-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let path = dir.appendingPathComponent("data.monodb").path

        let db = try await Database(path: path, enableWAL: true)

        // OP_QUERY body:
        // flags(4) + fullCollectionName(cstring) + skip(4) + return(4) + queryDoc
        let queryDoc = BSONDocument([("hello", .int32(1)), ("$db", .string("admin"))])
        let queryBytes = try BSONEncoder().encode(queryDoc)

        var body = Data()
        var tmp4 = Data(count: 4)
        DataEndian.writeUInt32LE(0, to: &tmp4, at: 0)
        body.append(tmp4)
        body.append(contentsOf: "admin.$cmd".utf8)
        body.append(0)
        DataEndian.writeUInt32LE(0, to: &tmp4, at: 0) // skip
        body.append(tmp4)
        DataEndian.writeUInt32LE(1, to: &tmp4, at: 0) // return 1
        body.append(tmp4)
        body.append(queryBytes)

        let req = WireMessage(
            header: WireMessageHeader(messageLength: Int32(16 + body.count), requestId: 42, responseTo: 0, opCode: WireOpCode.query.rawValue),
            body: body
        )

        let resp = try await ProtocolServer.handle(message: req, db: db)
        XCTAssertNotNil(resp)
        guard let resp else { return }
        XCTAssertEqual(resp.header.opCode, WireOpCode.reply.rawValue)
        XCTAssertEqual(resp.header.responseTo, 42)
        // body: responseFlags(4) + cursorId(8) + startingFrom(4) + numberReturned(4) + docs
        XCTAssertGreaterThan(resp.body.count, 20)
        let num = Int(DataEndian.readUInt32LE(resp.body, at: 16))
        XCTAssertEqual(num, 1)

        // first doc begins at offset 20
        let docLen = Int(DataEndian.readUInt32LE(resp.body, at: 20))
        let docBytes = Data(resp.body[20..<20 + docLen])
        let doc = try BSONDecoder().decode(docBytes)
        XCTAssertEqual(doc["ok"]?.intValue, 1)

        try await db.close()
    }

    func testOpQueryNonCmdReturnsDeprecatedErrorDoc() async throws {
        // Build an OP_QUERY against a non-$cmd namespace; Go parity expects an OP_REPLY with an error document.
        let queryDoc = BSONDocument([("ping", .int32(1)), ("$db", .string("test"))])
        let queryBytes = try BSONEncoder().encode(queryDoc)

        var body = Data()
        var tmp4 = Data(count: 4)
        DataEndian.writeUInt32LE(0, to: &tmp4, at: 0)
        body.append(tmp4) // flags
        body.append(contentsOf: "test.foo".utf8)
        body.append(0)
        DataEndian.writeUInt32LE(0, to: &tmp4, at: 0) // skip
        body.append(tmp4)
        DataEndian.writeUInt32LE(1, to: &tmp4, at: 0) // return 1
        body.append(tmp4)
        body.append(queryBytes)

        let wire = WireMessage(
            header: WireMessageHeader(
                messageLength: Int32(16 + body.count),
                requestId: 1234,
                responseTo: 0,
                opCode: WireOpCode.query.rawValue
            ),
            body: body
        )

        let dir = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
            .appendingPathComponent(".build/monolite_test_tmp/opquery-noncmd-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let path = dir.appendingPathComponent("data.monodb").path

        let db = try await Database(path: path, enableWAL: true)
        do {
            let resp = try await ProtocolServer.handle(message: wire, db: db)
            guard let resp else { return XCTFail("nil response") }
            XCTAssertEqual(resp.header.opCode, WireOpCode.reply.rawValue)
            XCTAssertEqual(resp.header.responseTo, 1234)

            let num = Int(DataEndian.readUInt32LE(resp.body, at: 16))
            XCTAssertEqual(num, 1)
            let docLen = Int(DataEndian.readUInt32LE(resp.body, at: 20))
            let docBytes = Data(resp.body[20..<20 + docLen])
            let doc = try BSONDecoder().decode(docBytes)
            XCTAssertEqual(doc["ok"]?.intValue, 0)
            XCTAssertEqual(doc["errmsg"]?.stringValue, "OP_QUERY is deprecated, use OP_MSG")

            try await db.close()
        } catch {
            try? await db.close()
            throw error
        }
    }
}


