// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

final class ProtocolTCPServerIntegrationTests: XCTestCase {
    func testTCPServerOpQueryHelloRoundtrip() async throws {
#if canImport(Darwin)
        let dir = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
            .appendingPathComponent(".build/monolite_test_tmp/tcp-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }

        let dbPath = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: dbPath, enableWAL: true)

        let server = try MongoWireTCPServer(addr: "127.0.0.1:0", db: db)
        try await server.start()

        do {
            // Connect and send legacy OP_QUERY hello against admin.$cmd
            let addr = server.addr
            guard let idx = addr.lastIndex(of: ":") else { XCTFail("bad addr"); return }
            let host = String(addr[..<idx]).isEmpty ? "127.0.0.1" : String(addr[..<idx])
            let port = UInt16(addr[addr.index(after: idx)...]) ?? 0

            let fd = Darwin.socket(AF_INET, SOCK_STREAM, 0)
            XCTAssertGreaterThanOrEqual(fd, 0)
            defer { Darwin.close(fd) }

            var sa = sockaddr_in()
            sa.sin_family = sa_family_t(AF_INET)
            sa.sin_port = port.bigEndian
            _ = host.withCString { cs in inet_pton(AF_INET, cs, &sa.sin_addr) }
            var sock = sockaddr()
            memcpy(&sock, &sa, MemoryLayout<sockaddr_in>.size)
            let connRes = withUnsafePointer(to: &sock) {
                $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { ptr in
                    Darwin.connect(fd, ptr, socklen_t(MemoryLayout<sockaddr_in>.size))
                }
            }
            XCTAssertEqual(connRes, 0)

            let queryDoc = BSONDocument([("hello", .int32(1)), ("$db", .string("admin"))])
            let queryBytes = try BSONEncoder().encode(queryDoc)

            var body = Data()
            var tmp4 = Data(count: 4)
            DataEndian.writeUInt32LE(0, to: &tmp4, at: 0)
            body.append(tmp4) // flags
            body.append(contentsOf: "admin.$cmd".utf8)
            body.append(0)
            DataEndian.writeUInt32LE(0, to: &tmp4, at: 0) // skip
            body.append(tmp4)
            DataEndian.writeUInt32LE(1, to: &tmp4, at: 0) // return 1
            body.append(tmp4)
            body.append(queryBytes)

            let req = WireMessage(
                header: WireMessageHeader(messageLength: Int32(16 + body.count), requestId: 777, responseTo: 0, opCode: WireOpCode.query.rawValue),
                body: body
            )

            try writeAll(fd: fd, data: req.bytes)
            let resp = try readWireMessage(fd: fd)
            XCTAssertEqual(resp.header.opCode, WireOpCode.reply.rawValue)
            XCTAssertEqual(resp.header.responseTo, 777)

            let num = Int(DataEndian.readUInt32LE(resp.body, at: 16))
            XCTAssertEqual(num, 1)
            let docLen = Int(DataEndian.readUInt32LE(resp.body, at: 20))
            let docBytes = Data(resp.body[20..<20 + docLen])
            let doc = try BSONDecoder().decode(docBytes)
            XCTAssertEqual(doc["ok"]?.intValue, 1)
        } catch {
            await server.stop()
            try? await db.close()
            throw error
        }

        await server.stop()
        try await db.close()
#else
        throw XCTSkip("TCP integration test requires Darwin sockets")
#endif
    }

    func testTCPServerOpCompressedReturnsStructuredProtocolError() async throws {
#if canImport(Darwin)
        let dir = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
            .appendingPathComponent(".build/monolite_test_tmp/tcp-cmpr-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }

        let dbPath = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: dbPath, enableWAL: true)

        let server = try MongoWireTCPServer(addr: "127.0.0.1:0", db: db)
        try await server.start()

        do {
            let addr = server.addr
            guard let idx = addr.lastIndex(of: ":") else { XCTFail("bad addr"); return }
            let host = String(addr[..<idx]).isEmpty ? "127.0.0.1" : String(addr[..<idx])
            let port = UInt16(addr[addr.index(after: idx)...]) ?? 0

            let fd = Darwin.socket(AF_INET, SOCK_STREAM, 0)
            XCTAssertGreaterThanOrEqual(fd, 0)
            defer { Darwin.close(fd) }

            var sa = sockaddr_in()
            sa.sin_family = sa_family_t(AF_INET)
            sa.sin_port = port.bigEndian
            _ = host.withCString { cs in inet_pton(AF_INET, cs, &sa.sin_addr) }
            var sock = sockaddr()
            memcpy(&sock, &sa, MemoryLayout<sockaddr_in>.size)
            let connRes = withUnsafePointer(to: &sock) {
                $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { ptr in
                    Darwin.connect(fd, ptr, socklen_t(MemoryLayout<sockaddr_in>.size))
                }
            }
            XCTAssertEqual(connRes, 0)

            // Send an OP_COMPRESSED message (body can be empty; server should return a structured protocol error).
            let req = WireMessage(
                header: WireMessageHeader(messageLength: 16, requestId: 888, responseTo: 0, opCode: WireOpCode.compressed.rawValue),
                body: Data()
            )
            try writeAll(fd: fd, data: req.bytes)

            let resp = try readWireMessage(fd: fd)
            XCTAssertEqual(resp.header.opCode, WireOpCode.msg.rawValue)
            XCTAssertEqual(resp.header.responseTo, 888)

            let opmsg = try OpMsgParser.parse(body: resp.body, fullMessage: resp.bytes)
            guard let doc = opmsg.body else {
                return XCTFail("missing OP_MSG body")
            }
            XCTAssertEqual(doc["ok"]?.intValue, 0)
            XCTAssertEqual(doc["code"]?.intValue, ErrorCode.protocolError.rawValue)
            XCTAssertEqual(doc["codeName"]?.stringValue, "ProtocolError")
            XCTAssertTrue((doc["errmsg"]?.stringValue ?? "").contains("OP_COMPRESSED"))
        } catch {
            await server.stop()
            try? await db.close()
            throw error
        }

        await server.stop()
        try await db.close()
#else
        throw XCTSkip("TCP integration test requires Darwin sockets")
#endif
    }

    private func readWireMessage(fd: Int32) throws -> WireMessage {
        let header = try readExact(fd: fd, count: 16)
        let msgLen = Int(Int32(bitPattern: DataEndian.readUInt32LE(header, at: 0)))
        let body = try readExact(fd: fd, count: msgLen - 16)
        var full = Data()
        full.append(header)
        full.append(body)
        return try WireMessage.parse(full)
    }

    private func readExact(fd: Int32, count: Int) throws -> Data {
        var buf = Data(count: count)
        var readTotal = 0
        while readTotal < count {
            let n = buf.withUnsafeMutableBytes { ptr in
                let base = ptr.baseAddress!.assumingMemoryBound(to: UInt8.self).advanced(by: readTotal)
                return Darwin.recv(fd, base, count - readTotal, 0)
            }
            if n == 0 { throw MonoError.protocolError("unexpected EOF") }
            if n < 0 { throw MonoError.protocolError("recv failed") }
            readTotal += n
        }
        return buf
    }

    private func writeAll(fd: Int32, data: Data) throws {
        var sent = 0
        while sent < data.count {
            let n = data.withUnsafeBytes { ptr in
                let base = ptr.baseAddress!.assumingMemoryBound(to: UInt8.self).advanced(by: sent)
                return Darwin.send(fd, base, data.count - sent, 0)
            }
            if n <= 0 { throw MonoError.protocolError("send failed") }
            sent += n
        }
    }
}


