// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

/// Integration tests simulating real MongoDB client behavior
/// These tests verify that the TCPServer and wire protocol implementation
/// can handle proper MongoDB wire protocol messages.
final class MongoDriverIntegrationTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    /// Test full wire protocol flow: connect -> hello -> insert -> find -> close
    func testWireProtocolFullFlow() async throws {
        try await withTestTimeout(seconds: 30, mode: .hard) {
            let dir = try self.makeWorkDir("driver-flow-\(UUID().uuidString)")
            let path = dir.appendingPathComponent("data.monodb").path
            let db = try await Database(path: path, enableWAL: true)

            // Start TCP server on random port
            let server = try MongoWireTCPServer(addr: "127.0.0.1:0", db: db)
            try await server.start()
            let serverAddr = server.addr
            let portStr = serverAddr.split(separator: ":").last ?? "0"
            let port = UInt16(portStr) ?? 0
            XCTAssertGreaterThan(port, 0, "expected server to bind to a port")

            // Connect client
            let clientSocket = socket(AF_INET, SOCK_STREAM, 0)
            XCTAssertGreaterThan(clientSocket, 0, "failed to create client socket")
            defer { close(clientSocket) }

            var sockAddr = sockaddr_in()
            sockAddr.sin_family = sa_family_t(AF_INET)
            sockAddr.sin_port = UInt16(port).bigEndian
            sockAddr.sin_addr.s_addr = inet_addr("127.0.0.1")

            let connectResult = withUnsafePointer(to: &sockAddr) {
                $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { ptr in
                    Darwin.connect(clientSocket, ptr, socklen_t(MemoryLayout<sockaddr_in>.size))
                }
            }
            XCTAssertEqual(connectResult, 0, "failed to connect to server")

            // 1. Send OP_MSG hello command
            let helloCmd = BSONDocument([("hello", .int32(1)), ("$db", .string("admin"))])
            let helloData = try BSONEncoder().encode(helloCmd)
            let helloMsg = self.buildOpMsgMessage(requestId: 1, body: helloData)
            let bytesSent = Darwin.send(clientSocket, Array(helloMsg), helloMsg.count, 0)
            XCTAssertEqual(bytesSent, helloMsg.count, "failed to send hello message")

            // Receive hello response
            var helloResponse = [UInt8](repeating: 0, count: 4096)
            let helloRecv = Darwin.recv(clientSocket, &helloResponse, helloResponse.count, 0)
            XCTAssertGreaterThan(helloRecv, 0, "failed to receive hello response")

            // Parse response header
            let responseLen = Int(helloResponse[0]) | (Int(helloResponse[1]) << 8) | (Int(helloResponse[2]) << 16) | (Int(helloResponse[3]) << 24)
            XCTAssertGreaterThan(responseLen, 16, "response too short")

            // 2. Send OP_MSG insert command
            let insertCmd = BSONDocument([
                ("insert", .string("test_col")),
                ("documents", .array(BSONArray([
                    .document(BSONDocument([("name", .string("Alice")), ("age", .int32(30))])),
                    .document(BSONDocument([("name", .string("Bob")), ("age", .int32(25))])),
                ]))),
                ("$db", .string("test")),
            ])
            let insertData = try BSONEncoder().encode(insertCmd)
            let insertMsg = self.buildOpMsgMessage(requestId: 2, body: insertData)
            let insertSent = Darwin.send(clientSocket, Array(insertMsg), insertMsg.count, 0)
            XCTAssertEqual(insertSent, insertMsg.count, "failed to send insert message")

            // Receive insert response
            var insertResponse = [UInt8](repeating: 0, count: 4096)
            let insertRecv = Darwin.recv(clientSocket, &insertResponse, insertResponse.count, 0)
            XCTAssertGreaterThan(insertRecv, 0, "failed to receive insert response")

            // 3. Send OP_MSG find command
            let findCmd = BSONDocument([
                ("find", .string("test_col")),
                ("filter", .document(BSONDocument())),
                ("$db", .string("test")),
            ])
            let findData = try BSONEncoder().encode(findCmd)
            let findMsg = self.buildOpMsgMessage(requestId: 3, body: findData)
            let findSent = Darwin.send(clientSocket, Array(findMsg), findMsg.count, 0)
            XCTAssertEqual(findSent, findMsg.count, "failed to send find message")

            // Receive find response
            var findResponse = [UInt8](repeating: 0, count: 8192)
            let findRecv = Darwin.recv(clientSocket, &findResponse, findResponse.count, 0)
            XCTAssertGreaterThan(findRecv, 0, "failed to receive find response")

            // Parse find response to verify documents
            let findResponseData = Data(findResponse[0..<findRecv])
            let parsedResponse = try self.parseOpMsgResponse(findResponseData)
            XCTAssertNotNil(parsedResponse, "failed to parse find response")

            // Verify cursor.firstBatch contains our documents
            if let cursor = parsedResponse?["cursor"]?.documentValue,
               let firstBatch = cursor["firstBatch"]?.arrayValue {
                XCTAssertEqual(firstBatch.count, 2, "expected 2 documents in firstBatch")
            } else {
                XCTFail("find response missing cursor.firstBatch")
            }

            // Cleanup
            await server.stop()
            try await db.close()
        }
    }

    /// Test wire protocol error handling
    func testWireProtocolErrorHandling() async throws {
        try await withTestTimeout(seconds: 15, mode: .hard) {
            let dir = try self.makeWorkDir("driver-error-\(UUID().uuidString)")
            let path = dir.appendingPathComponent("data.monodb").path
            let db = try await Database(path: path, enableWAL: true)

            let server = try MongoWireTCPServer(addr: "127.0.0.1:0", db: db)
            try await server.start()
            let serverAddr = server.addr
            let portStr = serverAddr.split(separator: ":").last ?? "0"
            let port = UInt16(portStr) ?? 0

            let clientSocket = socket(AF_INET, SOCK_STREAM, 0)
            defer { close(clientSocket) }

            var sockAddr = sockaddr_in()
            sockAddr.sin_family = sa_family_t(AF_INET)
            sockAddr.sin_port = UInt16(port).bigEndian
            sockAddr.sin_addr.s_addr = inet_addr("127.0.0.1")

            let connectResult = withUnsafePointer(to: &sockAddr) {
                $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { ptr in
                    Darwin.connect(clientSocket, ptr, socklen_t(MemoryLayout<sockaddr_in>.size))
                }
            }
            XCTAssertEqual(connectResult, 0)

            // Send invalid command
            let invalidCmd = BSONDocument([
                ("unknownCommand", .int32(1)),
                ("$db", .string("test")),
            ])
            let invalidData = try BSONEncoder().encode(invalidCmd)
            let invalidMsg = self.buildOpMsgMessage(requestId: 1, body: invalidData)
            _ = Darwin.send(clientSocket, Array(invalidMsg), invalidMsg.count, 0)

            // Receive error response
            var response = [UInt8](repeating: 0, count: 4096)
            let recvLen = Darwin.recv(clientSocket, &response, response.count, 0)
            XCTAssertGreaterThan(recvLen, 0, "expected error response")

            // Parse and verify error
            let responseData = Data(response[0..<recvLen])
            let parsed = try self.parseOpMsgResponse(responseData)
            XCTAssertNotNil(parsed)

            // Should have ok: 0 or errmsg
            if let ok = parsed?["ok"]?.intValue {
                XCTAssertEqual(ok, 0, "expected ok: 0 for invalid command")
            }

            await server.stop()
            try await db.close()
        }
    }

    // MARK: - Helpers

    private func buildOpMsgMessage(requestId: Int32, body: Data) -> Data {
        // OP_MSG format:
        // Header: messageLength(4) + requestID(4) + responseTo(4) + opCode(4)
        // Body: flagBits(4) + sections...
        // Section Kind 0: kind(1) + document(BSON)

        let flagBits: UInt32 = 0
        let sectionKind: UInt8 = 0  // Kind 0 = body

        let bodyLen = body.count
        let messageLen = 16 + 4 + 1 + bodyLen  // header + flags + kind + body

        var msg = Data(capacity: messageLen)

        // Header
        var len = UInt32(messageLen)
        msg.append(contentsOf: withUnsafeBytes(of: &len) { Array($0) })

        var reqId = requestId
        msg.append(contentsOf: withUnsafeBytes(of: &reqId) { Array($0) })

        var respTo: Int32 = 0
        msg.append(contentsOf: withUnsafeBytes(of: &respTo) { Array($0) })

        var opCode: Int32 = 2013  // OP_MSG
        msg.append(contentsOf: withUnsafeBytes(of: &opCode) { Array($0) })

        // Flags
        var flags = flagBits
        msg.append(contentsOf: withUnsafeBytes(of: &flags) { Array($0) })

        // Section Kind 0
        msg.append(sectionKind)
        msg.append(body)

        return msg
    }

    private func parseOpMsgResponse(_ data: Data) throws -> BSONDocument? {
        guard data.count >= 21 else { return nil }  // min: header(16) + flags(4) + kind(1)

        // Skip header (16 bytes) + flags (4 bytes) + kind (1 byte)
        let bodyStart = 21
        guard data.count > bodyStart else { return nil }

        let bodyData = Data(data[bodyStart...])
        return try? BSONDecoder().decode(bodyData)
    }
}
