// Created by Yanjunhui

import Foundation

/// MongoDB Wire Protocol opcodes (subset)
public enum WireOpCode: Int32, Sendable {
    case reply = 1
    case query = 2004
    case compressed = 2012
    case msg = 2013
}

/// 16-byte wire message header
public struct WireMessageHeader: Sendable {
    public var messageLength: Int32
    public var requestId: Int32
    public var responseTo: Int32
    public var opCode: Int32

    public init(messageLength: Int32, requestId: Int32, responseTo: Int32, opCode: Int32) {
        self.messageLength = messageLength
        self.requestId = requestId
        self.responseTo = responseTo
        self.opCode = opCode
    }
}

public struct WireMessage: Sendable {
    public var header: WireMessageHeader
    public var body: Data

    public init(header: WireMessageHeader, body: Data) {
        self.header = header
        self.body = body
    }

    public var bytes: Data {
        var out = Data(count: 16 + body.count)
        // header (little-endian)
        DataEndian.writeUInt32LE(UInt32(bitPattern: header.messageLength), to: &out, at: 0)
        DataEndian.writeUInt32LE(UInt32(bitPattern: header.requestId), to: &out, at: 4)
        DataEndian.writeUInt32LE(UInt32(bitPattern: header.responseTo), to: &out, at: 8)
        DataEndian.writeUInt32LE(UInt32(bitPattern: header.opCode), to: &out, at: 12)
        out.replaceSubrange(16..<16 + body.count, with: body)
        return out
    }

    public static func parse(_ data: Data) throws -> WireMessage {
        guard data.count >= 16 else {
            throw MonoError.protocolError("wire message too short: \(data.count)")
        }
        let len = Int32(bitPattern: DataEndian.readUInt32LE(data, at: 0))
        guard len >= 16 else {
            throw MonoError.protocolError("invalid message length: \(len)")
        }
        guard Int(len) == data.count else {
            throw MonoError.protocolError("message length mismatch: header=\(len) actual=\(data.count)")
        }

        let requestId = Int32(bitPattern: DataEndian.readUInt32LE(data, at: 4))
        let responseTo = Int32(bitPattern: DataEndian.readUInt32LE(data, at: 8))
        let opCode = Int32(bitPattern: DataEndian.readUInt32LE(data, at: 12))
        let body = Data(data[16...])
        return WireMessage(
            header: WireMessageHeader(messageLength: len, requestId: requestId, responseTo: responseTo, opCode: opCode),
            body: body
        )
    }
}


