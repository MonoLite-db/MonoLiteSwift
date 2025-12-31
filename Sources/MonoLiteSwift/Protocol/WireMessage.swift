// Created by Yanjunhui

import Foundation

// MARK: - Wire Protocol 操作码 / Wire Protocol Opcodes

/// MongoDB Wire Protocol 操作码（子集）
/// EN: MongoDB Wire Protocol opcodes (subset).
public enum WireOpCode: Int32, Sendable {
    /// 回复消息（已弃用）
    /// EN: Reply message (deprecated)
    case reply = 1
    /// 查询消息（已弃用，仅用于握手兼容）
    /// EN: Query message (deprecated, only for handshake compatibility)
    case query = 2004
    /// 压缩消息
    /// EN: Compressed message
    case compressed = 2012
    /// OP_MSG 消息（当前主要使用）
    /// EN: OP_MSG message (currently primary)
    case msg = 2013
}

// MARK: - Wire Protocol 消息头 / Wire Protocol Message Header

/// 16 字节的 Wire Protocol 消息头
/// EN: 16-byte Wire Protocol message header.
public struct WireMessageHeader: Sendable {
    /// 消息总长度（包括头部）
    /// EN: Total message length (including header)
    public var messageLength: Int32
    /// 请求 ID
    /// EN: Request ID
    public var requestId: Int32
    /// 响应目标请求 ID
    /// EN: Response to request ID
    public var responseTo: Int32
    /// 操作码
    /// EN: Operation code
    public var opCode: Int32

    /// 初始化消息头
    /// EN: Initialize message header.
    /// - Parameters:
    ///   - messageLength: 消息总长度 / EN: Total message length
    ///   - requestId: 请求 ID / EN: Request ID
    ///   - responseTo: 响应目标请求 ID / EN: Response to request ID
    ///   - opCode: 操作码 / EN: Operation code
    public init(messageLength: Int32, requestId: Int32, responseTo: Int32, opCode: Int32) {
        self.messageLength = messageLength
        self.requestId = requestId
        self.responseTo = responseTo
        self.opCode = opCode
    }
}

// MARK: - Wire Protocol 消息 / Wire Protocol Message

/// Wire Protocol 完整消息（头部 + 消息体）
/// EN: Wire Protocol complete message (header + body).
public struct WireMessage: Sendable {
    /// 消息头
    /// EN: Message header
    public var header: WireMessageHeader
    /// 消息体（不包括头部）
    /// EN: Message body (excluding header)
    public var body: Data

    /// 初始化 Wire 消息
    /// EN: Initialize Wire message.
    /// - Parameters:
    ///   - header: 消息头 / EN: Message header
    ///   - body: 消息体 / EN: Message body
    public init(header: WireMessageHeader, body: Data) {
        self.header = header
        self.body = body
    }

    /// 将消息序列化为字节数据
    /// EN: Serialize message to byte data.
    public var bytes: Data {
        var out = Data(count: 16 + body.count)
        // 写入头部（小端序）/ EN: Write header (little-endian)
        DataEndian.writeUInt32LE(UInt32(bitPattern: header.messageLength), to: &out, at: 0)
        DataEndian.writeUInt32LE(UInt32(bitPattern: header.requestId), to: &out, at: 4)
        DataEndian.writeUInt32LE(UInt32(bitPattern: header.responseTo), to: &out, at: 8)
        DataEndian.writeUInt32LE(UInt32(bitPattern: header.opCode), to: &out, at: 12)
        out.replaceSubrange(16..<16 + body.count, with: body)
        return out
    }

    /// 从原始字节数据解析 Wire 消息
    /// EN: Parse Wire message from raw byte data.
    /// - Parameter data: 原始字节数据 / EN: Raw byte data
    /// - Returns: 解析后的消息 / EN: Parsed message
    /// - Throws: 协议错误 / EN: Protocol error
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
