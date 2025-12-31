// Created by Yanjunhui

import Foundation

// MARK: - OP_QUERY 消息 / OP_QUERY Message

/// OP_QUERY 消息结构（已弃用，仅用于握手兼容）
/// EN: OP_QUERY message structure (deprecated, only for handshake compatibility).
public struct OpQueryMessage: Sendable {
    /// 查询标志位
    /// EN: Query flags
    public var flags: Int32
    /// 完整集合名称（如 "admin.$cmd"）
    /// EN: Full collection name (e.g., "admin.$cmd")
    public var fullCollectionName: String
    /// 跳过的文档数量
    /// EN: Number of documents to skip
    public var numberToSkip: Int32
    /// 返回的文档数量
    /// EN: Number of documents to return
    public var numberToReturn: Int32
    /// 查询文档
    /// EN: Query document
    public var query: BSONDocument
    /// 返回字段选择器（可选）
    /// EN: Return fields selector (optional)
    public var returnFieldsSelector: BSONDocument?
}

// MARK: - OP_QUERY 解析器 / OP_QUERY Parser

/// OP_QUERY 消息解析器
/// EN: OP_QUERY message parser.
public enum OpQueryParser {
    /// 解析 OP_QUERY 消息体
    /// EN: Parse OP_QUERY message body.
    /// - Parameter body: 消息体数据（不含 16 字节 Wire 头部）/ EN: Message body data (without 16-byte wire header)
    /// - Returns: 解析后的 OP_QUERY 消息 / EN: Parsed OP_QUERY message
    public static func parse(body: Data) throws -> OpQueryMessage {
        guard body.count >= 12 else {
            throw MonoError.protocolError("OP_QUERY body too short: \(body.count)")
        }

        // 读取标志位 / EN: Read flags
        let flags = Int32(bitPattern: DataEndian.readUInt32LE(body, at: 0))
        var pos = 4

        // 解析完整集合名称（C 字符串）
        // EN: Parse full collection name (cstring)
        guard let nameEnd = body[pos...].firstIndex(of: 0) else {
            throw MonoError.protocolError("OP_QUERY collection name not terminated")
        }
        let nameData = body[pos..<nameEnd]
        let fullCollectionName = String(data: nameData, encoding: .utf8) ?? ""
        pos = nameEnd + 1

        // 读取 numberToSkip 和 numberToReturn
        // EN: Read numberToSkip and numberToReturn
        guard pos + 8 <= body.count else {
            throw MonoError.protocolError("OP_QUERY missing skip/return fields")
        }
        let numberToSkip = Int32(bitPattern: DataEndian.readUInt32LE(body, at: pos))
        pos += 4
        let numberToReturn = Int32(bitPattern: DataEndian.readUInt32LE(body, at: pos))
        pos += 4

        // 解析查询文档
        // EN: Parse query document
        guard pos + 4 <= body.count else {
            throw MonoError.protocolError("OP_QUERY missing query document")
        }
        let docLen = Int(DataEndian.readUInt32LE(body, at: pos))
        guard docLen >= 5, pos + docLen <= body.count else {
            throw MonoError.protocolError("OP_QUERY query document extends beyond message")
        }
        let queryDoc = try BSONDecoder().decode(Data(body[pos..<pos + docLen]))
        pos += docLen

        // 解析可选的返回字段选择器
        // EN: Parse optional return fields selector
        var selector: BSONDocument? = nil
        if pos + 4 <= body.count {
            let selLen = Int(DataEndian.readUInt32LE(body, at: pos))
            if selLen >= 5, pos + selLen <= body.count {
                selector = try? BSONDecoder().decode(Data(body[pos..<pos + selLen]))
            }
        }

        return OpQueryMessage(
            flags: flags,
            fullCollectionName: fullCollectionName,
            numberToSkip: numberToSkip,
            numberToReturn: numberToReturn,
            query: queryDoc,
            returnFieldsSelector: selector
        )
    }
}

// MARK: - OP_REPLY 构建器 / OP_REPLY Builder

/// OP_REPLY 消息构建器（用于响应 OP_QUERY）
/// EN: OP_REPLY message builder (for responding to OP_QUERY).
public enum OpReplyBuilder {
    /// 构建 OP_REPLY 响应消息
    /// EN: Build OP_REPLY response message.
    /// - Parameters:
    ///   - requestId: 原始请求 ID / EN: Original request ID
    ///   - documents: 响应文档数组 / EN: Response documents array
    /// - Returns: Wire 消息 / EN: Wire message
    public static func buildReply(requestId: Int32, documents: [BSONDocument]) throws -> WireMessage {
        // 编码所有响应文档 / EN: Encode all response documents
        var docsBytes = Data()
        let enc = BSONEncoder()
        for d in documents {
            docsBytes.append(try enc.encode(d))
        }

        // 消息体长度：responseFlags(4) + cursorId(8) + startingFrom(4) + numberReturned(4) + documents
        // EN: Body length: responseFlags(4) + cursorId(8) + startingFrom(4) + numberReturned(4) + documents
        let bodyLen = 4 + 8 + 4 + 4 + docsBytes.count
        var body = Data(count: bodyLen)

        // responseFlags = 0（无特殊标志）/ EN: responseFlags = 0 (no special flags)
        DataEndian.writeUInt32LE(0, to: &body, at: 0)
        // cursorId = 0（无游标）/ EN: cursorId = 0 (no cursor)
        DataEndian.writeUInt64LE(0, to: &body, at: 4)
        // startingFrom = 0 / EN: startingFrom = 0
        DataEndian.writeUInt32LE(0, to: &body, at: 12)
        // numberReturned = 文档数量 / EN: numberReturned = document count
        DataEndian.writeUInt32LE(UInt32(documents.count), to: &body, at: 16)
        // 文档数据 / EN: Document data
        body.replaceSubrange(20..<20 + docsBytes.count, with: docsBytes)

        let header = WireMessageHeader(
            messageLength: Int32(16 + body.count),
            requestId: OpMsgParser.nextRequestId(),
            responseTo: requestId,
            opCode: WireOpCode.reply.rawValue
        )
        return WireMessage(header: header, body: body)
    }
}
