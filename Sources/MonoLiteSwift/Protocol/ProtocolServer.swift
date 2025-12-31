// Created by Yanjunhui

import Foundation

// MARK: - Wire Protocol 处理器 / Wire Protocol Handler

/// Wire Protocol 处理器（协议分发核心，不包含网络层）
/// EN: Wire Protocol handler (protocol dispatch core, no networking glue).
public enum ProtocolServer {

    // MARK: - 消息处理 / Message Handling

    /// 处理完整的 Wire 消息并返回响应（目前仅支持 OP_MSG）
    /// EN: Handle a full wire message and return a wire reply (OP_MSG only for now).
    /// - Parameters:
    ///   - message: 接收到的 Wire 消息 / EN: Received wire message
    ///   - db: 数据库实例 / EN: Database instance
    /// - Returns: 响应消息，如果不需要响应则返回 nil / EN: Response message, or nil if no response needed
    public static func handle(message: WireMessage, db: Database) async throws -> WireMessage? {
        switch WireOpCode(rawValue: message.header.opCode) {
        case .msg:
            // 处理 OP_MSG 消息 / EN: Handle OP_MSG message
            do {
                let opmsg = try OpMsgParser.parse(body: message.body, fullMessage: message.bytes)
                guard var cmd = opmsg.body else {
                    let resp = structuredErrorResponse(MonoError.protocolError("no command document found"))
                    return try OpMsgParser.buildReply(requestId: message.header.requestId, responseDoc: resp)
                }

                // 将文档序列附加到命令中（与 Go 版本保持一致）
                // EN: Attach document sequences to command (like Go does)
                for seq in opmsg.sequences {
                    let arr = BSONArray(seq.documents.map { .document($0) })
                    cmd[seq.identifier] = .array(arr)
                }

                do {
                    let resp = try await db.runCommand(cmd)
                    return try OpMsgParser.buildReply(requestId: message.header.requestId, responseDoc: resp)
                } catch {
                    let resp = structuredErrorResponse(error)
                    return try OpMsgParser.buildReply(requestId: message.header.requestId, responseDoc: resp)
                }
            } catch let e as ProtocolParseError {
                // Go 兼容性：协议解析错误应返回结构化的 ProtocolError，而不是断开连接
                // EN: Go parity: protocol parse errors should return a structured ProtocolError, not drop the connection.
                let resp = structuredErrorResponse(e.monoError)
                return try OpMsgParser.buildReply(requestId: message.header.requestId, responseDoc: resp)
            } catch {
                let resp = structuredErrorResponse(error)
                return try OpMsgParser.buildReply(requestId: message.header.requestId, responseDoc: resp)
            }

        case .query:
            // 处理遗留 OP_QUERY（握手兼容）
            // EN: Handle legacy OP_QUERY (handshake compatibility)
            do {
                let q = try OpQueryParser.parse(body: message.body)
                // Go 兼容性：此处仅支持 *.$cmd（通常是 isMaster/hello）
                // EN: Go parity: only *.$cmd is supported here (typically isMaster/hello).
                if !q.fullCollectionName.hasSuffix(".$cmd") {
                    return try OpReplyBuilder.buildReply(requestId: message.header.requestId, documents: [
                        BSONDocument([
                            ("ok", .int32(0)),
                            ("errmsg", .string("OP_QUERY is deprecated, use OP_MSG")),
                        ]),
                    ])
                }

                // 仅支持 admin.$cmd / <db>.$cmd
                // EN: Only support admin.$cmd / <db>.$cmd
                let cmd = q.query
                do {
                    let resp = try await db.runCommand(cmd)
                    return try OpReplyBuilder.buildReply(requestId: message.header.requestId, documents: [resp])
                } catch {
                    let resp = structuredErrorResponse(error)
                    return try OpReplyBuilder.buildReply(requestId: message.header.requestId, documents: [resp])
                }
            } catch {
                // Go 兼容性：此处的任何错误仍应返回 OP_MSG 结构化错误响应
                // EN: Go parity: any errors here should still return an OP_MSG structured error response.
                let resp = structuredErrorResponse(error)
                return try OpMsgParser.buildReply(requestId: message.header.requestId, responseDoc: resp)
            }

        case .compressed:
            // Go 兼容性：不支持 OP_COMPRESSED，返回结构化协议错误响应
            // EN: Go parity: OP_COMPRESSED is not supported; return a structured protocol error response.
            let resp = structuredErrorResponse(MonoError.protocolError(
                "OP_COMPRESSED is not supported. Server does not support compression."
            ))
            return try OpMsgParser.buildReply(requestId: message.header.requestId, responseDoc: resp)

        case .reply, .none:
            // Go 兼容性：返回结构化错误而不是抛出/关闭连接
            // EN: Go parity: return structured error instead of throwing/closing.
            let resp = structuredErrorResponse(MonoError.protocolError("unsupported opcode: \(message.header.opCode)"))
            return try OpMsgParser.buildReply(requestId: message.header.requestId, responseDoc: resp)
        }
    }

    // MARK: - 错误响应 / Error Response

    /// 构建结构化错误响应文档
    /// EN: Build structured error response document.
    /// - Parameter error: 错误对象 / EN: Error object
    /// - Returns: BSON 错误响应文档 / EN: BSON error response document
    internal static func structuredErrorResponse(_ error: Error) -> BSONDocument {
        if let e = error as? MonoError {
            return BSONDocument([
                ("ok", .int32(0)),
                ("errmsg", .string(e.message)),
                ("code", .int32(Int32(e.codeValue))),
                ("codeName", .string(e.codeName)),
            ])
        }
        return BSONDocument([
            ("ok", .int32(0)),
            ("errmsg", .string("\(error)")),
            ("code", .int32(Int32(ErrorCode.internalError.rawValue))),
            ("codeName", .string("InternalError")),
        ])
    }
}
