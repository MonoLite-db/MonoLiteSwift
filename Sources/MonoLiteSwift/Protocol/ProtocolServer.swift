// Created by Yanjunhui

import Foundation

/// Wire Protocol handler (no networking glue yet; this is the dispatch core).
public enum ProtocolServer {
    /// Handle a full wire message and return a wire reply (OP_MSG only for now).
    public static func handle(message: WireMessage, db: Database) async throws -> WireMessage? {
        switch WireOpCode(rawValue: message.header.opCode) {
        case .msg:
            do {
                let opmsg = try OpMsgParser.parse(body: message.body, fullMessage: message.bytes)
                guard var cmd = opmsg.body else {
                    let resp = structuredErrorResponse(MonoError.protocolError("no command document found"))
                    return try OpMsgParser.buildReply(requestId: message.header.requestId, responseDoc: resp)
                }

                // Attach document sequences to command (like Go does)
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
                // Go parity: protocol parse errors should return a structured ProtocolError, not drop the connection.
                let resp = structuredErrorResponse(e.monoError)
                return try OpMsgParser.buildReply(requestId: message.header.requestId, responseDoc: resp)
            } catch {
                let resp = structuredErrorResponse(error)
                return try OpMsgParser.buildReply(requestId: message.header.requestId, responseDoc: resp)
            }

        case .query:
            // legacy OP_QUERY (handshake compatibility)
            do {
                let q = try OpQueryParser.parse(body: message.body)
                // Go parity: only *.$cmd is supported here (typically isMaster/hello).
                if !q.fullCollectionName.hasSuffix(".$cmd") {
                    return try OpReplyBuilder.buildReply(requestId: message.header.requestId, documents: [
                        BSONDocument([
                            ("ok", .int32(0)),
                            ("errmsg", .string("OP_QUERY is deprecated, use OP_MSG")),
                        ]),
                    ])
                }

                // 仅支持 admin.$cmd / <db>.$cmd
                let cmd = q.query
                do {
                    let resp = try await db.runCommand(cmd)
                    return try OpReplyBuilder.buildReply(requestId: message.header.requestId, documents: [resp])
                } catch {
                    let resp = structuredErrorResponse(error)
                    return try OpReplyBuilder.buildReply(requestId: message.header.requestId, documents: [resp])
                }
            } catch {
                // Go parity: any errors here should still return an OP_MSG structured error response.
                let resp = structuredErrorResponse(error)
                return try OpMsgParser.buildReply(requestId: message.header.requestId, responseDoc: resp)
            }

        case .compressed:
            // Go parity: OP_COMPRESSED is not supported; return a structured protocol error response.
            let resp = structuredErrorResponse(MonoError.protocolError(
                "OP_COMPRESSED is not supported. Server does not support compression."
            ))
            return try OpMsgParser.buildReply(requestId: message.header.requestId, responseDoc: resp)

        case .reply, .none:
            // Go parity: return structured error instead of throwing/closing.
            let resp = structuredErrorResponse(MonoError.protocolError("unsupported opcode: \(message.header.opCode)"))
            return try OpMsgParser.buildReply(requestId: message.header.requestId, responseDoc: resp)
        }
    }

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


