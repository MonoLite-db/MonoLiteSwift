// Created by Yanjunhui

import Foundation

public struct OpQueryMessage: Sendable {
    public var flags: Int32
    public var fullCollectionName: String
    public var numberToSkip: Int32
    public var numberToReturn: Int32
    public var query: BSONDocument
    public var returnFieldsSelector: BSONDocument?
}

public enum OpQueryParser {
    public static func parse(body: Data) throws -> OpQueryMessage {
        guard body.count >= 12 else {
            throw MonoError.protocolError("OP_QUERY body too short: \(body.count)")
        }

        let flags = Int32(bitPattern: DataEndian.readUInt32LE(body, at: 0))
        var pos = 4

        // fullCollectionName cstring
        guard let nameEnd = body[pos...].firstIndex(of: 0) else {
            throw MonoError.protocolError("OP_QUERY collection name not terminated")
        }
        let nameData = body[pos..<nameEnd]
        let fullCollectionName = String(data: nameData, encoding: .utf8) ?? ""
        pos = nameEnd + 1

        guard pos + 8 <= body.count else {
            throw MonoError.protocolError("OP_QUERY missing skip/return fields")
        }
        let numberToSkip = Int32(bitPattern: DataEndian.readUInt32LE(body, at: pos))
        pos += 4
        let numberToReturn = Int32(bitPattern: DataEndian.readUInt32LE(body, at: pos))
        pos += 4

        // query document
        guard pos + 4 <= body.count else {
            throw MonoError.protocolError("OP_QUERY missing query document")
        }
        let docLen = Int(DataEndian.readUInt32LE(body, at: pos))
        guard docLen >= 5, pos + docLen <= body.count else {
            throw MonoError.protocolError("OP_QUERY query document extends beyond message")
        }
        let queryDoc = try BSONDecoder().decode(Data(body[pos..<pos + docLen]))
        pos += docLen

        // optional returnFieldsSelector
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

public enum OpReplyBuilder {
    public static func buildReply(requestId: Int32, documents: [BSONDocument]) throws -> WireMessage {
        var docsBytes = Data()
        let enc = BSONEncoder()
        for d in documents {
            docsBytes.append(try enc.encode(d))
        }

        let bodyLen = 4 + 8 + 4 + 4 + docsBytes.count
        var body = Data(count: bodyLen)

        // responseFlags = 0
        DataEndian.writeUInt32LE(0, to: &body, at: 0)
        // cursorId = 0
        DataEndian.writeUInt64LE(0, to: &body, at: 4)
        // startingFrom = 0
        DataEndian.writeUInt32LE(0, to: &body, at: 12)
        // numberReturned
        DataEndian.writeUInt32LE(UInt32(documents.count), to: &body, at: 16)
        // documents
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


