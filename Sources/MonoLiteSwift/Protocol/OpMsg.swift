// Created by Yanjunhui

import Foundation

public struct OpMsgFlags: OptionSet, Sendable {
    public let rawValue: UInt32
    public init(rawValue: UInt32) { self.rawValue = rawValue }

    public static let checksumPresent = OpMsgFlags(rawValue: 0x0000_0001)
    public static let moreToCome = OpMsgFlags(rawValue: 0x0000_0002)
    public static let exhaustAllowed = OpMsgFlags(rawValue: 0x0001_0000)
}

public enum OpMsgSectionKind: UInt8, Sendable {
    case body = 0
    case documentSequence = 1
}

public struct OpMsgDocumentSequence: Sendable {
    public var identifier: String
    public var documents: [BSONDocument]
}

public struct OpMsgMessage: Sendable {
    public var flags: UInt32
    public var body: BSONDocument?
    public var sequences: [OpMsgDocumentSequence]
    public var checksum: UInt32?
}

public enum ProtocolParseError: Error, Sendable {
    case protocolError(String)
}

extension ProtocolParseError {
    var monoError: MonoError {
        switch self {
        case .protocolError(let msg):
            return MonoError.protocolError(msg)
        }
    }
}

/// 全局请求 ID 计数器（原子递增，与 Go 版本对齐）
private let _requestIdCounter = RequestIdCounter()

private final class RequestIdCounter: @unchecked Sendable {
    private var value: Int32 = 0
    private let lock = NSLock()

    func next() -> Int32 {
        lock.lock()
        defer { lock.unlock() }
        value &+= 1
        return value
    }
}

public enum OpMsgParser {
    // required bits mask: bits 0-15
    private static let requiredBitsMask: UInt32 = 0xFFFF
    private static let knownRequiredBits: UInt32 = OpMsgFlags.checksumPresent.rawValue | OpMsgFlags.moreToCome.rawValue

    /// 获取下一个请求 ID（线程安全）
    static func nextRequestId() -> Int32 {
        _requestIdCounter.next()
    }

    /// Parse OP_MSG body (without 16-byte wire header).
    /// If `fullMessage` is provided (header+body, including checksum), it is used to verify CRC32C when checksumPresent flag is set.
    public static func parse(body: Data, fullMessage: Data? = nil) throws -> OpMsgMessage {
        guard body.count >= 5 else {
            throw ProtocolParseError.protocolError("OP_MSG body too short: \(body.count)")
        }

        let flags = DataEndian.readUInt32LE(body, at: 0)

        // required bits check (Go parity)
        let requiredBits = flags & requiredBitsMask
        let unknownRequiredBits = requiredBits & ~knownRequiredBits
        if unknownRequiredBits != 0 {
            throw ProtocolParseError.protocolError("OP_MSG contains unknown required flag bits: 0x\(String(unknownRequiredBits, radix: 16))")
        }

        var endPos = body.count
        var checksum: UInt32? = nil
        let hasChecksum = (flags & OpMsgFlags.checksumPresent.rawValue) != 0
        if hasChecksum {
            guard body.count >= 9 else {
                throw ProtocolParseError.protocolError("OP_MSG with checksum too short")
            }
            checksum = DataEndian.readUInt32LE(body, at: body.count - 4)
            endPos -= 4

            if let full = fullMessage {
                guard full.count >= 20 else {
                    throw ProtocolParseError.protocolError("full message too short for checksum verification")
                }
                // checksum covers full message excluding last 4 bytes
                let expected = DataEndian.readUInt32LE(full, at: full.count - 4)
                let actual = CRC32C.checksum(full.prefix(full.count - 4))
                if actual != expected {
                    throw ProtocolParseError.protocolError("OP_MSG checksum verification failed")
                }
            }
        }

        var pos = 4
        var bodyDoc: BSONDocument? = nil
        var sequences: [OpMsgDocumentSequence] = []
        let decoder = BSONDecoder()

        while pos < endPos {
            let kind = OpMsgSectionKind(rawValue: body[pos])
            pos += 1
            guard let kind else {
                throw ProtocolParseError.protocolError("unknown OP_MSG section kind")
            }

            switch kind {
            case .body:
                guard pos + 4 <= endPos else {
                    throw ProtocolParseError.protocolError("OP_MSG body section too short")
                }
                let docLen = Int(DataEndian.readUInt32LE(body, at: pos))
                guard docLen >= 5, pos + docLen <= endPos else {
                    throw ProtocolParseError.protocolError("OP_MSG document extends beyond message")
                }
                let docBytes = Data(body[pos..<pos + docLen])
                let doc = try decoder.decode(docBytes)
                bodyDoc = doc
                pos += docLen

            case .documentSequence:
                guard pos + 4 <= endPos else {
                    throw ProtocolParseError.protocolError("OP_MSG document sequence too short")
                }
                let seqLen = Int(DataEndian.readUInt32LE(body, at: pos))
                guard seqLen >= 4, pos + seqLen <= endPos else {
                    throw ProtocolParseError.protocolError("OP_MSG document sequence extends beyond message")
                }

                let seqStart = pos
                let seqEnd = pos + seqLen
                pos += 4

                // identifier cstring
                var identEnd = pos
                while identEnd < seqEnd, body[identEnd] != 0 { identEnd += 1 }
                guard identEnd < seqEnd else {
                    throw ProtocolParseError.protocolError("OP_MSG document sequence identifier not terminated")
                }
                let identifier = String(data: body[pos..<identEnd], encoding: .utf8) ?? ""
                pos = identEnd + 1

                var docs: [BSONDocument] = []
                while pos < seqEnd {
                    guard pos + 4 <= seqEnd else {
                        throw ProtocolParseError.protocolError("OP_MSG document sequence truncated: need 4 bytes for doc length")
                    }
                    let docLen = Int(DataEndian.readUInt32LE(body, at: pos))
                    guard docLen >= 5, pos + docLen <= seqEnd else {
                        throw ProtocolParseError.protocolError("OP_MSG document extends beyond sequence boundary")
                    }
                    let docBytes = Data(body[pos..<pos + docLen])
                    docs.append(try decoder.decode(docBytes))
                    pos += docLen
                }
                if pos != seqEnd {
                    throw ProtocolParseError.protocolError("OP_MSG document sequence did not consume all bytes")
                }

                // sanity: seqLen includes itself
                if seqEnd - seqStart != seqLen { /* ignore */ }
                sequences.append(OpMsgDocumentSequence(identifier: identifier, documents: docs))
            }
        }

        return OpMsgMessage(flags: flags, body: bodyDoc, sequences: sequences, checksum: checksum)
    }

    /// Build OP_MSG reply (kind 0 body) without checksum.
    public static func buildReply(requestId: Int32, responseDoc: BSONDocument) throws -> WireMessage {
        let enc = BSONEncoder()
        let bson = try enc.encode(responseDoc)

        var body = Data(count: 4 + 1 + bson.count)
        // flags = 0
        DataEndian.writeUInt32LE(0, to: &body, at: 0)
        body[4] = OpMsgSectionKind.body.rawValue
        body.replaceSubrange(5..<5 + bson.count, with: bson)

        let header = WireMessageHeader(
            messageLength: Int32(16 + body.count),
            requestId: nextRequestId(),
            responseTo: requestId,
            opCode: WireOpCode.msg.rawValue
        )
        return WireMessage(header: header, body: body)
    }
}


