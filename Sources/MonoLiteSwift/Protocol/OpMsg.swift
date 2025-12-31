// Created by Yanjunhui

import Foundation

// MARK: - OP_MSG 标志位 / OP_MSG Flags

/// OP_MSG 消息标志位
/// EN: OP_MSG message flags.
public struct OpMsgFlags: OptionSet, Sendable {
    public let rawValue: UInt32
    public init(rawValue: UInt32) { self.rawValue = rawValue }

    /// 消息包含校验和
    /// EN: Message contains checksum
    public static let checksumPresent = OpMsgFlags(rawValue: 0x0000_0001)
    /// 后续还有更多消息
    /// EN: More messages to come
    public static let moreToCome = OpMsgFlags(rawValue: 0x0000_0002)
    /// 允许耗尽游标
    /// EN: Exhaust cursor allowed
    public static let exhaustAllowed = OpMsgFlags(rawValue: 0x0001_0000)
}

// MARK: - OP_MSG Section 类型 / OP_MSG Section Kind

/// OP_MSG 消息 Section 类型
/// EN: OP_MSG message section kind.
public enum OpMsgSectionKind: UInt8, Sendable {
    /// 消息体（单个文档）
    /// EN: Body (single document)
    case body = 0
    /// 文档序列（多个文档）
    /// EN: Document sequence (multiple documents)
    case documentSequence = 1
}

// MARK: - OP_MSG 文档序列 / OP_MSG Document Sequence

/// OP_MSG 文档序列（Kind 1 Section）
/// EN: OP_MSG document sequence (Kind 1 Section).
public struct OpMsgDocumentSequence: Sendable {
    /// 序列标识符（对应命令中的字段名）
    /// EN: Sequence identifier (corresponds to field name in command)
    public var identifier: String
    /// 文档数组
    /// EN: Array of documents
    public var documents: [BSONDocument]
}

// MARK: - OP_MSG 消息 / OP_MSG Message

/// OP_MSG 消息结构
/// EN: OP_MSG message structure.
public struct OpMsgMessage: Sendable {
    /// 消息标志位
    /// EN: Message flags
    public var flags: UInt32
    /// 消息体（Kind 0 Section）
    /// EN: Message body (Kind 0 Section)
    public var body: BSONDocument?
    /// 文档序列数组（Kind 1 Sections）
    /// EN: Document sequences array (Kind 1 Sections)
    public var sequences: [OpMsgDocumentSequence]
    /// CRC32C 校验和（如果存在）
    /// EN: CRC32C checksum (if present)
    public var checksum: UInt32?
}

// MARK: - 协议解析错误 / Protocol Parse Error

/// 协议解析错误
/// EN: Protocol parse error.
public enum ProtocolParseError: Error, Sendable {
    /// 协议错误
    /// EN: Protocol error
    case protocolError(String)
}

extension ProtocolParseError {
    /// 转换为 MonoError
    /// EN: Convert to MonoError.
    var monoError: MonoError {
        switch self {
        case .protocolError(let msg):
            return MonoError.protocolError(msg)
        }
    }
}

// MARK: - 请求 ID 计数器 / Request ID Counter

/// 全局请求 ID 计数器（原子递增，与 Go 版本对齐）
/// EN: Global request ID counter (atomic increment, aligned with Go version).
private let _requestIdCounter = RequestIdCounter()

/// 请求 ID 计数器（线程安全）
/// EN: Request ID counter (thread-safe).
private final class RequestIdCounter: @unchecked Sendable {
    /// 当前计数值
    /// EN: Current counter value
    private var value: Int32 = 0
    /// 互斥锁
    /// EN: Mutex lock
    private let lock = NSLock()

    /// 获取下一个请求 ID
    /// EN: Get next request ID.
    func next() -> Int32 {
        lock.lock()
        defer { lock.unlock() }
        value &+= 1
        return value
    }
}

// MARK: - OP_MSG 解析器 / OP_MSG Parser

/// OP_MSG 消息解析器和构建器
/// EN: OP_MSG message parser and builder.
public enum OpMsgParser {
    /// 必需位掩码：bits 0-15
    /// EN: Required bits mask: bits 0-15
    private static let requiredBitsMask: UInt32 = 0xFFFF
    /// 已知的必需位
    /// EN: Known required bits
    private static let knownRequiredBits: UInt32 = OpMsgFlags.checksumPresent.rawValue | OpMsgFlags.moreToCome.rawValue

    /// 获取下一个请求 ID（线程安全）
    /// EN: Get next request ID (thread-safe).
    static func nextRequestId() -> Int32 {
        _requestIdCounter.next()
    }

    // MARK: - 消息解析 / Message Parsing

    /// 解析 OP_MSG 消息体（不含 16 字节 Wire 头部）
    /// EN: Parse OP_MSG body (without 16-byte wire header).
    /// - Parameters:
    ///   - body: 消息体数据 / EN: Message body data
    ///   - fullMessage: 完整消息（头部+消息体，包括校验和），用于 CRC32C 校验 / EN: Full message (header+body, including checksum), used for CRC32C verification
    /// - Returns: 解析后的 OP_MSG 消息 / EN: Parsed OP_MSG message
    public static func parse(body: Data, fullMessage: Data? = nil) throws -> OpMsgMessage {
        guard body.count >= 5 else {
            throw ProtocolParseError.protocolError("OP_MSG body too short: \(body.count)")
        }

        let flags = DataEndian.readUInt32LE(body, at: 0)

        // 必需位检查（Go 兼容性）
        // EN: Required bits check (Go parity)
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

            // 如果提供了完整消息，验证 CRC32C 校验和
            // EN: If full message is provided, verify CRC32C checksum
            if let full = fullMessage {
                guard full.count >= 20 else {
                    throw ProtocolParseError.protocolError("full message too short for checksum verification")
                }
                // 校验和覆盖完整消息（不含最后 4 字节）
                // EN: Checksum covers full message excluding last 4 bytes
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

        // 解析 Sections
        // EN: Parse Sections
        while pos < endPos {
            let kind = OpMsgSectionKind(rawValue: body[pos])
            pos += 1
            guard let kind else {
                throw ProtocolParseError.protocolError("unknown OP_MSG section kind")
            }

            switch kind {
            case .body:
                // Kind 0: 单个 BSON 文档
                // EN: Kind 0: Single BSON document
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
                // Kind 1: 文档序列
                // EN: Kind 1: Document sequence
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

                // 解析标识符 C 字符串
                // EN: Parse identifier cstring
                var identEnd = pos
                while identEnd < seqEnd, body[identEnd] != 0 { identEnd += 1 }
                guard identEnd < seqEnd else {
                    throw ProtocolParseError.protocolError("OP_MSG document sequence identifier not terminated")
                }
                let identifier = String(data: body[pos..<identEnd], encoding: .utf8) ?? ""
                pos = identEnd + 1

                // 解析序列中的文档
                // EN: Parse documents in sequence
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

                // 完整性检查：seqLen 包含自身
                // EN: Sanity check: seqLen includes itself
                if seqEnd - seqStart != seqLen { /* ignore */ }
                sequences.append(OpMsgDocumentSequence(identifier: identifier, documents: docs))
            }
        }

        return OpMsgMessage(flags: flags, body: bodyDoc, sequences: sequences, checksum: checksum)
    }

    // MARK: - 消息构建 / Message Building

    /// 构建 OP_MSG 回复消息（Kind 0 消息体，不含校验和）
    /// EN: Build OP_MSG reply (kind 0 body) without checksum.
    /// - Parameters:
    ///   - requestId: 原始请求 ID / EN: Original request ID
    ///   - responseDoc: 响应文档 / EN: Response document
    /// - Returns: Wire 消息 / EN: Wire message
    public static func buildReply(requestId: Int32, responseDoc: BSONDocument) throws -> WireMessage {
        let enc = BSONEncoder()
        let bson = try enc.encode(responseDoc)

        var body = Data(count: 4 + 1 + bson.count)
        // flags = 0（无标志）/ EN: flags = 0 (no flags)
        DataEndian.writeUInt32LE(0, to: &body, at: 0)
        // Kind 0 Section 标记 / EN: Kind 0 Section marker
        body[4] = OpMsgSectionKind.body.rawValue
        // BSON 文档 / EN: BSON document
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
