// Created by Yanjunhui

import Foundation

/// BSON 解码器
/// EN: BSON decoder.
/// 将 BSON 二进制数据解析为 BSONDocument
/// EN: Parses BSON binary data into BSONDocument.
public struct BSONDecoder {
    public init() {}

    /// 解码 BSON 数据为文档
    /// EN: Decodes BSON data to document.
    public func decode(_ data: Data) throws -> BSONDocument {
        var reader = BSONReader(data: data)
        return try reader.readDocument()
    }

    /// 解码 BSON 数据为数组
    /// EN: Decodes BSON data to array.
    public func decodeArray(_ data: Data) throws -> BSONArray {
        let doc = try decode(data)
        // 将文档转换为数组（按索引顺序）
        // EN: Convert document to array (sorted by index)
        let sortedElements = doc.sorted { a, b in
            (Int(a.key) ?? 0) < (Int(b.key) ?? 0)
        }
        return BSONArray(sortedElements.map { $0.value })
    }
}

/// BSON 数据读取器
/// EN: BSON data reader.
private struct BSONReader {
    private var data: Data
    private var offset: Int = 0

    init(data: Data) {
        self.data = data
    }

    var bytesRemaining: Int {
        data.count - offset
    }

    // MARK: - 读取文档 / Read Document

    mutating func readDocument() throws -> BSONDocument {
        let startOffset = offset

        // 读取文档长度
        // EN: Read document length
        let length = try readInt32()

        guard length >= 5 else {
            throw BSONError.invalidDocument("Document too short: \(length) bytes")
        }

        guard startOffset + Int(length) <= data.count else {
            throw BSONError.unexpectedEndOfData
        }

        var document = BSONDocument()

        // 读取元素直到遇到结束符
        // EN: Read elements until terminator
        while offset < startOffset + Int(length) - 1 {
            let typeCode = try readByte()

            // 检查结束符
            // EN: Check for terminator
            if typeCode == 0x00 {
                break
            }

            let key = try readCString()
            let value = try readValue(typeCode: typeCode)

            document.append(key: key, value: value)
        }

        // 验证结束符
        // EN: Verify terminator
        let terminator = try readByte()
        if terminator != 0x00 {
            throw BSONError.invalidDocument("Missing document terminator")
        }

        return document
    }

    // MARK: - 读取值 / Read Value

    mutating func readValue(typeCode: UInt8) throws -> BSONValue {
        guard let type = BSONTypeCode(rawValue: typeCode) else {
            throw BSONError.invalidType(typeCode)
        }

        switch type {
        case .double:
            return .double(try readDouble())

        case .string:
            return .string(try readString())

        case .document:
            return .document(try readDocument())

        case .array:
            let doc = try readDocument()
            // 转换为数组
            // EN: Convert to array
            let sortedElements = doc.sorted { a, b in
                (Int(a.key) ?? 0) < (Int(b.key) ?? 0)
            }
            return .array(BSONArray(sortedElements.map { $0.value }))

        case .binary:
            return .binary(try readBinary())

        case .undefined:
            return .undefined(BSONUndefined.shared)

        case .objectId:
            return .objectId(try readObjectId())

        case .bool:
            let byte = try readByte()
            return .bool(byte != 0x00)

        case .dateTime:
            let millis = try readInt64()
            return .dateTime(Date(timeIntervalSince1970: Double(millis) / 1000.0))

        case .null:
            return .null

        case .regex:
            let pattern = try readCString()
            let options = try readCString()
            return .regex(BSONRegex(pattern: pattern, options: options))

        case .dbPointer:
            let ref = try readString()
            let id = try readObjectId()
            return .dbPointer(BSONDBPointer(ref: ref, id: id))

        case .javascript:
            let code = try readString()
            return .javascript(BSONJavaScript(code: code))

        case .symbol:
            let value = try readString()
            return .symbol(BSONSymbol(value))

        case .javascriptWithScope:
            // 读取总长度
            // EN: Read total length
            _ = try readInt32()
            let code = try readString()
            let scope = try readDocument()
            return .javascriptWithScope(BSONJavaScriptWithScope(code: code, scope: scope))

        case .int32:
            return .int32(try readInt32())

        case .timestamp:
            let i = try readUInt32()
            let t = try readUInt32()
            return .timestamp(BSONTimestamp(t: t, i: i))

        case .int64:
            return .int64(try readInt64())

        case .decimal128:
            let low = try readUInt64()
            let high = try readUInt64()
            return .decimal128(Decimal128(high: high, low: low))

        case .minKey:
            return .minKey

        case .maxKey:
            return .maxKey
        }
    }

    // MARK: - 基础读取方法 / Basic Read Methods

    mutating func readByte() throws -> UInt8 {
        guard offset < data.count else {
            throw BSONError.unexpectedEndOfData
        }
        let byte = data[offset]
        offset += 1
        return byte
    }

    mutating func readBytes(_ count: Int) throws -> Data {
        guard offset + count <= data.count else {
            throw BSONError.unexpectedEndOfData
        }
        let bytes = data[offset..<offset + count]
        offset += count
        return Data(bytes)
    }

    mutating func readCString() throws -> String {
        guard let nullIndex = data[offset...].firstIndex(of: 0x00) else {
            throw BSONError.invalidDocument("Unterminated C string")
        }

        let stringData = data[offset..<nullIndex]
        offset = nullIndex + 1

        guard let string = String(data: Data(stringData), encoding: .utf8) else {
            throw BSONError.invalidUtf8
        }

        return string
    }

    mutating func readString() throws -> String {
        let length = try readInt32()

        guard length >= 1 else {
            throw BSONError.invalidDocument("String length too short: \(length)")
        }

        let stringData = try readBytes(Int(length) - 1)

        // 读取并验证 null 终止符
        // EN: Read and verify null terminator
        let terminator = try readByte()
        if terminator != 0x00 {
            throw BSONError.invalidDocument("Missing string terminator")
        }

        guard let string = String(data: stringData, encoding: .utf8) else {
            throw BSONError.invalidUtf8
        }

        return string
    }

    mutating func readInt32() throws -> Int32 {
        let bytes = try readBytes(4)
        return bytes.withUnsafeBytes { ptr in
            Int32(littleEndian: ptr.load(as: Int32.self))
        }
    }

    mutating func readUInt32() throws -> UInt32 {
        let bytes = try readBytes(4)
        return bytes.withUnsafeBytes { ptr in
            UInt32(littleEndian: ptr.load(as: UInt32.self))
        }
    }

    mutating func readInt64() throws -> Int64 {
        let bytes = try readBytes(8)
        return bytes.withUnsafeBytes { ptr in
            Int64(littleEndian: ptr.load(as: Int64.self))
        }
    }

    mutating func readUInt64() throws -> UInt64 {
        let bytes = try readBytes(8)
        return bytes.withUnsafeBytes { ptr in
            UInt64(littleEndian: ptr.load(as: UInt64.self))
        }
    }

    mutating func readDouble() throws -> Double {
        let bytes = try readBytes(8)
        let bits = bytes.withUnsafeBytes { ptr in
            UInt64(littleEndian: ptr.load(as: UInt64.self))
        }
        return Double(bitPattern: bits)
    }

    mutating func readObjectId() throws -> ObjectID {
        let bytes = try readBytes(12)
        return ObjectID(bytes: bytes)
    }

    mutating func readBinary() throws -> BSONBinary {
        let length = try readInt32()
        let subtypeByte = try readByte()
        let subtype = BSONBinary.Subtype(rawValue: subtypeByte) ?? .generic
        let binaryData = try readBytes(Int(length))
        return BSONBinary(subtype: subtype, data: binaryData)
    }
}

// MARK: - 便捷方法 / Convenience Methods

extension BSONDocument {
    /// 从 BSON 数据解析
    /// EN: Parses from BSON data.
    public static func fromBSON(_ data: Data) throws -> BSONDocument {
        try BSONDecoder().decode(data)
    }
}

extension BSONArray {
    /// 从 BSON 数据解析
    /// EN: Parses from BSON data.
    public static func fromBSON(_ data: Data) throws -> BSONArray {
        try BSONDecoder().decodeArray(data)
    }
}
