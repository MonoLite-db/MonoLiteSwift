// Created by Yanjunhui

import Foundation

/// BSON 编码器
/// 将 BSONDocument 序列化为 BSON 二进制格式
public struct BSONEncoder {
    public init() {}

    /// 编码文档为 BSON 数据
    public func encode(_ document: BSONDocument) throws -> Data {
        var buffer = Data()

        // 预留 4 字节用于文档长度
        buffer.append(contentsOf: [0, 0, 0, 0])

        // 编码每个元素
        for (key, value) in document {
            try encodeElement(key: key, value: value, to: &buffer)
        }

        // 添加结束符
        buffer.append(0x00)

        // 写入文档长度（小端序）
        let length = UInt32(buffer.count)
        buffer.replaceSubrange(0..<4, with: withUnsafeBytes(of: length.littleEndian) { Data($0) })

        return buffer
    }

    /// 编码单个元素
    private func encodeElement(key: String, value: BSONValue, to buffer: inout Data) throws {
        // 写入类型标识
        buffer.append(value.typeCode.rawValue)

        // 写入键名（C 字符串）
        encodeCString(key, to: &buffer)

        // 写入值
        try encodeValue(value, to: &buffer)
    }

    /// 编码值
    private func encodeValue(_ value: BSONValue, to buffer: inout Data) throws {
        switch value {
        case .double(let v):
            encodeDouble(v, to: &buffer)

        case .string(let v):
            encodeString(v, to: &buffer)

        case .document(let v):
            let data = try encode(v)
            buffer.append(data)

        case .array(let v):
            let doc = v.toDocument()
            let data = try encode(doc)
            buffer.append(data)

        case .binary(let v):
            encodeBinary(v, to: &buffer)

        case .undefined:
            // undefined 没有值数据
            break

        case .objectId(let v):
            buffer.append(v.bytes)

        case .bool(let v):
            buffer.append(v ? 0x01 : 0x00)

        case .dateTime(let v):
            let millis = Int64(v.timeIntervalSince1970 * 1000)
            encodeInt64(millis, to: &buffer)

        case .null:
            // null 没有值数据
            break

        case .regex(let v):
            encodeCString(v.pattern, to: &buffer)
            encodeCString(v.options, to: &buffer)

        case .dbPointer(let v):
            encodeString(v.ref, to: &buffer)
            buffer.append(v.id.bytes)

        case .javascript(let v):
            encodeString(v.code, to: &buffer)

        case .symbol(let v):
            encodeString(v.value, to: &buffer)

        case .javascriptWithScope(let v):
            // 需要先编码整个结构的长度
            var innerBuffer = Data()

            // 编码 JavaScript 代码
            encodeString(v.code, to: &innerBuffer)

            // 编码 scope 文档
            let scopeData = try encode(v.scope)
            innerBuffer.append(scopeData)

            // 写入总长度（4字节）+ 内容
            let totalLength = UInt32(4 + innerBuffer.count)
            buffer.append(contentsOf: withUnsafeBytes(of: totalLength.littleEndian) { Data($0) })
            buffer.append(innerBuffer)

        case .int32(let v):
            encodeInt32(v, to: &buffer)

        case .timestamp(let v):
            // 先写 i，再写 t（小端序）
            encodeUInt32(v.i, to: &buffer)
            encodeUInt32(v.t, to: &buffer)

        case .int64(let v):
            encodeInt64(v, to: &buffer)

        case .decimal128(let v):
            // 先写 low，再写 high（小端序）
            encodeUInt64(v.low, to: &buffer)
            encodeUInt64(v.high, to: &buffer)

        case .minKey, .maxKey:
            // 没有值数据
            break
        }
    }

    // MARK: - 辅助方法

    /// 编码 C 字符串（以 null 结尾）
    private func encodeCString(_ string: String, to buffer: inout Data) {
        buffer.append(contentsOf: string.utf8)
        buffer.append(0x00)
    }

    /// 编码 BSON 字符串（长度前缀）
    private func encodeString(_ string: String, to buffer: inout Data) {
        let utf8 = Data(string.utf8)
        let length = UInt32(utf8.count + 1)  // +1 for null terminator
        buffer.append(contentsOf: withUnsafeBytes(of: length.littleEndian) { Data($0) })
        buffer.append(utf8)
        buffer.append(0x00)
    }

    /// 编码 Double
    private func encodeDouble(_ value: Double, to buffer: inout Data) {
        buffer.append(contentsOf: withUnsafeBytes(of: value.bitPattern.littleEndian) { Data($0) })
    }

    /// 编码 Int32
    private func encodeInt32(_ value: Int32, to buffer: inout Data) {
        buffer.append(contentsOf: withUnsafeBytes(of: value.littleEndian) { Data($0) })
    }

    /// 编码 UInt32
    private func encodeUInt32(_ value: UInt32, to buffer: inout Data) {
        buffer.append(contentsOf: withUnsafeBytes(of: value.littleEndian) { Data($0) })
    }

    /// 编码 Int64
    private func encodeInt64(_ value: Int64, to buffer: inout Data) {
        buffer.append(contentsOf: withUnsafeBytes(of: value.littleEndian) { Data($0) })
    }

    /// 编码 UInt64
    private func encodeUInt64(_ value: UInt64, to buffer: inout Data) {
        buffer.append(contentsOf: withUnsafeBytes(of: value.littleEndian) { Data($0) })
    }

    /// 编码二进制数据
    private func encodeBinary(_ value: BSONBinary, to buffer: inout Data) {
        let length = UInt32(value.data.count)
        encodeInt32(Int32(length), to: &buffer)
        buffer.append(value.subtype.rawValue)
        buffer.append(value.data)
    }
}

// MARK: - 便捷方法

extension BSONDocument {
    /// 序列化为 BSON 数据
    public func toBSON() throws -> Data {
        try BSONEncoder().encode(self)
    }
}

extension BSONArray {
    /// 序列化为 BSON 数据（作为文档，索引为键）
    public func toBSON() throws -> Data {
        try BSONEncoder().encode(toDocument())
    }
}
