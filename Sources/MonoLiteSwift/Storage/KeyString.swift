// Created by Yanjunhui

import Foundation

/// KeyString 类型标识符
/// MongoDB 兼容的排序优先级
/// EN: KeyString type identifier.
/// MongoDB-compatible sorting priority.
public enum KeyStringType: UInt8, Sendable {
    case minKey = 0x00
    case null = 0x05
    case number = 0x10      // Double/Int
    case bigInt = 0x11      // 超精度 Int64 / EN: Extended precision Int64
    case string = 0x14
    case object = 0x18
    case array = 0x1C
    case binData = 0x20
    case objectId = 0x24
    case bool = 0x28
    case date = 0x2C
    case timestamp = 0x30
    case regex = 0x34
    case maxKey = 0xFF

    /// 字段分隔符
    /// EN: Field separator.
    static let end: UInt8 = 0x04
}

/// KeyString 构建器
/// 用于将 BSON 值编码为可比较的字节序列
/// EN: KeyString builder.
/// Used to encode BSON values into comparable byte sequences.
public struct KeyStringBuilder {
    private var buffer: Data

    /// 创建空的构建器
    /// EN: Create an empty builder.
    public init() {
        self.buffer = Data()
    }

    /// 重置构建器
    /// EN: Reset the builder.
    public mutating func reset() {
        buffer.removeAll(keepingCapacity: true)
    }

    /// 获取构建的字节数据
    /// EN: Get the built byte data.
    public func bytes() -> Data {
        buffer
    }

    /// 追加 BSON 值
    /// - Parameters:
    ///   - value: 要编码的值
    ///   - direction: 排序方向（1=升序，-1=降序）
    /// EN: Append BSON value.
    /// - Parameters:
    ///   - value: The value to encode.
    ///   - direction: Sort direction (1=ascending, -1=descending).
    public mutating func appendValue(_ value: BSONValue, direction: Int = 1) {
        // 以 Go 参考实现为准：每个 field 单独编码成 valueBuf，然后按 direction 决定是否对该字段做逐字节取反
        // EN: Aligned with Go reference implementation: encode each field into valueBuf separately, then decide whether to invert bytes based on direction
        var valueBuf = Data()
        encodeValue(into: &valueBuf, value)

        if direction >= 0 {
            buffer.append(valueBuf)
            buffer.append(KeyStringType.end)
        } else {
            buffer.append(contentsOf: valueBuf.map { ~$0 })
            buffer.append(~KeyStringType.end)
        }
    }

    // MARK: - 编码方法 / Encoding Methods

    private func encodeValue(into out: inout Data, _ value: BSONValue) {
        switch value {
        case .minKey:
            out.append(KeyStringType.minKey.rawValue)

        case .maxKey:
            out.append(KeyStringType.maxKey.rawValue)

        case .null:
            out.append(KeyStringType.null.rawValue)

        case .int32(let v):
            encodeNumber(into: &out, Double(v))

        case .int64(let v):
            // 检查是否超出 Double 精度范围
            // EN: Check if it exceeds Double precision range
            let maxSafeInt: Int64 = 1 << 53
            if v >= -maxSafeInt && v <= maxSafeInt {
                encodeNumber(into: &out, Double(v))
            } else {
                encodeBigInt(into: &out, v)
            }

        case .double(let v):
            encodeNumber(into: &out, v)

        case .decimal128(let v):
            // 简化处理：转换为 Double
            // EN: Simplified handling: convert to Double
            encodeNumber(into: &out, v.doubleValue)

        case .string(let v):
            encodeTypedString(into: &out, v)

        case .symbol(let v):
            encodeTypedString(into: &out, v.value)

        case .objectId(let v):
            out.append(KeyStringType.objectId.rawValue)
            out.append(v.bytes)

        case .bool(let v):
            // Go 参考实现：true=0x02, false=0x01
            // EN: Go reference implementation: true=0x02, false=0x01
            out.append(KeyStringType.bool.rawValue)
            out.append(v ? 0x02 : 0x01)

        case .dateTime(let v):
            out.append(KeyStringType.date.rawValue)
            let millis = Int64(v.timeIntervalSince1970 * 1000)
            encodeInt64(into: &out, millis)

        case .timestamp(let v):
            out.append(KeyStringType.timestamp.rawValue)
            encodeUInt64(into: &out, v.value)

        case .binary(let v):
            encodeBinary(into: &out, v)

        case .regex(let v):
            encodeRegex(into: &out, v)

        case .document(let v):
            encodeDocument(into: &out, v)

        case .array(let v):
            encodeArray(into: &out, v)

        case .javascript(let v):
            // Go 参考实现没有显式区分 JS，降级为 string
            // EN: Go reference implementation doesn't explicitly distinguish JS, fallback to string
            encodeTypedString(into: &out, v.code)

        case .javascriptWithScope(let v):
            // Go 参考实现没有显式区分 JSWithScope，降级为 string + object
            // EN: Go reference implementation doesn't explicitly distinguish JSWithScope, fallback to string + object
            encodeTypedString(into: &out, v.code)
            encodeDocument(into: &out, v.scope)

        case .undefined, .dbPointer:
            // 不常用类型，简单处理
            // EN: Uncommon types, simplified handling
            out.append(KeyStringType.null.rawValue)
        }
    }

    /// 编码 Double（保持字节序可比较）
    /// EN: Encode Double (maintain byte order comparability).
    private func encodeNumber(into out: inout Data, _ value: Double) {
        out.append(KeyStringType.number.rawValue)

        // Go 参考实现：NaN 使用固定 payload（math.NaN()）
        // EN: Go reference implementation: NaN uses fixed payload (math.NaN())
        var bits: UInt64
        if value.isNaN {
            bits = 0x7ff8000000000001
        } else {
            bits = value.bitPattern
        }

        // 处理符号位使负数正确排序
        // EN: Handle sign bit for correct negative number sorting
        if (bits & (1 << 63)) != 0 {
            // 负数：全部位反转
            // EN: Negative: invert all bits
            bits = ~bits
        } else {
            // 正数或零：仅翻转符号位
            // EN: Positive or zero: only flip sign bit
            bits ^= (1 << 63)
        }

        // 大端序写入（保持可比较性）
        // EN: Write in big-endian (maintain comparability)
        for i in (0..<8).reversed() {
            out.append(UInt8((bits >> (i * 8)) & 0xFF))
        }
    }

    /// 编码超精度 Int64
    /// EN: Encode extended precision Int64.
    private func encodeBigInt(into out: inout Data, _ value: Int64) {
        out.append(KeyStringType.bigInt.rawValue)

        var encoded = UInt64(bitPattern: value)
        // 翻转符号位使负数正确排序
        // EN: Flip sign bit for correct negative number sorting
        encoded ^= (1 << 63)

        // 大端序写入
        // EN: Write in big-endian
        for i in (0..<8).reversed() {
            out.append(UInt8((encoded >> (i * 8)) & 0xFF))
        }
    }

    /// 编码 Int64（大端序）
    /// EN: Encode Int64 (big-endian).
    private func encodeInt64(into out: inout Data, _ value: Int64) {
        var encoded = UInt64(bitPattern: value)
        encoded ^= (1 << 63)
        for i in (0..<8).reversed() {
            out.append(UInt8((encoded >> (i * 8)) & 0xFF))
        }
    }

    /// 编码 UInt64（大端序）
    /// EN: Encode UInt64 (big-endian).
    private func encodeUInt64(into out: inout Data, _ value: UInt64) {
        for i in (0..<8).reversed() {
            out.append(UInt8((value >> (i * 8)) & 0xFF))
        }
    }

    /// Go 参考实现的"字符串内容"编码（无类型前缀）：转义 0x00 和 0xFF，并以 0x00 0x00 结束
    /// EN: Go reference implementation's "string content" encoding (no type prefix): escape 0x00 and 0xFF, end with 0x00 0x00.
    private func encodeStringBody(into out: inout Data, _ value: String) {
        for byte in value.utf8 {
            if byte == 0x00 {
                // 转义 null
                // EN: Escape null
                out.append(0x00)
                out.append(0xFF)
            } else if byte == 0xFF {
                // 转义 0xFF
                // EN: Escape 0xFF
                out.append(0xFF)
                out.append(0x00)
            } else {
                out.append(byte)
            }
        }

        // 结束标记
        // EN: End marker
        out.append(0x00)
        out.append(0x00)
    }

    /// 编码"类型为 string 的值"
    /// EN: Encode value with type "string".
    private func encodeTypedString(into out: inout Data, _ value: String) {
        out.append(KeyStringType.string.rawValue)
        encodeStringBody(into: &out, value)
    }

    /// 编码二进制数据（Go 参考实现：不包含 subtype；仅 length + data）
    /// EN: Encode binary data (Go reference implementation: no subtype; only length + data).
    private func encodeBinary(into out: inout Data, _ value: BSONBinary) {
        out.append(KeyStringType.binData.rawValue)

        // 长度（大端序 4 字节）
        // EN: Length (big-endian 4 bytes)
        let length = UInt32(value.data.count)
        for i in (0..<4).reversed() {
            out.append(UInt8((length >> (i * 8)) & 0xFF))
        }

        out.append(value.data)
    }

    /// 编码正则表达式
    /// EN: Encode regular expression.
    private func encodeRegex(into out: inout Data, _ value: BSONRegex) {
        out.append(KeyStringType.regex.rawValue)
        encodeStringBody(into: &out, value.pattern)
        encodeStringBody(into: &out, value.options)
    }

    /// 编码文档
    /// EN: Encode document.
    private func encodeDocument(into out: inout Data, _ doc: BSONDocument) {
        out.append(KeyStringType.object.rawValue)

        for (key, value) in doc {
            // Go 参考实现：字段名用 encodeStringBody（双 0 结束 + 转义）
            // EN: Go reference implementation: field name uses encodeStringBody (double 0 ending + escape)
            encodeStringBody(into: &out, key)
            encodeValue(into: &out, value)
        }

        out.append(0x00)  // 对象结束标记 / EN: Object end marker
    }

    /// 编码数组
    /// EN: Encode array.
    private func encodeArray(into out: inout Data, _ arr: BSONArray) {
        out.append(KeyStringType.array.rawValue)

        for value in arr {
            encodeValue(into: &out, value)
        }

        out.append(0x00)  // 数组结束标记 / EN: Array end marker
    }
}

// MARK: - 索引键编码 / Index Key Encoding

/// 编码索引键
/// - Parameters:
///   - keys: 索引键定义，如 ["name": 1, "age": -1]
///   - doc: 文档
/// - Returns: 编码后的键数据
/// EN: Encode index key.
/// - Parameters:
///   - keys: Index key definition, e.g. ["name": 1, "age": -1]
///   - doc: Document
/// - Returns: Encoded key data.
public func encodeIndexKey(keys: BSONDocument, doc: BSONDocument) -> Data {
    var builder = KeyStringBuilder()

    for (fieldName, directionValue) in keys {
        // 获取字段值
        // EN: Get field value
        let value = doc.getValue(forPath: fieldName) ?? .null

        // 获取排序方向
        // EN: Get sort direction
        let direction = directionValue.intValue ?? 1

        builder.appendValue(value, direction: direction)
    }

    return builder.bytes()
}

/// 记录标识符
/// EN: Record identifier.
public struct RecordID: Hashable, Sendable {
    /// 页面 ID
    /// EN: Page ID.
    public let pageId: PageID

    /// 槽索引
    /// EN: Slot index.
    public let slotIndex: UInt16

    /// 序列化大小
    /// EN: Serialization size.
    public static let size = 6

    public init(pageId: PageID, slotIndex: UInt16) {
        self.pageId = pageId
        self.slotIndex = slotIndex
    }

    /// 序列化
    /// EN: Serialize.
    public func marshal() -> Data {
        var data = Data(count: Self.size)
        DataEndian.writeUInt32LE(pageId, to: &data, at: 0)
        DataEndian.writeUInt16LE(slotIndex, to: &data, at: 4)
        return data
    }

    /// 反序列化
    /// EN: Deserialize.
    public static func unmarshal(_ data: Data) -> RecordID {
        let pageId = DataEndian.readUInt32LE(data, at: 0)
        let slotIndex = DataEndian.readUInt16LE(data, at: 4)
        return RecordID(pageId: pageId, slotIndex: slotIndex)
    }
}
