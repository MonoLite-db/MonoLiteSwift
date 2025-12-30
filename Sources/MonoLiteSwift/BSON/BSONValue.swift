// Created by Yanjunhui

import Foundation

/// BSON 类型标识符（用于序列化）
public enum BSONTypeCode: UInt8, Sendable {
    case double = 0x01
    case string = 0x02
    case document = 0x03
    case array = 0x04
    case binary = 0x05
    case undefined = 0x06  // 已废弃
    case objectId = 0x07
    case bool = 0x08
    case dateTime = 0x09
    case null = 0x0A
    case regex = 0x0B
    case dbPointer = 0x0C  // 已废弃
    case javascript = 0x0D
    case symbol = 0x0E     // 已废弃
    case javascriptWithScope = 0x0F  // 已废弃
    case int32 = 0x10
    case timestamp = 0x11
    case int64 = 0x12
    case decimal128 = 0x13
    case minKey = 0xFF
    case maxKey = 0x7F
}

/// BSON 值类型
/// 遵循 MongoDB BSON 类型系统
public enum BSONValue: Sendable, CustomStringConvertible {
    case double(Double)
    case string(String)
    case document(BSONDocument)
    case array(BSONArray)
    case binary(BSONBinary)
    case undefined(BSONUndefined)
    case objectId(ObjectID)
    case bool(Bool)
    case dateTime(Date)
    case null
    case regex(BSONRegex)
    case dbPointer(BSONDBPointer)
    case javascript(BSONJavaScript)
    case symbol(BSONSymbol)
    case javascriptWithScope(BSONJavaScriptWithScope)
    case int32(Int32)
    case timestamp(BSONTimestamp)
    case int64(Int64)
    case decimal128(Decimal128)
    case minKey
    case maxKey

    // MARK: - 类型代码

    /// 获取 BSON 类型代码
    public var typeCode: BSONTypeCode {
        switch self {
        case .double: return .double
        case .string: return .string
        case .document: return .document
        case .array: return .array
        case .binary: return .binary
        case .undefined: return .undefined
        case .objectId: return .objectId
        case .bool: return .bool
        case .dateTime: return .dateTime
        case .null: return .null
        case .regex: return .regex
        case .dbPointer: return .dbPointer
        case .javascript: return .javascript
        case .symbol: return .symbol
        case .javascriptWithScope: return .javascriptWithScope
        case .int32: return .int32
        case .timestamp: return .timestamp
        case .int64: return .int64
        case .decimal128: return .decimal128
        case .minKey: return .minKey
        case .maxKey: return .maxKey
        }
    }

    // MARK: - 类型优先级（用于比较排序）

    /// MongoDB 类型排序优先级
    public var typeOrder: Int {
        switch self {
        case .minKey: return 0
        case .null: return 1
        case .double, .int32, .int64, .decimal128: return 2  // 数字类型
        case .symbol, .string: return 3
        case .document: return 4
        case .array: return 5
        case .binary: return 6
        case .objectId: return 7
        case .bool: return 8
        case .dateTime: return 9
        case .timestamp: return 10
        case .regex: return 11
        case .dbPointer: return 12
        case .javascript, .javascriptWithScope: return 13
        case .undefined: return 14
        case .maxKey: return 15
        }
    }

    // MARK: - 描述

    public var description: String {
        switch self {
        case .double(let v): return "\(v)"
        case .string(let v): return "\"\(v)\""
        case .document(let v): return v.description
        case .array(let v): return v.description
        case .binary(let v): return "Binary(\(v.subtype), \(v.data.count) bytes)"
        case .undefined: return "undefined"
        case .objectId(let v): return v.description
        case .bool(let v): return v ? "true" : "false"
        case .dateTime(let v): return "ISODate(\"\(ISO8601DateFormatter().string(from: v))\")"
        case .null: return "null"
        case .regex(let v): return v.description
        case .dbPointer(let v): return "DBPointer(\(v.ref))"
        case .javascript(let v): return v.description
        case .symbol(let v): return v.description
        case .javascriptWithScope(let v): return "JavaScript(\"\(v.code)\", scope)"
        case .int32(let v): return "\(v)"
        case .timestamp(let v): return v.description
        case .int64(let v): return "NumberLong(\(v))"
        case .decimal128(let v): return v.description
        case .minKey: return "MinKey"
        case .maxKey: return "MaxKey"
        }
    }

    // MARK: - 便捷访问

    /// 作为整数值（如果可以转换）
    public var intValue: Int? {
        switch self {
        case .int32(let v): return Int(v)
        case .int64(let v): return Int(v)
        case .double(let v) where v == Double(Int(v)): return Int(v)
        default: return nil
        }
    }

    /// 作为 Int64 值（如果可以转换）
    public var int64Value: Int64? {
        switch self {
        case .int64(let v):
            return v
        case .int32(let v):
            return Int64(v)
        case .double(let v) where v.isFinite:
            let i = Int64(v)
            return Double(i) == v ? i : nil
        default:
            return nil
        }
    }

    /// 作为 Double 值（如果可以转换）
    public var doubleValue: Double? {
        switch self {
        case .double(let v): return v
        case .int32(let v): return Double(v)
        case .int64(let v): return Double(v)
        case .decimal128(let v): return v.doubleValue
        default: return nil
        }
    }

    /// 作为字符串值
    public var stringValue: String? {
        switch self {
        case .string(let v): return v
        case .symbol(let v): return v.value
        default: return nil
        }
    }

    /// 作为布尔值
    public var boolValue: Bool? {
        switch self {
        case .bool(let v): return v
        default: return nil
        }
    }

    /// 作为文档
    public var documentValue: BSONDocument? {
        switch self {
        case .document(let v): return v
        default: return nil
        }
    }

    /// 作为数组
    public var arrayValue: BSONArray? {
        switch self {
        case .array(let v): return v
        default: return nil
        }
    }

    /// 作为 ObjectID
    public var objectIdValue: ObjectID? {
        switch self {
        case .objectId(let v): return v
        default: return nil
        }
    }

    /// 作为 Date
    public var dateValue: Date? {
        switch self {
        case .dateTime(let v): return v
        default: return nil
        }
    }

    /// 是否为 null
    public var isNull: Bool {
        if case .null = self { return true }
        return false
    }
}

// MARK: - Hashable

extension BSONValue: Hashable {
    public static func == (lhs: BSONValue, rhs: BSONValue) -> Bool {
        switch (lhs, rhs) {
        case (.double(let a), .double(let b)): return a == b
        case (.string(let a), .string(let b)): return a == b
        case (.document(let a), .document(let b)): return a == b
        case (.array(let a), .array(let b)): return a == b
        case (.binary(let a), .binary(let b)): return a == b
        case (.undefined, .undefined): return true
        case (.objectId(let a), .objectId(let b)): return a == b
        case (.bool(let a), .bool(let b)): return a == b
        case (.dateTime(let a), .dateTime(let b)): return a == b
        case (.null, .null): return true
        case (.regex(let a), .regex(let b)): return a == b
        case (.dbPointer(let a), .dbPointer(let b)): return a == b
        case (.javascript(let a), .javascript(let b)): return a == b
        case (.symbol(let a), .symbol(let b)): return a == b
        case (.javascriptWithScope(let a), .javascriptWithScope(let b)):
            return a == b
        case (.int32(let a), .int32(let b)): return a == b
        case (.timestamp(let a), .timestamp(let b)): return a == b
        case (.int64(let a), .int64(let b)): return a == b
        case (.decimal128(let a), .decimal128(let b)): return a == b
        case (.minKey, .minKey): return true
        case (.maxKey, .maxKey): return true
        // 跨数字类型比较
        case (.int32(let a), .int64(let b)): return Int64(a) == b
        case (.int64(let a), .int32(let b)): return a == Int64(b)
        case (.int32(let a), .double(let b)): return Double(a) == b
        case (.double(let a), .int32(let b)): return a == Double(b)
        case (.int64(let a), .double(let b)): return Double(a) == b
        case (.double(let a), .int64(let b)): return a == Double(b)
        default: return false
        }
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(typeCode.rawValue)
        switch self {
        case .double(let v): hasher.combine(v)
        case .string(let v): hasher.combine(v)
        case .document(let v): hasher.combine(v)
        case .array(let v): hasher.combine(v)
        case .binary(let v): hasher.combine(v)
        case .undefined: break
        case .objectId(let v): hasher.combine(v)
        case .bool(let v): hasher.combine(v)
        case .dateTime(let v): hasher.combine(v)
        case .null: break
        case .regex(let v): hasher.combine(v)
        case .dbPointer(let v): hasher.combine(v)
        case .javascript(let v): hasher.combine(v)
        case .symbol(let v): hasher.combine(v)
        case .javascriptWithScope(let v): hasher.combine(v)
        case .int32(let v): hasher.combine(v)
        case .timestamp(let v): hasher.combine(v)
        case .int64(let v): hasher.combine(v)
        case .decimal128(let v): hasher.combine(v)
        case .minKey: break
        case .maxKey: break
        }
    }
}

// MARK: - ExpressibleBy Literals

extension BSONValue: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self = .string(value)
    }
}

extension BSONValue: ExpressibleByIntegerLiteral {
    public init(integerLiteral value: Int) {
        if value >= Int32.min && value <= Int32.max {
            self = .int32(Int32(value))
        } else {
            self = .int64(Int64(value))
        }
    }
}

extension BSONValue: ExpressibleByFloatLiteral {
    public init(floatLiteral value: Double) {
        self = .double(value)
    }
}

extension BSONValue: ExpressibleByBooleanLiteral {
    public init(booleanLiteral value: Bool) {
        self = .bool(value)
    }
}

extension BSONValue: ExpressibleByNilLiteral {
    public init(nilLiteral: ()) {
        self = .null
    }
}

extension BSONValue: ExpressibleByArrayLiteral {
    public init(arrayLiteral elements: BSONValue...) {
        self = .array(BSONArray(elements))
    }
}

extension BSONValue: ExpressibleByDictionaryLiteral {
    public init(dictionaryLiteral elements: (String, BSONValue)...) {
        self = .document(BSONDocument(elements))
    }
}
