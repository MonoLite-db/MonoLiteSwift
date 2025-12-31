// Created by Yanjunhui

import Foundation

/// BSON 值比较器
/// EN: BSON value comparator.
/// 遵循 MongoDB 的比较规则和类型排序优先级
/// EN: Follows MongoDB's comparison rules and type sorting priority.
public enum BSONCompare {
    /// 比较两个 BSON 值
    /// EN: Compares two BSON values.
    /// - Returns: 负数表示 lhs < rhs，0 表示相等，正数表示 lhs > rhs
    /// EN: Returns negative if lhs < rhs, 0 if equal, positive if lhs > rhs.
    public static func compare(_ lhs: BSONValue, _ rhs: BSONValue) -> Int {
        // 首先比较类型优先级
        // EN: First compare type priority
        let typeOrder = lhs.typeOrder - rhs.typeOrder
        if typeOrder != 0 {
            return typeOrder
        }

        // 同类型比较
        // EN: Same type comparison
        return compareValues(lhs, rhs)
    }

    /// 比较同类型的值
    /// EN: Compares values of the same type.
    private static func compareValues(_ lhs: BSONValue, _ rhs: BSONValue) -> Int {
        switch (lhs, rhs) {
        case (.minKey, .minKey):
            return 0

        case (.maxKey, .maxKey):
            return 0

        case (.null, .null):
            return 0

        case (.undefined(_), .undefined(_)):
            return 0

        // 数字类型（统一比较）
        // EN: Numeric types (unified comparison)
        case (.double(_), _), (.int32(_), _), (.int64(_), _), (.decimal128(_), _):
            return compareNumeric(lhs, rhs)

        case (.string(let a), .string(let b)):
            return a.compare(b).rawValue

        case (.symbol(let a), .symbol(let b)):
            return a.value.compare(b.value).rawValue

        case (.document(let a), .document(let b)):
            return compareDocuments(a, b)

        case (.array(let a), .array(let b)):
            return compareArrays(a, b)

        case (.binary(let a), .binary(let b)):
            return compareBinary(a, b)

        case (.objectId(let a), .objectId(let b)):
            return a.bytes.lexicographicallyPrecedes(b.bytes) ? -1 :
                   (a.bytes == b.bytes ? 0 : 1)

        case (.bool(let a), .bool(let b)):
            if a == b { return 0 }
            return a ? 1 : -1  // false < true

        case (.dateTime(let a), .dateTime(let b)):
            return a.compare(b) == .orderedAscending ? -1 :
                   (a == b ? 0 : 1)

        case (.timestamp(let a), .timestamp(let b)):
            return a < b ? -1 : (a == b ? 0 : 1)

        case (.regex(let a), .regex(let b)):
            let patternCmp = a.pattern.compare(b.pattern).rawValue
            if patternCmp != 0 { return patternCmp }
            return a.options.compare(b.options).rawValue

        case (.javascript(let a), .javascript(let b)):
            return a.code.compare(b.code).rawValue

        case (.javascriptWithScope(let a), .javascriptWithScope(let b)):
            let codeCmp = a.code.compare(b.code).rawValue
            if codeCmp != 0 { return codeCmp }
            return compareDocuments(a.scope, b.scope)

        case (.dbPointer(let a), .dbPointer(let b)):
            let refCmp = a.ref.compare(b.ref).rawValue
            if refCmp != 0 { return refCmp }
            return a.id.bytes.lexicographicallyPrecedes(b.id.bytes) ? -1 :
                   (a.id.bytes == b.id.bytes ? 0 : 1)

        default:
            // 不同类型（但 typeOrder 相同）的比较
            // EN: Different types (but same typeOrder) comparison
            return 0
        }
    }

    // MARK: - 数字比较 / Numeric Comparison

    /// 比较数字类型
    /// EN: Compares numeric types.
    /// 支持跨类型比较（int32, int64, double, decimal128）
    /// EN: Supports cross-type comparison (int32, int64, double, decimal128).
    private static func compareNumeric(_ lhs: BSONValue, _ rhs: BSONValue) -> Int {
        // Decimal128 vs Decimal128: exact compare (Go parity)
        if case .decimal128(let a) = lhs, case .decimal128(let b) = rhs {
            return a.compare(to: b)
        }

        let lhsNum = numericValue(lhs)
        let rhsNum = numericValue(rhs)

        switch (lhsNum, rhsNum) {
        case (.int(let a), .int(let b)):
            return a < b ? -1 : (a == b ? 0 : 1)

        case (.double(let a), .double(let b)):
            return compareDoubles(a, b)

        case (.int(let a), .double(let b)):
            // 检查 double 是否能精确表示该整数
            // EN: Check if double can precisely represent the integer
            if b.isNaN { return -1 }  // NaN 大于任何整数 / EN: NaN is greater than any integer
            let maxSafeInt: Int64 = 1 << 53
            if a > -maxSafeInt && a < maxSafeInt {
                return compareDoubles(Double(a), b)
            }
            // 大整数需要特殊处理
            // EN: Large integers need special handling
            if b.isInfinite { return b > 0 ? -1 : 1 }
            let bInt = Int64(b)
            if a < bInt { return -1 }
            if a > bInt { return 1 }
            // 整数部分相等，检查小数部分
            // EN: Integer parts equal, check fractional part
            return b > Double(bInt) ? -1 : 0

        case (.double, .int):
            return -compareNumeric(rhs, lhs)

        case (.decimal(_, _), _), (_, .decimal(_, _)):
            // Decimal128 vs other numeric: approximate via Double (matches Go's non-decimal comparison path)
            let aDouble = lhs.doubleValue ?? 0
            let bDouble = rhs.doubleValue ?? 0
            return compareDoubles(aDouble, bDouble)
        }
    }

    /// 数字值的统一表示
    /// EN: Unified representation of numeric values.
    private enum NumericValue {
        case int(Int64)
        case double(Double)
        case decimal(UInt64, UInt64)
    }

    /// 提取数字值
    /// EN: Extracts numeric value.
    private static func numericValue(_ value: BSONValue) -> NumericValue {
        switch value {
        case .int32(let v):
            return .int(Int64(v))
        case .int64(let v):
            return .int(v)
        case .double(let v):
            return .double(v)
        case .decimal128(let v):
            return .decimal(v.high, v.low)
        default:
            return .double(0)
        }
    }

    /// 比较两个 Double
    /// EN: Compares two Double values.
    private static func compareDoubles(_ a: Double, _ b: Double) -> Int {
        // 处理 NaN
        // EN: Handle NaN
        if a.isNaN && b.isNaN { return 0 }
        if a.isNaN { return 1 }  // NaN 大于其他 / EN: NaN is greater than others
        if b.isNaN { return -1 }

        if a < b { return -1 }
        if a > b { return 1 }
        return 0
    }

    // MARK: - 文档比较 / Document Comparison

    /// 比较两个文档
    /// EN: Compares two documents.
    private static func compareDocuments(_ lhs: BSONDocument, _ rhs: BSONDocument) -> Int {
        // 首先比较字段数量
        // EN: First compare field count
        if lhs.count != rhs.count {
            return lhs.count < rhs.count ? -1 : 1
        }

        // 按顺序比较每个字段
        // EN: Compare each field in order
        for (lElem, rElem) in zip(lhs, rhs) {
            // 比较键
            // EN: Compare keys
            let keyCmp = lElem.key.compare(rElem.key).rawValue
            if keyCmp != 0 { return keyCmp }

            // 比较值
            // EN: Compare values
            let valueCmp = compare(lElem.value, rElem.value)
            if valueCmp != 0 { return valueCmp }
        }

        return 0
    }

    // MARK: - 数组比较 / Array Comparison

    /// 比较两个数组
    /// EN: Compares two arrays.
    private static func compareArrays(_ lhs: BSONArray, _ rhs: BSONArray) -> Int {
        let minCount = min(lhs.count, rhs.count)

        // 逐元素比较
        // EN: Compare element by element
        for i in 0..<minCount {
            let cmp = compare(lhs[i], rhs[i])
            if cmp != 0 { return cmp }
        }

        // 短数组小于长数组
        // EN: Shorter array is less than longer array
        if lhs.count < rhs.count { return -1 }
        if lhs.count > rhs.count { return 1 }
        return 0
    }

    // MARK: - 二进制比较 / Binary Comparison

    /// 比较两个二进制值
    /// EN: Compares two binary values.
    private static func compareBinary(_ lhs: BSONBinary, _ rhs: BSONBinary) -> Int {
        // 首先比较长度
        // EN: First compare length
        if lhs.data.count != rhs.data.count {
            return lhs.data.count < rhs.data.count ? -1 : 1
        }

        // 然后比较子类型
        // EN: Then compare subtype
        if lhs.subtype.rawValue != rhs.subtype.rawValue {
            return lhs.subtype.rawValue < rhs.subtype.rawValue ? -1 : 1
        }

        // 最后比较数据
        // EN: Finally compare data
        for (a, b) in zip(lhs.data, rhs.data) {
            if a != b {
                return a < b ? -1 : 1
            }
        }

        return 0
    }
}

// MARK: - Comparable Extension

extension BSONValue: Comparable {
    public static func < (lhs: BSONValue, rhs: BSONValue) -> Bool {
        BSONCompare.compare(lhs, rhs) < 0
    }
}

// MARK: - 排序辅助 / Sorting Helpers

extension Array where Element == BSONDocument {
    /// 根据排序规范排序文档数组
    /// EN: Sorts document array by sort specification.
    /// - Parameter sortSpec: 排序规范，例如 ["name": 1, "age": -1]
    /// EN: Sort specification, e.g. ["name": 1, "age": -1]
    public mutating func sort(by sortSpec: BSONDocument) {
        sort { lhs, rhs in
            for (key, direction) in sortSpec {
                let lhsValue = lhs.getValue(forPath: key) ?? .null
                let rhsValue = rhs.getValue(forPath: key) ?? .null

                let cmp = BSONCompare.compare(lhsValue, rhsValue)
                if cmp != 0 {
                    // direction > 0 为升序，< 0 为降序
                    // EN: direction > 0 is ascending, < 0 is descending
                    let dir = direction.intValue ?? 1
                    return dir > 0 ? cmp < 0 : cmp > 0
                }
            }
            return false
        }
    }

    /// 返回排序后的新数组
    /// EN: Returns a new sorted array.
    public func sorted(by sortSpec: BSONDocument) -> [BSONDocument] {
        var result = self
        result.sort(by: sortSpec)
        return result
    }
}

// MARK: - 便捷比较函数 / Convenience Comparison Functions

/// 比较两个 BSON 值
/// EN: Compares two BSON values.
public func compareBSONValues(_ lhs: BSONValue, _ rhs: BSONValue) -> Int {
    BSONCompare.compare(lhs, rhs)
}
