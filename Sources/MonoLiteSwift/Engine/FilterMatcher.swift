// Created by Yanjunhui

import Foundation

/// 查询过滤器匹配器
/// 支持 MongoDB 风格的查询运算符
public struct FilterMatcher: Sendable {
    /// 过滤条件
    private let filter: BSONDocument

    /// 创建过滤器匹配器
    public init(_ filter: BSONDocument) {
        self.filter = filter
    }

    // MARK: - 主匹配方法

    /// 检查文档是否匹配过滤器
    public func match(_ doc: BSONDocument) -> Bool {
        if filter.isEmpty {
            return true
        }

        for (key, value) in filter {
            if !matchElement(doc: doc, key: key, value: value) {
                return false
            }
        }

        return true
    }

    // MARK: - 元素匹配

    /// 匹配单个过滤条件
    private func matchElement(doc: BSONDocument, key: String, value: BSONValue) -> Bool {
        // 处理逻辑运算符
        switch key {
        case "$and":
            return matchAnd(doc: doc, value: value)
        case "$or":
            return matchOr(doc: doc, value: value)
        case "$not":
            return matchNot(doc: doc, value: value)
        case "$nor":
            return matchNor(doc: doc, value: value)
        default:
            break
        }

        // 获取文档字段值
        let docVal = doc.getValue(forPath: key)

        // 如果 value 是 document，可能包含比较运算符
        if case .document(let operators) = value {
            return matchOperators(docVal: docVal, operators: operators)
        }

        // 直接相等比较
        return valuesEqual(docVal, value)
    }

    // MARK: - 运算符匹配

    /// 匹配比较运算符
    private func matchOperators(docVal: BSONValue?, operators: BSONDocument) -> Bool {
        for (op, operand) in operators {
            if !matchOperator(docVal: docVal, operator: op, operand: operand) {
                return false
            }
        }
        return true
    }

    /// 匹配单个运算符
    private func matchOperator(docVal: BSONValue?, operator op: String, operand: BSONValue) -> Bool {
        switch op {
        case "$eq":
            return valuesEqual(docVal, operand)

        case "$ne":
            return !valuesEqual(docVal, operand)

        case "$gt":
            guard let docVal = docVal else { return false }
            return compareBSONValues(docVal, operand) > 0

        case "$gte":
            guard let docVal = docVal else { return false }
            return compareBSONValues(docVal, operand) >= 0

        case "$lt":
            guard let docVal = docVal else { return false }
            return compareBSONValues(docVal, operand) < 0

        case "$lte":
            guard let docVal = docVal else { return false }
            return compareBSONValues(docVal, operand) <= 0

        case "$in":
            return matchIn(docVal: docVal, operand: operand)

        case "$nin":
            return !matchIn(docVal: docVal, operand: operand)

        case "$exists":
            let exists = docVal != nil
            if case .bool(let want) = operand {
                return exists == want
            }
            return exists

        case "$type":
            return matchType(docVal: docVal, operand: operand)

        case "$regex":
            return matchRegex(docVal: docVal, operand: operand)

        case "$size":
            return matchSize(docVal: docVal, operand: operand)

        case "$all":
            return matchAll(docVal: docVal, operand: operand)

        case "$elemMatch":
            return matchElemMatch(docVal: docVal, operand: operand)

        case "$mod":
            return matchMod(docVal: docVal, operand: operand)

        default:
            // 未知运算符
            return false
        }
    }

    // MARK: - 逻辑运算符

    /// 处理 $and
    private func matchAnd(doc: BSONDocument, value: BSONValue) -> Bool {
        guard case .array(let arr) = value else { return false }

        for item in arr {
            if case .document(let subFilter) = item {
                let subMatcher = FilterMatcher(subFilter)
                if !subMatcher.match(doc) {
                    return false
                }
            }
        }
        return true
    }

    /// 处理 $or
    private func matchOr(doc: BSONDocument, value: BSONValue) -> Bool {
        guard case .array(let arr) = value else { return false }

        for item in arr {
            if case .document(let subFilter) = item {
                let subMatcher = FilterMatcher(subFilter)
                if subMatcher.match(doc) {
                    return true
                }
            }
        }
        return false
    }

    /// 处理 $not
    private func matchNot(doc: BSONDocument, value: BSONValue) -> Bool {
        if case .document(let subFilter) = value {
            let subMatcher = FilterMatcher(subFilter)
            return !subMatcher.match(doc)
        }
        return true
    }

    /// 处理 $nor
    private func matchNor(doc: BSONDocument, value: BSONValue) -> Bool {
        return !matchOr(doc: doc, value: value)
    }

    // MARK: - 比较运算符

    /// 处理 $in
    private func matchIn(docVal: BSONValue?, operand: BSONValue) -> Bool {
        guard case .array(let arr) = operand else { return false }

        for item in arr {
            if valuesEqual(docVal, item) {
                return true
            }
        }
        return false
    }

    /// 处理 $type
    private func matchType(docVal: BSONValue?, operand: BSONValue) -> Bool {
        guard let docVal = docVal else { return false }

        let expectedType: Int
        switch operand {
        case .int32(let t):
            expectedType = Int(t)
        case .int64(let t):
            expectedType = Int(t)
        case .string(let typeName):
            expectedType = typeNameToNumber(typeName)
        default:
            return false
        }

        let actualType = getBsonTypeNumber(docVal)
        return actualType == expectedType
    }

    /// 处理 $regex
    private func matchRegex(docVal: BSONValue?, operand: BSONValue) -> Bool {
        guard let docVal = docVal else { return false }
        guard case .string(let str) = docVal else { return false }

        let pattern: String
        switch operand {
        case .string(let p):
            pattern = p
        case .regex(let regex):
            pattern = regex.pattern
        default:
            return false
        }

        do {
            let re = try NSRegularExpression(pattern: pattern)
            let range = NSRange(str.startIndex..., in: str)
            return re.firstMatch(in: str, range: range) != nil
        } catch {
            return false
        }
    }

    /// 处理 $size
    private func matchSize(docVal: BSONValue?, operand: BSONValue) -> Bool {
        guard let docVal = docVal else { return false }
        guard case .array(let arr) = docVal else { return false }

        let expectedSize: Int
        switch operand {
        case .int32(let s):
            expectedSize = Int(s)
        case .int64(let s):
            expectedSize = Int(s)
        case .double(let s):
            expectedSize = Int(s)
        default:
            return false
        }

        return arr.count == expectedSize
    }

    /// 处理 $all
    private func matchAll(docVal: BSONValue?, operand: BSONValue) -> Bool {
        guard let docVal = docVal else { return false }
        guard case .array(let arr) = docVal else { return false }
        guard case .array(let required) = operand else { return false }

        for req in required {
            var found = false
            for item in arr {
                if valuesEqual(item, req) {
                    found = true
                    break
                }
            }
            if !found {
                return false
            }
        }
        return true
    }

    /// 处理 $elemMatch
    private func matchElemMatch(docVal: BSONValue?, operand: BSONValue) -> Bool {
        guard let docVal = docVal else { return false }
        guard case .array(let arr) = docVal else { return false }
        guard case .document(let subFilter) = operand else { return false }

        let subMatcher = FilterMatcher(subFilter)
        for item in arr {
            if case .document(let itemDoc) = item {
                if subMatcher.match(itemDoc) {
                    return true
                }
            }
        }
        return false
    }

    /// 处理 $mod
    private func matchMod(docVal: BSONValue?, operand: BSONValue) -> Bool {
        guard let docVal = docVal else { return false }
        guard case .array(let arr) = operand, arr.count == 2 else { return false }

        let divisor: Int64
        let remainder: Int64

        switch arr[0] {
        case .int32(let d): divisor = Int64(d)
        case .int64(let d): divisor = d
        case .double(let d): divisor = Int64(d)
        default: return false
        }

        switch arr[1] {
        case .int32(let r): remainder = Int64(r)
        case .int64(let r): remainder = r
        case .double(let r): remainder = Int64(r)
        default: return false
        }

        guard divisor != 0 else { return false }

        let value: Int64
        switch docVal {
        case .int32(let v): value = Int64(v)
        case .int64(let v): value = v
        case .double(let v): value = Int64(v)
        default: return false
        }

        return value % divisor == remainder
    }

    // MARK: - 辅助方法

    /// 比较两个值是否相等
    private func valuesEqual(_ a: BSONValue?, _ b: BSONValue?) -> Bool {
        guard let a = a, let b = b else {
            return a == nil && b == nil
        }
        return compareBSONValues(a, b) == 0
    }

    /// 类型名转换为 BSON 类型号
    private func typeNameToNumber(_ name: String) -> Int {
        switch name {
        case "double": return 1
        case "string": return 2
        case "object": return 3
        case "array": return 4
        case "binData": return 5
        case "undefined": return 6
        case "objectId": return 7
        case "bool": return 8
        case "date": return 9
        case "null": return 10
        case "regex": return 11
        case "dbPointer": return 12
        case "javascript": return 13
        case "symbol": return 14
        case "javascriptWithScope": return 15
        case "int": return 16
        case "timestamp": return 17
        case "long": return 18
        case "decimal": return 19
        case "minKey": return -1
        case "maxKey": return 127
        default: return -2
        }
    }

    /// 获取 BSON 类型号
    private func getBsonTypeNumber(_ value: BSONValue) -> Int {
        switch value {
        case .double: return 1
        case .string: return 2
        case .document: return 3
        case .array: return 4
        case .binary: return 5
        case .undefined: return 6
        case .objectId: return 7
        case .bool: return 8
        case .dateTime: return 9
        case .null: return 10
        case .regex: return 11
        case .dbPointer: return 12
        case .javascript: return 13
        case .symbol: return 14
        case .javascriptWithScope: return 15
        case .int32: return 16
        case .timestamp: return 17
        case .int64: return 18
        case .decimal128: return 19
        case .minKey: return -1
        case .maxKey: return 127
        }
    }
}

// MARK: - 便捷函数

/// 检查文档是否匹配过滤器
public func matchesFilter(_ doc: BSONDocument, _ filter: BSONDocument) -> Bool {
    let matcher = FilterMatcher(filter)
    return matcher.match(doc)
}
