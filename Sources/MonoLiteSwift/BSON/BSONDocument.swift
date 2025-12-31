// Created by Yanjunhui

import Foundation

/// BSON 有序文档
/// EN: BSON ordered document.
/// 保持键的插入顺序（类似于 Go 的 bson.D）
/// EN: Maintains key insertion order (similar to Go's bson.D).
public struct BSONDocument: Sendable, CustomStringConvertible {
    /// 内部存储（有序键值对）
    /// EN: Internal storage (ordered key-value pairs).
    private var elements: [(key: String, value: BSONValue)]

    // MARK: - 初始化 / Initialization

    /// 创建空文档
    /// EN: Creates an empty document.
    public init() {
        self.elements = []
    }

    /// 从键值对数组创建
    /// EN: Creates from an array of key-value pairs.
    public init(_ elements: [(String, BSONValue)]) {
        self.elements = elements.map { (key: $0.0, value: $0.1) }
    }

    /// 从键值对序列创建
    /// EN: Creates from a sequence of key-value pairs.
    public init<S: Sequence>(_ sequence: S) where S.Element == (String, BSONValue) {
        self.elements = sequence.map { (key: $0.0, value: $0.1) }
    }

    // MARK: - 访问 / Access

    /// 键的数量
    /// EN: Number of keys.
    public var count: Int {
        elements.count
    }

    /// 是否为空
    /// EN: Returns true if the document is empty.
    public var isEmpty: Bool {
        elements.isEmpty
    }

    /// 所有键（保持顺序）
    /// EN: All keys (order preserved).
    public var keys: [String] {
        elements.map { $0.key }
    }

    /// 所有值（保持顺序）
    /// EN: All values (order preserved).
    public var values: [BSONValue] {
        elements.map { $0.value }
    }

    /// 下标访问（通过键）
    /// EN: Subscript access (by key).
    public subscript(key: String) -> BSONValue? {
        get {
            elements.first { $0.key == key }?.value
        }
        set {
            if let newValue = newValue {
                if let index = elements.firstIndex(where: { $0.key == key }) {
                    elements[index] = (key: key, value: newValue)
                } else {
                    elements.append((key: key, value: newValue))
                }
            } else {
                elements.removeAll { $0.key == key }
            }
        }
    }

    /// 下标访问（通过索引）
    /// EN: Subscript access (by index).
    public subscript(index: Int) -> (key: String, value: BSONValue) {
        get {
            elements[index]
        }
        set {
            elements[index] = newValue
        }
    }

    /// 是否包含指定键
    /// EN: Returns true if the document contains the specified key.
    public func contains(key: String) -> Bool {
        elements.contains { $0.key == key }
    }

    // MARK: - 修改 / Modification

    /// 追加键值对
    /// EN: Appends a key-value pair.
    public mutating func append(key: String, value: BSONValue) {
        elements.append((key: key, value: value))
    }

    /// 插入键值对
    /// EN: Inserts a key-value pair at the specified index.
    public mutating func insert(key: String, value: BSONValue, at index: Int) {
        elements.insert((key: key, value: value), at: index)
    }

    /// 移除指定键
    /// EN: Removes the value for the specified key.
    @discardableResult
    public mutating func removeValue(forKey key: String) -> BSONValue? {
        guard let index = elements.firstIndex(where: { $0.key == key }) else {
            return nil
        }
        return elements.remove(at: index).value
    }

    /// 移除指定索引
    /// EN: Removes the element at the specified index.
    @discardableResult
    public mutating func remove(at index: Int) -> (key: String, value: BSONValue) {
        elements.remove(at: index)
    }

    /// 清空文档
    /// EN: Removes all elements from the document.
    public mutating func removeAll() {
        elements.removeAll()
    }

    // MARK: - 嵌套访问 / Nested Access

    /// 获取嵌套字段值（支持点号路径）
    /// EN: Gets nested field value (supports dot notation path).
    /// 例如：doc.getValue(forPath: "user.name")
    /// EN: Example: doc.getValue(forPath: "user.name")
    public func getValue(forPath path: String) -> BSONValue? {
        let parts = path.split(separator: ".").map(String.init)
        return getValue(forPathComponents: parts)
    }

    /// 获取嵌套字段值（使用路径组件数组）
    /// EN: Gets nested field value (using path component array).
    public func getValue(forPathComponents components: [String]) -> BSONValue? {
        guard !components.isEmpty else { return nil }

        var currentValue: BSONValue = .document(self)

        for component in components {
            switch currentValue {
            case .document(let doc):
                guard let value = doc[component] else { return nil }
                currentValue = value

            case .array(let arr):
                // 数组索引访问
                // EN: Array index access
                if let index = Int(component), index >= 0, index < arr.count {
                    currentValue = arr[index]
                } else {
                    // 尝试在数组元素中查找（MongoDB 数组匹配语义）
                    // EN: Try to find in array elements (MongoDB array matching semantics)
                    return nil
                }

            default:
                return nil
            }
        }

        return currentValue
    }

    /// 设置嵌套字段值
    /// EN: Sets nested field value.
    public mutating func setValue(_ value: BSONValue, forPath path: String) {
        let parts = path.split(separator: ".").map(String.init)
        setValue(value, forPathComponents: parts)
    }

    /// 设置嵌套字段值（使用路径组件数组）
    /// EN: Sets nested field value (using path component array).
    public mutating func setValue(_ value: BSONValue, forPathComponents components: [String]) {
        guard !components.isEmpty else { return }

        if components.count == 1 {
            self[components[0]] = value
            return
        }

        let firstKey = components[0]
        var subDoc: BSONDocument

        if case .document(let existing) = self[firstKey] {
            subDoc = existing
        } else {
            subDoc = BSONDocument()
        }

        subDoc.setValue(value, forPathComponents: Array(components.dropFirst()))
        self[firstKey] = .document(subDoc)
    }

    // MARK: - 迭代 / Iteration

    /// 遍历所有键值对
    /// EN: Iterates over all key-value pairs.
    public func forEach(_ body: ((key: String, value: BSONValue)) throws -> Void) rethrows {
        try elements.forEach(body)
    }

    /// 映射
    /// EN: Maps the elements.
    public func map<T>(_ transform: ((key: String, value: BSONValue)) throws -> T) rethrows -> [T] {
        try elements.map(transform)
    }

    /// 过滤
    /// EN: Filters the elements.
    public func filter(_ isIncluded: ((key: String, value: BSONValue)) throws -> Bool) rethrows -> BSONDocument {
        BSONDocument(try elements.filter(isIncluded).map { ($0.key, $0.value) })
    }

    // MARK: - 描述 / Description

    public var description: String {
        let contents = elements.map { "\"\($0.key)\": \($0.value.description)" }.joined(separator: ", ")
        return "{ \(contents) }"
    }

    // MARK: - 合并 / Merging

    /// 合并另一个文档（相同键会被覆盖）
    /// EN: Merges another document (existing keys will be overwritten).
    public mutating func merge(_ other: BSONDocument) {
        for (key, value) in other.elements {
            self[key] = value
        }
    }

    /// 返回合并后的新文档
    /// EN: Returns a new merged document.
    public func merging(_ other: BSONDocument) -> BSONDocument {
        var result = self
        result.merge(other)
        return result
    }
}

// MARK: - Sequence

extension BSONDocument: Sequence {
    public func makeIterator() -> IndexingIterator<[(key: String, value: BSONValue)]> {
        elements.makeIterator()
    }
}

// MARK: - Collection

extension BSONDocument: Collection {
    public typealias Index = Int

    public var startIndex: Int { elements.startIndex }
    public var endIndex: Int { elements.endIndex }

    public func index(after i: Int) -> Int {
        elements.index(after: i)
    }
}

// MARK: - Hashable

extension BSONDocument: Hashable {
    public static func == (lhs: BSONDocument, rhs: BSONDocument) -> Bool {
        guard lhs.elements.count == rhs.elements.count else { return false }
        for (l, r) in zip(lhs.elements, rhs.elements) {
            if l.key != r.key || l.value != r.value {
                return false
            }
        }
        return true
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(elements.count)
        for (key, value) in elements {
            hasher.combine(key)
            hasher.combine(value)
        }
    }
}

// MARK: - ExpressibleByDictionaryLiteral

extension BSONDocument: ExpressibleByDictionaryLiteral {
    public init(dictionaryLiteral elements: (String, BSONValue)...) {
        self.elements = elements.map { (key: $0.0, value: $0.1) }
    }
}

// MARK: - Codable Support (Optional) / Codable 支持（可选）

extension BSONDocument {
    /// 转换为 Dictionary（会丢失顺序）
    /// EN: Converts to Dictionary (order will be lost).
    public func toDictionary() -> [String: BSONValue] {
        var dict = [String: BSONValue]()
        for (key, value) in elements {
            dict[key] = value
        }
        return dict
    }

    /// 从 Dictionary 创建（顺序不确定）
    /// EN: Creates from Dictionary (order is not guaranteed).
    public init(dictionary: [String: BSONValue]) {
        self.elements = dictionary.map { (key: $0.key, value: $0.value) }
    }
}
