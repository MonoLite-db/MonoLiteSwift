// Created by Yanjunhui

import Foundation

/// BSON 数组
/// EN: BSON array.
public struct BSONArray: Sendable, CustomStringConvertible {
    /// 内部存储
    /// EN: Internal storage.
    private var elements: [BSONValue]

    // MARK: - 初始化 / Initialization

    /// 创建空数组
    /// EN: Creates an empty array.
    public init() {
        self.elements = []
    }

    /// 从 BSONValue 数组创建
    /// EN: Creates from an array of BSONValue.
    public init(_ elements: [BSONValue]) {
        self.elements = elements
    }

    /// 从序列创建
    /// EN: Creates from a sequence.
    public init<S: Sequence>(_ sequence: S) where S.Element == BSONValue {
        self.elements = Array(sequence)
    }

    // MARK: - 访问 / Access

    /// 元素数量
    /// EN: Number of elements.
    public var count: Int {
        elements.count
    }

    /// 是否为空
    /// EN: Returns true if the array is empty.
    public var isEmpty: Bool {
        elements.isEmpty
    }

    /// 第一个元素
    /// EN: The first element.
    public var first: BSONValue? {
        elements.first
    }

    /// 最后一个元素
    /// EN: The last element.
    public var last: BSONValue? {
        elements.last
    }

    /// 下标访问
    /// EN: Subscript access.
    public subscript(index: Int) -> BSONValue {
        get {
            elements[index]
        }
        set {
            elements[index] = newValue
        }
    }

    /// 安全下标访问
    /// EN: Safe subscript access.
    public subscript(safe index: Int) -> BSONValue? {
        guard index >= 0 && index < elements.count else { return nil }
        return elements[index]
    }

    // MARK: - 修改 / Modification

    /// 追加元素
    /// EN: Appends an element.
    public mutating func append(_ value: BSONValue) {
        elements.append(value)
    }

    /// 追加多个元素
    /// EN: Appends multiple elements.
    public mutating func append(contentsOf values: [BSONValue]) {
        elements.append(contentsOf: values)
    }

    /// 插入元素
    /// EN: Inserts an element at the specified index.
    public mutating func insert(_ value: BSONValue, at index: Int) {
        elements.insert(value, at: index)
    }

    /// 移除元素
    /// EN: Removes an element at the specified index.
    @discardableResult
    public mutating func remove(at index: Int) -> BSONValue {
        elements.remove(at: index)
    }

    /// 移除最后一个元素
    /// EN: Removes the last element.
    @discardableResult
    public mutating func removeLast() -> BSONValue {
        elements.removeLast()
    }

    /// 移除第一个元素
    /// EN: Removes the first element.
    @discardableResult
    public mutating func removeFirst() -> BSONValue {
        elements.removeFirst()
    }

    /// 清空数组
    /// EN: Removes all elements from the array.
    public mutating func removeAll() {
        elements.removeAll()
    }

    // MARK: - 转换 / Conversion

    /// 转换为 BSONDocument（索引作为键）
    /// EN: Converts to BSONDocument (indices as keys).
    public func toDocument() -> BSONDocument {
        var doc = BSONDocument()
        for (index, value) in elements.enumerated() {
            doc.append(key: String(index), value: value)
        }
        return doc
    }

    // MARK: - 描述 / Description

    public var description: String {
        let contents = elements.map { $0.description }.joined(separator: ", ")
        return "[ \(contents) ]"
    }
}

// MARK: - Sequence

extension BSONArray: Sequence {
    public func makeIterator() -> IndexingIterator<[BSONValue]> {
        elements.makeIterator()
    }
}

// MARK: - Collection

extension BSONArray: Collection {
    public typealias Index = Int

    public var startIndex: Int { elements.startIndex }
    public var endIndex: Int { elements.endIndex }

    public func index(after i: Int) -> Int {
        elements.index(after: i)
    }
}

// MARK: - RandomAccessCollection

extension BSONArray: RandomAccessCollection {}

// MARK: - MutableCollection

extension BSONArray: MutableCollection {}

// MARK: - RangeReplaceableCollection

extension BSONArray: RangeReplaceableCollection {
    public mutating func replaceSubrange<C>(_ subrange: Range<Int>, with newElements: C)
        where C: Collection, C.Element == BSONValue
    {
        elements.replaceSubrange(subrange, with: newElements)
    }
}

// MARK: - Hashable

extension BSONArray: Hashable {
    public static func == (lhs: BSONArray, rhs: BSONArray) -> Bool {
        guard lhs.elements.count == rhs.elements.count else { return false }
        for (l, r) in zip(lhs.elements, rhs.elements) {
            if l != r { return false }
        }
        return true
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(elements.count)
        for element in elements {
            hasher.combine(element)
        }
    }
}

// MARK: - ExpressibleByArrayLiteral

extension BSONArray: ExpressibleByArrayLiteral {
    public init(arrayLiteral elements: BSONValue...) {
        self.elements = elements
    }
}
