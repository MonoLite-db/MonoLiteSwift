// Created by Yanjunhui

import Foundation

/// B+Tree 节点
/// EN: B+Tree node.
public struct BTreeNode: Sendable {
    /// 页面 ID
    /// EN: Page ID.
    public var pageId: PageID

    /// 是否为叶子节点
    /// EN: Whether this is a leaf node.
    public var isLeaf: Bool

    /// 键数量
    /// EN: Key count.
    public var keyCount: Int

    /// 键列表
    /// EN: Key list.
    public var keys: [Data]

    /// 值列表（仅叶子节点）
    /// EN: Value list (leaf nodes only).
    public var values: [Data]

    /// 子节点 ID（仅内部节点）
    /// EN: Child node IDs (internal nodes only).
    public var children: [PageID]

    /// 下一个叶子节点 ID（链表指针）
    /// EN: Next leaf node ID (linked list pointer).
    public var next: PageID

    /// 上一个叶子节点 ID（双向链表）
    /// EN: Previous leaf node ID (doubly linked list).
    public var prev: PageID

    // MARK: - 初始化 / Initialization

    /// 创建空的叶子节点
    /// EN: Create an empty leaf node.
    public init(pageId: PageID, isLeaf: Bool = true) {
        self.pageId = pageId
        self.isLeaf = isLeaf
        self.keyCount = 0
        self.keys = []
        self.values = []
        self.children = []
        self.next = StorageConstants.invalidPageId
        self.prev = StorageConstants.invalidPageId
    }

    // MARK: - 序列化 / Serialization

    /// 节点头大小：11 字节
    /// IsLeaf(1) + KeyCount(2) + Next(4) + Prev(4)
    /// EN: Node header size: 11 bytes.
    /// IsLeaf(1) + KeyCount(2) + Next(4) + Prev(4)
    public static let headerSize = 11

    /// 序列化为 Data
    /// EN: Serialize to Data.
    public func marshal() -> Data {
        var buffer = Data()

        // 头部
        // EN: Header
        buffer.append(isLeaf ? 0x01 : 0x00)
        var tmp = Data(count: 2)
        DataEndian.writeUInt16LE(UInt16(keyCount), to: &tmp, at: 0)
        buffer.append(tmp)

        tmp = Data(count: 4)
        DataEndian.writeUInt32LE(next, to: &tmp, at: 0)
        buffer.append(tmp)

        tmp = Data(count: 4)
        DataEndian.writeUInt32LE(prev, to: &tmp, at: 0)
        buffer.append(tmp)

        // 键
        // EN: Keys
        for key in keys {
            let keyLen = UInt16(key.count)
            tmp = Data(count: 2)
            DataEndian.writeUInt16LE(keyLen, to: &tmp, at: 0)
            buffer.append(tmp)
            buffer.append(key)
        }

        if isLeaf {
            // 值
            // EN: Values
            for value in values {
                let valueLen = UInt16(value.count)
                tmp = Data(count: 2)
                DataEndian.writeUInt16LE(valueLen, to: &tmp, at: 0)
                buffer.append(tmp)
                buffer.append(value)
            }
        } else {
            // 子节点 ID
            // EN: Child node IDs
            for child in children {
                tmp = Data(count: 4)
                DataEndian.writeUInt32LE(child, to: &tmp, at: 0)
                buffer.append(tmp)
            }
        }

        return buffer
    }

    /// 从 Data 反序列化
    /// EN: Deserialize from Data.
    public static func unmarshal(_ data: Data, pageId: PageID) throws -> BTreeNode {
        guard data.count >= headerSize else {
            throw StorageError.pageCorrupted(pageId)
        }

        var node = BTreeNode(pageId: pageId)
        node.isLeaf = data[0] != 0
        node.keyCount = Int(DataEndian.readUInt16LE(data, at: 1))
        node.next = DataEndian.readUInt32LE(data, at: 3)
        node.prev = DataEndian.readUInt32LE(data, at: 7)

        // 读取键
        // EN: Read keys
        var offset2 = headerSize
        node.keys.reserveCapacity(node.keyCount)
        for _ in 0..<node.keyCount {
            guard offset2 + 2 <= data.count else {
                throw StorageError.pageCorrupted(pageId)
            }

            let keyLen = Int(DataEndian.readUInt16LE(data, at: offset2))
            offset2 += 2

            guard offset2 + keyLen <= data.count else {
                throw StorageError.pageCorrupted(pageId)
            }
            node.keys.append(Data(data[offset2..<offset2 + keyLen]))
            offset2 += keyLen
        }

        if node.isLeaf {
            // 读取值
            // EN: Read values
            node.values.reserveCapacity(node.keyCount)
            for _ in 0..<node.keyCount {
                guard offset2 + 2 <= data.count else {
                    throw StorageError.pageCorrupted(pageId)
                }

                let valueLen = Int(DataEndian.readUInt16LE(data, at: offset2))
                offset2 += 2

                guard offset2 + valueLen <= data.count else {
                    throw StorageError.pageCorrupted(pageId)
                }
                node.values.append(Data(data[offset2..<offset2 + valueLen]))
                offset2 += valueLen
            }
        } else {
            // 读取子节点 ID（keyCount + 1 个）
            // EN: Read child node IDs (keyCount + 1)
            let childCount = node.keyCount + 1
            node.children.reserveCapacity(childCount)
            for _ in 0..<childCount {
                guard offset2 + 4 <= data.count else {
                    throw StorageError.pageCorrupted(pageId)
                }

                let childId = DataEndian.readUInt32LE(data, at: offset2)
                node.children.append(childId)
                offset2 += 4
            }
        }

        // 以 Go 参考实现为准：一致性验证
        // EN: Consistency validation aligned with Go reference implementation
        guard node.keys.count == node.keyCount else {
            throw StorageError.pageCorrupted(pageId)
        }
        if node.isLeaf {
            guard node.values.count == node.keyCount else {
                throw StorageError.pageCorrupted(pageId)
            }
        } else {
            guard node.children.count == node.keyCount + 1 else {
                throw StorageError.pageCorrupted(pageId)
            }
        }

        return node
    }

    // MARK: - 大小计算 / Size Calculation

    /// 计算节点当前字节大小
    /// EN: Calculate current byte size of the node.
    public func byteSize() -> Int {
        var size = Self.headerSize

        // 键
        // EN: Keys
        for key in keys {
            size += 2 + key.count  // 2 字节长度 + 数据 / EN: 2 bytes length + data
        }

        if isLeaf {
            // 值
            // EN: Values
            for value in values {
                size += 2 + value.count
            }
        } else {
            // 子节点 ID
            // EN: Child node IDs
            size += children.count * 4
        }

        return size
    }

    /// 是否需要分裂
    /// EN: Whether the node needs to split.
    public func needsSplit() -> Bool {
        // 以 Go 参考实现为准：分裂触发条件使用 keyCount 上限（BTreeOrder-1）
        // EN: Aligned with Go reference implementation: split trigger condition uses keyCount upper limit (BTreeOrder-1)
        keyCount >= StorageConstants.btreeOrder - 1
    }

    /// 是否为空
    /// EN: Whether the node is empty.
    public var isEmpty: Bool {
        keyCount == 0
    }

    /// 最小键数（除根节点外）
    /// 以 Go 参考实现为准：MinKeys = (BTreeOrder - 1) / 2
    /// EN: Minimum key count (except root node).
    /// Aligned with Go reference implementation: MinKeys = (BTreeOrder - 1) / 2
    public static let minKeys = (StorageConstants.btreeOrder - 1) / 2

    /// 是否需要合并
    /// EN: Whether the node needs to merge.
    public func needsMerge() -> Bool {
        keyCount < Self.minKeys
    }

    // MARK: - 键操作 / Key Operations

    /// 二分查找键的位置
    /// 返回第一个 >= key 的位置
    /// EN: Binary search for key position.
    /// Returns the first position where key >= target.
    public func findKeyIndex(_ key: Data) -> Int {
        var left = 0
        var right = keyCount

        while left < right {
            let mid = (left + right) / 2
            if compareKeys(keys[mid], key) < 0 {
                left = mid + 1
            } else {
                right = mid
            }
        }

        return left
    }

    /// 查找精确匹配的键
    /// EN: Find exact matching key.
    public func findExactKey(_ key: Data) -> Int? {
        let index = findKeyIndex(key)
        if index < keyCount && compareKeys(keys[index], key) == 0 {
            return index
        }
        return nil
    }

    /// 在叶子节点插入键值对
    /// EN: Insert key-value pair in leaf node.
    public mutating func insertInLeaf(key: Data, value: Data, at index: Int) {
        keys.insert(key, at: index)
        values.insert(value, at: index)
        keyCount += 1
    }

    /// 在内部节点插入键和子节点
    /// EN: Insert key and child node in internal node.
    public mutating func insertInInternal(key: Data, leftChild: PageID, rightChild: PageID, at index: Int) {
        keys.insert(key, at: index)
        if index < children.count {
            children[index] = leftChild
            children.insert(rightChild, at: index + 1)
        } else {
            children.append(rightChild)
        }
        keyCount += 1
    }

    /// 从叶子节点删除键值对
    /// EN: Remove key-value pair from leaf node.
    public mutating func removeFromLeaf(at index: Int) {
        keys.remove(at: index)
        values.remove(at: index)
        keyCount -= 1
    }

    /// 从内部节点删除键
    /// EN: Remove key from internal node.
    public mutating func removeFromInternal(at index: Int) {
        keys.remove(at: index)
        children.remove(at: index + 1)
        keyCount -= 1
    }
}

// MARK: - 键比较 / Key Comparison

/// 比较两个键
/// EN: Compare two keys.
public func compareKeys(_ a: Data, _ b: Data) -> Int {
    let minLen = min(a.count, b.count)

    for i in 0..<minLen {
        if a[i] < b[i] { return -1 }
        if a[i] > b[i] { return 1 }
    }

    if a.count < b.count { return -1 }
    if a.count > b.count { return 1 }
    return 0
}
