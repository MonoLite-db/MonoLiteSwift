// Created by Yanjunhui

import Foundation

/// B+Tree 索引
/// EN: B+Tree index.
public actor BTree {
    /// 页面管理器
    /// EN: Page manager.
    private let pager: Pager

    /// 根页面 ID
    /// EN: Root page ID.
    private var rootPageId: PageID

    /// 索引名称
    /// EN: Index name.
    public let name: String

    /// 是否唯一索引
    /// EN: Whether this is a unique index.
    public let unique: Bool

    // MARK: - 初始化 / Initialization

    /// 创建新的 B+Tree
    /// EN: Create a new B+Tree.
    public init(pager: Pager, name: String, unique: Bool) async throws {
        self.pager = pager
        self.name = name
        self.unique = unique

        // 分配根页面（叶子节点）
        // EN: Allocate root page (leaf node)
        let rootPage = try await pager.allocatePage(type: .index)
        self.rootPageId = rootPage.id

        // 初始化空的根节点
        // EN: Initialize empty root node
        var root = BTreeNode(pageId: rootPage.id, isLeaf: true)
        try await writeNode(&root)
    }

    /// 打开现有的 B+Tree
    /// EN: Open existing B+Tree.
    public init(pager: Pager, rootPageId: PageID, name: String, unique: Bool) {
        self.pager = pager
        self.rootPageId = rootPageId
        self.name = name
        self.unique = unique
    }

    /// 获取根页面 ID
    /// EN: Get root page ID.
    public var rootPage: PageID {
        rootPageId
    }

    // MARK: - 节点 I/O / Node I/O

    /// 读取节点
    /// EN: Read node.
    private func readNode(_ pageId: PageID) async throws -> BTreeNode {
        let page = try await pager.readPage(pageId)
        return try BTreeNode.unmarshal(page.data, pageId: pageId)
    }

    /// 写入节点
    /// EN: Write node.
    private func writeNode(_ node: inout BTreeNode) async throws {
        let page = try await pager.readPage(node.pageId)
        // 以 Go 参考实现为准：写满整页数据区（MaxPageData），避免尾部残留旧数据
        // EN: Aligned with Go reference implementation: write full page data area (MaxPageData), avoid trailing stale data
        let encoded = node.marshal()
        guard encoded.count <= StorageConstants.maxPageData else {
            throw StorageError.pageCorrupted(node.pageId)
        }
        var full = Data(count: StorageConstants.maxPageData)
        full.replaceSubrange(0..<encoded.count, with: encoded)
        page.setData(full, at: 0)
        page.isDirty = true
        await pager.markDirty(node.pageId)
    }

    // MARK: - 查询 / Query

    /// 精确查找
    /// EN: Exact search.
    public func search(_ key: Data) async throws -> Data? {
        var node = try await readNode(rootPageId)

        while !node.isLeaf {
            // 以 Go 参考实现为准：选择第一个使 key < keys[i] 的位置 i（upperBound），走 children[i]
            // EN: Aligned with Go reference implementation: select first position i where key < keys[i] (upperBound), go to children[i]
            var i = 0
            while i < node.keyCount && compareKeys(key, node.keys[i]) >= 0 {
                i += 1
            }
            if i >= node.children.count {
                i = max(0, node.children.count - 1)
            }
            node = try await readNode(node.children[i])
        }

        // 叶子节点：线性扫描（与 Go 行为一致）
        // EN: Leaf node: linear scan (consistent with Go behavior)
        for i in 0..<node.keyCount {
            let cmp = compareKeys(key, node.keys[i])
            if cmp == 0 { return node.values[i] }
            if cmp < 0 { break }
        }
        return nil
    }

    /// 范围查询
    /// EN: Range query.
    public func searchRange(
        minKey: Data?,
        maxKey: Data?,
        includeMin: Bool = true,
        includeMax: Bool = true
    ) async throws -> [Data] {
        var results: [Data] = []

        // 找到起始叶子节点
        // EN: Find starting leaf node
        var node = try await readNode(rootPageId)
        while !node.isLeaf {
            var i = 0
            if let mk = minKey {
                // Go：i++ while mk >= keys[i]
                while i < node.keyCount && compareKeys(mk, node.keys[i]) >= 0 {
                    i += 1
                }
            } else {
                i = 0
            }
            if i >= node.children.count {
                i = max(0, node.children.count - 1)
            }
            node = try await readNode(node.children[i])
        }

        // 遍历叶子链表
        // EN: Traverse leaf linked list
        while true {
            for i in 0..<node.keyCount {
                let key = node.keys[i]

                // 检查下界
                // EN: Check lower bound
                if let mk = minKey {
                    let cmp = compareKeys(key, mk)
                    if cmp < 0 || (cmp == 0 && !includeMin) {
                        continue
                    }
                }

                // 检查上界
                // EN: Check upper bound
                if let xk = maxKey {
                    let cmp = compareKeys(key, xk)
                    if cmp > 0 || (cmp == 0 && !includeMax) {
                        return results
                    }
                }

                results.append(node.values[i])
            }

            // 移动到下一个叶子
            // EN: Move to next leaf
            if node.next == StorageConstants.invalidPageId {
                break
            }
            node = try await readNode(node.next)
        }

        return results
    }

    // MARK: - 插入 / Insert

    /// 插入键值对
    /// EN: Insert key-value pair.
    public func insert(_ key: Data, _ value: Data) async throws {
        // 验证键值大小
        // EN: Validate key-value size
        guard key.count <= StorageConstants.maxIndexKeyBytes else {
            throw MonoError.badValue("Index key too large: \(key.count) bytes")
        }
        guard value.count <= StorageConstants.maxIndexValueBytes else {
            throw MonoError.badValue("Index value too large: \(value.count) bytes")
        }

        var root = try await readNode(rootPageId)

        // 检查根节点是否需要分裂
        // 以 Go 参考实现为准：按 keyCount 上限判断（BTreeOrder-1）
        // EN: Check if root node needs to split
        // Aligned with Go reference implementation: check by keyCount upper limit (BTreeOrder-1)
        if root.keyCount >= StorageConstants.btreeOrder - 1 {
            // 创建新根
            // EN: Create new root
            let newRootPage = try await pager.allocatePage(type: .index)
            var newRoot = BTreeNode(pageId: newRootPage.id, isLeaf: false)
            newRoot.children.append(rootPageId)

            // 分裂旧根
            // EN: Split old root
            try await splitChild(&newRoot, index: 0)

            rootPageId = newRootPage.id
            root = newRoot
        }

        try await insertNonFull(&root, key: key, value: value)
    }

    /// 向非满节点插入
    /// EN: Insert into non-full node.
    private func insertNonFull(_ node: inout BTreeNode, key: Data, value: Data) async throws {
        if node.isLeaf {
            // 以 Go 参考实现为准：从右向左找到插入点
            // EN: Aligned with Go reference implementation: find insertion point from right to left
            var i = node.keyCount - 1
            while i >= 0 && compareKeys(key, node.keys[i]) < 0 {
                i -= 1
            }

            // 唯一索引：在插入位置附近做原子检查（Go 的 BUG-007 修复）
            // EN: Unique index: atomic check near insertion position (Go's BUG-007 fix)
            if unique {
                let checkRight = i + 1
                if checkRight < node.keyCount && compareKeys(node.keys[checkRight], key) == 0 {
                    throw MonoError.duplicateKey(keyPattern: name, keyValue: key.base64EncodedString())
                }
                if i >= 0 && compareKeys(node.keys[i], key) == 0 {
                    throw MonoError.duplicateKey(keyPattern: name, keyValue: key.base64EncodedString())
                }
            }

            let insertPos = i + 1
            node.insertInLeaf(key: key, value: value, at: insertPos)
            try await writeNode(&node)
        } else {
            // 以 Go 参考实现为准：找到子节点位置
            // EN: Aligned with Go reference implementation: find child node position
            var i = node.keyCount - 1
            while i >= 0 && compareKeys(key, node.keys[i]) < 0 {
                i -= 1
            }
            i += 1

            // children 必须为 keyCount+1（否则结构已损坏，继续会触发越界/崩溃）
            // EN: children must be keyCount+1 (otherwise structure is corrupted, continuing will cause out-of-bounds/crash)
            guard node.children.count == node.keyCount + 1 else {
                throw StorageError.pageCorrupted(node.pageId)
            }
            i = Swift.max(0, Swift.min(i, node.children.count - 1))

            var child = try await readNode(node.children[i])

            // Go：按 keyCount 判断满节点
            // EN: Go: check full node by keyCount
            if child.keyCount >= StorageConstants.btreeOrder - 1 {
                try await splitChild(&node, index: i)

                // 决定走哪个子节点
                // EN: Decide which child node to go to
                if i < node.keyCount && compareKeys(key, node.keys[i]) > 0 {
                    i += 1
                }
                i = Swift.max(0, Swift.min(i, node.children.count - 1))
                child = try await readNode(node.children[i])
            }

            try await insertNonFull(&child, key: key, value: value)
        }
    }

    /// 分裂子节点
    /// EN: Split child node.
    private func splitChild(_ parent: inout BTreeNode, index: Int) async throws {
        guard index >= 0, index < parent.children.count else {
            throw StorageError.pageCorrupted(parent.pageId)
        }
        let childPageId = parent.children[index]
        var child = try await readNode(childPageId)

        // 结构不变量检查（避免 slice 越界）
        // EN: Structure invariant check (avoid slice out of bounds)
        if child.isLeaf {
            guard child.keys.count == child.keyCount, child.values.count == child.keyCount else {
                throw StorageError.pageCorrupted(child.pageId)
            }
        } else {
            guard child.keys.count == child.keyCount, child.children.count == child.keyCount + 1 else {
                throw StorageError.pageCorrupted(child.pageId)
            }
        }

        // 找到分裂点（字节驱动）
        // EN: Find split point (byte-driven)
        let mid = findSplitPoint(child)
        guard mid > 0, mid < child.keyCount else {
            throw StorageError.pageCorrupted(child.pageId)
        }

        // 创建新节点
        // EN: Create new node
        let newPage = try await pager.allocatePage(type: .index)
        var newNode = BTreeNode(pageId: newPage.id, isLeaf: child.isLeaf)

        if child.isLeaf {
            // 叶子节点：复制后半部分到新节点
            // EN: Leaf node: copy second half to new node
            newNode.keys = Array(child.keys[mid...])
            newNode.values = Array(child.values[mid...])
            newNode.keyCount = newNode.keys.count
            guard !newNode.keys.isEmpty else {
                try await pager.freePage(newPage.id)
                throw StorageError.pageCorrupted(child.pageId)
            }

            // 更新链表指针
            // EN: Update linked list pointers
            newNode.next = child.next
            newNode.prev = child.pageId
            child.next = newNode.pageId

            // Go：需要更新 nextNode.Prev（若失败需回滚新分配页）
            // EN: Go: need to update nextNode.Prev (rollback newly allocated page if failed)
            if newNode.next != StorageConstants.invalidPageId {
                do {
                    var nextNode = try await readNode(newNode.next)
                    nextNode.prev = newNode.pageId
                    try await writeNode(&nextNode)
                } catch {
                    try await pager.freePage(newPage.id)
                    throw error
                }
            }

            // 截断原节点
            // EN: Truncate original node
            child.keys = Array(child.keys[..<mid])
            child.values = Array(child.values[..<mid])
            child.keyCount = child.keys.count

            // 提升的键是新节点的第一个键
            // EN: Promoted key is the first key of new node
            let promotedKey = newNode.keys[0]
            parent.keys.insert(promotedKey, at: index)
            parent.children.insert(newNode.pageId, at: index + 1)
            parent.keyCount += 1
        } else {
            // 内部节点：中间键提升到父节点
            // EN: Internal node: middle key promoted to parent
            guard mid < child.keys.count else {
                try await pager.freePage(newPage.id)
                throw StorageError.pageCorrupted(child.pageId)
            }
            let promotedKey = child.keys[mid]

            let rightKeyStart = mid + 1
            guard rightKeyStart <= child.keys.count else {
                try await pager.freePage(newPage.id)
                throw StorageError.pageCorrupted(child.pageId)
            }
            guard rightKeyStart <= child.children.count else {
                try await pager.freePage(newPage.id)
                throw StorageError.pageCorrupted(child.pageId)
            }
            newNode.keys = Array(child.keys[rightKeyStart...])
            newNode.children = Array(child.children[rightKeyStart...])
            newNode.keyCount = newNode.keys.count

            child.keys = Array(child.keys[..<mid])
            child.children = Array(child.children[...(mid)])
            child.keyCount = child.keys.count

            guard child.children.count == child.keyCount + 1 else {
                try await pager.freePage(newPage.id)
                throw StorageError.pageCorrupted(child.pageId)
            }
            guard newNode.children.count == newNode.keyCount + 1 else {
                try await pager.freePage(newPage.id)
                throw StorageError.pageCorrupted(newNode.pageId)
            }

            parent.keys.insert(promotedKey, at: index)
            parent.children.insert(newNode.pageId, at: index + 1)
            parent.keyCount += 1
        }

        // 以 Go 参考实现为准：splitChild 内部写入 child/newNode/parent
        // EN: Aligned with Go reference implementation: splitChild writes child/newNode/parent internally
        try await writeNode(&child)
        try await writeNode(&newNode)
        try await writeNode(&parent)
    }

    /// 找到字节驱动的分裂点
    /// EN: Find byte-driven split point.
    private func findSplitPoint(_ node: BTreeNode) -> Int {
        if node.keyCount <= 1 {
            return 0
        }

        let totalSize = node.byteSize()
        let targetSize = totalSize / 2

        var leftSize = BTreeNode.headerSize
        var bestMid = node.keyCount / 2

        for i in 0..<node.keyCount {
            leftSize += 2 + node.keys[i].count
            if node.isLeaf {
                leftSize += 2 + node.values[i].count
            } else if i < node.children.count {
                leftSize += 4
            }

            if leftSize >= targetSize {
                if i < 1 {
                    bestMid = 1
                } else if i >= node.keyCount - 1 {
                    bestMid = node.keyCount - 1
                } else {
                    bestMid = i
                }
                break
            }
        }

        if bestMid < 1 { bestMid = 1 }
        if bestMid >= node.keyCount { bestMid = node.keyCount - 1 }
        return bestMid
    }

    // MARK: - 删除 / Delete

    /// 删除键
    /// EN: Delete key.
    public func delete(_ key: Data) async throws {
        try await deleteInternal(nodeId: rootPageId, key: key, parentId: nil, childIndex: -1)

        // 保险：若根节点为空且为内部节点，提升唯一子节点为新根（Go 语义也会在 fixAfterDelete/mergeNodes 处理）
        // EN: Safety: if root is empty and internal node, promote the only child as new root (Go semantics also handles this in fixAfterDelete/mergeNodes)
        let root = try await readNode(rootPageId)
        if !root.isLeaf && root.keyCount == 0 && !root.children.isEmpty {
            rootPageId = root.children[0]
        }
    }

    /// deleteInternal：递归删除（Go 对齐版）
    /// EN: deleteInternal: recursive deletion (Go-aligned version).
    private func deleteInternal(nodeId: PageID, key: Data, parentId: PageID?, childIndex: Int) async throws {
        let node = try await readNode(nodeId)

        if node.isLeaf {
            try await deleteFromLeaf(nodeId: nodeId, key: key, parentId: parentId, childIndex: childIndex)
            return
        }

        // 内部节点：找到子节点
        // EN: Internal node: find child node
        var i = 0
        while i < node.keyCount && compareKeys(key, node.keys[i]) >= 0 {
            i += 1
        }

        // 确保索引有效
        // EN: Ensure index is valid
        var safeI = i
        if safeI >= node.children.count {
            safeI = max(0, node.children.count - 1)
        }
        if safeI < 0 || node.children.isEmpty {
            return
        }

        // 递归删除
        // EN: Recursive deletion
        try await deleteInternal(nodeId: node.children[safeI], key: key, parentId: nodeId, childIndex: safeI)

        // 重新读取节点（可能已被修改）
        // EN: Re-read node (may have been modified)
        let reloaded = try await readNode(nodeId)

        // 检查子节点是否需要修复（索引可能已变化）
        // EN: Check if child node needs fix (index may have changed)
        if safeI < reloaded.children.count {
            try await fixAfterDelete(parentId: nodeId, childIndex: safeI)
        }
    }

    /// deleteFromLeaf：从叶子节点删除（Go 对齐版）
    /// EN: deleteFromLeaf: delete from leaf node (Go-aligned version).
    private func deleteFromLeaf(nodeId: PageID, key: Data, parentId: PageID?, childIndex: Int) async throws {
        var node = try await readNode(nodeId)

        // 查找键（线性扫描，保持与 Go 行为一致）
        // EN: Find key (linear scan, consistent with Go behavior)
        var found = -1
        if node.keyCount > 0 {
            for i in 0..<node.keyCount {
                if compareKeys(node.keys[i], key) == 0 {
                    found = i
                    break
                }
            }
        }

        if found == -1 {
            return // 键不存在，静默成功 / EN: Key doesn't exist, silent success
        }

        // 删除键值对
        // EN: Delete key-value pair
        node.keys.remove(at: found)
        node.values.remove(at: found)
        node.keyCount -= 1

        try await writeNode(&node)

        // 检查是否需要修复（根节点不需要）
        // EN: Check if fix is needed (root node doesn't need)
        if let parentId, node.keyCount < BTreeNode.minKeys {
            try await fixUnderflow(nodeId: nodeId, parentId: parentId, childIndex: childIndex)
        }
    }

    /// fixAfterDelete：删除后修复节点（Go 对齐版）
    /// EN: fixAfterDelete: fix node after deletion (Go-aligned version).
    private func fixAfterDelete(parentId: PageID, childIndex: Int) async throws {
        let parent = try await readNode(parentId)
        guard childIndex >= 0, childIndex < parent.children.count else { return }

        let child = try await readNode(parent.children[childIndex])

        // 如果子节点键数足够，无需修复
        // EN: If child has enough keys, no fix needed
        if child.keyCount >= BTreeNode.minKeys {
            return
        }

        // 检查是否为根节点且已空
        // EN: Check if root node is empty
        if parentId == rootPageId && parent.keyCount == 0 {
            if !parent.children.isEmpty {
                rootPageId = parent.children[0]
            }
            return
        }

        try await fixUnderflow(nodeId: parent.children[childIndex], parentId: parentId, childIndex: childIndex)
    }

    /// fixUnderflow：修复下溢节点（Go 对齐版）
    /// EN: fixUnderflow: fix underflow node (Go-aligned version).
    private func fixUnderflow(nodeId: PageID, parentId: PageID, childIndex: Int) async throws {
        let parent = try await readNode(parentId)

        // 尝试从左兄弟借键
        // EN: Try to borrow key from left sibling
        if childIndex > 0 {
            let leftId = parent.children[childIndex - 1]
            let leftSibling = try await readNode(leftId)
            if leftSibling.keyCount > BTreeNode.minKeys {
                try await borrowFromLeft(nodeId: nodeId, leftSiblingId: leftId, parentId: parentId, childIndex: childIndex)
                return
            }
        }

        // 尝试从右兄弟借键
        // EN: Try to borrow key from right sibling
        if childIndex < parent.children.count - 1 {
            let rightId = parent.children[childIndex + 1]
            let rightSibling = try await readNode(rightId)
            if rightSibling.keyCount > BTreeNode.minKeys {
                try await borrowFromRight(nodeId: nodeId, rightSiblingId: rightId, parentId: parentId, childIndex: childIndex)
                return
            }
        }

        // 无法借键，需要合并
        // EN: Cannot borrow key, need to merge
        if childIndex > 0 {
            let leftId = parent.children[childIndex - 1]
            try await mergeNodes(leftId: leftId, rightId: nodeId, parentId: parentId, separatorIndex: childIndex - 1)
        } else if childIndex < parent.children.count - 1 {
            let rightId = parent.children[childIndex + 1]
            try await mergeNodes(leftId: nodeId, rightId: rightId, parentId: parentId, separatorIndex: childIndex)
        }
    }

    /// borrowFromLeft：从左兄弟借一个键（Go 对齐版）
    /// EN: borrowFromLeft: borrow one key from left sibling (Go-aligned version).
    private func borrowFromLeft(nodeId: PageID, leftSiblingId: PageID, parentId: PageID, childIndex: Int) async throws {
        var node = try await readNode(nodeId)
        var left = try await readNode(leftSiblingId)
        var parent = try await readNode(parentId)

        if node.isLeaf {
            let borrowedKey = left.keys[left.keyCount - 1]
            let borrowedVal = left.values[left.keyCount - 1]

            left.keys.removeLast()
            left.values.removeLast()
            left.keyCount -= 1

            node.keys.insert(borrowedKey, at: 0)
            node.values.insert(borrowedVal, at: 0)
            node.keyCount += 1

            parent.keys[childIndex - 1] = node.keys[0]
        } else {
            let separatorKey = parent.keys[childIndex - 1]
            let borrowedChild = left.children[left.children.count - 1]

            let newParentKey = left.keys[left.keyCount - 1]
            left.keys.removeLast()
            left.children.removeLast()
            left.keyCount -= 1

            node.keys.insert(separatorKey, at: 0)
            node.children.insert(borrowedChild, at: 0)
            node.keyCount += 1

            parent.keys[childIndex - 1] = newParentKey
        }

        try await writeNode(&left)
        try await writeNode(&node)
        try await writeNode(&parent)
    }

    /// borrowFromRight：从右兄弟借一个键（Go 对齐版）
    /// EN: borrowFromRight: borrow one key from right sibling (Go-aligned version).
    private func borrowFromRight(nodeId: PageID, rightSiblingId: PageID, parentId: PageID, childIndex: Int) async throws {
        var node = try await readNode(nodeId)
        var right = try await readNode(rightSiblingId)
        var parent = try await readNode(parentId)

        if node.isLeaf {
            let borrowedKey = right.keys[0]
            let borrowedVal = right.values[0]

            right.keys.removeFirst()
            right.values.removeFirst()
            right.keyCount -= 1

            node.keys.append(borrowedKey)
            node.values.append(borrowedVal)
            node.keyCount += 1

            // Go：parent.Keys[childIndex] = rightSibling.Keys[0]
            parent.keys[childIndex] = right.keys[0]
        } else {
            let separatorKey = parent.keys[childIndex]
            let borrowedChild = right.children[0]

            let newParentKey = right.keys[0]
            right.keys.removeFirst()
            right.children.removeFirst()
            right.keyCount -= 1

            node.keys.append(separatorKey)
            node.children.append(borrowedChild)
            node.keyCount += 1

            parent.keys[childIndex] = newParentKey
        }

        try await writeNode(&right)
        try await writeNode(&node)
        try await writeNode(&parent)
    }

    /// mergeNodes：合并两个节点（Go 对齐版）
    /// EN: mergeNodes: merge two nodes (Go-aligned version).
    private func mergeNodes(leftId: PageID, rightId: PageID, parentId: PageID, separatorIndex: Int) async throws {
        var left = try await readNode(leftId)
        let right = try await readNode(rightId)
        var parent = try await readNode(parentId)

        var nextNodeToWrite: BTreeNode? = nil

        if left.isLeaf {
            left.keys.append(contentsOf: right.keys)
            left.values.append(contentsOf: right.values)
            left.keyCount = left.keys.count

            left.next = right.next
            if right.next != StorageConstants.invalidPageId {
                var nextNode = try await readNode(right.next)
                nextNode.prev = left.pageId
                nextNodeToWrite = nextNode
            }
        } else {
            let separatorKey = parent.keys[separatorIndex]
            left.keys.append(separatorKey)
            left.keys.append(contentsOf: right.keys)
            left.children.append(contentsOf: right.children)
            left.keyCount = left.keys.count
        }

        // 从父节点移除分隔键和右子节点指针
        // EN: Remove separator key and right child pointer from parent
        parent.keys.remove(at: separatorIndex)
        parent.children.remove(at: separatorIndex + 1)
        parent.keyCount -= 1

        // 检查是否需要更新根节点
        // EN: Check if root node needs to be updated
        if parent.pageId == rootPageId && parent.keyCount == 0 {
            rootPageId = left.pageId
        }

        // 原子写入（先写节点，再释放 right）
        // EN: Atomic write (write nodes first, then free right)
        if var nextNode = nextNodeToWrite {
            try await writeNode(&nextNode)
        }
        try await writeNode(&left)
        try await writeNode(&parent)

        try await pager.freePage(right.pageId)
    }

    // MARK: - 统计 / Statistics

    /// 计算键数量
    /// EN: Count keys.
    public func count() async throws -> Int {
        var total = 0
        var node = try await readNode(rootPageId)

        // 找到最左叶子
        // EN: Find leftmost leaf
        while !node.isLeaf {
            node = try await readNode(node.children[0])
        }

        // 遍历叶子链表
        // EN: Traverse leaf linked list
        while true {
            total += node.keyCount
            if node.next == StorageConstants.invalidPageId {
                break
            }
            node = try await readNode(node.next)
        }

        return total
    }

    /// 释放整棵树的所有节点页（用于 drop collection / drop index）
    /// EN: Free all node pages of the entire tree (for drop collection / drop index).
    public func drop() async throws {
        var toVisit: [PageID] = [rootPageId]
        var visited = Set<PageID>()

        while let id = toVisit.popLast() {
            if id == 0 { continue }
            if visited.contains(id) { continue }
            visited.insert(id)

            let node = try await readNode(id)
            if !node.isLeaf {
                toVisit.append(contentsOf: node.children)
            }
        }

        // 先释放非根（顺序不重要，只要最终都 free）
        // EN: Free non-root first (order doesn't matter, as long as all are freed)
        for id in visited {
            try await pager.freePage(id)
        }
    }

    /// 基本树结构检查（不做值语义验证）
    /// EN: Basic tree structure check (no value semantic validation).
    public func checkTreeIntegrity() async throws {
        var toVisit: [PageID] = [rootPageId]
        var visited = Set<PageID>()
        while let id = toVisit.popLast() {
            if id == 0 { continue }
            if visited.contains(id) { continue }
            visited.insert(id)

            let node = try await readNode(id)
            // 结构一致性已由 unmarshal 做过；这里只检查 children 数量关系
            // EN: Structure consistency already checked by unmarshal; here only check children count relationship
            if node.isLeaf {
                if node.children.count != 0 {
                    throw StorageError.pageCorrupted(id)
                }
            } else {
                if node.children.count != node.keyCount + 1 {
                    throw StorageError.pageCorrupted(id)
                }
                toVisit.append(contentsOf: node.children)
            }
        }
    }

    /// 叶子链表检查：prev/next 双向一致，且 key 递增
    /// EN: Leaf linked list check: prev/next bidirectional consistency, and key ascending.
    public func checkLeafChain() async throws {
        // 找最左叶子
        // EN: Find leftmost leaf
        var node = try await readNode(rootPageId)
        while !node.isLeaf {
            guard let first = node.children.first else { break }
            node = try await readNode(first)
        }

        var prevId: PageID = 0
        var lastKey: Data? = nil
        while true {
            if node.prev != prevId {
                throw StorageError.pageCorrupted(node.pageId)
            }

            for k in node.keys {
                if let lastKey, compareKeys(lastKey, k) >= 0 {
                    throw StorageError.pageCorrupted(node.pageId)
                }
                lastKey = k
            }

            if node.next == 0 { break }
            prevId = node.pageId
            node = try await readNode(node.next)
        }
    }

    // MARK: - 高级查询（与 Go 版本对齐）/ Advanced Queries (Go-aligned)

    /// 从尾部反向获取 limit 条记录
    /// 用于分段采样验证，获取索引尾部的记录
    /// EN: Get limit records from the tail in reverse.
    /// Used for segmented sampling validation, get records from the end of the index.
    public func searchRangeLimitReverse(_ limit: Int) async throws -> [Data] {
        guard limit > 0 else { return [] }

        // 找到最后一个叶子节点
        // EN: Find the last leaf node
        var node = try await readNode(rootPageId)
        while !node.isLeaf {
            // 走到最右边的子节点
            // EN: Go to the rightmost child node
            guard node.keyCount < node.children.count else {
                break
            }
            node = try await readNode(node.children[node.keyCount])
        }

        // 从最后一个叶子节点反向遍历
        // EN: Traverse from the last leaf node in reverse
        var results: [Data] = []
        results.reserveCapacity(limit)

        while results.count < limit {
            // 从节点末尾向前遍历
            // EN: Traverse from the end of the node backward
            for i in stride(from: node.keyCount - 1, through: 0, by: -1) {
                if results.count >= limit { break }
                results.append(node.values[i])
            }

            // 移动到前一个叶子节点
            // EN: Move to the previous leaf node
            if node.prev == StorageConstants.invalidPageId {
                break
            }
            node = try await readNode(node.prev)
        }

        return results
    }

    /// 跳过 skip 条记录后获取 limit 条记录
    /// 用于分段采样验证，获取索引中间位置的记录
    /// EN: Get limit records after skipping skip records.
    /// Used for segmented sampling validation, get records from the middle of the index.
    public func searchRangeLimitSkip(skip: Int, limit: Int) async throws -> [Data] {
        guard limit > 0 else { return [] }

        var results: [Data] = []
        results.reserveCapacity(limit)
        var skipped = 0

        // 找到起始叶子节点
        // EN: Find starting leaf node
        var node = try await readNode(rootPageId)
        while !node.isLeaf {
            node = try await readNode(node.children[0])
        }

        // 遍历叶子节点链表
        // EN: Traverse leaf node linked list
        while true {
            for i in 0..<node.keyCount {
                // 先跳过 skip 条
                // EN: Skip skip records first
                if skipped < skip {
                    skipped += 1
                    continue
                }

                // 检查是否已达到限制
                // EN: Check if limit reached
                if results.count >= limit {
                    return results
                }

                results.append(node.values[i])
            }

            // 移动到下一个叶子节点
            // EN: Move to next leaf node
            if node.next == StorageConstants.invalidPageId {
                break
            }
            node = try await readNode(node.next)
        }

        return results
    }

    /// 获取所有键
    /// EN: Get all keys.
    public func getAllKeys() async throws -> [Data] {
        var keys: [Data] = []

        // 找到第一个叶子节点
        // EN: Find the first leaf node
        var node = try await readNode(rootPageId)
        while !node.isLeaf {
            node = try await readNode(node.children[0])
        }

        // 遍历叶子链表
        // EN: Traverse leaf linked list
        while true {
            keys.append(contentsOf: node.keys)
            if node.next == StorageConstants.invalidPageId {
                break
            }
            node = try await readNode(node.next)
        }

        return keys
    }

    /// 获取树的高度
    /// EN: Get tree height.
    public func height() async throws -> Int {
        var h = 0
        var node = try await readNode(rootPageId)

        while !node.isLeaf {
            h += 1
            node = try await readNode(node.children[0])
        }

        return h + 1  // 包括叶子层 / EN: Including leaf level
    }

    /// 完整验证树（检查键的顺序、节点键数范围等）
    /// EN: Full tree verification (check key order, node key count range, etc.).
    public func verify() async throws {
        try await checkTreeIntegrity()
        try await checkLeafChain()
    }
}
