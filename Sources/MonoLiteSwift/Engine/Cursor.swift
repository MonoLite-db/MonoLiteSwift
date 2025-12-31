// Created by Yanjunhui

import Foundation

/// 游标 ID 类型
/// EN: Cursor ID type
public typealias CursorID = Int64

/// 游标状态
/// EN: Cursor state
public enum CursorState: Sendable {
    /// 打开
    /// EN: Open
    case open

    /// 已关闭
    /// EN: Closed
    case closed

    /// 已耗尽
    /// EN: Exhausted
    case exhausted
}

/// 游标
/// EN: Cursor
/// 用于遍历查询结果集
/// EN: Used to iterate over query result set
public actor Cursor {
    /// 游标 ID
    /// EN: Cursor ID
    public let id: CursorID

    /// 命名空间（数据库.集合）
    /// EN: Namespace (database.collection)
    public let namespace: String

    /// 结果文档
    /// EN: Result documents
    private var documents: [BSONDocument]

    /// 当前位置
    /// EN: Current position
    private var position: Int

    /// 批量大小
    /// EN: Batch size
    private var batchSize: Int

    /// 游标状态
    /// EN: Cursor state
    private var state: CursorState

    /// 创建时间
    /// EN: Creation time
    public let createdAt: Date

    /// 最后访问时间
    /// EN: Last access time
    private var lastAccessAt: Date

    /// 超时时间（秒）
    /// EN: Timeout duration (seconds)
    private let timeout: TimeInterval

    // MARK: - 初始化 / Initialization

    /// 创建游标
    /// EN: Create cursor
    public init(
        id: CursorID,
        namespace: String,
        documents: [BSONDocument],
        batchSize: Int = 100,
        timeout: TimeInterval = 600  // 10 分钟超时 / EN: 10 minutes timeout
    ) {
        self.id = id
        self.namespace = namespace
        self.documents = documents
        self.position = 0
        self.batchSize = batchSize
        self.state = .open
        self.createdAt = Date()
        self.lastAccessAt = Date()
        self.timeout = timeout
    }

    // MARK: - 遍历 / Iteration

    /// 是否还有更多文档
    /// EN: Whether there are more documents
    public var hasNext: Bool {
        state == .open && position < documents.count
    }

    /// 获取下一批文档
    /// EN: Get next batch of documents
    public func next() -> [BSONDocument] {
        guard state == .open else {
            return []
        }

        lastAccessAt = Date()

        let start = position
        let end = min(position + batchSize, documents.count)

        if start >= documents.count {
            state = .exhausted
            return []
        }

        let batch = Array(documents[start..<end])
        position = end

        if position >= documents.count {
            state = .exhausted
        }

        return batch
    }

    /// 获取下一个文档
    /// EN: Get next document
    public func nextOne() -> BSONDocument? {
        guard state == .open, position < documents.count else {
            return nil
        }

        lastAccessAt = Date()

        let doc = documents[position]
        position += 1

        if position >= documents.count {
            state = .exhausted
        }

        return doc
    }

    /// 获取所有剩余文档
    /// EN: Get all remaining documents
    public func toArray() -> [BSONDocument] {
        guard state == .open else {
            return []
        }

        lastAccessAt = Date()

        let remaining = Array(documents[position...])
        position = documents.count
        state = .exhausted

        return remaining
    }

    // MARK: - 状态管理 / State Management

    /// 关闭游标
    /// EN: Close cursor
    public func close() {
        state = .closed
        documents = []
    }

    /// 是否已关闭
    /// EN: Whether closed
    public var isClosed: Bool {
        state == .closed || state == .exhausted
    }

    /// 是否已超时
    /// EN: Whether timed out
    public var isTimedOut: Bool {
        Date().timeIntervalSince(lastAccessAt) > timeout
    }

    /// 剩余文档数量
    /// EN: Remaining documents count
    public var remaining: Int {
        max(0, documents.count - position)
    }

    /// 总文档数量
    /// EN: Total documents count
    public var count: Int {
        documents.count
    }

    // MARK: - 设置 / Settings

    /// 设置批量大小
    /// EN: Set batch size
    public func setBatchSize(_ size: Int) {
        batchSize = max(1, size)
    }

    /// 跳过指定数量的文档
    /// EN: Skip specified number of documents
    public func skip(_ count: Int) {
        guard state == .open else { return }
        position = min(position + count, documents.count)
        if position >= documents.count {
            state = .exhausted
        }
    }

    /// 限制返回的文档数量
    /// EN: Limit the number of documents returned
    public func limit(_ count: Int) {
        guard state == .open, count > 0 else { return }
        let remaining = documents.count - position
        if remaining > count {
            documents = Array(documents[..<(position + count)])
        }
    }
}

/// 游标管理器
/// EN: Cursor manager
public actor CursorManager {
    /// 游标字典
    /// EN: Cursors dictionary
    private var cursors: [CursorID: Cursor]

    /// 下一个游标 ID
    /// EN: Next cursor ID
    private var nextCursorId: CursorID

    /// 初始化
    /// EN: Initialize
    public init() {
        self.cursors = [:]
        self.nextCursorId = 1
    }

    /// 创建游标
    /// EN: Create cursor
    public func createCursor(
        namespace: String,
        documents: [BSONDocument],
        batchSize: Int = 100
    ) -> Cursor {
        let id = nextCursorId
        nextCursorId += 1

        let cursor = Cursor(
            id: id,
            namespace: namespace,
            documents: documents,
            batchSize: batchSize
        )

        cursors[id] = cursor

        return cursor
    }

    /// 获取游标
    /// EN: Get cursor
    public func getCursor(_ id: CursorID) -> Cursor? {
        return cursors[id]
    }

    /// 关闭游标
    /// EN: Close cursor
    public func closeCursor(_ id: CursorID) async {
        if let cursor = cursors[id] {
            await cursor.close()
            cursors.removeValue(forKey: id)
        }
    }

    /// 清理超时的游标
    /// EN: Clean up timed out cursors
    public func cleanupTimedOutCursors() async {
        var toRemove: [CursorID] = []

        for (id, cursor) in cursors {
            if await cursor.isTimedOut {
                await cursor.close()
                toRemove.append(id)
            }
        }

        for id in toRemove {
            cursors.removeValue(forKey: id)
        }
    }

    /// 游标数量
    /// EN: Cursors count
    public var count: Int {
        cursors.count
    }

    /// 关闭所有游标
    /// EN: Close all cursors
    public func closeAll() async {
        for (_, cursor) in cursors {
            await cursor.close()
        }
        cursors.removeAll()
    }

    // MARK: - Mongo wire-protocol 辅助（对齐 Go）/ Mongo Wire-protocol Helpers (Go-aligned)

    /// 获取 firstBatch 并决定是否返回 cursorId（0 表示不需要后续 getMore）
    /// EN: Get firstBatch and decide whether to return cursorId (0 means no subsequent getMore needed)
    public func getFirstBatch(namespace: String, documents: [BSONDocument], batchSize: Int64) async -> (batch: [BSONDocument], cursorId: CursorID) {
        // 与 Go/常见驱动行为一致：默认 101
        // EN: Aligned with Go/common driver behavior: default 101
        let bs = batchSize > 0 ? Int(batchSize) : 101
        let cursor = createCursor(namespace: namespace, documents: documents, batchSize: bs)
        let batch = await cursor.next()

        if await cursor.isClosed {
            // exhausted：不保留 cursor
            // EN: exhausted: don't keep cursor
            cursors.removeValue(forKey: cursor.id)
            return (batch, 0)
        }
        return (batch, cursor.id)
    }

    /// 获取 nextBatch；如果耗尽会自动关闭并返回 cursorId=0
    /// EN: Get nextBatch; if exhausted, will auto-close and return cursorId=0
    public func getNextBatch(cursorId: CursorID, batchSize: Int64) async -> (batch: [BSONDocument], cursorId: CursorID, namespace: String?) {
        guard let cursor = cursors[cursorId] else {
            return ([], 0, nil)
        }
        if batchSize > 0 {
            await cursor.setBatchSize(Int(batchSize))
        }
        let ns = cursor.namespace
        let batch = await cursor.next()
        if await cursor.isClosed {
            cursors.removeValue(forKey: cursorId)
            return (batch, 0, ns)
        }
        return (batch, cursorId, ns)
    }

    /// 关闭一组 cursor，返回成功关闭的 cursorId 列表（用于 killCursors）
    /// EN: Close a group of cursors, return list of successfully closed cursorIds (for killCursors)
    public func killCursors(_ ids: [CursorID]) async -> [CursorID] {
        var killed: [CursorID] = []
        killed.reserveCapacity(ids.count)
        for id in ids {
            if let cursor = cursors[id] {
                await cursor.close()
                cursors.removeValue(forKey: id)
                killed.append(id)
            }
        }
        return killed
    }
}
