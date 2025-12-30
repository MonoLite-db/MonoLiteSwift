// Created by Yanjunhui

import Foundation

/// 游标 ID 类型
public typealias CursorID = Int64

/// 游标状态
public enum CursorState: Sendable {
    case open
    case closed
    case exhausted
}

/// 游标
/// 用于遍历查询结果集
public actor Cursor {
    /// 游标 ID
    public let id: CursorID

    /// 命名空间（数据库.集合）
    public let namespace: String

    /// 结果文档
    private var documents: [BSONDocument]

    /// 当前位置
    private var position: Int

    /// 批量大小
    private var batchSize: Int

    /// 游标状态
    private var state: CursorState

    /// 创建时间
    public let createdAt: Date

    /// 最后访问时间
    private var lastAccessAt: Date

    /// 超时时间（秒）
    private let timeout: TimeInterval

    // MARK: - 初始化

    /// 创建游标
    public init(
        id: CursorID,
        namespace: String,
        documents: [BSONDocument],
        batchSize: Int = 100,
        timeout: TimeInterval = 600  // 10 分钟超时
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

    // MARK: - 遍历

    /// 是否还有更多文档
    public var hasNext: Bool {
        state == .open && position < documents.count
    }

    /// 获取下一批文档
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

    // MARK: - 状态管理

    /// 关闭游标
    public func close() {
        state = .closed
        documents = []
    }

    /// 是否已关闭
    public var isClosed: Bool {
        state == .closed || state == .exhausted
    }

    /// 是否已超时
    public var isTimedOut: Bool {
        Date().timeIntervalSince(lastAccessAt) > timeout
    }

    /// 剩余文档数量
    public var remaining: Int {
        max(0, documents.count - position)
    }

    /// 总文档数量
    public var count: Int {
        documents.count
    }

    // MARK: - 设置

    /// 设置批量大小
    public func setBatchSize(_ size: Int) {
        batchSize = max(1, size)
    }

    /// 跳过指定数量的文档
    public func skip(_ count: Int) {
        guard state == .open else { return }
        position = min(position + count, documents.count)
        if position >= documents.count {
            state = .exhausted
        }
    }

    /// 限制返回的文档数量
    public func limit(_ count: Int) {
        guard state == .open, count > 0 else { return }
        let remaining = documents.count - position
        if remaining > count {
            documents = Array(documents[..<(position + count)])
        }
    }
}

/// 游标管理器
public actor CursorManager {
    /// 游标字典
    private var cursors: [CursorID: Cursor]

    /// 下一个游标 ID
    private var nextCursorId: CursorID

    /// 初始化
    public init() {
        self.cursors = [:]
        self.nextCursorId = 1
    }

    /// 创建游标
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
    public func getCursor(_ id: CursorID) -> Cursor? {
        return cursors[id]
    }

    /// 关闭游标
    public func closeCursor(_ id: CursorID) async {
        if let cursor = cursors[id] {
            await cursor.close()
            cursors.removeValue(forKey: id)
        }
    }

    /// 清理超时的游标
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
    public var count: Int {
        cursors.count
    }

    /// 关闭所有游标
    public func closeAll() async {
        for (_, cursor) in cursors {
            await cursor.close()
        }
        cursors.removeAll()
    }

    // MARK: - Mongo wire-protocol helpers (Go-aligned)

    /// 获取 firstBatch 并决定是否返回 cursorId（0 表示不需要后续 getMore）
    public func getFirstBatch(namespace: String, documents: [BSONDocument], batchSize: Int64) async -> (batch: [BSONDocument], cursorId: CursorID) {
        let bs = batchSize > 0 ? Int(batchSize) : 101 // 与 Go/常见驱动行为一致：默认 101
        let cursor = createCursor(namespace: namespace, documents: documents, batchSize: bs)
        let batch = await cursor.next()

        if await cursor.isClosed {
            // exhausted：不保留 cursor
            cursors.removeValue(forKey: cursor.id)
            return (batch, 0)
        }
        return (batch, cursor.id)
    }

    /// 获取 nextBatch；如果耗尽会自动关闭并返回 cursorId=0
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
