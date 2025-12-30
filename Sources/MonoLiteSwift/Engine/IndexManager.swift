// Created by Yanjunhui

import Foundation

/// 文档查找协议（用于索引构建）
public protocol DocumentFinder: AnyObject, Sendable {
    func findUnlocked(filter: BSONDocument) async throws -> [BSONDocument]
}

/// 索引管理器
public actor IndexManager {
    /// 文档查找器（用于索引构建时获取现有文档）
    private var documentFinder: DocumentFinder?

    /// 索引字典
    private var indexes: [String: Index]

    /// 页面管理器
    private let pager: Pager

    /// 写操作串行队列（解决 actor 重入导致的并发 BTree 写入竞态问题）
    private let writeQueue = SerialWriteQueue()

    /// 创建索引管理器
    public init(pager: Pager) {
        self.pager = pager
        self.indexes = [:]
    }

    /// 设置文档查找器
    public func setDocumentFinder(_ finder: DocumentFinder) {
        self.documentFinder = finder
    }

    // MARK: - 索引创建与删除

    /// 创建索引（使用写锁保护整个过程）
    public func createIndex(keys: BSONDocument, options: BSONDocument) async throws -> String {
        try await writeQueue.withLock {
            try await self.createIndexLocked(keys: keys, options: options)
        }
    }

    /// 创建索引（内部方法，需在 writeQueue 保护下调用）
    private func createIndexLocked(keys: BSONDocument, options: BSONDocument) async throws -> String {
        // 生成索引名称
        var name = generateIndexName(keys)
        if let nameVal = options["name"], case .string(let n) = nameVal {
            name = n
        }

        // 检查索引是否已存在
        if indexes[name] != nil {
            return name  // 已存在，直接返回
        }

        // 解析选项
        var unique = false
        if let uniqueVal = options["unique"], case .bool(let u) = uniqueVal {
            unique = u
        }

        var background = false
        if let bgVal = options["background"], case .bool(let bg) = bgVal {
            background = bg
        }

        // 创建索引信息
        let info = IndexInfo(
            name: name,
            keys: keys,
            unique: unique,
            background: background
        )

        // 创建索引
        let index = try await Index(pager: pager, info: info)
        indexes[name] = index

        // 为现有文档建立索引
        try await buildIndexLocked(index)

        return name
    }

    /// 为现有文档建立索引（内部方法，需在 writeQueue 保护下调用）
    private func buildIndexLocked(_ index: Index) async throws {
        guard let finder = documentFinder else { return }

        let docs = try await finder.findUnlocked(filter: BSONDocument())

        for doc in docs {
            let indexInfo = await index.info
            guard let key = encodeIndexEntryKey(keys: indexInfo.keys, doc: doc, unique: indexInfo.unique) else {
                continue
            }

            // 获取文档 _id
            guard let idVal = doc.getValue(forPath: "_id") else {
                continue
            }

            let idDoc = BSONDocument([("_id", idVal)])
            let encoder = BSONEncoder()
            guard let idBytes = try? encoder.encode(idDoc) else {
                continue
            }

            do {
                try await index.insert(key: key, value: idBytes)
            } catch {
                let indexName = await index.info.name
                if await index.info.unique {
                    throw MonoError.duplicateKey(keyPattern: indexName, keyValue: key)
                }
                throw MonoError.cannotCreateIndex("failed to build index \(indexName): \(error)")
            }
        }
    }

    /// 删除索引（使用写锁保护）
    public func dropIndex(_ name: String) async throws {
        try await writeQueue.withLock {
            try await self.dropIndexLocked(name)
        }
    }

    /// 删除索引（内部方法，需在 writeQueue 保护下调用）
    private func dropIndexLocked(_ name: String) async throws {
        if name == "_id_" {
            throw MonoError.illegalOperation("cannot drop _id index")
        }
        indexes.removeValue(forKey: name)
    }

    /// 列出所有索引（Mongo listIndexes 视图）
    public func listIndexes() async -> [BSONDocument] {
        var result: [BSONDocument] = []

        // 按名称稳定排序
        let names = indexes.keys.sorted()
        for name in names {
            guard let index = indexes[name] else { continue }
            let info = await index.info

            var doc = BSONDocument()
            doc["name"] = .string(info.name)
            doc["key"] = .document(info.keys)
            doc["unique"] = .bool(info.unique)
            doc["v"] = .int32(2)
            result.append(doc)
        }

        return result
    }

    /// 获取索引
    public func getIndex(_ name: String) -> Index? {
        return indexes[name]
    }

    /// 获取所有索引元数据
    public func getIndexMetas() async -> [IndexMeta] {
        var metas: [IndexMeta] = []
        for (_, index) in indexes {
            let info = await index.info
            let rootPage = await index.rootPage
            metas.append(IndexMeta(
                name: info.name,
                keys: info.keys,
                unique: info.unique,
                rootPageId: rootPage
            ))
        }
        return metas
    }

    // MARK: - 唯一约束检查

    /// 预检查唯一索引约束（内部方法，需在 writeQueue 保护下调用）
    private func checkUniqueConstraintsLocked(_ doc: BSONDocument, excludingId: BSONValue? = nil) async throws {
        for (_, index) in indexes {
            let info = await index.info
            guard info.unique else { continue }

            guard let key = encodeIndexEntryKey(keys: info.keys, doc: doc, unique: true) else {
                continue
            }

            // 检查键是否已存在
            if let existing = try await index.search(key: key) {
                if let excludingId {
                    if let existingId = try decodeIdValue(existing), compareBSONValues(existingId, excludingId) == 0 {
                        continue
                    }
                }
                throw MonoError.duplicateKey(keyPattern: info.name, keyValue: key)
            }
        }
    }

    /// 预检查唯一索引约束（公开方法，获取写锁）
    public func checkUniqueConstraints(_ doc: BSONDocument, excludingId: BSONValue? = nil) async throws {
        try await writeQueue.withLock {
            try await self.checkUniqueConstraintsLocked(doc, excludingId: excludingId)
        }
    }

    private func decodeIdValue(_ data: Data) throws -> BSONValue? {
        // index value is BSON encoded { _id: <value> }
        let doc = try BSONDecoder().decode(data)
        return doc.getValue(forPath: "_id")
    }

    // MARK: - 文档索引操作

    /// 插入文档时更新索引（内部方法，需在 writeQueue 保护下调用）
    private func insertDocumentLocked(_ doc: BSONDocument) async throws {
        // 记录已成功更新的索引信息，用于失败时回滚
        var insertedEntries: [(index: Index, key: Data)] = []

        for (_, index) in indexes {
            let info = await index.info
            guard let key = encodeIndexEntryKey(keys: info.keys, doc: doc, unique: info.unique) else {
                continue
            }

            guard let idVal = doc.getValue(forPath: "_id") else {
                continue
            }

            let idDoc = BSONDocument([("_id", idVal)])
            let encoder = BSONEncoder()
            guard let idBytes = try? encoder.encode(idDoc) else {
                continue
            }

            do {
                try await index.insert(key: key, value: idBytes)
                insertedEntries.append((index: index, key: key))
            } catch {
                // 索引插入失败，回滚已成功的索引条目
                for entry in insertedEntries.reversed() {
                    try? await entry.index.delete(key: entry.key)
                }
                throw MonoError.internalError("failed to update index '\(info.name)': \(error)")
            }
        }
    }

    /// 插入文档时更新索引（公开方法，获取写锁）
    public func insertDocument(_ doc: BSONDocument) async throws {
        try await writeQueue.withLock {
            try await self.insertDocumentLocked(doc)
        }
    }

    /// 原子操作：检查唯一约束 + 插入索引条目（用于 Collection.insert）
    public func checkAndInsertDocument(_ doc: BSONDocument, excludingId: BSONValue? = nil) async throws {
        try await writeQueue.withLock {
            try await self.checkUniqueConstraintsLocked(doc, excludingId: excludingId)
            try await self.insertDocumentLocked(doc)
        }
    }

    /// 删除文档时更新索引（内部方法，需在 writeQueue 保护下调用）
    private func deleteDocumentLocked(_ doc: BSONDocument) async throws {
        // 记录已成功删除的索引信息，用于失败时回滚
        var deletedEntries: [(index: Index, key: Data, idBytes: Data)] = []

        // 预先获取 _id
        var idBytes: Data?
        if let idVal = doc.getValue(forPath: "_id") {
            let idDoc = BSONDocument([("_id", idVal)])
            let encoder = BSONEncoder()
            idBytes = try? encoder.encode(idDoc)
        }

        for (_, index) in indexes {
            let info = await index.info
            guard let key = encodeIndexEntryKey(keys: info.keys, doc: doc, unique: info.unique) else {
                continue
            }

            do {
                try await index.delete(key: key)
                if let bytes = idBytes {
                    deletedEntries.append((index: index, key: key, idBytes: bytes))
                }
            } catch {
                // 索引删除失败，回滚已成功删除的索引条目（重新插入）
                for entry in deletedEntries.reversed() {
                    try? await entry.index.insert(key: entry.key, value: entry.idBytes)
                }
                throw MonoError.internalError("failed to delete index '\(info.name)': \(error)")
            }
        }
    }

    /// 删除文档时更新索引（公开方法，获取写锁）
    public func deleteDocument(_ doc: BSONDocument) async throws {
        try await writeQueue.withLock {
            try await self.deleteDocumentLocked(doc)
        }
    }

    /// 回滚文档的索引条目
    public func rollbackDocumentById(_ docId: BSONValue) {
        // 由于我们没有完整的文档，无法生成正确的索引键来删除
        // 这个方法主要用于记录回滚意图
        // 在当前实现中，由于 insertDocument 在索引失败时已经回滚了数据，
        // 所以这里实际上不需要做额外操作
    }

    // MARK: - 索引恢复

    /// 从持久化的索引元数据恢复索引
    public func restoreIndexes(_ metas: [IndexMeta]) async {
        for meta in metas {
            let info = IndexInfo(
                name: meta.name,
                keys: meta.keys,
                unique: meta.unique,
                rootPageId: meta.rootPageId
            )
            let index = Index(pager: pager, info: info, rootPageId: meta.rootPageId)
            indexes[meta.name] = index
        }
    }
}
