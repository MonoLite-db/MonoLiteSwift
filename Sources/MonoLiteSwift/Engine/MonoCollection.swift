// Created by Yanjunhui

import Foundation

/// 数据库回调协议（用于保存 catalog）
public protocol DatabaseCallback: AnyObject, Sendable {
    func saveCatalog() async throws
}

/// 文档集合
/// 提供 MongoDB 风格的 CRUD 操作
public actor MonoCollection: DocumentFinder {
    /// 集合元信息
    public private(set) var info: CollectionInfo

    /// 数据库回调
    private var dbCallback: DatabaseCallback?

    /// 页面管理器
    private let pager: Pager

    /// 索引管理器
    private var indexManager: IndexManager?

    /// 写操作串行队列（解决 actor 重入导致的数据页+索引操作竞态问题）
    private let writeQueue = SerialWriteQueue()

    // MARK: - 初始化

    /// 创建集合
    public init(info: CollectionInfo, pager: Pager) async {
        self.info = info
        self.pager = pager

        // 创建索引管理器
        let im = IndexManager(pager: pager)
        self.indexManager = im

        // 设置文档查找器
        await im.setDocumentFinder(self)

        // 恢复索引
        if !info.indexes.isEmpty {
            await im.restoreIndexes(info.indexes)
        }
    }

    /// 设置数据库回调
    public func setDatabaseCallback(_ callback: DatabaseCallback) {
        self.dbCallback = callback
    }

    /// 供 Database 写入 catalog 的快照（包含最新索引元数据）
    public func catalogInfoForDatabase() async -> CollectionInfo {
        var snapshot = info
        if let im = indexManager {
            snapshot.indexes = await im.getIndexMetas()
        }
        return snapshot
    }

    /// 确保默认 `_id_` 唯一索引存在（与 Go 行为对齐）
    public func ensureIdIndex() async throws {
        // 已持久化的 meta 里已经有就跳过
        if info.indexes.contains(where: { $0.name == "_id_" }) {
            return
        }
        guard let im = indexManager else { return }

        // 如果 indexManager 已有也跳过
        if await im.getIndex("_id_") != nil {
            return
        }

        let keys = BSONDocument([("_id", .int32(1))])
        let options = BSONDocument([("name", .string("_id_")), ("unique", .bool(true))])
        _ = try await im.createIndex(keys: keys, options: options)
        info.indexes = await im.getIndexMetas()
        try await dbCallback?.saveCatalog()
    }

    /// 集合名称
    public var name: String {
        info.name
    }

    /// 文档数量
    public var count: Int64 {
        info.documentCount
    }

    // MARK: - 插入

    /// 插入一个或多个文档（使用写锁防止竞态）
    /// 返回插入文档的 _id 列表
    public func insert(_ docs: [BSONDocument]) async throws -> [BSONValue] {
        try await writeQueue.withLock {
            try await self.insertLocked(docs)
        }
    }

    /// 插入文档的内部实现（需在 writeQueue 保护下调用）
    private func insertLocked(_ docs: [BSONDocument]) async throws -> [BSONValue] {
        guard docs.count <= Limits.maxWriteBatchSize else {
            throw MonoError.badValue("insert batch size exceeds maximum of \(Limits.maxWriteBatchSize)")
        }

        var ids: [BSONValue] = []
        var insertedRecords: [InsertedRecord] = []

        for var doc in docs {
            // 验证文档结构
            try Validation.validateDocument(doc)

            // 确保 _id 字段存在
            let id = try ensureId(&doc)

            // 序列化文档
            let encoder = BSONEncoder()
            let data = try encoder.encode(doc)

            // 检查文档大小
            do {
                try Validation.checkDocumentSize(data)
            } catch {
                await rollbackInsertedRecordsLocked(insertedRecords)
                throw error
            }

            // 原子操作：检查唯一约束 + 插入索引
            // 注意：这里先写数据页，再更新索引（如果索引更新失败会回滚数据页）
            // 原因：数据页写入是本地操作，索引更新需要协调

            // 写入数据页
            let (pageId, slotIndex) = try await writeDocumentWithLocation(data)
            insertedRecords.append(InsertedRecord(pageId: pageId, slotIndex: slotIndex, id: id))

            // 检查唯一约束并更新索引（原子操作）
            if let im = indexManager {
                do {
                    try await im.checkAndInsertDocument(doc)
                } catch {
                    await rollbackInsertedRecordsLocked(insertedRecords)
                    throw error
                }
            }

            ids.append(id)
            info.documentCount += 1
        }

        // 保存集合元信息
        try await dbCallback?.saveCatalog()

        return ids
    }

    /// 插入单个文档
    public func insertOne(_ doc: BSONDocument) async throws -> BSONValue {
        let ids = try await insert([doc])
        return ids[0]
    }

    /// 插入单个文档（事务版本：加写锁 + 记录 undo）
    public func insertOne(_ doc: BSONDocument, in txn: Transaction?) async throws -> BSONValue {
        guard let txn else { return try await insertOne(doc) }
        try await txn.acquireLock(resource: "collection:\(info.name)", type: .write)
        let id = try await insertOne(doc)
        await txn.appendUndo(UndoRecord(operation: "insert", collection: info.name, docId: id, oldDoc: nil))
        return id
    }

    /// 确保文档有 _id 字段
    private func ensureId(_ doc: inout BSONDocument) throws -> BSONValue {
        if let existing = doc["_id"] {
            // 验证 _id 类型
            try validateIdType(existing)
            return existing
        }

        // 生成新 ObjectID
        let id = ObjectID.generate()
        var newDoc = BSONDocument([("_id", .objectId(id))])
        for (key, value) in doc {
            newDoc[key] = value
        }
        doc = newDoc

        return .objectId(id)
    }

    /// 验证 _id 类型
    private func validateIdType(_ value: BSONValue) throws {
        switch value {
        case .array:
            throw MonoError.invalidIdField("_id cannot be an array")
        case .regex:
            throw MonoError.invalidIdField("_id cannot be a regex")
        case .null:
            throw MonoError.invalidIdField("_id cannot be null")
        default:
            break
        }
    }

    /// 将文档写入数据页并返回位置
    private func writeDocumentWithLocation(_ data: Data) async throws -> (PageID, Int) {
        // 尝试在现有页面中插入
        if info.lastPageId != StorageConstants.invalidPageId {
            let page = try await pager.readPage(info.lastPageId)
            let sp = SlottedPage(page: page)

            if let slotIndex = try? sp.insertRecord(data) {
                await pager.markDirty(page.id)
                return (page.id, slotIndex)
            }
        }

        // 需要分配新页面
        let page = try await pager.allocatePage(type: .data)
        let sp = SlottedPage(page: page)

        let slotIndex = try sp.insertRecord(data)

        // 更新链表
        if info.firstPageId == StorageConstants.invalidPageId {
            info.firstPageId = page.id
        } else {
            // 链接到上一页
            let lastPage = try await pager.readPage(info.lastPageId)
            lastPage.nextPageId = page.id
            page.prevPageId = info.lastPageId
            await pager.markDirty(info.lastPageId)
        }

        info.lastPageId = page.id
        await pager.markDirty(page.id)

        return (page.id, slotIndex)
    }

    /// 回滚已插入的记录（需在 writeQueue 保护下调用）
    private func rollbackInsertedRecordsLocked(_ records: [InsertedRecord]) async {
        for rec in records {
            // 回滚数据
            if let page = try? await pager.readPage(rec.pageId) {
                let sp = SlottedPage(page: page)
                try? sp.deleteRecord(at: rec.slotIndex)
                await pager.markDirty(rec.pageId)
            }

            // 回滚索引
            await indexManager?.rollbackDocumentById(rec.id)

            info.documentCount -= 1
        }
    }

    // MARK: - 查询

    /// 查询文档
    public func find(_ filter: BSONDocument) async throws -> [BSONDocument] {
        try await findUnlocked(filter: filter)
    }

    /// 无锁查询（内部使用，实现 DocumentFinder 协议）
    public func findUnlocked(filter: BSONDocument) async throws -> [BSONDocument] {
        var results: [BSONDocument] = []
        let matcher = FilterMatcher(filter)

        var currentPageId = info.firstPageId
        while currentPageId != StorageConstants.invalidPageId {
            let page = try await pager.readPage(currentPageId)
            let docs = try readDocumentsFromPage(page)

            for doc in docs {
                if matcher.match(doc) {
                    results.append(doc)
                }
            }

            currentPageId = page.nextPageId
        }

        return results
    }

    /// 带选项的查询
    public func findWithOptions(_ filter: BSONDocument, options: QueryOptions) async throws -> [BSONDocument] {
        var results = try await find(filter)
        results = applyQueryOptions(results, options)
        return results
    }

    /// 查询单个文档
    public func findOne(_ filter: BSONDocument) async throws -> BSONDocument? {
        let matcher = FilterMatcher(filter)

        var currentPageId = info.firstPageId
        while currentPageId != StorageConstants.invalidPageId {
            let page = try await pager.readPage(currentPageId)
            let docs = try readDocumentsFromPage(page)

            for doc in docs {
                if matcher.match(doc) {
                    return doc
                }
            }

            currentPageId = page.nextPageId
        }

        return nil
    }

    /// 根据 _id 查询文档
    public func findById(_ id: BSONValue) async throws -> BSONDocument? {
        let filter = BSONDocument([("_id", id)])
        return try await findOne(filter)
    }

    /// 从页面读取所有文档
    private func readDocumentsFromPage(_ page: Page) throws -> [BSONDocument] {
        var docs: [BSONDocument] = []
        let sp = SlottedPage(page: page)

        for i in 0..<Int(page.itemCount) {
            guard let record = try? sp.getRecord(at: i) else {
                continue
            }

            guard record.count >= 5 else {  // BSON 最小长度
                continue
            }

            let decoder = BSONDecoder()
            if let doc = try? decoder.decode(record) {
                docs.append(doc)
            }
        }

        return docs
    }

    // MARK: - 更新

    /// 更新文档
    public func update(_ filter: BSONDocument, update updateDoc: BSONDocument, upsert: Bool = false) async throws -> UpdateResult {
        try await writeQueue.withLock {
            try await self.updateLocked(filter, update: updateDoc, upsert: upsert)
        }
    }

    /// 更新多个文档（内部实现，需在 writeQueue 保护下调用）
    private func updateLocked(_ filter: BSONDocument, update updateDoc: BSONDocument, upsert: Bool) async throws -> UpdateResult {
        var result = UpdateResult()

        var currentPageId = info.firstPageId
        while currentPageId != StorageConstants.invalidPageId {
            let page = try await pager.readPage(currentPageId)
            let sp = SlottedPage(page: page)

            for i in 0..<Int(page.itemCount) {
                guard let record = try? sp.getRecord(at: i) else {
                    continue
                }

                guard record.count >= 5 else {
                    continue
                }

                let decoder = BSONDecoder()
                guard var doc = try? decoder.decode(record) else {
                    continue
                }

                let matcher = FilterMatcher(filter)
                if matcher.match(doc) {
                    result.matchedCount += 1

                    let originalDoc = copyDocument(doc)

                    // 应用更新操作
                    try applyUpdate(&doc, update: updateDoc)

                    // 重新序列化
                    let encoder = BSONEncoder()
                    let newData = try encoder.encode(doc)

                    // 检查数据是否改变
                    if record != newData {
                        // 检查唯一约束
                        if let im = indexManager {
                            try await im.checkUniqueConstraints(doc)
                        }

                        // 更新记录
                        try sp.updateRecord(at: i, newData)

                        // 更新索引
                        if let im = indexManager {
                            try await im.deleteDocument(originalDoc)
                            try await im.insertDocument(doc)
                        }

                        await pager.markDirty(page.id)
                        result.modifiedCount += 1
                    }
                }
            }

            currentPageId = page.nextPageId
        }

        // 处理 upsert
        if result.matchedCount == 0 && upsert {
            var newDoc = BSONDocument()

            // 复制 filter 中的非操作符字段
            for (key, value) in filter {
                if !key.hasPrefix("$") {
                    newDoc[key] = value
                }
            }

            // 应用更新
            try applyUpdate(&newDoc, update: updateDoc)

            let id = try await insertLocked([newDoc]).first ?? .null
            result.upsertedCount = 1
            result.upsertedID = id
            result.matchedCount = 0
        }

        try await dbCallback?.saveCatalog()

        return result
    }

    /// 更新单个文档（使用写锁防止竞态）
    public func updateOne(_ filter: BSONDocument, update updateDoc: BSONDocument, upsert: Bool = false) async throws -> UpdateResult {
        try await writeQueue.withLock {
            try await self.updateOneLocked(filter, update: updateDoc, upsert: upsert)
        }
    }

    /// 更新单个文档（内部实现，需在 writeQueue 保护下调用）
    private func updateOneLocked(_ filter: BSONDocument, update updateDoc: BSONDocument, upsert: Bool) async throws -> UpdateResult {
        var result = UpdateResult()

        var currentPageId = info.firstPageId
        while currentPageId != StorageConstants.invalidPageId {
            let page = try await pager.readPage(currentPageId)
            let sp = SlottedPage(page: page)

            for i in 0..<Int(page.itemCount) {
                guard let record = try? sp.getRecord(at: i) else {
                    continue
                }

                guard record.count >= 5 else {
                    continue
                }

                let decoder = BSONDecoder()
                guard var doc = try? decoder.decode(record) else {
                    continue
                }

                let matcher = FilterMatcher(filter)
                if matcher.match(doc) {
                    result.matchedCount = 1

                    let originalDoc = copyDocument(doc)

                    try applyUpdate(&doc, update: updateDoc)

                    let encoder = BSONEncoder()
                    let newData = try encoder.encode(doc)

                    if record != newData {
                        if let im = indexManager {
                            let currentId = originalDoc.getValue(forPath: "_id")
                            try await im.checkUniqueConstraints(doc, excludingId: currentId)
                        }

                        try sp.updateRecord(at: i, newData)

                        if let im = indexManager {
                            try await im.deleteDocument(originalDoc)
                            try await im.insertDocument(doc)
                        }

                        await pager.markDirty(page.id)
                        result.modifiedCount = 1
                    }

                    try await dbCallback?.saveCatalog()
                    return result
                }
            }

            currentPageId = page.nextPageId
        }

        // 处理 upsert
        if result.matchedCount == 0 && upsert {
            var newDoc = BSONDocument()
            for (key, value) in filter {
                if !key.hasPrefix("$") {
                    newDoc[key] = value
                }
            }
            try applyUpdate(&newDoc, update: updateDoc)

            let id = try await insertLocked([newDoc]).first ?? .null
            result.upsertedCount = 1
            result.upsertedID = id
        }

        return result
    }

    /// 更新单个文档（事务版本：加写锁 + 记录 undo）
    public func updateOne(_ filter: BSONDocument, update: BSONDocument, upsert: Bool = false, in txn: Transaction?) async throws -> UpdateResult {
        guard let txn else { return try await updateOne(filter, update: update, upsert: upsert) }
        try await txn.acquireLock(resource: "collection:\(info.name)", type: .write)

        let old = try await findOne(filter)
        let res = try await updateOne(filter, update: update, upsert: upsert)
        if res.modifiedCount > 0, let old, let id = old.getValue(forPath: "_id") {
            await txn.appendUndo(UndoRecord(operation: "update", collection: info.name, docId: id, oldDoc: old))
        }
        return res
    }

    // MARK: - 删除

    /// 删除文档
    public func delete(_ filter: BSONDocument) async throws -> Int64 {
        try await writeQueue.withLock {
            try await self.deleteLocked(filter)
        }
    }

    /// 删除多个文档（内部实现，需在 writeQueue 保护下调用）
    private func deleteLocked(_ filter: BSONDocument) async throws -> Int64 {
        var deletedCount: Int64 = 0
        let matcher = FilterMatcher(filter)

        var currentPageId = info.firstPageId
        while currentPageId != StorageConstants.invalidPageId {
            let page = try await pager.readPage(currentPageId)
            let sp = SlottedPage(page: page)

            for i in 0..<Int(page.itemCount) {
                guard let record = try? sp.getRecord(at: i) else {
                    continue
                }

                guard record.count >= 5 else {
                    continue
                }

                let decoder = BSONDecoder()
                guard let doc = try? decoder.decode(record) else {
                    continue
                }

                if matcher.match(doc) {
                    // 删除索引
                    if let im = indexManager {
                        try await im.deleteDocument(doc)
                    }

                    // 删除记录
                    try sp.deleteRecord(at: i)
                    await pager.markDirty(page.id)
                    deletedCount += 1
                    info.documentCount -= 1
                }
            }

            currentPageId = page.nextPageId
        }

        try await dbCallback?.saveCatalog()
        return deletedCount
    }

    /// 删除单个文档（使用写锁防止竞态）
    public func deleteOne(_ filter: BSONDocument) async throws -> Int64 {
        try await writeQueue.withLock {
            try await self.deleteOneLocked(filter)
        }
    }

    /// 删除单个文档（内部实现，需在 writeQueue 保护下调用）
    private func deleteOneLocked(_ filter: BSONDocument) async throws -> Int64 {
        let matcher = FilterMatcher(filter)

        var currentPageId = info.firstPageId
        while currentPageId != StorageConstants.invalidPageId {
            let page = try await pager.readPage(currentPageId)
            let sp = SlottedPage(page: page)

            for i in 0..<Int(page.itemCount) {
                guard let record = try? sp.getRecord(at: i) else {
                    continue
                }

                guard record.count >= 5 else {
                    continue
                }

                let decoder = BSONDecoder()
                guard let doc = try? decoder.decode(record) else {
                    continue
                }

                if matcher.match(doc) {
                    if let im = indexManager {
                        try await im.deleteDocument(doc)
                    }

                    try sp.deleteRecord(at: i)
                    await pager.markDirty(page.id)
                    info.documentCount -= 1

                    try await dbCallback?.saveCatalog()
                    return 1
                }
            }

            currentPageId = page.nextPageId
        }

        return 0
    }

    /// 删除单个文档（事务版本：加写锁 + 记录 undo）
    public func deleteOne(_ filter: BSONDocument, in txn: Transaction?) async throws -> Int64 {
        guard let txn else { return try await deleteOne(filter) }
        try await txn.acquireLock(resource: "collection:\(info.name)", type: .write)

        let old = try await findOne(filter)
        let deleted = try await deleteOne(filter)
        if deleted > 0, let old, let id = old.getValue(forPath: "_id") {
            await txn.appendUndo(UndoRecord(operation: "delete", collection: info.name, docId: id, oldDoc: old))
        }
        return deleted
    }

    // MARK: - 索引操作

    /// 创建索引
    public func createIndex(keys: BSONDocument, options: BSONDocument = BSONDocument()) async throws -> String {
        guard let im = indexManager else {
            throw MonoError.internalError("index manager not initialized")
        }

        let name = try await im.createIndex(keys: keys, options: options)

        // 更新集合元信息
        info.indexes = await im.getIndexMetas()
        try await dbCallback?.saveCatalog()

        return name
    }

    /// 删除索引
    public func dropIndex(_ name: String) async throws {
        guard let im = indexManager else {
            throw MonoError.internalError("index manager not initialized")
        }

        try await im.dropIndex(name)

        info.indexes = await im.getIndexMetas()
        try await dbCallback?.saveCatalog()
    }

    /// 列出所有索引
    public func listIndexes() async -> [BSONDocument] {
        guard let im = indexManager else { return [] }
        return await im.listIndexes()
    }

    // MARK: - 其他操作

    /// 获取不重复值
    public func distinct(_ field: String, filter: BSONDocument = BSONDocument()) async throws -> [BSONValue] {
        let docs = try await find(filter)

        var seen = Set<String>()
        var result: [BSONValue] = []

        for doc in docs {
            if let val = doc.getValue(forPath: field) {
                let key = "\(val)"
                if !seen.contains(key) {
                    seen.insert(key)
                    result.append(val)
                }
            }
        }

        return result
    }

    /// 替换单个文档（使用写锁防止竞态）
    public func replaceOne(_ filter: BSONDocument, replacement: BSONDocument) async throws -> Int64 {
        try await writeQueue.withLock {
            try await self.replaceOneLocked(filter, replacement: replacement)
        }
    }

    /// 替换单个文档（内部实现，需在 writeQueue 保护下调用）
    private func replaceOneLocked(_ filter: BSONDocument, replacement: BSONDocument) async throws -> Int64 {
        let matcher = FilterMatcher(filter)

        var currentPageId = info.firstPageId
        while currentPageId != StorageConstants.invalidPageId {
            let page = try await pager.readPage(currentPageId)
            let sp = SlottedPage(page: page)

            for i in 0..<Int(page.itemCount) {
                guard let record = try? sp.getRecord(at: i) else {
                    continue
                }

                guard record.count >= 5 else {
                    continue
                }

                let decoder = BSONDecoder()
                guard let doc = try? decoder.decode(record) else {
                    continue
                }

                if matcher.match(doc) {
                    // 保留原 _id
                    let originalId = doc["_id"]

                    var newDoc = BSONDocument()
                    if let id = originalId {
                        newDoc["_id"] = id
                    }

                    for (key, value) in replacement {
                        if key != "_id" {
                            newDoc[key] = value
                        }
                    }

                    // 检查唯一约束
                    if let im = indexManager {
                        let currentId = doc.getValue(forPath: "_id")
                        try await im.checkUniqueConstraints(newDoc, excludingId: currentId)
                    }

                    let encoder = BSONEncoder()
                    let newData = try encoder.encode(newDoc)

                    try sp.updateRecord(at: i, newData)

                    // 更新索引
                    if let im = indexManager {
                        try await im.deleteDocument(doc)
                        try await im.insertDocument(newDoc)
                    }

                    await pager.markDirty(page.id)
                    try await dbCallback?.saveCatalog()

                    return 1
                }
            }

            currentPageId = page.nextPageId
        }

        return 0
    }

    /// 替换单个文档（事务版本：加写锁 + 记录 undo）
    public func replaceOne(_ filter: BSONDocument, replacement: BSONDocument, in txn: Transaction?) async throws -> Int64 {
        guard let txn else { return try await replaceOne(filter, replacement: replacement) }
        try await txn.acquireLock(resource: "collection:\(info.name)", type: .write)

        let old = try await findOne(filter)
        let modified = try await replaceOne(filter, replacement: replacement)
        if modified > 0, let old, let id = old.getValue(forPath: "_id") {
            await txn.appendUndo(UndoRecord(operation: "update", collection: info.name, docId: id, oldDoc: old))
        }
        return modified
    }
}
