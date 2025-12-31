// Created by Yanjunhui

import Foundation

/// 数据库实例（对齐 Go: engine/database.go）
/// EN: Database instance (aligned with Go: engine/database.go)
///
/// 说明：
/// EN: Description:
/// - 负责 catalog 的加载/保存
/// EN: - Responsible for loading/saving the catalog
/// - 管理 collections 生命周期
/// EN: - Manages collections lifecycle
/// - 作为 `MonoCollection` 的 `DatabaseCallback`，在集合元数据变更时持久化 catalog
/// EN: - Acts as `DatabaseCallback` for `MonoCollection`, persisting catalog when collection metadata changes
public actor Database: DatabaseCallback {
    public let path: String

    /// 页面管理器（internal：供 validate/transaction/protocol 层访问，同模块，不暴露给外部使用者）
    /// EN: Pager (internal: accessible by validate/transaction/protocol layers within the module, not exposed to external users)
    let pager: Pager

    /// 集合字典（internal：供 validate/transaction/protocol 层访问，同模块）
    /// EN: Collections dictionary (internal: accessible by validate/transaction/protocol layers within the module)
    var collections: [String: MonoCollection] = [:]

    /// Go: catalogMagic = "MPCT" little-endian
    /// EN: Go: catalogMagic = "MPCT" little-endian
    static let catalogMagic: UInt32 = 0x4D504354

    /// catalog 头部长度：magic(4) + totalLen(4) + pageCount(4)
    /// EN: Catalog header length: magic(4) + totalLen(4) + pageCount(4)
    static let catalogHeaderLen: Int = 12

    /// 事务管理器（对齐 Go database.go：txnManager）
    /// EN: Transaction manager (aligned with Go database.go: txnManager)
    /// lazy：避免 init 阶段 self 逃逸
    /// EN: lazy: avoid self escaping during init phase
    public lazy var txnManager: TransactionManager = TransactionManager(db: self)

    /// 会话管理器（对齐 Go database.go：sessionManager）
    /// EN: Session manager (aligned with Go database.go: sessionManager)
    public lazy var sessionManager: SessionManager = SessionManager(db: self)

    /// 游标管理器
    /// EN: Cursor manager
    public lazy var cursorManager: CursorManager = CursorManager()

    /// 数据库启动时间
    /// EN: Database start time
    internal let startTime: Date = Date()

    /// 初始化数据库
    /// EN: Initialize database
    /// - Parameters:
    ///   - path: 数据库文件路径 / EN: Database file path
    ///   - enableWAL: 是否启用 WAL / EN: Whether to enable WAL
    public init(path: String, enableWAL: Bool = true) async throws {
        self.path = path
        self.pager = try await Pager(path: path, enableWAL: enableWAL)
        try await loadCatalog()
    }

    // MARK: - 集合 API / Collection APIs

    /// 只获取集合，不创建（MongoDB 行为：读操作不隐式创建集合）
    /// EN: Only get collection, do not create (MongoDB behavior: read operations do not implicitly create collections)
    public func getCollection(_ name: String) -> MonoCollection? {
        collections[name]
    }

    /// 获取或创建集合
    /// EN: Get or create collection
    public func collection(_ name: String) async throws -> MonoCollection {
        try validateCollectionName(name)

        if let existing = collections[name] {
            return existing
        }

        var info = CollectionInfo(name: name)
        // Go 语义：空指针用 0
        // EN: Go semantics: null pointer uses 0
        info.firstPageId = 0
        info.lastPageId = 0

        let col = await MonoCollection(info: info, pager: pager)
        await col.setDatabaseCallback(self)

        // 确保默认 _id_ 唯一索引存在（真实索引，不是虚拟输出）
        // EN: Ensure default _id_ unique index exists (real index, not virtual output)
        try await col.ensureIdIndex()

        collections[name] = col
        try await saveCatalog()
        return col
    }

    /// 删除集合（不存在视为成功）
    /// EN: Drop collection (non-existence is considered success)
    public func dropCollection(_ name: String) async throws {
        guard let col = collections[name] else { return }

        let info = await col.info

        // 释放数据页链表
        // EN: Free data page chain
        var current = info.firstPageId
        while current != 0 {
            let page = try await pager.readPage(current)
            let next = page.nextPageId
            try await pager.freePage(current)
            current = next
        }

        // 释放索引页（尽量回收）
        // EN: Free index pages (try to reclaim)
        for meta in info.indexes {
            // _id_ 也属于索引页，需要回收
            // EN: _id_ is also an index page, needs to be reclaimed
            let tree = BTree(pager: pager, rootPageId: meta.rootPageId, name: meta.name, unique: meta.unique)
            try await tree.drop()
        }

        collections.removeValue(forKey: name)
        try await saveCatalog()
    }

    /// 列出所有集合名称
    /// EN: List all collection names
    public func listCollections() -> [String] {
        collections.keys.sorted()
    }

    // MARK: - Catalog 持久化 / Catalog Persistence

    /// 保存 catalog 到磁盘
    /// EN: Save catalog to disk
    public func saveCatalog() async throws {
        let bson = try await buildCatalogBSON()
        if bson.count <= StorageConstants.maxPageData {
            try await saveCatalogSinglePage(bson)
        } else {
            try await saveCatalogMultiPage(bson)
        }
    }

    /// 构建 catalog 的 BSON 数据
    /// EN: Build BSON data for catalog
    private func buildCatalogBSON() async throws -> Data {
        // Go: catalogData { collections: [...] }
        // EN: Go: catalogData { collections: [...] }
        var collectionsArr = BSONArray()

        for (_, col) in collections {
            let info = await col.catalogInfoForDatabase()
            collectionsArr.append(.document(info.toBSONDocument()))
        }

        var root = BSONDocument()
        root["collections"] = .array(collectionsArr)

        let enc = BSONEncoder()
        return try enc.encode(root)
    }

    /// 从磁盘加载 catalog
    /// EN: Load catalog from disk
    private func loadCatalog() async throws {
        let catalogPageId = await pager.catalogPageId
        guard catalogPageId != 0 else { return }

        let firstPage = try await pager.readPage(catalogPageId)
        let data = firstPage.data

        guard data.count >= 5 else { return }

        let magic = DataEndian.readUInt32LE(data, at: 0)
        if magic == Self.catalogMagic {
            try await loadCatalogMultiPage(firstPage: firstPage)
            return
        }

        // 单页格式：u32 bsonLen + bson bytes（向后兼容 Go 单页格式）
        // EN: Single page format: u32 bsonLen + bson bytes (backward compatible with Go single page format)
        let bsonLen = Int(DataEndian.readUInt32LE(data, at: 0))
        guard bsonLen >= 5, bsonLen <= data.count else { return }

        let bsonData = Data(data[0..<bsonLen])
        try await restoreCollectionsFromCatalog(bsonData)
    }

    /// 从多页 catalog 加载数据
    /// EN: Load data from multi-page catalog
    private func loadCatalogMultiPage(firstPage: Page) async throws {
        let data = firstPage.data
        guard data.count >= Self.catalogHeaderLen else {
            throw MonoError.internalError("invalid multi-page catalog header")
        }

        let totalLen = Int(DataEndian.readUInt32LE(data, at: 4))
        let pageCount = Int(DataEndian.readUInt32LE(data, at: 8))
        guard totalLen > 0, pageCount > 0 else {
            throw MonoError.internalError("invalid multi-page catalog: totalLen=\(totalLen) pageCount=\(pageCount)")
        }

        let firstCap = StorageConstants.maxPageData - Self.catalogHeaderLen
        var bsonData = Data()
        bsonData.reserveCapacity(totalLen)

        let takeFirst = min(totalLen, firstCap)
        if takeFirst > 0 {
            bsonData.append(data[Self.catalogHeaderLen..<Self.catalogHeaderLen + takeFirst])
        }

        var currentId = firstPage.nextPageId
        while bsonData.count < totalLen && currentId != 0 {
            let page = try await pager.readPage(currentId)
            let remain = min(totalLen - bsonData.count, page.data.count)
            bsonData.append(page.data.prefix(remain))
            currentId = page.nextPageId
        }

        guard bsonData.count >= totalLen else {
            throw MonoError.internalError("incomplete multi-page catalog: got \(bsonData.count), expected \(totalLen)")
        }

        try await restoreCollectionsFromCatalog(bsonData.prefix(totalLen))
    }

    /// 从 catalog BSON 数据恢复集合
    /// EN: Restore collections from catalog BSON data
    private func restoreCollectionsFromCatalog(_ bsonData: Data) async throws {
        let dec = BSONDecoder()
        let root = try dec.decode(bsonData)

        guard let colsVal = root["collections"], case .array(let colsArr) = colsVal else {
            return
        }

        for item in colsArr {
            guard case .document(let colDoc) = item,
                  let info = CollectionInfo.fromBSONDocument(colDoc) else {
                continue
            }

            let col = await MonoCollection(info: info, pager: pager)
            await col.setDatabaseCallback(self)
            // 确保 _id_ index 存在（对老数据也要补齐）
            // EN: Ensure _id_ index exists (also for old data)
            try? await col.ensureIdIndex()
            collections[info.name] = col
        }
    }

    /// 保存单页 catalog
    /// EN: Save single page catalog
    private func saveCatalogSinglePage(_ bsonData: Data) async throws {
        let catalogPageId = await pager.catalogPageId
        let page: Page

        if catalogPageId == 0 {
            page = try await pager.allocatePage(type: .catalog)
            try await pager.setCatalogPageId(page.id)
        } else {
            page = try await pager.readPage(catalogPageId)
            // 如果之前是多页 catalog，清理后续链
            // EN: If previously multi-page catalog, clean up subsequent chain
            try await freeCatalogChain(page.nextPageId)
            page.nextPageId = 0
        }

        page.clearData()
        page.pageType = .catalog
        page.setData(bsonData, at: 0)
        await pager.markDirty(page.id)
    }

    /// 保存多页 catalog
    /// EN: Save multi-page catalog
    private func saveCatalogMultiPage(_ bsonData: Data) async throws {
        let firstCap = StorageConstants.maxPageData - Self.catalogHeaderLen
        let subsequentCap = StorageConstants.maxPageData

        var pagesNeeded = 1
        if bsonData.count > firstCap {
            let remaining = bsonData.count - firstCap
            pagesNeeded += (remaining + subsequentCap - 1) / subsequentCap
        }

        // 构建头部
        // EN: Build header
        var header = Data(count: Self.catalogHeaderLen)
        DataEndian.writeUInt32LE(Self.catalogMagic, to: &header, at: 0)
        DataEndian.writeUInt32LE(UInt32(bsonData.count), to: &header, at: 4)
        DataEndian.writeUInt32LE(UInt32(pagesNeeded), to: &header, at: 8)

        // 获取或分配第一页
        // EN: Get or allocate first page
        let catalogPageId = await pager.catalogPageId
        let firstPage: Page
        if catalogPageId == 0 {
            firstPage = try await pager.allocatePage(type: .catalog)
            try await pager.setCatalogPageId(firstPage.id)
        } else {
            firstPage = try await pager.readPage(catalogPageId)
        }

        // 写第一页：header + data chunk
        // EN: Write first page: header + data chunk
        let dataForFirst = bsonData.prefix(firstCap)
        var firstData = Data(count: StorageConstants.maxPageData)
        firstData.replaceSubrange(0..<Self.catalogHeaderLen, with: header)
        firstData.replaceSubrange(Self.catalogHeaderLen..<(Self.catalogHeaderLen + dataForFirst.count), with: dataForFirst)

        firstPage.clearData()
        firstPage.pageType = .catalog
        firstPage.setData(firstData, at: 0)
        await pager.markDirty(firstPage.id)

        // 写后续页
        // EN: Write subsequent pages
        var dataOffset = dataForFirst.count
        var currentPage = firstPage
        var existingNextId = currentPage.nextPageId

        while dataOffset < bsonData.count {
            let nextPage: Page
            if existingNextId != 0 {
                nextPage = try await pager.readPage(existingNextId)
                existingNextId = nextPage.nextPageId
            } else {
                nextPage = try await pager.allocatePage(type: .catalog)
            }

            // 链接 prev/next
            // EN: Link prev/next
            currentPage.nextPageId = nextPage.id
            nextPage.prevPageId = currentPage.id

            let chunk = bsonData.dropFirst(dataOffset).prefix(subsequentCap)
            var pageData = Data(count: StorageConstants.maxPageData)
            pageData.replaceSubrange(0..<chunk.count, with: chunk)

            nextPage.clearData()
            nextPage.pageType = .catalog
            nextPage.setData(pageData, at: 0)
            await pager.markDirty(nextPage.id)
            await pager.markDirty(currentPage.id)

            currentPage = nextPage
            dataOffset += chunk.count
        }

        // 清理多余的后续页面（从多页切换到更短的多页/单页等）
        // EN: Clean up excess subsequent pages (switching from multi-page to shorter multi-page/single-page, etc.)
        if currentPage.nextPageId != 0 {
            try await freeCatalogChain(currentPage.nextPageId)
            currentPage.nextPageId = 0
            await pager.markDirty(currentPage.id)
        }
    }

    /// 释放 catalog 页面链
    /// EN: Free catalog page chain
    private func freeCatalogChain(_ startId: PageID) async throws {
        var current = startId
        while current != 0 {
            let page = try await pager.readPage(current)
            let next = page.nextPageId
            try await pager.freePage(current)
            current = next
        }
    }

    // MARK: - 关闭 / Close

    /// 关闭数据库
    /// EN: Close database
    public func close() async throws {
        // 先停止后台任务，以便 `swift test` 能够干净退出
        // EN: Stop background tasks first so `swift test` can exit cleanly
        await cursorManager.closeAll()
        await sessionManager.shutdown()
        try await pager.close()
    }

    /// 刷新数据到磁盘
    /// EN: Flush data to disk
    public func flush() async throws {
        try await pager.flush()
    }
}

// MARK: - 验证辅助函数（对齐 Go）/ Validation Helpers (Go-aligned)

/// 验证集合名称
/// EN: Validate collection name
private func validateCollectionName(_ name: String) throws {
    if name.isEmpty {
        throw MonoError.invalidNamespace("collection name cannot be empty")
    }
    if name.hasPrefix("system.") {
        throw MonoError.invalidNamespace("collection name cannot start with 'system.'")
    }
    if name.contains("$") {
        throw MonoError.invalidNamespace("collection name cannot contain '$'")
    }
    if name.contains("\u{0000}") {
        throw MonoError.invalidNamespace("collection name cannot contain null bytes")
    }
    if name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
        throw MonoError.invalidNamespace("collection name cannot be empty or whitespace only")
    }
}
