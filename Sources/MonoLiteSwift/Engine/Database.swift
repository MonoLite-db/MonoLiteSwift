// Created by Yanjunhui

import Foundation

/// 数据库实例（对齐 Go: engine/database.go）
///
/// 说明：
/// - 负责 catalog 的加载/保存
/// - 管理 collections 生命周期
/// - 作为 `MonoCollection` 的 `DatabaseCallback`，在集合元数据变更时持久化 catalog
public actor Database: DatabaseCallback {
    public let path: String
    // internal：供 validate/transaction/protocol 层访问（同模块），不暴露给外部使用者
    let pager: Pager

    // internal：供 validate/transaction/protocol 层访问（同模块）
    var collections: [String: MonoCollection] = [:]

    // Go: catalogMagic = "MPCT" little-endian
    static let catalogMagic: UInt32 = 0x4D504354
    static let catalogHeaderLen: Int = 12 // magic(4) + totalLen(4) + pageCount(4)

    // Managers (对齐 Go database.go：txnManager/sessionManager)
    // lazy：避免 init 阶段 self 逃逸
    public lazy var txnManager: TransactionManager = TransactionManager(db: self)
    public lazy var sessionManager: SessionManager = SessionManager(db: self)
    public lazy var cursorManager: CursorManager = CursorManager()
    internal let startTime: Date = Date()

    public init(path: String, enableWAL: Bool = true) async throws {
        self.path = path
        self.pager = try await Pager(path: path, enableWAL: enableWAL)
        try await loadCatalog()
    }

    // MARK: - Collection APIs

    /// 只获取集合，不创建（MongoDB 行为：读操作不隐式创建集合）
    public func getCollection(_ name: String) -> MonoCollection? {
        collections[name]
    }

    /// 获取或创建集合
    public func collection(_ name: String) async throws -> MonoCollection {
        try validateCollectionName(name)

        if let existing = collections[name] {
            return existing
        }

        var info = CollectionInfo(name: name)
        // Go 语义：空指针用 0
        info.firstPageId = 0
        info.lastPageId = 0

        let col = await MonoCollection(info: info, pager: pager)
        await col.setDatabaseCallback(self)

        // 确保默认 _id_ 唯一索引存在（真实索引，不是虚拟输出）
        try await col.ensureIdIndex()

        collections[name] = col
        try await saveCatalog()
        return col
    }

    /// 删除集合（不存在视为成功）
    public func dropCollection(_ name: String) async throws {
        guard let col = collections[name] else { return }

        let info = await col.info

        // 释放数据页链表
        var current = info.firstPageId
        while current != 0 {
            let page = try await pager.readPage(current)
            let next = page.nextPageId
            try await pager.freePage(current)
            current = next
        }

        // 释放索引页（尽量回收）
        for meta in info.indexes {
            // _id_ 也属于索引页，需要回收
            let tree = BTree(pager: pager, rootPageId: meta.rootPageId, name: meta.name, unique: meta.unique)
            try await tree.drop()
        }

        collections.removeValue(forKey: name)
        try await saveCatalog()
    }

    public func listCollections() -> [String] {
        collections.keys.sorted()
    }

    // MARK: - Catalog persistence

    public func saveCatalog() async throws {
        let bson = try await buildCatalogBSON()
        if bson.count <= StorageConstants.maxPageData {
            try await saveCatalogSinglePage(bson)
        } else {
            try await saveCatalogMultiPage(bson)
        }
    }

    private func buildCatalogBSON() async throws -> Data {
        // Go: catalogData { collections: [...] }
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
        let bsonLen = Int(DataEndian.readUInt32LE(data, at: 0))
        guard bsonLen >= 5, bsonLen <= data.count else { return }

        let bsonData = Data(data[0..<bsonLen])
        try await restoreCollectionsFromCatalog(bsonData)
    }

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
            try? await col.ensureIdIndex()
            collections[info.name] = col
        }
    }

    private func saveCatalogSinglePage(_ bsonData: Data) async throws {
        let catalogPageId = await pager.catalogPageId
        let page: Page

        if catalogPageId == 0 {
            page = try await pager.allocatePage(type: .catalog)
            try await pager.setCatalogPageId(page.id)
        } else {
            page = try await pager.readPage(catalogPageId)
            // 如果之前是多页 catalog，清理后续链
            try await freeCatalogChain(page.nextPageId)
            page.nextPageId = 0
        }

        page.clearData()
        page.pageType = .catalog
        page.setData(bsonData, at: 0)
        await pager.markDirty(page.id)
    }

    private func saveCatalogMultiPage(_ bsonData: Data) async throws {
        let firstCap = StorageConstants.maxPageData - Self.catalogHeaderLen
        let subsequentCap = StorageConstants.maxPageData

        var pagesNeeded = 1
        if bsonData.count > firstCap {
            let remaining = bsonData.count - firstCap
            pagesNeeded += (remaining + subsequentCap - 1) / subsequentCap
        }

        // header
        var header = Data(count: Self.catalogHeaderLen)
        DataEndian.writeUInt32LE(Self.catalogMagic, to: &header, at: 0)
        DataEndian.writeUInt32LE(UInt32(bsonData.count), to: &header, at: 4)
        DataEndian.writeUInt32LE(UInt32(pagesNeeded), to: &header, at: 8)

        // 获取或分配第一页
        let catalogPageId = await pager.catalogPageId
        let firstPage: Page
        if catalogPageId == 0 {
            firstPage = try await pager.allocatePage(type: .catalog)
            try await pager.setCatalogPageId(firstPage.id)
        } else {
            firstPage = try await pager.readPage(catalogPageId)
        }

        // 写第一页：header + data chunk
        let dataForFirst = bsonData.prefix(firstCap)
        var firstData = Data(count: StorageConstants.maxPageData)
        firstData.replaceSubrange(0..<Self.catalogHeaderLen, with: header)
        firstData.replaceSubrange(Self.catalogHeaderLen..<(Self.catalogHeaderLen + dataForFirst.count), with: dataForFirst)

        firstPage.clearData()
        firstPage.pageType = .catalog
        firstPage.setData(firstData, at: 0)
        await pager.markDirty(firstPage.id)

        // 写后续页
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
        if currentPage.nextPageId != 0 {
            try await freeCatalogChain(currentPage.nextPageId)
            currentPage.nextPageId = 0
            await pager.markDirty(currentPage.id)
        }
    }

    private func freeCatalogChain(_ startId: PageID) async throws {
        var current = startId
        while current != 0 {
            let page = try await pager.readPage(current)
            let next = page.nextPageId
            try await pager.freePage(current)
            current = next
        }
    }

    // MARK: - Close

    public func close() async throws {
        // Stop background tasks first so `swift test` can exit cleanly.
        await cursorManager.closeAll()
        await sessionManager.shutdown()
        try await pager.close()
    }

    public func flush() async throws {
        try await pager.flush()
    }
}

// MARK: - Validation helpers (Go-aligned)

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


