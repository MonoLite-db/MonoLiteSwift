// Created by Yanjunhui

import Foundation

/// 页面管理器
/// 负责页面的分配、释放、缓存和持久化
/// EN: Page manager.
/// Responsible for page allocation, deallocation, caching, and persistence.
public actor Pager {
    /// 数据文件句柄
    /// EN: Data file handle.
    private var fileHandle: FileHandle?

    /// 文件路径
    /// EN: File path.
    private let path: String

    /// 文件头
    /// EN: File header.
    private var header: FileHeader

    /// 页面缓存
    /// EN: Page cache.
    private var cache: [PageID: Page]

    /// 脏页集合
    /// EN: Dirty page set.
    private var dirty: Set<PageID>

    /// 页面 LSN 记录
    /// EN: Page LSN records.
    private var pageLSN: [PageID: LSN]

    /// 空闲页列表
    /// EN: Free page list.
    private var freePages: [PageID]

    /// WAL 日志
    /// EN: WAL log.
    private var wal: WAL?

    /// WAL 是否启用
    /// EN: Whether WAL is enabled.
    public let walEnabled: Bool

    /// 最大缓存页数
    /// EN: Maximum cached page count.
    private let maxCached: Int

    /// 最后一次刷盘错误
    /// EN: Last flush error.
    private var lastFlushError: Error?

    // MARK: - 初始化 / Initialization

    /// 创建或打开 Pager
    /// EN: Create or open Pager.
    public init(path: String, enableWAL: Bool = true, cacheSize: Int = StorageConstants.defaultCacheSize) async throws {
        self.path = path
        self.walEnabled = enableWAL
        self.maxCached = cacheSize
        self.header = FileHeader()
        self.cache = [:]
        self.dirty = []
        self.pageLSN = [:]
        self.freePages = []

        try await open()
    }

    /// 打开数据文件
    /// EN: Open data file.
    private func open() async throws {
        let fileManager = FileManager.default
        let walPath = path + ".wal"

        if fileManager.fileExists(atPath: path) {
            // 打开现有文件
            // EN: Open existing file
            fileHandle = try FileHandle(forUpdating: URL(fileURLWithPath: path))

            // 读取文件头
            // EN: Read file header
            try fileHandle?.seek(toOffset: 0)
            guard let headerData = try fileHandle?.read(upToCount: StorageConstants.fileHeaderSize) else {
                throw StorageError.fileCorrupted("Cannot read header")
            }
            header = try FileHeader.unmarshal(headerData)
            try header.validate()

            // 加载空闲页列表
            // EN: Load free page list
            try await loadFreeList()

            // 如果启用 WAL，进行恢复
            // EN: If WAL is enabled, perform recovery
            if walEnabled {
                wal = try await WAL(path: walPath)
                try await recover()
            }
        } else {
            // 创建新文件
            // EN: Create new file
            fileManager.createFile(atPath: path, contents: nil)
            fileHandle = try FileHandle(forUpdating: URL(fileURLWithPath: path))

            // 初始化新文件
            // EN: Initialize new file
            try await initNewFile()

            // 创建 WAL
            // EN: Create WAL
            if walEnabled {
                wal = try await WAL(path: walPath)
            }
        }
    }

    /// 初始化新文件
    /// EN: Initialize new file.
    private func initNewFile() async throws {
        // 写入文件头
        // EN: Write file header
        header = FileHeader()
        try await writeHeader()

        // 创建元数据页（页面 0）
        // EN: Create metadata page (page 0)
        let metaPage = Page(id: 0, type: .meta)
        try await writePage(metaPage)
        header.metaPageId = 0
        header.pageCount = 1

        try await writeHeader()
        try fileHandle?.synchronize()
    }

    /// 加载空闲页列表
    /// EN: Load free page list.
    private func loadFreeList() async throws {
        freePages.removeAll()

        var currentId = header.freeListHead
        while currentId != StorageConstants.invalidPageId {
            freePages.append(currentId)

            // 读取页面获取下一个空闲页
            // EN: Read page to get next free page
            let page = try await readPageDirect(currentId)
            currentId = page.nextPageId

            // 防止无限循环
            // EN: Prevent infinite loop
            if freePages.count > Int(header.pageCount) {
                throw StorageError.fileCorrupted("Free list cycle detected")
            }
        }
    }

    /// 崩溃恢复
    /// EN: Crash recovery.
    private func recover() async throws {
        guard let wal = wal else { return }

        let checkpointLSN = await wal.lastCheckpointLSN
        let records = try await wal.readRecordsFrom(checkpointLSN + 1)
        if records.isEmpty {
            return
        }

        guard let handle = fileHandle else {
            throw StorageError.fileNotOpen(path)
        }

        // 读取恢复开始时的数据文件大小：用于判断某 PageId 的物理页是否已存在（free list 复用窗口）
        // EN: Read data file size at recovery start: used to determine if physical page for PageId already exists (free list reuse window)
        try handle.seek(toOffset: 0)
        let actualSizeAtStart = Int64(try handle.seekToEnd())

        // 记录分配页的类型（用于 ensureFileSize 补齐缺失页时初始化正确类型）
        // EN: Record allocated page types (for ensureFileSize to initialize missing pages with correct type)
        var allocPageTypes: [PageID: PageType] = [:]

        // Redo：回放所有记录
        // EN: Redo: replay all records
        for record in records {
            switch record.type {
            case .pageWrite:
                // Go 参考实现：仅当数据为完整 PageSize 时才重放
                // EN: Go reference implementation: only replay when data is complete PageSize
                if record.dataLen > 0, record.data.count == StorageConstants.pageSize {
                    try await writePageDirect(record.pageId, record.data)
                }

            case .allocPage:
                // Go 参考实现：确保 pageCount 正确
                // EN: Go reference implementation: ensure pageCount is correct
                if record.pageId >= header.pageCount {
                    header.pageCount = record.pageId + 1
                }

                // record.data: [pageType]
                let pageType = (record.dataLen >= 1 && record.data.count >= 1)
                    ? (PageType(rawValue: record.data[0]) ?? .data)
                    : .data
                allocPageTypes[record.pageId] = pageType

                // 关键修复：如果该 PageId 的物理页已存在，则在恢复阶段写入"初始化页"，防止仍为 Free
                // EN: Critical fix: if physical page for PageId already exists, write "initialized page" during recovery to prevent it from remaining Free
                let pageOffset = Int64(StorageConstants.fileHeaderSize)
                    + Int64(record.pageId) * Int64(StorageConstants.pageSize)
                if pageOffset + Int64(StorageConstants.pageSize) <= actualSizeAtStart {
                    let initPage = Page(id: record.pageId, type: pageType)
                    try await writePageDirect(record.pageId, initPage.marshal())
                }

            case .freePage:
                // Go 参考实现：FreeListHead 在 MetaUpdate 中处理，这里无需特殊处理
                // EN: Go reference implementation: FreeListHead is handled in MetaUpdate, no special handling needed here
                break

            case .metaUpdate:
                // 数据格式：[metaType(1)][oldValue(4)][newValue(4)] = 9 bytes
                // EN: Data format: [metaType(1)][oldValue(4)][newValue(4)] = 9 bytes
                guard record.data.count >= 9 else { break }

                let metaTypeRaw = record.data[0]
                let newValue = DataEndian.readUInt32LE(record.data, at: 5)

                switch MetaUpdateType(rawValue: metaTypeRaw) {
                case .freeListHead:
                    header.freeListHead = newValue
                case .pageCount:
                    header.pageCount = newValue
                case .catalogPageId:
                    header.catalogPageId = newValue
                case .none:
                    break
                }

            case .commit, .checkpoint:
                // 不需要重放
                // EN: No replay needed
                break
            }
        }

        // 同步数据文件
        // EN: Sync data file
        try handle.synchronize()

        // 关键：确保文件大小与 PageCount 一致（包括半页修复），并尽量按 allocPageTypes 初始化缺失页
        // EN: Critical: ensure file size matches PageCount (including partial page fix), initialize missing pages using allocPageTypes where possible
        try await ensureFileSize(allocPageTypes: allocPageTypes)

        // 更新并持久化文件头（恢复后的状态）
        // EN: Update and persist file header (state after recovery)
        try await writeHeader()
        try handle.synchronize()

        // 重新加载 free list（从恢复后的 header.freeListHead 开始）
        // EN: Reload free list (starting from recovered header.freeListHead)
        try await loadFreeList()
    }

    // MARK: - 页面操作 / Page Operations

    /// 读取页面
    /// EN: Read page.
    public func readPage(_ id: PageID) async throws -> Page {
        // 先检查缓存
        // EN: Check cache first
        if let page = cache[id] {
            return page
        }

        // 从磁盘读取
        // EN: Read from disk
        let page = try await readPageDirect(id)

        // 添加到缓存
        // EN: Add to cache
        try await addToCache(page)

        return page
    }

    /// 直接从磁盘读取页面
    /// EN: Read page directly from disk.
    private func readPageDirect(_ id: PageID) async throws -> Page {
        guard let handle = fileHandle else {
            throw StorageError.fileNotOpen(path)
        }

        let offset = UInt64(StorageConstants.fileHeaderSize) + UInt64(id) * UInt64(StorageConstants.pageSize)
        try handle.seek(toOffset: offset)

        guard let data = try handle.read(upToCount: StorageConstants.pageSize),
              data.count == StorageConstants.pageSize else {
            throw StorageError.pageNotFound(id)
        }

        return try Page(id: id, rawData: data)
    }

    /// 分配新页面
    /// EN: Allocate new page.
    public func allocatePage(type: PageType) async throws -> Page {
        // 以 Go 参考实现为准：先写 WAL（alloc + meta）并 Sync，再更新内存/磁盘结构
        // EN: Aligned with Go reference implementation: write WAL (alloc + meta) and Sync first, then update memory/disk structures
        let pageId: PageID
        let oldFreeListHead = header.freeListHead
        let oldPageCount = header.pageCount
        let newFreeListHead: PageID
        let newPageCount: UInt32

        let fromFreeList = !freePages.isEmpty
        if fromFreeList {
            pageId = freePages[0]

            // 读取 free page 获取其 next（与 Go 保持一致，而不是仅依赖 freePages[1]）
            // EN: Read free page to get its next (consistent with Go, not just relying on freePages[1])
            let freePage = try await readPageDirect(pageId)
            newFreeListHead = freePage.nextPageId
            newPageCount = oldPageCount
        } else {
            pageId = oldPageCount
            newPageCount = oldPageCount + 1
            newFreeListHead = oldFreeListHead
        }

        if let wal {
            try await wal.writeAllocRecord(pageId, pageType: type)
            if fromFreeList {
                try await wal.writeMetaRecord(.freeListHead, oldValue: oldFreeListHead, newValue: newFreeListHead)
            } else {
                try await wal.writeMetaRecord(.pageCount, oldValue: oldPageCount, newValue: newPageCount)
            }
            try await wal.sync()
        }

        // WAL 已持久化，现在安全更新内存状态 + 持久化 header（Go 会立即写 header）
        // EN: WAL persisted, now safe to update memory state + persist header (Go writes header immediately)
        if fromFreeList {
            _ = freePages.removeFirst()
            header.freeListHead = newFreeListHead
        } else {
            header.pageCount = newPageCount
        }
        header.touch()
        try await writeHeader()

        // 创建新页面
        // EN: Create new page
        let page = Page(id: pageId, type: type)
        page.isDirty = true

        // Go 语义：
        // - 从 free list 复用：不立即写初始化页（崩溃窗口由 recover.alloc-init 兜底）
        // - 新增页：立即写初始化页到 data file，确保物理文件大小与 PageCount 一致（但不额外写 pageWrite WAL）
        // EN: Go semantics:
        // - Reuse from free list: don't write init page immediately (crash window covered by recover.alloc-init)
        // - New page: write init page to data file immediately, ensure physical file size matches PageCount (but no extra pageWrite WAL)
        if !fromFreeList {
            try await writePageDirect(page.id, page.marshal())
        }

        // 添加到缓存并标记为脏（flush 时会写 pageWrite WAL + data file）
        // EN: Add to cache and mark as dirty (flush will write pageWrite WAL + data file)
        try await addToCache(page)
        dirty.insert(pageId)

        return page
    }

    /// 释放页面
    /// EN: Free page.
    public func freePage(_ id: PageID) async throws {
        let page: Page
        if let cachedPage = cache[id] {
            page = cachedPage
        } else {
            page = try await readPageDirect(id)
            // 进入缓存（保持与 Go 语义接近：后续可能被复用）
            // EN: Enter cache (keep close to Go semantics: may be reused later)
            try await addToCache(page)
        }

        let oldFreeListHead = header.freeListHead
        let newFreeListHead = id

        // 以 Go 参考实现为准：先写 WAL（free + meta）并 Sync
        // EN: Aligned with Go reference implementation: write WAL (free + meta) and Sync first
        if let wal {
            try await wal.writeFreeRecord(id)
            try await wal.writeMetaRecord(.freeListHead, oldValue: oldFreeListHead, newValue: newFreeListHead)
            try await wal.sync()
        }

        // WAL 已持久化，现在安全更新页与 header，并将 free 页落盘
        // EN: WAL persisted, now safe to update page and header, and persist free page
        page.pageType = .free
        page.nextPageId = oldFreeListHead
        page.isDirty = true

        // 写入 free 页（会写 WAL pageWrite 记录；与 Go 保持一致）
        // EN: Write free page (will write WAL pageWrite record; consistent with Go)
        try await writePage(page)

        header.freeListHead = newFreeListHead
        header.touch()
        try await writeHeader()

        // 维护 freePages（LIFO，与 Go 一致）
        // EN: Maintain freePages (LIFO, consistent with Go)
        freePages.insert(id, at: 0)
        dirty.remove(id)
    }

    /// 标记页面为脏
    /// EN: Mark page as dirty.
    public func markDirty(_ id: PageID) {
        dirty.insert(id)
    }

    /// 写入页面到磁盘
    /// EN: Write page to disk.
    private func writePage(_ page: Page) async throws {
        // WAL 先行
        // EN: WAL first
        let raw = page.marshal()
        if let wal {
            let lsn = try await wal.writePageRecord(page.id, raw)
            pageLSN[page.id] = lsn
        }

        try await writePageDirect(page.id, raw)
    }

    /// 直接写入页面数据
    /// EN: Write page data directly.
    private func writePageDirect(_ id: PageID, _ data: Data) async throws {
        guard let handle = fileHandle else {
            throw StorageError.fileNotOpen(path)
        }

        let offset = UInt64(StorageConstants.fileHeaderSize) + UInt64(id) * UInt64(StorageConstants.pageSize)
        try handle.seek(toOffset: offset)
        try handle.write(contentsOf: data)
    }

    /// 确保文件大小与 PageCount 一致（Go 参考实现语义）
    /// EN: Ensure file size matches PageCount (Go reference implementation semantics).
    ///
    /// - 如果实际文件大小小于 PageCount 指示的大小，则扩展文件
    /// - 如果尾部存在"半页"（崩溃导致短写），从该页起重写为完整初始化页
    /// - 尽量使用 WAL 中记录的分配页类型初始化缺失页；未知则回退为 Data
    /// EN:
    /// - If actual file size is less than PageCount indicates, extend the file
    /// - If there's a "partial page" at the end (short write due to crash), rewrite from that page as complete initialized page
    /// - Use allocated page types recorded in WAL to initialize missing pages where possible; fallback to Data if unknown
    private func ensureFileSize(allocPageTypes: [PageID: PageType]? = nil) async throws {
        guard let handle = fileHandle else { return }

        let expectedSize = Int64(StorageConstants.fileHeaderSize)
            + Int64(header.pageCount) * Int64(StorageConstants.pageSize)

        try handle.seek(toOffset: 0)
        let actualSize = Int64(try handle.seekToEnd())
        guard actualSize < expectedSize else { return }

        // 计算起始 offset：如果存在半页，回到半页起点
        // EN: Calculate start offset: if partial page exists, go back to partial page start
        var startOffset = actualSize
        if actualSize > Int64(StorageConstants.fileHeaderSize) {
            let rel = actualSize - Int64(StorageConstants.fileHeaderSize)
            let rem = rel % Int64(StorageConstants.pageSize)
            if rem != 0 {
                startOffset = actualSize - rem
            }
        } else {
            startOffset = Int64(StorageConstants.fileHeaderSize)
        }

        var offset = startOffset
        while offset < expectedSize {
            let pageId = PageID((offset - Int64(StorageConstants.fileHeaderSize)) / Int64(StorageConstants.pageSize))
            let pageType = allocPageTypes?[pageId] ?? .data
            let page = Page(id: pageId, type: pageType)
            try await writePageDirect(pageId, page.marshal())
            offset += Int64(StorageConstants.pageSize)
        }

        // 确保最终文件大小正确
        // EN: Ensure final file size is correct
        try handle.truncate(atOffset: UInt64(expectedSize))
    }

    // MARK: - 缓存管理 / Cache Management

    /// 添加页面到缓存
    /// EN: Add page to cache.
    private func addToCache(_ page: Page) async throws {
        // 检查缓存是否已满
        // EN: Check if cache is full
        if cache.count >= maxCached {
            try await evictFromCache()
        }

        cache[page.id] = page
    }

    /// 从缓存驱逐页面
    /// EN: Evict page from cache.
    private func evictFromCache() async throws {
        // 找到一个非脏页移除
        // EN: Find a non-dirty page to remove
        for (id, _) in cache {
            if !dirty.contains(id) {
                cache.removeValue(forKey: id)
                return
            }
        }

        // 如果所有页都是脏页，强制刷盘最老的一个
        // EN: If all pages are dirty, force flush the oldest one
        if let (id, page) = cache.first {
            do {
                try await writePage(page)
                dirty.remove(id)
                cache.removeValue(forKey: id)
            } catch {
                // 刷盘失败，记录错误但不从缓存移除
                // EN: Flush failed, record error but don't remove from cache
                lastFlushError = error
            }
        }
    }

    // MARK: - 刷盘 / Flush

    /// 刷盘所有脏页
    /// EN: Flush all dirty pages.
    public func flush() async throws {
        // 以 Go 参考实现为准：先 Sync WAL（注意：Go 的 writePage 会在之后追加 pageWrite 记录）
        // EN: Aligned with Go reference implementation: Sync WAL first (note: Go's writePage appends pageWrite record afterwards)
        if let wal {
            try await wal.sync()
        }

        // 写入所有脏页（写 WAL pageWrite + 写数据文件）
        // EN: Write all dirty pages (write WAL pageWrite + write data file)
        for id in dirty {
            if let page = cache[id], page.isDirty {
                try await writePage(page)
                page.isDirty = false
            }
        }
        dirty.removeAll()

        // 更新并写入文件头
        // EN: Update and write file header
        header.touch()
        try await writeHeader()

        // 同步数据文件
        // EN: Sync data file
        try fileHandle?.synchronize()

        // 创建检查点：checkpointLSN = "已安全落盘的最大 LSN" = currentLSN - 1（Go 语义）
        // EN: Create checkpoint: checkpointLSN = "max LSN safely persisted" = currentLSN - 1 (Go semantics)
        if let wal {
            let current = await wal.currentLogSequenceNumber
            if current > 1 {
                try await wal.checkpoint(current - 1)
            }
        }
    }

    /// 写入文件头
    /// EN: Write file header.
    private func writeHeader() async throws {
        guard let handle = fileHandle else { return }
        try handle.seek(toOffset: 0)
        try handle.write(contentsOf: header.marshal())
    }

    // MARK: - 关闭 / Close

    /// 关闭 Pager
    /// EN: Close Pager.
    public func close() async throws {
        // 刷盘
        // EN: Flush
        try await flush()

        // 关闭 WAL
        // EN: Close WAL
        if let wal = wal {
            try await wal.close()
        }

        // 关闭数据文件
        // EN: Close data file
        try fileHandle?.close()
        fileHandle = nil
    }

    /// 模拟崩溃：直接关闭底层句柄，不 flush、不 checkpoint（仅测试使用）
    /// EN: Simulate crash: close underlying handle directly, no flush, no checkpoint (for testing only).
    ///
    /// 说明：该方法不会写回 header，也不会同步 WAL；用于验证恢复路径在异常关闭场景下的正确性。
    /// EN: Note: This method doesn't write back header or sync WAL; used to verify recovery path correctness in abnormal close scenarios.
    func simulateCrashCloseForTesting() async {
        if let wal = wal {
            await wal.simulateCrashCloseForTesting()
        }
        try? fileHandle?.close()
        fileHandle = nil
    }

    // MARK: - 属性访问 / Property Access

    /// 页面计数
    /// EN: Page count.
    public var pageCount: UInt32 {
        header.pageCount
    }

    /// 空闲页数量（内存中的 free list 缓存）
    /// EN: Free page count (in-memory free list cache).
    public var freePageCount: UInt32 {
        UInt32(freePages.count)
    }

    /// 获取文件头快照（用于 validate 等只读场景）
    /// EN: Get file header snapshot (for read-only scenarios like validate).
    public func headerSnapshot() -> FileHeader {
        header
    }

    /// 目录页 ID
    /// EN: Catalog page ID.
    public var catalogPageId: PageID {
        get { header.catalogPageId }
        set {
            header.catalogPageId = newValue
            dirty.insert(0)  // 标记需要更新头部 / EN: Mark header needs update
        }
    }

    /// 设置目录页 ID
    /// EN: Set catalog page ID.
    public func setCatalogPageId(_ id: PageID) async throws {
        // 与 Go 对齐：仅更新内存 header；由后续 flush 持久化
        // EN: Aligned with Go: only update in-memory header; persisted by subsequent flush
        header.catalogPageId = id
        dirty.insert(0)
    }

    /// 获取缓存中的页面（如果存在）
    /// EN: Get cached page (if exists).
    public func getCachedPage(_ id: PageID) -> Page? {
        cache[id]
    }
}
