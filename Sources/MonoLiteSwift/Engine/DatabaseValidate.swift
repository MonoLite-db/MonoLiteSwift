// Created by Yanjunhui

import Foundation

/// 校验统计信息
/// EN: Validation statistics
public struct ValidationStats: Sendable {
    /// 总页面数
    /// EN: Total pages count
    public var totalPages: Int = 0

    /// 数据页数
    /// EN: Data pages count
    public var dataPages: Int = 0

    /// 索引页数
    /// EN: Index pages count
    public var indexPages: Int = 0

    /// 空闲页数
    /// EN: Free pages count
    public var freePages: Int = 0

    /// 元数据页数
    /// EN: Meta pages count
    public var metaPages: Int = 0

    /// 集合数
    /// EN: Collections count
    public var collections: Int = 0

    /// 总文档数
    /// EN: Total documents count
    public var totalDocuments: Int = 0

    /// 总索引数
    /// EN: Total indexes count
    public var totalIndexes: Int = 0
}

/// 校验结果
/// EN: Validation result
public struct ValidationResult: Sendable {
    /// 是否有效
    /// EN: Whether valid
    public var valid: Bool = true

    /// 错误列表
    /// EN: Errors list
    public var errors: [String] = []

    /// 警告列表
    /// EN: Warnings list
    public var warnings: [String] = []

    /// 统计信息
    /// EN: Statistics
    public var stats: ValidationStats = ValidationStats()
}

extension Database {
    /// 校验数据库结构完整性（对齐 Go: engine/validate.go 的主流程）
    /// EN: Validate database structure integrity (aligned with Go: engine/validate.go main flow)
    public func validate() async -> ValidationResult {
        var result = ValidationResult()

        await validateFreeList(&result)
        await validatePages(&result)
        await validateCatalog(&result)
        await validateCollections(&result)

        return result
    }

    /// 校验空闲列表
    /// EN: Validate free list
    private func validateFreeList(_ result: inout ValidationResult) async {
        let header = await pager.headerSnapshot()
        let head = header.freeListHead
        if head == 0 { return }

        var visited = Set<PageID>()
        var current = head
        var chainLen = 0

        while current != 0 {
            if visited.contains(current) {
                result.valid = false
                result.errors.append("free list has cycle at page \(current)")
                return
            }
            visited.insert(current)
            chainLen += 1

            do {
                let page = try await pager.readPage(current)
                if page.pageType != .free {
                    result.valid = false
                    result.errors.append("free list page \(current) has wrong type: \(page.pageType)")
                }
                current = page.nextPageId
                result.stats.freePages += 1
            } catch {
                result.valid = false
                result.errors.append("cannot read free list page \(current): \(error)")
                return
            }

            if chainLen > Int(header.pageCount) {
                result.valid = false
                result.errors.append("free list chain exceeds total page count")
                return
            }
        }
    }

    /// 校验所有页面
    /// EN: Validate all pages
    private func validatePages(_ result: inout ValidationResult) async {
        let header = await pager.headerSnapshot()
        result.stats.totalPages = Int(header.pageCount)

        for i in 0..<Int(header.pageCount) {
            do {
                let page = try await pager.readPage(PageID(i))
                switch page.pageType {
                case .data:
                    result.stats.dataPages += 1
                case .index:
                    result.stats.indexPages += 1
                case .catalog:
                    // 通过 header.catalogPageId 跟踪
                    // EN: Tracked via header.catalogPageId
                    break
                case .free:
                    // 在 validateFreeList 中计数
                    // EN: Counted in validateFreeList
                    break
                case .meta:
                    result.stats.metaPages += 1
                case .overflow:
                    break
                case .freeList:
                    // 兼容：目前 Swift 可能存在该类型页；不计入核心统计
                    // EN: Compatibility: Swift may have this page type; not counted in core statistics
                    break
                }
            } catch {
                result.valid = false
                result.errors.append("cannot read page \(i): \(error)")
            }
        }
    }

    /// 校验 catalog
    /// EN: Validate catalog
    private func validateCatalog(_ result: inout ValidationResult) async {
        let header = await pager.headerSnapshot()
        let catalogId = header.catalogPageId
        if catalogId == 0 { return }

        do {
            let page = try await pager.readPage(catalogId)
            if page.pageType != .catalog {
                result.warnings.append("catalog page \(catalogId) has unexpected type: \(page.pageType) (expected catalog)")
            }

            // 深度校验：尝试解析 catalog（支持单页/多页）
            // EN: Deep validation: try to parse catalog (supports single/multi-page)
            do {
                let magic = DataEndian.readUInt32LE(page.data, at: 0)
                if magic == Self.catalogMagic {
                    // multi-page：只要能成功 decode 即可
                    // EN: multi-page: just need to successfully decode
                    // 复用 Database.loadCatalogMultiPage 的逻辑：读取后 decode
                    // EN: Reuse Database.loadCatalogMultiPage logic: read then decode
                    // 这里不改变内存状态，因此简单地重跑解析流程
                    // EN: Not changing memory state here, so simply re-run parsing flow
                    _ = try await decodeCatalogFromMultiPage(firstPage: page)
                } else {
                    let bsonLen = Int(DataEndian.readUInt32LE(page.data, at: 0))
                    if bsonLen >= 5, bsonLen <= page.data.count {
                        let dec = BSONDecoder()
                        _ = try dec.decode(Data(page.data[0..<bsonLen]))
                    }
                }
            } catch {
                result.valid = false
                result.errors.append("catalog decode failed: \(error)")
            }
        } catch {
            result.valid = false
            result.errors.append("cannot read catalog page \(catalogId): \(error)")
        }
    }

    /// 从多页 catalog 解码
    /// EN: Decode from multi-page catalog
    private func decodeCatalogFromMultiPage(firstPage: Page) async throws -> BSONDocument {
        let data = firstPage.data
        if data.count < Self.catalogHeaderLen {
            throw MonoError.internalError("invalid multi-page catalog header")
        }
        let totalLen = Int(DataEndian.readUInt32LE(data, at: 4))
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
        if bsonData.count < totalLen {
            throw MonoError.internalError("incomplete multi-page catalog")
        }
        let dec = BSONDecoder()
        return try dec.decode(bsonData.prefix(totalLen))
    }

    /// 校验所有集合
    /// EN: Validate all collections
    private func validateCollections(_ result: inout ValidationResult) async {
        result.stats.collections = collections.count

        for (name, col) in collections {
            let info = await col.info
            await validateDataPageChain(collectionName: name, info: info, result: &result)
            await validateIndexes(collectionName: name, info: info, result: &result)
        }
    }

    /// 校验数据页链表
    /// EN: Validate data page chain
    private func validateDataPageChain(collectionName: String, info: CollectionInfo, result: inout ValidationResult) async {
        if info.firstPageId == 0 { return }

        var visited = Set<PageID>()
        var current = info.firstPageId
        var prev: PageID = 0
        var docCount = 0

        while current != 0 {
            if visited.contains(current) {
                result.valid = false
                result.errors.append("collection \(collectionName): data page chain has cycle at page \(current)")
                return
            }
            visited.insert(current)

            do {
                let page = try await pager.readPage(current)
                if page.pageType != .data {
                    result.valid = false
                    result.errors.append("collection \(collectionName): page \(current) in data chain has wrong type: \(page.pageType)")
                }
                if page.prevPageId != prev {
                    result.valid = false
                    result.errors.append("collection \(collectionName): page \(current) has wrong PrevPageId: \(page.prevPageId) (expected \(prev))")
                }
                let sp = SlottedPage(page: page)
                docCount += sp.liveCount
                prev = current
                current = page.nextPageId
            } catch {
                result.valid = false
                result.errors.append("collection \(collectionName): cannot read data page \(current): \(error)")
                return
            }
        }

        result.stats.totalDocuments += docCount
        if docCount != Int(info.documentCount) {
            result.warnings.append("collection \(collectionName): document count mismatch: found \(docCount), expected \(info.documentCount)")
        }
    }

    /// 校验索引
    /// EN: Validate indexes
    private func validateIndexes(collectionName: String, info: CollectionInfo, result: inout ValidationResult) async {
        for meta in info.indexes {
            do {
                let tree = BTree(pager: pager, rootPageId: meta.rootPageId, name: meta.name, unique: meta.unique)
                try await tree.checkTreeIntegrity()
                try await tree.checkLeafChain()
                result.stats.totalIndexes += 1
            } catch {
                result.valid = false
                result.errors.append("collection \(collectionName): index \(meta.name) validation failed: \(error)")
            }
        }
    }
}
