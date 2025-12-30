// Created by Yanjunhui

import Foundation

public struct ValidationStats: Sendable {
    public var totalPages: Int = 0
    public var dataPages: Int = 0
    public var indexPages: Int = 0
    public var freePages: Int = 0
    public var metaPages: Int = 0
    public var collections: Int = 0
    public var totalDocuments: Int = 0
    public var totalIndexes: Int = 0
}

public struct ValidationResult: Sendable {
    public var valid: Bool = true
    public var errors: [String] = []
    public var warnings: [String] = []
    public var stats: ValidationStats = ValidationStats()
}

extension Database {
    /// 校验数据库结构完整性（对齐 Go: engine/validate.go 的主流程）
    public func validate() async -> ValidationResult {
        var result = ValidationResult()

        await validateFreeList(&result)
        await validatePages(&result)
        await validateCatalog(&result)
        await validateCollections(&result)

        return result
    }

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
                    // tracked via header.catalogPageId
                    break
                case .free:
                    // counted in validateFreeList
                    break
                case .meta:
                    result.stats.metaPages += 1
                case .overflow:
                    break
                case .freeList:
                    // 兼容：目前 Swift 可能存在该类型页；不计入核心统计
                    break
                }
            } catch {
                result.valid = false
                result.errors.append("cannot read page \(i): \(error)")
            }
        }
    }

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
            do {
                let magic = DataEndian.readUInt32LE(page.data, at: 0)
                if magic == Self.catalogMagic {
                    // multi-page：只要能成功 decode 即可
                    // 复用 Database.loadCatalogMultiPage 的逻辑：读取后 decode
                    // 这里不改变内存状态，因此简单地重跑解析流程
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

    private func validateCollections(_ result: inout ValidationResult) async {
        result.stats.collections = collections.count

        for (name, col) in collections {
            let info = await col.info
            await validateDataPageChain(collectionName: name, info: info, result: &result)
            await validateIndexes(collectionName: name, info: info, result: &result)
        }
    }

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


