// Created by Yanjunhui

import Foundation

/// 集合元信息
public struct CollectionInfo: Sendable {
    /// 集合名称
    public var name: String

    /// 数据页链表头
    public var firstPageId: PageID

    /// 数据页链表尾
    public var lastPageId: PageID

    /// 文档数量
    public var documentCount: Int64

    /// 索引元数据列表
    public var indexes: [IndexMeta]

    public init(
        name: String,
        firstPageId: PageID = StorageConstants.invalidPageId,
        lastPageId: PageID = StorageConstants.invalidPageId,
        documentCount: Int64 = 0,
        indexes: [IndexMeta] = []
    ) {
        self.name = name
        self.firstPageId = firstPageId
        self.lastPageId = lastPageId
        self.documentCount = documentCount
        self.indexes = indexes
    }

    // MARK: - 序列化

    /// 序列化为 BSON 文档
    public func toBSONDocument() -> BSONDocument {
        var doc = BSONDocument()
        doc["name"] = .string(name)
        doc["firstPageId"] = .int64(Int64(firstPageId))
        doc["lastPageId"] = .int64(Int64(lastPageId))
        doc["documentCount"] = .int64(documentCount)

        var indexesArr = BSONArray()
        for idx in indexes {
            indexesArr.append(.document(idx.toBSONDocument()))
        }
        doc["indexes"] = .array(indexesArr)

        return doc
    }

    /// 从 BSON 文档反序列化
    public static func fromBSONDocument(_ doc: BSONDocument) -> CollectionInfo? {
        guard let nameVal = doc["name"], case .string(let name) = nameVal else {
            return nil
        }

        var info = CollectionInfo(name: name)

        if let fpVal = doc["firstPageId"] {
            info.firstPageId = PageID(fpVal.intValue ?? 0)
        }

        if let lpVal = doc["lastPageId"] {
            info.lastPageId = PageID(lpVal.intValue ?? 0)
        }

        if let dcVal = doc["documentCount"] {
            info.documentCount = dcVal.int64Value ?? 0
        }

        if let idxVal = doc["indexes"], case .array(let idxArr) = idxVal {
            for item in idxArr {
                if case .document(let idxDoc) = item,
                   let meta = IndexMeta.fromBSONDocument(idxDoc) {
                    info.indexes.append(meta)
                }
            }
        }

        return info
    }
}

/// 已插入记录信息（用于回滚）
struct InsertedRecord {
    let pageId: PageID
    let slotIndex: Int
    let id: BSONValue
}
