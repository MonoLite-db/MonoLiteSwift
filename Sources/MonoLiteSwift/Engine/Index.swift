// Created by Yanjunhui

import Foundation

/// 索引信息
/// EN: Index information
public struct IndexInfo: Sendable {
    /// 索引名称
    /// EN: Index name
    public let name: String

    /// 索引键（字段名和排序方向）
    /// EN: Index keys (field names and sort directions)
    public let keys: BSONDocument

    /// 是否唯一索引
    /// EN: Whether unique index
    public let unique: Bool

    /// 是否后台创建
    /// EN: Whether background creation
    public let background: Bool

    /// B+Tree 根页面 ID
    /// EN: B+Tree root page ID
    public var rootPageId: PageID

    /// 初始化索引信息
    /// EN: Initialize index information
    public init(
        name: String,
        keys: BSONDocument,
        unique: Bool = false,
        background: Bool = false,
        rootPageId: PageID = StorageConstants.invalidPageId
    ) {
        self.name = name
        self.keys = keys
        self.unique = unique
        self.background = background
        self.rootPageId = rootPageId
    }
}

/// 索引元数据（用于持久化）
/// EN: Index metadata (for persistence)
public struct IndexMeta: Sendable {
    /// 索引名称
    /// EN: Index name
    public let name: String

    /// 索引键
    /// EN: Index keys
    public let keys: BSONDocument

    /// 是否唯一索引
    /// EN: Whether unique index
    public let unique: Bool

    /// B+Tree 根页面 ID
    /// EN: B+Tree root page ID
    public let rootPageId: PageID

    /// 初始化索引元数据
    /// EN: Initialize index metadata
    public init(name: String, keys: BSONDocument, unique: Bool, rootPageId: PageID) {
        self.name = name
        self.keys = keys
        self.unique = unique
        self.rootPageId = rootPageId
    }

    /// 序列化为 BSON 文档
    /// EN: Serialize to BSON document
    public func toBSONDocument() -> BSONDocument {
        var doc = BSONDocument()
        doc["name"] = .string(name)
        doc["keys"] = .document(keys)
        doc["unique"] = .bool(unique)
        doc["rootPageId"] = .int64(Int64(rootPageId))
        return doc
    }

    /// 从 BSON 文档反序列化
    /// EN: Deserialize from BSON document
    public static func fromBSONDocument(_ doc: BSONDocument) -> IndexMeta? {
        guard let nameVal = doc["name"], case .string(let name) = nameVal,
              let keysVal = doc["keys"], case .document(let keys) = keysVal else {
            return nil
        }

        var unique = false
        if let uniqueVal = doc["unique"], case .bool(let u) = uniqueVal {
            unique = u
        }

        var rootPageId: PageID = StorageConstants.invalidPageId
        if let rootVal = doc["rootPageId"] {
            rootPageId = PageID(rootVal.intValue ?? 0)
        }

        return IndexMeta(name: name, keys: keys, unique: unique, rootPageId: rootPageId)
    }
}

/// 索引
/// EN: Index
public actor Index {
    /// 索引信息
    /// EN: Index information
    public var info: IndexInfo

    /// B+Tree
    /// EN: B+Tree
    private let tree: BTree

    /// 页面管理器
    /// EN: Pager
    private let pager: Pager

    /// 创建新索引
    /// EN: Create new index
    public init(pager: Pager, info: IndexInfo) async throws {
        self.pager = pager
        self.tree = try await BTree(pager: pager, name: info.name, unique: info.unique)
        var infoWithRoot = info
        infoWithRoot.rootPageId = await tree.rootPage
        self.info = infoWithRoot
    }

    /// 打开现有索引
    /// EN: Open existing index
    public init(pager: Pager, info: IndexInfo, rootPageId: PageID) {
        self.pager = pager
        self.tree = BTree(pager: pager, rootPageId: rootPageId, name: info.name, unique: info.unique)
        self.info = info
    }

    /// 获取根页面 ID
    /// EN: Get root page ID
    public var rootPage: PageID {
        get async {
            await tree.rootPage
        }
    }

    /// 插入索引条目
    /// EN: Insert index entry
    public func insert(key: Data, value: Data) async throws {
        try await tree.insert(key, value)
    }

    /// 删除索引条目
    /// EN: Delete index entry
    public func delete(key: Data) async throws {
        try await tree.delete(key)
    }

    /// 搜索索引
    /// EN: Search index
    public func search(key: Data) async throws -> Data? {
        try await tree.search(key)
    }

    /// 范围搜索
    /// EN: Range search
    public func searchRange(
        minKey: Data?,
        maxKey: Data?,
        includeMin: Bool = true,
        includeMax: Bool = true
    ) async throws -> [Data] {
        try await tree.searchRange(minKey: minKey, maxKey: maxKey, includeMin: includeMin, includeMax: includeMax)
    }

    /// 获取索引条目数量
    /// EN: Get index entry count
    public func count() async throws -> Int {
        try await tree.count()
    }
}

// MARK: - 索引键编码 / Index Key Encoding

/// 为索引条目生成 B+Tree 键
/// EN: Generate B+Tree key for index entry
/// - unique 索引：key 仅由索引字段组成（KeyString）
/// EN: - unique index: key only consists of index fields (KeyString)
/// - non-unique 索引：key = KeyString(fields) + 0x00 + BSON(_id)
/// EN: - non-unique index: key = KeyString(fields) + 0x00 + BSON(_id)
public func encodeIndexEntryKey(keys: BSONDocument, doc: BSONDocument, unique: Bool) -> Data? {
    let base = encodeIndexKey(keys: keys, doc: doc)
    if base.isEmpty {
        return nil
    }

    if unique {
        return base
    }

    // 非唯一索引：追加 _id 以区分相同键值的记录
    // EN: Non-unique index: append _id to distinguish records with same key value
    guard let idVal = doc.getValue(forPath: "_id") else {
        return base
    }

    do {
        let idDoc = BSONDocument([("_id", idVal)])
        let encoder = BSONEncoder()
        let idBytes = try encoder.encode(idDoc)

        var key = Data(capacity: base.count + 1 + idBytes.count)
        key.append(base)
        key.append(0x00)
        key.append(idBytes)
        return key
    } catch {
        return base
    }
}

/// 生成索引名称
/// EN: Generate index name
public func generateIndexName(_ keys: BSONDocument) -> String {
    var name = ""
    var first = true

    for (fieldName, value) in keys {
        if !first {
            name += "_"
        }
        first = false

        let dir = value.intValue ?? 1
        name += "\(fieldName)_\(dir)"
    }

    return name
}
