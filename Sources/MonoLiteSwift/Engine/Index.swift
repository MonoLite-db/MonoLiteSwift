// Created by Yanjunhui

import Foundation

/// 索引信息
public struct IndexInfo: Sendable {
    /// 索引名称
    public let name: String

    /// 索引键（字段名和排序方向）
    public let keys: BSONDocument

    /// 是否唯一索引
    public let unique: Bool

    /// 是否后台创建
    public let background: Bool

    /// B+Tree 根页面 ID
    public var rootPageId: PageID

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
public struct IndexMeta: Sendable {
    /// 索引名称
    public let name: String

    /// 索引键
    public let keys: BSONDocument

    /// 是否唯一索引
    public let unique: Bool

    /// B+Tree 根页面 ID
    public let rootPageId: PageID

    public init(name: String, keys: BSONDocument, unique: Bool, rootPageId: PageID) {
        self.name = name
        self.keys = keys
        self.unique = unique
        self.rootPageId = rootPageId
    }

    /// 序列化为 BSON 文档
    public func toBSONDocument() -> BSONDocument {
        var doc = BSONDocument()
        doc["name"] = .string(name)
        doc["keys"] = .document(keys)
        doc["unique"] = .bool(unique)
        doc["rootPageId"] = .int64(Int64(rootPageId))
        return doc
    }

    /// 从 BSON 文档反序列化
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
public actor Index {
    /// 索引信息
    public var info: IndexInfo

    /// B+Tree
    private let tree: BTree

    /// 页面管理器
    private let pager: Pager

    /// 创建新索引
    public init(pager: Pager, info: IndexInfo) async throws {
        self.pager = pager
        self.tree = try await BTree(pager: pager, name: info.name, unique: info.unique)
        var infoWithRoot = info
        infoWithRoot.rootPageId = await tree.rootPage
        self.info = infoWithRoot
    }

    /// 打开现有索引
    public init(pager: Pager, info: IndexInfo, rootPageId: PageID) {
        self.pager = pager
        self.tree = BTree(pager: pager, rootPageId: rootPageId, name: info.name, unique: info.unique)
        self.info = info
    }

    /// 获取根页面 ID
    public var rootPage: PageID {
        get async {
            await tree.rootPage
        }
    }

    /// 插入索引条目
    public func insert(key: Data, value: Data) async throws {
        try await tree.insert(key, value)
    }

    /// 删除索引条目
    public func delete(key: Data) async throws {
        try await tree.delete(key)
    }

    /// 搜索索引
    public func search(key: Data) async throws -> Data? {
        try await tree.search(key)
    }

    /// 范围搜索
    public func searchRange(
        minKey: Data?,
        maxKey: Data?,
        includeMin: Bool = true,
        includeMax: Bool = true
    ) async throws -> [Data] {
        try await tree.searchRange(minKey: minKey, maxKey: maxKey, includeMin: includeMin, includeMax: includeMax)
    }

    /// 获取索引条目数量
    public func count() async throws -> Int {
        try await tree.count()
    }
}

// MARK: - 索引键编码

/// 为索引条目生成 B+Tree 键
/// - unique 索引：key 仅由索引字段组成（KeyString）
/// - non-unique 索引：key = KeyString(fields) + 0x00 + BSON(_id)
public func encodeIndexEntryKey(keys: BSONDocument, doc: BSONDocument, unique: Bool) -> Data? {
    let base = encodeIndexKey(keys: keys, doc: doc)
    if base.isEmpty {
        return nil
    }

    if unique {
        return base
    }

    // 非唯一索引：追加 _id 以区分相同键值的记录
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
