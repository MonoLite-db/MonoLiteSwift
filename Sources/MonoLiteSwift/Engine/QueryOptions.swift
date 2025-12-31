// Created by Yanjunhui

import Foundation

/// 查询选项
/// EN: Query options
public struct QueryOptions: Sendable {
    /// 排序规则
    /// EN: Sort specification
    public var sort: BSONDocument

    /// 返回数量限制
    /// EN: Limit on number of results
    public var limit: Int64

    /// 跳过数量
    /// EN: Number to skip
    public var skip: Int64

    /// 投影（字段选择）
    /// EN: Projection (field selection)
    public var projection: BSONDocument

    /// 创建默认查询选项
    /// EN: Create default query options
    public init(
        sort: BSONDocument = BSONDocument(),
        limit: Int64 = 0,
        skip: Int64 = 0,
        projection: BSONDocument = BSONDocument()
    ) {
        self.sort = sort
        self.limit = limit
        self.skip = skip
        self.projection = projection
    }
}

// MARK: - 查询选项应用 / Query Options Application

/// 应用查询选项到结果集
/// EN: Apply query options to result set
public func applyQueryOptions(_ docs: [BSONDocument], _ opts: QueryOptions?) -> [BSONDocument] {
    guard let opts = opts else {
        return docs
    }

    var result = docs

    // 排序
    // EN: Sort
    if !opts.sort.isEmpty {
        result = sortDocuments(result, opts.sort)
    }

    // Skip
    // EN: Skip
    if opts.skip > 0 {
        if Int64(result.count) <= opts.skip {
            return []
        }
        result = Array(result.dropFirst(Int(opts.skip)))
    }

    // Limit
    // EN: Limit
    if opts.limit > 0 && Int64(result.count) > opts.limit {
        result = Array(result.prefix(Int(opts.limit)))
    }

    // Projection
    // EN: Projection
    if !opts.projection.isEmpty {
        result = applyProjection(result, opts.projection)
    }

    return result
}

// MARK: - 排序 / Sorting

/// 对文档排序（使用 MongoDB 标准 BSON 类型比较规则）
/// EN: Sort documents (using MongoDB standard BSON type comparison rules)
public func sortDocuments(_ docs: [BSONDocument], _ sortSpec: BSONDocument) -> [BSONDocument] {
    var result = docs

    result.sort { docA, docB in
        for (field, directionValue) in sortSpec {
            let direction: Int
            switch directionValue {
            case .int32(let d):
                direction = Int(d)
            case .int64(let d):
                direction = Int(d)
            case .double(let d):
                direction = Int(d)
            default:
                direction = 1
            }

            let valA = docA.getValue(forPath: field)
            let valB = docB.getValue(forPath: field)

            // 使用 BSON 比较规则
            // EN: Use BSON comparison rules
            let cmp = compareBSONOptional(valA, valB)
            if cmp != 0 {
                if direction < 0 {
                    return cmp > 0
                }
                return cmp < 0
            }
        }
        return false
    }

    return result
}

/// 比较可选的 BSON 值
/// EN: Compare optional BSON values
private func compareBSONOptional(_ a: BSONValue?, _ b: BSONValue?) -> Int {
    switch (a, b) {
    case (.none, .none):
        return 0
    case (.none, .some):
        return -1  // null 排在前面 / EN: null sorts first
    case (.some, .none):
        return 1
    case (.some(let aVal), .some(let bVal)):
        return compareBSONValues(aVal, bVal)
    }
}

// MARK: - 投影 / Projection

/// 应用投影到文档集
/// EN: Apply projection to document set
public func applyProjection(_ docs: [BSONDocument], _ projection: BSONDocument) -> [BSONDocument] {
    if projection.isEmpty {
        return docs
    }

    // 判断是包含还是排除模式
    // EN: Determine include or exclude mode
    var includeMode = true
    var hasIdExclusion = false

    // _id 的排除不依赖字段顺序（MongoDB/Go 行为）
    // EN: _id exclusion does not depend on field order (MongoDB/Go behavior)
    if let v = projection["_id"], let intVal = v.intValue, intVal == 0 {
        hasIdExclusion = true
    }

    // include/exclude 模式由第一个"非 _id 且可解析为数值"的字段决定
    // EN: include/exclude mode is determined by first "non-_id field that can be parsed as number"
    for (key, value) in projection {
        if key == "_id" { continue }
        if let intVal = value.intValue {
            includeMode = intVal != 0
            break
        }
    }

    return docs.map { doc in
        projectDocument(doc, projection, includeMode: includeMode, excludeId: hasIdExclusion)
    }
}

/// 对单个文档应用投影
/// EN: Apply projection to single document
private func projectDocument(
    _ doc: BSONDocument,
    _ projection: BSONDocument,
    includeMode: Bool,
    excludeId: Bool
) -> BSONDocument {
    var result = BSONDocument()

    if includeMode {
        // 包含模式：只返回指定的字段
        // EN: Include mode: only return specified fields
        // 默认包含 _id，除非明确排除
        // EN: Include _id by default, unless explicitly excluded
        if !excludeId, let idValue = doc["_id"] {
            result["_id"] = idValue
        }

        for (key, value) in projection {
            if key == "_id" { continue }

            if let intVal = value.intValue, intVal != 0 {
                if let docVal = doc.getValue(forPath: key) {
                    setNestedValue(&result, path: key, value: docVal)
                }
            }
        }
    } else {
        // 排除模式：返回除指定字段外的所有字段
        // EN: Exclude mode: return all fields except specified ones
        for (key, value) in doc {
            if key == "_id" {
                if !excludeId {
                    result[key] = value
                }
                continue
            }

            var shouldInclude = true
            for (projKey, projValue) in projection {
                if projKey == key {
                    if let intVal = projValue.intValue, intVal == 0 {
                        shouldInclude = false
                    }
                    break
                }
            }

            if shouldInclude {
                result[key] = value
            }
        }
    }

    return result
}

/// 设置嵌套字段值
/// EN: Set nested field value
private func setNestedValue(_ doc: inout BSONDocument, path: String, value: BSONValue) {
    let parts = path.split(separator: ".").map(String.init)

    if parts.count == 1 {
        doc[path] = value
        return
    }

    // 处理嵌套路径
    // EN: Handle nested path
    var current = doc
    for i in 0..<(parts.count - 1) {
        let part = parts[i]
        if let existing = current[part], case .document(let nested) = existing {
            current = nested
        } else {
            current = BSONDocument()
        }
    }

    current[parts.last!] = value

    // 反向构建嵌套文档
    // EN: Build nested document in reverse
    var nested = current
    for i in stride(from: parts.count - 2, through: 0, by: -1) {
        var parent = BSONDocument()
        parent[parts[i]] = .document(nested)
        nested = parent
    }

    // 合并到结果
    // EN: Merge to result
    for (key, val) in nested {
        doc[key] = val
    }
}

// MARK: - 更新结果 / Update Result

/// 更新操作的结果
/// EN: Update operation result
public struct UpdateResult: Sendable {
    /// 匹配的文档数
    /// EN: Number of matched documents
    public var matchedCount: Int64

    /// 实际修改的文档数
    /// EN: Number of actually modified documents
    public var modifiedCount: Int64

    /// upsert 插入的文档数
    /// EN: Number of documents inserted by upsert
    public var upsertedCount: Int64

    /// upsert 插入的文档 _id（如果有）
    /// EN: _id of document inserted by upsert (if any)
    public var upsertedID: BSONValue?

    /// 初始化更新结果
    /// EN: Initialize update result
    public init(
        matchedCount: Int64 = 0,
        modifiedCount: Int64 = 0,
        upsertedCount: Int64 = 0,
        upsertedID: BSONValue? = nil
    ) {
        self.matchedCount = matchedCount
        self.modifiedCount = modifiedCount
        self.upsertedCount = upsertedCount
        self.upsertedID = upsertedID
    }
}

// MARK: - FindAndModify 选项 / FindAndModify Options

/// findAndModify 的选项
/// EN: Options for findAndModify
public struct FindAndModifyOptions: Sendable {
    /// 查询条件
    /// EN: Query filter
    public var query: BSONDocument

    /// 更新操作（与 remove 互斥）
    /// EN: Update operation (mutually exclusive with remove)
    public var update: BSONDocument?

    /// 是否删除
    /// EN: Whether to delete
    public var remove: Bool

    /// 返回修改后的文档（默认返回原文档）
    /// EN: Return modified document (default returns original document)
    public var returnNew: Bool

    /// 未找到时是否插入
    /// EN: Whether to insert if not found
    public var upsert: Bool

    /// 排序
    /// EN: Sort specification
    public var sort: BSONDocument

    /// 初始化 findAndModify 选项
    /// EN: Initialize findAndModify options
    public init(
        query: BSONDocument = BSONDocument(),
        update: BSONDocument? = nil,
        remove: Bool = false,
        returnNew: Bool = false,
        upsert: Bool = false,
        sort: BSONDocument = BSONDocument()
    ) {
        self.query = query
        self.update = update
        self.remove = remove
        self.returnNew = returnNew
        self.upsert = upsert
        self.sort = sort
    }
}
