// Created by Yanjunhui

import Foundation

/// 资源限制常量（对齐 MongoDB 7.0）
/// EN: Resource limit constants (aligned with MongoDB 7.0)
public enum Limits {
    // MARK: - 文档大小限制 / Document Size Limits

    /// 最大 BSON 文档大小：16 MB
    /// EN: Maximum BSON document size: 16 MB
    public static let maxBSONDocumentSize = 16 * 1024 * 1024

    // MARK: - 嵌套深度限制 / Nesting Depth Limits

    /// 最大文档嵌套深度
    /// EN: Maximum document nesting depth
    public static let maxDocumentNestingDepth = 100

    // MARK: - 字段名长度限制 / Field Name Length Limits

    /// 最大字段名长度
    /// EN: Maximum field name length
    public static let maxFieldNameLength = 1024

    // MARK: - 命名空间限制 / Namespace Limits

    /// 最大命名空间长度（数据库名 + 集合名）
    /// EN: Maximum namespace length (database name + collection name)
    public static let maxNamespaceLength = 255

    /// 最大数据库名长度
    /// EN: Maximum database name length
    public static let maxDatabaseNameLength = 64

    /// 最大集合名长度
    /// EN: Maximum collection name length
    public static let maxCollectionNameLength = 255

    // MARK: - 批量操作限制 / Bulk Operation Limits

    /// 最大批量写入文档数
    /// EN: Maximum number of documents in bulk write
    public static let maxWriteBatchSize = 100_000

    /// 最大批量操作数
    /// EN: Maximum number of bulk operations
    public static let maxBulkOpsCount = 100_000

    // MARK: - 索引限制 / Index Limits

    /// 每个集合最大索引数
    /// EN: Maximum number of indexes per collection
    public static let maxIndexesPerCollection = 64

    /// 索引键总长度限制
    /// EN: Maximum total length of index key
    public static let maxIndexKeyLength = 1024

    /// 复合索引最大字段数
    /// EN: Maximum number of fields in compound index
    public static let maxCompoundIndexFields = 32

    // MARK: - 数组/对象元素限制 / Array/Object Element Limits

    /// 数组最大元素数
    /// EN: Maximum number of elements in array
    public static let maxArrayLength = 1_000_000

    /// 对象最大字段数
    /// EN: Maximum number of fields in object
    public static let maxObjectFields = 100_000

    // MARK: - 字符串限制 / String Limits

    /// 最大字符串长度：16 MB
    /// EN: Maximum string length: 16 MB
    public static let maxStringLength = 16 * 1024 * 1024

    // MARK: - 查询限制 / Query Limits

    /// 最大排序字段数
    /// EN: Maximum number of sort fields
    public static let maxSortPatternLength = 32

    /// 最大投影字段数
    /// EN: Maximum number of projection fields
    public static let maxProjectionFields = 100
}

/// 可配置的资源限制
/// EN: Configurable resource limits
public struct ConfigurableLimits: Sendable {
    /// 最大文档大小
    /// EN: Maximum document size
    public var maxDocumentSize: Int

    /// 最大嵌套深度
    /// EN: Maximum nesting depth
    public var maxNestingDepth: Int

    /// 最大批量写入数
    /// EN: Maximum write batch size
    public var maxWriteBatchSize: Int

    /// 最大命名空间长度
    /// EN: Maximum namespace length
    public var maxNamespaceLength: Int

    /// 每个集合最大索引数
    /// EN: Maximum indexes per collection
    public var maxIndexesPerCollection: Int

    /// 初始化可配置限制
    /// EN: Initialize configurable limits
    public init(
        maxDocumentSize: Int = Limits.maxBSONDocumentSize,
        maxNestingDepth: Int = Limits.maxDocumentNestingDepth,
        maxWriteBatchSize: Int = Limits.maxWriteBatchSize,
        maxNamespaceLength: Int = Limits.maxNamespaceLength,
        maxIndexesPerCollection: Int = Limits.maxIndexesPerCollection
    ) {
        self.maxDocumentSize = maxDocumentSize
        self.maxNestingDepth = maxNestingDepth
        self.maxWriteBatchSize = maxWriteBatchSize
        self.maxNamespaceLength = maxNamespaceLength
        self.maxIndexesPerCollection = maxIndexesPerCollection
    }

    /// 默认限制配置
    /// EN: Default limits configuration
    public static let `default` = ConfigurableLimits()
}
