// Created by Yanjunhui

import Foundation

/// 资源限制常量（对齐 MongoDB 7.0）
public enum Limits {
    // MARK: - 文档大小限制

    /// 最大 BSON 文档大小：16 MB
    public static let maxBSONDocumentSize = 16 * 1024 * 1024

    // MARK: - 嵌套深度限制

    /// 最大文档嵌套深度
    public static let maxDocumentNestingDepth = 100

    // MARK: - 字段名长度限制

    /// 最大字段名长度
    public static let maxFieldNameLength = 1024

    // MARK: - 命名空间限制

    /// 最大命名空间长度（数据库名 + 集合名）
    public static let maxNamespaceLength = 255

    /// 最大数据库名长度
    public static let maxDatabaseNameLength = 64

    /// 最大集合名长度
    public static let maxCollectionNameLength = 255

    // MARK: - 批量操作限制

    /// 最大批量写入文档数
    public static let maxWriteBatchSize = 100_000

    /// 最大批量操作数
    public static let maxBulkOpsCount = 100_000

    // MARK: - 索引限制

    /// 每个集合最大索引数
    public static let maxIndexesPerCollection = 64

    /// 索引键总长度限制
    public static let maxIndexKeyLength = 1024

    /// 复合索引最大字段数
    public static let maxCompoundIndexFields = 32

    // MARK: - 数组/对象元素限制

    /// 数组最大元素数
    public static let maxArrayLength = 1_000_000

    /// 对象最大字段数
    public static let maxObjectFields = 100_000

    // MARK: - 字符串限制

    /// 最大字符串长度：16 MB
    public static let maxStringLength = 16 * 1024 * 1024

    // MARK: - 查询限制

    /// 最大排序字段数
    public static let maxSortPatternLength = 32

    /// 最大投影字段数
    public static let maxProjectionFields = 100
}

/// 可配置的资源限制
public struct ConfigurableLimits: Sendable {
    public var maxDocumentSize: Int
    public var maxNestingDepth: Int
    public var maxWriteBatchSize: Int
    public var maxNamespaceLength: Int
    public var maxIndexesPerCollection: Int

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
    public static let `default` = ConfigurableLimits()
}
