// Created by Yanjunhui

import Foundation

/// 查询执行计划统计信息（对齐 Go: engine/explain.go）
/// EN: Query execution plan statistics (Go parity: engine/explain.go)
public struct ExplainResult: Sendable {
    /// 检查的索引键总数
    /// EN: Total index keys examined
    public var totalKeysExamined: Int64 = 0

    /// 检查的文档总数
    /// EN: Total documents examined
    public var totalDocsExamined: Int64 = 0

    /// 执行时间（毫秒）
    /// EN: Execution time in milliseconds
    public var executionTimeMs: Int64 = 0

    /// 命名空间（数据库.集合）
    /// EN: Namespace (db.collection)
    public var namespace: String = ""

    /// 使用的索引名称（空表示全表扫描）
    /// EN: Index name used (empty = collection scan)
    public var indexUsed: String = ""

    /// 索引边界（用于显示）
    /// EN: Index bounds (for display)
    public var indexBounds: BSONDocument?

    /// 执行阶段类型（COLLSCAN / IXSCAN / FETCH / EOF）
    /// EN: Stage type (COLLSCAN / IXSCAN / FETCH / EOF)
    public var stage: String = "COLLSCAN"

    /// 索引是否为多键索引
    /// EN: Whether index is multikey
    public var isMultiKey: Bool = false

    /// 是否有排序阶段
    /// EN: Whether sort stage exists
    public var hasSortStage: Bool = false

    /// 是否请求了投影
    /// EN: Whether projection is requested
    public var hasProjection: Bool = false

    /// 返回的文档数量
    /// EN: Number of documents returned
    public var nReturned: Int64 = 0

    /// 初始化
    /// EN: Initialize
    public init() {}

    /// 转换为 BSON 文档（对齐 Go: ToBSON）
    /// EN: Convert to BSON document (Go parity: ToBSON)
    public func toBSON() -> BSONDocument {
        let winningPlan = BSONDocument([
            ("stage", .string(stage)),
            ("indexName", .string(indexUsed.isEmpty ? "" : indexUsed)),
            ("isMultiKey", .bool(isMultiKey)),
        ])

        let queryPlanner = BSONDocument([
            ("namespace", .string(namespace)),
            ("winningPlan", .document(winningPlan)),
        ])

        let executionStats = BSONDocument([
            ("nReturned", .int64(nReturned)),
            ("executionTimeMillis", .int64(executionTimeMs)),
            ("totalKeysExamined", .int64(totalKeysExamined)),
            ("totalDocsExamined", .int64(totalDocsExamined)),
            ("hasSortStage", .bool(hasSortStage)),
            ("hasProjection", .bool(hasProjection)),
            ("note", .string("Index scan (IXSCAN) not yet implemented; always using COLLSCAN")),
        ])

        return BSONDocument([
            ("queryPlanner", .document(queryPlanner)),
            ("executionStats", .document(executionStats)),
            ("ok", .double(1.0)),
        ])
    }
}

/// Explain 详细程度级别
/// EN: Explain verbosity level
public enum ExplainVerbosity: String, Sendable {
    /// 仅查询计划
    /// EN: Query planner only
    case queryPlanner = "queryPlanner"

    /// 执行统计信息
    /// EN: Execution stats
    case executionStats = "executionStats"

    /// 所有计划执行
    /// EN: All plans execution
    case allPlansExecution = "allPlansExecution"
}

// MARK: - MonoCollection.explain

extension MonoCollection {
    /// 解释查询执行计划（对齐 Go: Collection.Explain）
    /// EN: Explain query execution plan (Go parity: Collection.Explain)
    ///
    /// 注意：当前实现始终输出 COLLSCAN，因为索引扫描执行尚未实现。
    /// EN: Note: Current implementation always outputs COLLSCAN because index scan execution is not implemented yet.
    public func explain(filter: BSONDocument, sort: BSONDocument? = nil, projection: BSONDocument? = nil) async -> ExplainResult {
        var result = ExplainResult()
        result.namespace = "\(info.name)"
        result.stage = "COLLSCAN"

        // 检查排序阶段
        // EN: Check sort stage
        if let s = sort, !s.isEmpty {
            result.hasSortStage = true
        }

        // 检查投影
        // EN: Check projection
        if let p = projection, !p.isEmpty {
            result.hasProjection = true
        }

        // 文档数量
        // EN: Document count
        result.totalDocsExamined = Int64(info.documentCount)
        result.totalKeysExamined = 0

        return result
    }
}
