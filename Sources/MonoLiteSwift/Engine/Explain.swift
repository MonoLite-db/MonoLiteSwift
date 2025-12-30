// Created by Yanjunhui

import Foundation

/// ExplainResult contains query execution plan statistics (Go parity: engine/explain.go)
public struct ExplainResult: Sendable {
    /// Total index keys examined
    public var totalKeysExamined: Int64 = 0

    /// Total documents examined
    public var totalDocsExamined: Int64 = 0

    /// Execution time in milliseconds
    public var executionTimeMs: Int64 = 0

    /// Namespace (db.collection)
    public var namespace: String = ""

    /// Index name used (empty = collection scan)
    public var indexUsed: String = ""

    /// Index bounds (for display)
    public var indexBounds: BSONDocument?

    /// Stage type (COLLSCAN / IXSCAN / FETCH / EOF)
    public var stage: String = "COLLSCAN"

    /// Whether index is multikey
    public var isMultiKey: Bool = false

    /// Whether sort stage exists
    public var hasSortStage: Bool = false

    /// Whether projection is requested
    public var hasProjection: Bool = false

    /// Number of documents returned
    public var nReturned: Int64 = 0

    public init() {}

    /// Convert to BSON document (Go parity: ToBSON)
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

/// Explain verbosity level
public enum ExplainVerbosity: String, Sendable {
    case queryPlanner = "queryPlanner"
    case executionStats = "executionStats"
    case allPlansExecution = "allPlansExecution"
}

// MARK: - MonoCollection.explain

extension MonoCollection {
    /// Explain query execution plan (Go parity: Collection.Explain)
    ///
    /// Note: Current implementation always outputs COLLSCAN because index scan execution is not implemented yet.
    public func explain(filter: BSONDocument, sort: BSONDocument? = nil, projection: BSONDocument? = nil) async -> ExplainResult {
        var result = ExplainResult()
        result.namespace = "\(info.name)"
        result.stage = "COLLSCAN"

        // Check sort stage
        if let s = sort, !s.isEmpty {
            result.hasSortStage = true
        }

        // Check projection
        if let p = projection, !p.isEmpty {
            result.hasProjection = true
        }

        // Document count
        result.totalDocsExamined = Int64(info.documentCount)
        result.totalKeysExamined = 0

        return result
    }
}

