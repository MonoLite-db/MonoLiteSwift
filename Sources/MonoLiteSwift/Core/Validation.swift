// Created by Yanjunhui

import Foundation

/// 验证工具
/// EN: Validation utilities
public enum Validation {
    // MARK: - 数据库名验证 / Database Name Validation

    /// 验证数据库名
    /// EN: Validate database name
    public static func validateDatabaseName(_ name: String) throws {
        guard !name.isEmpty else {
            throw MonoError.invalidNamespace("database name cannot be empty")
        }

        guard name.count <= Limits.maxDatabaseNameLength else {
            throw MonoError.invalidNamespace(
                "database name exceeds maximum length of \(Limits.maxDatabaseNameLength)"
            )
        }

        // 检查非法字符 / EN: Check for invalid characters
        let invalidChars: [Character] = ["/", "\\", ".", " ", "\"", "$", "*", "<", ">", ":", "|", "?", "\0"]
        for char in invalidChars {
            if name.contains(char) {
                throw MonoError.invalidNamespace("database name cannot contain '\(char)'")
            }
        }

        // 不能是空白字符串 / EN: Cannot be whitespace only string
        guard !name.trimmingCharacters(in: .whitespaces).isEmpty else {
            throw MonoError.invalidNamespace("database name cannot be empty or whitespace only")
        }
    }

    // MARK: - 集合名验证 / Collection Name Validation

    /// 验证集合名
    /// EN: Validate collection name
    public static func validateCollectionName(_ name: String) throws {
        guard !name.isEmpty else {
            throw MonoError.invalidNamespace("collection name cannot be empty")
        }

        guard name.count <= Limits.maxCollectionNameLength else {
            throw MonoError.invalidNamespace(
                "collection name exceeds maximum length of \(Limits.maxCollectionNameLength)"
            )
        }

        // 不能以 system. 开头（系统保留）/ EN: Cannot start with 'system.' (reserved for system)
        guard !name.hasPrefix("system.") else {
            throw MonoError.invalidNamespace("collection name cannot start with 'system.'")
        }

        // 检查非法字符 / EN: Check for invalid characters
        guard !name.contains("$") else {
            throw MonoError.invalidNamespace("collection name cannot contain '$'")
        }

        guard !name.contains("\0") else {
            throw MonoError.invalidNamespace("collection name cannot contain null bytes")
        }

        // 不能是空白字符串 / EN: Cannot be whitespace only string
        guard !name.trimmingCharacters(in: .whitespaces).isEmpty else {
            throw MonoError.invalidNamespace("collection name cannot be empty or whitespace only")
        }
    }

    // MARK: - 命名空间验证 / Namespace Validation

    /// 验证命名空间（db.collection）
    /// EN: Validate namespace (db.collection)
    public static func validateNamespace(_ namespace: String) throws {
        guard namespace.count <= Limits.maxNamespaceLength else {
            throw MonoError.invalidNamespace(
                "namespace exceeds maximum length of \(Limits.maxNamespaceLength)"
            )
        }

        let parts = namespace.split(separator: ".", maxSplits: 1, omittingEmptySubsequences: false)
        guard parts.count == 2 else {
            throw MonoError.invalidNamespace("namespace must be in format 'database.collection'")
        }

        try validateDatabaseName(String(parts[0]))
        try validateCollectionName(String(parts[1]))
    }

    // MARK: - 字段名验证 / Field Name Validation

    /// 验证文档字段名（用于存储）
    /// EN: Validate document field name (for storage)
    /// - Parameters:
    ///   - name: 字段名 / EN: Field name
    ///   - isTopLevel: 是否是顶级字段 / EN: Whether it is a top-level field
    public static func validateDocumentFieldName(_ name: String, isTopLevel: Bool) throws {
        guard !name.isEmpty else {
            throw MonoError.emptyFieldName()
        }

        guard name.count <= Limits.maxFieldNameLength else {
            throw MonoError.badValue("field name exceeds maximum length of \(Limits.maxFieldNameLength)")
        }

        // 检查空字符 / EN: Check for null character
        guard !name.contains("\0") else {
            throw MonoError.badValue("field names cannot contain null bytes")
        }

        // 检查有效 UTF-8（Swift String 保证是有效 UTF-8）
        // EN: Check valid UTF-8 (Swift String guarantees valid UTF-8)
        // 文档的顶级字段不能以 $ 开头（MongoDB 规则）
        // EN: Top-level fields in documents cannot start with '$' (MongoDB rule)
        if isTopLevel && name.hasPrefix("$") {
            throw MonoError.dollarPrefixedFieldName(name)
        }
    }

    /// 验证表达式字段名（用于 filter/update/pipeline）
    /// EN: Validate expression field name (for filter/update/pipeline)
    /// 允许 $ 开头的操作符 / EN: Allows operators starting with '$'
    public static func validateExpressionFieldName(_ name: String) throws {
        guard !name.isEmpty else {
            throw MonoError.emptyFieldName()
        }

        guard name.count <= Limits.maxFieldNameLength else {
            throw MonoError.badValue("field name exceeds maximum length of \(Limits.maxFieldNameLength)")
        }

        guard !name.contains("\0") else {
            throw MonoError.badValue("field names cannot contain null bytes")
        }

        // 表达式中，$ 开头的必须是已知操作符
        // EN: In expressions, names starting with '$' must be known operators
        if name.hasPrefix("$") && !isKnownOperator(name) {
            throw MonoError(code: .dollarPrefixedFieldName, message: "unknown operator '\(name)'")
        }
    }

    // MARK: - 批量大小验证 / Batch Size Validation

    /// 验证批量大小
    /// EN: Validate batch size
    public static func validateBatchSize(_ size: Int) throws {
        guard size >= 0 else {
            throw MonoError.badValue("batch size cannot be negative")
        }
        guard size <= Limits.maxWriteBatchSize else {
            throw MonoError.badValue("batch size exceeds maximum of \(Limits.maxWriteBatchSize)")
        }
    }

    // MARK: - 文档大小验证 / Document Size Validation

    /// 检查序列化后的文档大小
    /// EN: Check serialized document size
    public static func checkDocumentSize(_ data: Data) throws {
        guard data.count <= Limits.maxBSONDocumentSize else {
            throw MonoError.documentTooLarge(size: data.count, maxSize: Limits.maxBSONDocumentSize)
        }
    }

    // MARK: - 文档结构验证（存储）/ Document Structure Validation (Storage)

    /// 验证文档结构（用于存储）
    /// EN: Validate document structure (for storage)
    ///
    /// 目标：与 Go 版"存储前校验"的语义对齐：
    /// EN: Goal: Align with Go version's "pre-storage validation" semantics:
    /// - 字段名合法（顶级字段不允许 `$` 前缀）/ EN: Valid field names (top-level fields cannot have '$' prefix)
    /// - 嵌套深度限制 / EN: Nesting depth limits
    /// - 结构递归校验（document/array）/ EN: Recursive structure validation (document/array)
    public static func validateDocument(_ doc: BSONDocument) throws {
        try validateDocumentInternal(doc, depth: 0)
    }

    /// 内部文档验证方法
    /// EN: Internal document validation method
    private static func validateDocumentInternal(_ doc: BSONDocument, depth: Int) throws {
        if depth > Limits.maxDocumentNestingDepth {
            throw BSONError.nestingTooDeep(depth)
        }

        for (key, value) in doc {
            try validateDocumentFieldName(key, isTopLevel: depth == 0)
            try validateValueInternal(value, depth: depth + 1)
        }
    }

    /// 内部数组验证方法
    /// EN: Internal array validation method
    private static func validateArrayInternal(_ arr: BSONArray, depth: Int) throws {
        if depth > Limits.maxDocumentNestingDepth {
            throw BSONError.nestingTooDeep(depth)
        }
        if arr.count > Limits.maxArrayLength {
            throw MonoError.badValue("array exceeds maximum length of \(Limits.maxArrayLength)")
        }

        for v in arr {
            try validateValueInternal(v, depth: depth + 1)
        }
    }

    /// 内部值验证方法
    /// EN: Internal value validation method
    private static func validateValueInternal(_ value: BSONValue, depth: Int) throws {
        switch value {
        case .document(let doc):
            try validateDocumentInternal(doc, depth: depth)
        case .array(let arr):
            try validateArrayInternal(arr, depth: depth)
        default:
            break
        }
    }

    // MARK: - 已知操作符 / Known Operators

    /// 已知的操作符集合
    /// EN: Set of known operators
    private static let knownOperators: Set<String> = [
        // 查询操作符 / EN: Query operators
        "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin",
        "$and", "$or", "$not", "$nor",
        "$exists", "$type", "$regex", "$options",
        "$elemMatch", "$size", "$all",
        "$mod", "$text", "$where",
        // 更新操作符 / EN: Update operators
        "$set", "$unset", "$inc", "$dec",
        "$mul", "$min", "$max", "$rename",
        "$push", "$pull", "$pop", "$addToSet",
        "$each", "$slice", "$sort", "$position",
        "$bit", "$currentDate", "$setOnInsert",
        // 聚合操作符 / EN: Aggregation operators
        "$match", "$project", "$group",
        "$limit", "$skip", "$unwind", "$lookup",
        "$count", "$facet", "$bucket", "$sample",
        "$sum", "$avg", "$first", "$last",
        "$concat", "$substr", "$toLower", "$toUpper",
        "$cond", "$ifNull", "$switch",
        // 其他 / EN: Others
        "$db", "$readPreference", "$clusterTime",
        "$replaceRoot", "$addFields", "$unset",
    ]

    /// 检查是否是已知的操作符
    /// EN: Check if the name is a known operator
    private static func isKnownOperator(_ name: String) -> Bool {
        knownOperators.contains(name)
    }
}
