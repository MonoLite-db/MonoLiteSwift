// Created by Yanjunhui

import Foundation

// MARK: - 管道阶段协议 / Pipeline Stage Protocol

/// 管道阶段协议，所有聚合阶段都需要实现此协议
/// EN: Pipeline stage protocol, all aggregation stages must implement this protocol
public protocol PipelineStage: Sendable {
    /// 执行阶段处理，对输入文档进行转换
    /// EN: Execute stage processing, transform input documents
    func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument]

    /// 阶段名称（如 $match, $project 等）
    /// EN: Stage name (e.g., $match, $project, etc.)
    var name: String { get }
}

// MARK: - 聚合管道 / Aggregation Pipeline

/// 聚合管道，用于执行一系列数据转换操作
/// EN: Aggregation pipeline for executing a series of data transformation operations
public struct Pipeline: Sendable {
    /// 管道阶段列表
    /// EN: List of pipeline stages
    private let stages: [any PipelineStage]

    /// 数据库引用（用于 $lookup 等跨集合操作）
    /// EN: Database reference (for cross-collection operations like $lookup)
    private let db: Database?

    /// 初始化管道（不带数据库上下文）
    /// EN: Initialize pipeline (without database context)
    /// - Parameter stages: 阶段定义文档数组 / Array of stage definition documents
    public init(stages: [BSONDocument]) throws {
        try self.init(stages: stages, db: nil)
    }

    /// 初始化管道（带数据库上下文）
    /// EN: Initialize pipeline (with database context)
    /// - Parameters:
    ///   - stages: 阶段定义文档数组 / Array of stage definition documents
    ///   - db: 数据库实例 / Database instance
    public init(stages: [BSONDocument], db: Database?) throws {
        var parsed: [any PipelineStage] = []
        parsed.reserveCapacity(stages.count)

        for stageDoc in stages {
            // 每个阶段文档必须恰好包含一个字段 / EN: Each stage document must have exactly one field
            guard stageDoc.count == 1, let first = stageDoc.first else {
                throw MonoError.badValue("invalid pipeline stage: each stage must have exactly one field")
            }
            let stage = try Pipeline.createStage(name: first.key, spec: first.value, db: db)
            parsed.append(stage)
        }
        self.stages = parsed
        self.db = db
    }

    /// 执行管道，依次处理所有阶段
    /// EN: Execute pipeline, process all stages sequentially
    /// - Parameter docs: 输入文档数组 / Input document array
    /// - Returns: 处理后的文档数组 / Processed document array
    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        var result = docs
        for stage in stages {
            result = try await stage.execute(result)
        }
        return result
    }

    /// 根据阶段名称创建对应的阶段实例
    /// EN: Create stage instance based on stage name
    /// - Parameters:
    ///   - name: 阶段名称 / Stage name
    ///   - spec: 阶段规格 / Stage specification
    ///   - db: 数据库实例 / Database instance
    /// - Returns: 管道阶段实例 / Pipeline stage instance
    private static func createStage(name: String, spec: BSONValue, db: Database?) throws -> any PipelineStage {
        switch name {
        case "$match":
            // 筛选阶段 / EN: Filter stage
            guard case .document(let d) = spec else { throw MonoError.badValue("$match requires a document") }
            return MatchStage(filter: d)
        case "$project":
            // 投影阶段 / EN: Projection stage
            guard case .document(let d) = spec else { throw MonoError.badValue("$project requires a document") }
            return ProjectStage(projection: d)
        case "$sort":
            // 排序阶段 / EN: Sort stage
            guard case .document(let d) = spec else { throw MonoError.badValue("$sort requires a document") }
            return SortStage(sortSpec: d)
        case "$limit":
            // 限制数量阶段 / EN: Limit stage
            let n = Int64(spec.intValue ?? 0)
            return LimitStage(limit: n)
        case "$skip":
            // 跳过数量阶段 / EN: Skip stage
            let n = Int64(spec.intValue ?? 0)
            return SkipStage(skip: n)
        case "$group":
            // 分组阶段 / EN: Group stage
            guard case .document(let d) = spec else { throw MonoError.badValue("$group requires a document") }
            return GroupStage(groupSpec: d)
        case "$count":
            // 计数阶段 / EN: Count stage
            guard case .string(let fieldName) = spec, !fieldName.isEmpty else {
                throw MonoError.badValue("$count requires a non-empty string field name")
            }
            return CountStage(field: fieldName)
        case "$unwind":
            // 展开数组阶段 / EN: Unwind array stage
            return try UnwindStage(spec: spec)
        case "$addFields", "$set":
            // 添加字段阶段 / EN: Add fields stage
            guard case .document(let d) = spec else { throw MonoError.badValue("\(name) requires a document") }
            return AddFieldsStage(fields: d, stageName: name)
        case "$unset":
            // 移除字段阶段 / EN: Remove fields stage
            return try UnsetStage(spec: spec)
        case "$replaceRoot":
            // 替换根文档阶段 / EN: Replace root document stage
            guard case .document(let d) = spec else { throw MonoError.badValue("$replaceRoot requires a document") }
            return try ReplaceRootStage(spec: d)
        case "$lookup":
            // 关联查询阶段 / EN: Lookup/join stage
            guard case .document(let d) = spec else { throw MonoError.badValue("$lookup requires a document") }
            return LookupStage(spec: d, db: db)
        default:
            throw MonoError.badValue("unsupported pipeline stage: \(name)")
        }
    }
}

// MARK: - $match 筛选阶段 / Match Stage

/// $match 筛选阶段，用于过滤文档
/// EN: $match stage for filtering documents
public struct MatchStage: PipelineStage {
    public let name: String = "$match"

    /// 筛选条件
    /// EN: Filter condition
    private let filter: BSONDocument

    /// 初始化筛选阶段
    /// EN: Initialize match stage
    /// - Parameter filter: 筛选条件文档 / Filter condition document
    public init(filter: BSONDocument) {
        self.filter = filter
    }

    /// 执行筛选，返回符合条件的文档
    /// EN: Execute filtering, return documents matching the condition
    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        // 空筛选条件返回所有文档 / EN: Empty filter returns all documents
        if filter.isEmpty { return docs }
        let matcher = FilterMatcher(filter)
        return docs.filter { matcher.match($0) }
    }
}

// MARK: - $project 投影阶段 / Project Stage

/// $project 投影阶段，用于选择、重命名或计算字段
/// EN: $project stage for selecting, renaming, or computing fields
public struct ProjectStage: PipelineStage {
    public let name: String = "$project"

    /// 投影规格
    /// EN: Projection specification
    private let projection: BSONDocument

    /// 初始化投影阶段
    /// EN: Initialize project stage
    /// - Parameter projection: 投影规格文档 / Projection specification document
    public init(projection: BSONDocument) {
        self.projection = projection
    }

    /// 执行投影，返回处理后的文档
    /// EN: Execute projection, return processed documents
    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        applyProjection(docs, projection)
    }
}

// MARK: - $sort 排序阶段 / Sort Stage

/// $sort 排序阶段，用于对文档进行排序
/// EN: $sort stage for sorting documents
public struct SortStage: PipelineStage {
    public let name: String = "$sort"

    /// 排序规格
    /// EN: Sort specification
    private let sortSpec: BSONDocument

    /// 初始化排序阶段
    /// EN: Initialize sort stage
    /// - Parameter sortSpec: 排序规格文档（1 为升序，-1 为降序） / Sort specification document (1 for ascending, -1 for descending)
    public init(sortSpec: BSONDocument) {
        self.sortSpec = sortSpec
    }

    /// 执行排序，返回排序后的文档
    /// EN: Execute sorting, return sorted documents
    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        sortDocuments(docs, sortSpec)
    }
}

// MARK: - $limit 限制阶段 / Limit Stage

/// $limit 限制阶段，用于限制返回的文档数量
/// EN: $limit stage for limiting the number of returned documents
public struct LimitStage: PipelineStage {
    public let name: String = "$limit"

    /// 限制数量
    /// EN: Limit count
    private let limit: Int64

    /// 初始化限制阶段
    /// EN: Initialize limit stage
    /// - Parameter limit: 最大返回文档数 / Maximum number of documents to return
    public init(limit: Int64) {
        self.limit = limit
    }

    /// 执行限制，返回前 N 个文档
    /// EN: Execute limit, return first N documents
    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        // 限制数小于等于 0 时返回所有文档 / EN: Return all documents when limit <= 0
        if limit <= 0 { return docs }
        // 文档数不超过限制时直接返回 / EN: Return directly when document count doesn't exceed limit
        if Int64(docs.count) <= limit { return docs }
        return Array(docs.prefix(Int(limit)))
    }
}

// MARK: - $skip 跳过阶段 / Skip Stage

/// $skip 跳过阶段，用于跳过指定数量的文档
/// EN: $skip stage for skipping a specified number of documents
public struct SkipStage: PipelineStage {
    public let name: String = "$skip"

    /// 跳过数量
    /// EN: Skip count
    private let skip: Int64

    /// 初始化跳过阶段
    /// EN: Initialize skip stage
    /// - Parameter skip: 要跳过的文档数 / Number of documents to skip
    public init(skip: Int64) {
        self.skip = skip
    }

    /// 执行跳过，返回剩余文档
    /// EN: Execute skip, return remaining documents
    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        // 跳过数小于等于 0 时返回所有文档 / EN: Return all documents when skip <= 0
        if skip <= 0 { return docs }
        // 跳过数大于等于文档数时返回空数组 / EN: Return empty array when skip >= document count
        if Int64(docs.count) <= skip { return [] }
        return Array(docs.dropFirst(Int(skip)))
    }
}

// MARK: - $count 计数阶段 / Count Stage

/// $count 计数阶段，用于统计文档数量
/// EN: $count stage for counting documents
public struct CountStage: PipelineStage {
    public let name: String = "$count"

    /// 输出字段名
    /// EN: Output field name
    private let field: String

    /// 初始化计数阶段
    /// EN: Initialize count stage
    /// - Parameter field: 存储计数结果的字段名 / Field name to store the count result
    public init(field: String) {
        self.field = field
    }

    /// 执行计数，返回包含文档数量的单个文档
    /// EN: Execute count, return a single document containing the document count
    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        return [BSONDocument([(field, .int64(Int64(docs.count)))])]
    }
}

// MARK: - $group 分组阶段 / Group Stage

/// $group 分组阶段，用于按指定键分组并进行聚合计算
/// EN: $group stage for grouping by specified key and performing aggregation calculations
public struct GroupStage: PipelineStage {
    public let name: String = "$group"

    /// 分组键表达式
    /// EN: Group key expression
    private let idExpr: BSONValue?

    /// 累加器定义
    /// EN: Accumulator definitions
    private let accumulators: BSONDocument

    /// 初始化分组阶段
    /// EN: Initialize group stage
    /// - Parameter groupSpec: 分组规格文档 / Group specification document
    public init(groupSpec: BSONDocument) {
        self.idExpr = groupSpec["_id"]
        var acc = BSONDocument()
        // 提取除 _id 外的所有累加器定义 / EN: Extract all accumulator definitions except _id
        for (k, v) in groupSpec where k != "_id" {
            acc[k] = v
        }
        self.accumulators = acc
    }

    /// 执行分组聚合
    /// EN: Execute group aggregation
    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        /// 分组状态，用于存储每个分组的 ID 和累积值
        /// EN: Group state for storing each group's ID and accumulated values
        struct GroupState {
            var id: BSONValue
            var values: [String: [BSONValue]]
        }

        var groups: [String: GroupState] = [:]
        var order: [String] = [] // 保持分组顺序 / EN: Maintain group order

        // 遍历文档进行分组 / EN: Iterate documents for grouping
        for doc in docs {
            let keyVal = computeGroupKey(doc)
            let keyStr = keyVal.description
            if groups[keyStr] == nil {
                groups[keyStr] = GroupState(id: keyVal, values: [:])
                order.append(keyStr)
            }

            // 收集每个累加器的值 / EN: Collect values for each accumulator
            for (fieldName, specVal) in accumulators {
                guard case .document(let accSpec) = specVal, accSpec.count == 1, let accFirst = accSpec.first else {
                    continue
                }
                let op = accFirst.key
                let expr = accFirst.value
                let v = evaluateExpression(expr, doc: doc)
                let bucketKey = "\(fieldName)_\(op)"
                groups[keyStr]!.values[bucketKey, default: []].append(v)
            }
        }

        // 计算最终结果 / EN: Calculate final results
        var result: [BSONDocument] = []
        result.reserveCapacity(groups.count)

        for keyStr in order {
            guard let state = groups[keyStr] else { continue }
            var out = BSONDocument([("_id", state.id)])

            // 应用每个累加器 / EN: Apply each accumulator
            for (fieldName, specVal) in accumulators {
                guard case .document(let accSpec) = specVal, accSpec.count == 1, let accFirst = accSpec.first else {
                    continue
                }
                let op = accFirst.key
                let values = state.values["\(fieldName)_\(op)"] ?? []
                let finalVal: BSONValue

                switch op {
                case "$sum":
                    // 求和 / EN: Sum
                    finalVal = .double(computeSum(values))
                case "$avg":
                    // 平均值 / EN: Average
                    finalVal = .double(computeAvg(values))
                case "$min":
                    // 最小值 / EN: Minimum
                    finalVal = computeMin(values)
                case "$max":
                    // 最大值 / EN: Maximum
                    finalVal = computeMax(values)
                case "$first":
                    // 第一个值 / EN: First value
                    finalVal = values.first ?? .null
                case "$last":
                    // 最后一个值 / EN: Last value
                    finalVal = values.last ?? .null
                case "$count":
                    // 计数 / EN: Count
                    finalVal = .int64(Int64(values.count))
                case "$push":
                    // 收集为数组 / EN: Collect into array
                    finalVal = .array(BSONArray(values))
                case "$addToSet":
                    // 收集为去重集合 / EN: Collect into deduplicated set
                    finalVal = .array(computeAddToSet(values))
                default:
                    throw MonoError.badValue("unsupported accumulator: \(op)")
                }

                out[fieldName] = finalVal
            }

            result.append(out)
        }

        return result
    }

    /// 计算文档的分组键
    /// EN: Calculate the group key for a document
    private func computeGroupKey(_ doc: BSONDocument) -> BSONValue {
        guard let idExpr else { return .null }
        switch idExpr {
        case .string(let s) where s.hasPrefix("$"):
            // 字段引用 / EN: Field reference
            return doc.getValue(forPath: String(s.dropFirst())) ?? .null
        case .document(let d):
            // 复合键 / EN: Compound key
            var out = BSONDocument()
            for (k, v) in d {
                if case .string(let s) = v, s.hasPrefix("$") {
                    out[k] = doc.getValue(forPath: String(s.dropFirst())) ?? .null
                } else {
                    out[k] = v
                }
            }
            return .document(out)
        default:
            // 常量值 / EN: Constant value
            return idExpr
        }
    }

    /// 计算表达式的值
    /// EN: Evaluate expression value
    private func evaluateExpression(_ expr: BSONValue, doc: BSONDocument) -> BSONValue {
        if case .string(let s) = expr, s.hasPrefix("$") {
            return doc.getValue(forPath: String(s.dropFirst())) ?? .null
        }
        return expr
    }

    /// 计算数值求和
    /// EN: Calculate numeric sum
    private func computeSum(_ values: [BSONValue]) -> Double {
        values.reduce(0.0) { $0 + (toDouble($1) ?? 0.0) }
    }

    /// 计算数值平均值
    /// EN: Calculate numeric average
    private func computeAvg(_ values: [BSONValue]) -> Double {
        guard !values.isEmpty else { return 0.0 }
        return computeSum(values) / Double(values.count)
    }

    /// 计算最小值
    /// EN: Calculate minimum value
    private func computeMin(_ values: [BSONValue]) -> BSONValue {
        guard var m = values.first else { return .null }
        for v in values.dropFirst() {
            if BSONCompare.compare(v, m) < 0 { m = v }
        }
        return m
    }

    /// 计算最大值
    /// EN: Calculate maximum value
    private func computeMax(_ values: [BSONValue]) -> BSONValue {
        guard var m = values.first else { return .null }
        for v in values.dropFirst() {
            if BSONCompare.compare(v, m) > 0 { m = v }
        }
        return m
    }

    /// 计算去重集合
    /// EN: Calculate deduplicated set
    private func computeAddToSet(_ values: [BSONValue]) -> BSONArray {
        var seen: Set<String> = []
        var out = BSONArray()
        for v in values {
            let k = v.description
            if seen.insert(k).inserted {
                out.append(v)
            }
        }
        return out
    }

    /// 将 BSON 值转换为 Double
    /// EN: Convert BSON value to Double
    private func toDouble(_ v: BSONValue) -> Double? {
        switch v {
        case .double(let d): return d
        case .int32(let i): return Double(i)
        case .int64(let i): return Double(i)
        case .decimal128(let d): return d.doubleValue
        default: return nil
        }
    }
}

// MARK: - $unwind 展开阶段 / Unwind Stage

/// $unwind 展开阶段，用于将数组字段展开为多个文档
/// EN: $unwind stage for expanding array fields into multiple documents
public struct UnwindStage: PipelineStage {
    public let name: String = "$unwind"

    /// 要展开的数组字段路径
    /// EN: Path of the array field to unwind
    private let path: String

    /// 是否保留空数组和 null 值的文档
    /// EN: Whether to preserve documents with empty arrays and null values
    private let preserveNullAndEmptyArrays: Bool

    /// 初始化展开阶段
    /// EN: Initialize unwind stage
    /// - Parameter spec: 展开规格（字符串或文档） / Unwind specification (string or document)
    public init(spec: BSONValue) throws {
        switch spec {
        case .string(let s):
            // 简写形式：直接指定路径 / EN: Short form: directly specify path
            guard s.hasPrefix("$") else { throw MonoError.badValue("$unwind path must start with $") }
            self.path = String(s.dropFirst())
            self.preserveNullAndEmptyArrays = false
        case .document(let d):
            // 完整形式：包含 path 和选项 / EN: Full form: includes path and options
            guard let p = d["path"]?.stringValue, p.hasPrefix("$") else {
                throw MonoError.badValue("$unwind requires a path")
            }
            self.path = String(p.dropFirst())
            self.preserveNullAndEmptyArrays = d["preserveNullAndEmptyArrays"]?.boolValue ?? false
        default:
            throw MonoError.badValue("$unwind requires a string or document")
        }
    }

    /// 执行展开，将数组中的每个元素生成一个独立文档
    /// EN: Execute unwind, generate a separate document for each element in the array
    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        var result: [BSONDocument] = []
        for doc in docs {
            let fieldVal = doc.getValue(forPath: path)
            guard case .array(let arr)? = fieldVal else {
                // 非数组值：根据选项决定是否保留 / EN: Non-array value: preserve based on option
                if preserveNullAndEmptyArrays { result.append(doc) }
                continue
            }
            if arr.isEmpty {
                // 空数组：根据选项决定是否保留 / EN: Empty array: preserve based on option
                if preserveNullAndEmptyArrays { result.append(doc) }
                continue
            }
            // 展开数组中的每个元素 / EN: Unwind each element in the array
            for elem in arr {
                var newDoc = doc
                newDoc.setValue(elem, forPath: path)
                result.append(newDoc)
            }
        }
        return result
    }
}

// MARK: - $addFields / $set 添加字段阶段 / Add Fields Stage

/// $addFields / $set 添加字段阶段，用于添加或覆盖字段
/// EN: $addFields / $set stage for adding or overwriting fields
public struct AddFieldsStage: PipelineStage {
    public let name: String

    /// 要添加的字段定义
    /// EN: Field definitions to add
    private let fields: BSONDocument

    /// 初始化添加字段阶段
    /// EN: Initialize add fields stage
    /// - Parameters:
    ///   - fields: 字段定义文档 / Field definition document
    ///   - stageName: 阶段名称（$addFields 或 $set） / Stage name ($addFields or $set)
    public init(fields: BSONDocument, stageName: String) {
        self.fields = fields
        self.name = stageName
    }

    /// 执行添加字段，返回包含新字段的文档
    /// EN: Execute add fields, return documents with new fields
    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        var result: [BSONDocument] = []
        result.reserveCapacity(docs.count)
        for doc in docs {
            var newDoc = doc
            for (k, v) in fields {
                let val = evaluateExpression(v, doc: doc)
                newDoc.setValue(val, forPath: k)
            }
            result.append(newDoc)
        }
        return result
    }

    /// 计算表达式的值
    /// EN: Evaluate expression value
    private func evaluateExpression(_ expr: BSONValue, doc: BSONDocument) -> BSONValue {
        if case .string(let s) = expr, s.hasPrefix("$") {
            return doc.getValue(forPath: String(s.dropFirst())) ?? .null
        }
        return expr
    }
}

// MARK: - $unset 移除字段阶段 / Unset Stage

/// $unset 移除字段阶段，用于从文档中移除指定字段
/// EN: $unset stage for removing specified fields from documents
public struct UnsetStage: PipelineStage {
    public let name: String = "$unset"

    /// 要移除的字段列表
    /// EN: List of fields to remove
    private let fields: [String]

    /// 初始化移除字段阶段
    /// EN: Initialize unset stage
    /// - Parameter spec: 字段名或字段名数组 / Field name or array of field names
    public init(spec: BSONValue) throws {
        switch spec {
        case .string(let s):
            // 单个字段 / EN: Single field
            self.fields = [s]
        case .array(let arr):
            // 多个字段 / EN: Multiple fields
            self.fields = arr.compactMap { $0.stringValue }
        default:
            throw MonoError.badValue("$unset requires a string or array of strings")
        }
    }

    /// 执行移除字段
    /// EN: Execute field removal
    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        var result: [BSONDocument] = []
        result.reserveCapacity(docs.count)
        for doc in docs {
            var newDoc = doc
            for f in fields {
                unsetField(&newDoc, path: f)
            }
            result.append(newDoc)
        }
        return result
    }
}

// MARK: - $replaceRoot 替换根文档阶段 / Replace Root Stage

/// $replaceRoot 替换根文档阶段，用于将子文档提升为根文档
/// EN: $replaceRoot stage for promoting a subdocument to the root document
public struct ReplaceRootStage: PipelineStage {
    public let name: String = "$replaceRoot"

    /// 新的根文档表达式
    /// EN: New root document expression
    private let newRoot: BSONValue

    /// 初始化替换根文档阶段
    /// EN: Initialize replace root stage
    /// - Parameter spec: 规格文档（必须包含 newRoot 字段） / Specification document (must contain newRoot field)
    public init(spec: BSONDocument) throws {
        guard let nr = spec["newRoot"] else {
            throw MonoError.badValue("$replaceRoot requires newRoot field")
        }
        self.newRoot = nr
    }

    /// 执行替换根文档
    /// EN: Execute root document replacement
    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        var result: [BSONDocument] = []
        result.reserveCapacity(docs.count)
        for doc in docs {
            if let d = evaluateNewRoot(doc) {
                result.append(d)
            }
        }
        return result
    }

    /// 计算新的根文档
    /// EN: Evaluate new root document
    private func evaluateNewRoot(_ doc: BSONDocument) -> BSONDocument? {
        switch newRoot {
        case .string(let s) where s.hasPrefix("$"):
            // 字段引用：取子文档 / EN: Field reference: get subdocument
            if case .document(let d) = doc.getValue(forPath: String(s.dropFirst())) {
                return d
            }
            return nil
        case .document(let d):
            // 文档表达式：构建新文档 / EN: Document expression: build new document
            var out = BSONDocument()
            for (k, v) in d {
                out[k] = evaluateExpr(v, doc: doc)
            }
            return out
        default:
            return nil
        }
    }

    /// 计算表达式的值
    /// EN: Evaluate expression value
    private func evaluateExpr(_ expr: BSONValue, doc: BSONDocument) -> BSONValue {
        if case .string(let s) = expr, s.hasPrefix("$") {
            return doc.getValue(forPath: String(s.dropFirst())) ?? .null
        }
        return expr
    }
}

// MARK: - $lookup 关联查询阶段 / Lookup Stage

/// $lookup 关联查询阶段，用于执行左外连接操作
/// EN: $lookup stage for performing left outer join operations
public struct LookupStage: PipelineStage {
    public let name: String = "$lookup"

    /// 要关联的集合名
    /// EN: Name of the collection to join
    private let from: String

    /// 本地字段（左表字段）
    /// EN: Local field (left table field)
    private let localField: String

    /// 外部字段（右表字段）
    /// EN: Foreign field (right table field)
    private let foreignField: String

    /// 输出字段名
    /// EN: Output field name
    private let asField: String

    /// 数据库引用
    /// EN: Database reference
    private let db: Database?

    /// 初始化关联查询阶段
    /// EN: Initialize lookup stage
    /// - Parameters:
    ///   - spec: 关联规格文档 / Lookup specification document
    ///   - db: 数据库实例 / Database instance
    public init(spec: BSONDocument, db: Database?) {
        self.from = spec["from"]?.stringValue ?? ""
        self.localField = spec["localField"]?.stringValue ?? ""
        self.foreignField = spec["foreignField"]?.stringValue ?? ""
        self.asField = spec["as"]?.stringValue ?? ""
        self.db = db
    }

    /// 执行关联查询
    /// EN: Execute lookup query
    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        // 验证必需的数据库上下文 / EN: Validate required database context
        guard let db else {
            throw MonoError.badValue("$lookup requires database context")
        }
        if from.isEmpty || asField.isEmpty {
            throw MonoError.badValue("$lookup requires 'from' and 'as' fields")
        }

        // 获取外部集合的所有文档 / EN: Fetch all documents from the foreign collection
        var foreignDocs: [BSONDocument] = []
        if let foreign = await db.getCollection(from) {
            foreignDocs = try await foreign.find(BSONDocument())
        }

        var out: [BSONDocument] = []
        out.reserveCapacity(docs.count)

        // 对每个输入文档执行关联匹配 / EN: Perform join matching for each input document
        for doc in docs {
            let localVal = doc.getValue(forPath: localField) ?? .null
            var matches: [BSONValue] = []
            // 查找所有匹配的外部文档 / EN: Find all matching foreign documents
            for fdoc in foreignDocs {
                let fv = fdoc.getValue(forPath: foreignField) ?? .null
                if BSONCompare.compare(localVal, fv) == 0 {
                    matches.append(.document(fdoc))
                }
            }
            var newDoc = doc
            newDoc[asField] = .array(BSONArray(matches))
            out.append(newDoc)
        }
        return out
    }
}

// MARK: - 辅助函数 / Helper Functions

/// 移除文档中指定路径的字段
/// EN: Remove the field at the specified path from a document
/// - Parameters:
///   - doc: 要修改的文档 / Document to modify
///   - path: 字段路径（支持点分隔的嵌套路径） / Field path (supports dot-separated nested paths)
private func unsetField(_ doc: inout BSONDocument, path: String) {
    // 按路径组件分解 / EN: Split by path components
    let parts = path.split(separator: ".").map(String.init)
    guard let first = parts.first else { return }
    if parts.count == 1 {
        // 单层路径：直接移除 / EN: Single-level path: remove directly
        doc[first] = nil
        return
    }
    // 多层路径：递归处理 / EN: Multi-level path: process recursively
    guard case .document(var sub)? = doc[first] else { return }
    let rest = parts.dropFirst().joined(separator: ".")
    unsetField(&sub, path: rest)
    doc[first] = .document(sub)
}
