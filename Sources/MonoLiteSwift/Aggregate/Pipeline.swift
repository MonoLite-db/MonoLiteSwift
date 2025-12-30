// Created by Yanjunhui

import Foundation

public protocol PipelineStage: Sendable {
    func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument]
    var name: String { get }
}

public struct Pipeline: Sendable {
    private let stages: [any PipelineStage]
    private let db: Database?

    public init(stages: [BSONDocument]) throws {
        try self.init(stages: stages, db: nil)
    }

    public init(stages: [BSONDocument], db: Database?) throws {
        var parsed: [any PipelineStage] = []
        parsed.reserveCapacity(stages.count)

        for stageDoc in stages {
            guard stageDoc.count == 1, let first = stageDoc.first else {
                throw MonoError.badValue("invalid pipeline stage: each stage must have exactly one field")
            }
            let stage = try Pipeline.createStage(name: first.key, spec: first.value, db: db)
            parsed.append(stage)
        }
        self.stages = parsed
        self.db = db
    }

    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        var result = docs
        for stage in stages {
            result = try await stage.execute(result)
        }
        return result
    }

    private static func createStage(name: String, spec: BSONValue, db: Database?) throws -> any PipelineStage {
        switch name {
        case "$match":
            guard case .document(let d) = spec else { throw MonoError.badValue("$match requires a document") }
            return MatchStage(filter: d)
        case "$project":
            guard case .document(let d) = spec else { throw MonoError.badValue("$project requires a document") }
            return ProjectStage(projection: d)
        case "$sort":
            guard case .document(let d) = spec else { throw MonoError.badValue("$sort requires a document") }
            return SortStage(sortSpec: d)
        case "$limit":
            let n = Int64(spec.intValue ?? 0)
            return LimitStage(limit: n)
        case "$skip":
            let n = Int64(spec.intValue ?? 0)
            return SkipStage(skip: n)
        case "$group":
            guard case .document(let d) = spec else { throw MonoError.badValue("$group requires a document") }
            return GroupStage(groupSpec: d)
        case "$count":
            guard case .string(let fieldName) = spec, !fieldName.isEmpty else {
                throw MonoError.badValue("$count requires a non-empty string field name")
            }
            return CountStage(field: fieldName)
        case "$unwind":
            return try UnwindStage(spec: spec)
        case "$addFields", "$set":
            guard case .document(let d) = spec else { throw MonoError.badValue("\(name) requires a document") }
            return AddFieldsStage(fields: d, stageName: name)
        case "$unset":
            return try UnsetStage(spec: spec)
        case "$replaceRoot":
            guard case .document(let d) = spec else { throw MonoError.badValue("$replaceRoot requires a document") }
            return try ReplaceRootStage(spec: d)
        case "$lookup":
            guard case .document(let d) = spec else { throw MonoError.badValue("$lookup requires a document") }
            return LookupStage(spec: d, db: db)
        default:
            throw MonoError.badValue("unsupported pipeline stage: \(name)")
        }
    }
}

// MARK: - Stages

public struct MatchStage: PipelineStage {
    public let name: String = "$match"
    private let filter: BSONDocument

    public init(filter: BSONDocument) {
        self.filter = filter
    }

    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        if filter.isEmpty { return docs }
        let matcher = FilterMatcher(filter)
        return docs.filter { matcher.match($0) }
    }
}

public struct ProjectStage: PipelineStage {
    public let name: String = "$project"
    private let projection: BSONDocument

    public init(projection: BSONDocument) {
        self.projection = projection
    }

    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        applyProjection(docs, projection)
    }
}

public struct SortStage: PipelineStage {
    public let name: String = "$sort"
    private let sortSpec: BSONDocument

    public init(sortSpec: BSONDocument) {
        self.sortSpec = sortSpec
    }

    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        sortDocuments(docs, sortSpec)
    }
}

public struct LimitStage: PipelineStage {
    public let name: String = "$limit"
    private let limit: Int64

    public init(limit: Int64) {
        self.limit = limit
    }

    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        if limit <= 0 { return docs }
        if Int64(docs.count) <= limit { return docs }
        return Array(docs.prefix(Int(limit)))
    }
}

public struct SkipStage: PipelineStage {
    public let name: String = "$skip"
    private let skip: Int64

    public init(skip: Int64) {
        self.skip = skip
    }

    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        if skip <= 0 { return docs }
        if Int64(docs.count) <= skip { return [] }
        return Array(docs.dropFirst(Int(skip)))
    }
}

public struct CountStage: PipelineStage {
    public let name: String = "$count"
    private let field: String

    public init(field: String) {
        self.field = field
    }

    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        return [BSONDocument([(field, .int64(Int64(docs.count)))])]
    }
}

// MARK: - $group

public struct GroupStage: PipelineStage {
    public let name: String = "$group"
    private let idExpr: BSONValue?
    private let accumulators: BSONDocument

    public init(groupSpec: BSONDocument) {
        self.idExpr = groupSpec["_id"]
        var acc = BSONDocument()
        for (k, v) in groupSpec where k != "_id" {
            acc[k] = v
        }
        self.accumulators = acc
    }

    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        struct GroupState {
            var id: BSONValue
            var values: [String: [BSONValue]]
        }

        var groups: [String: GroupState] = [:]
        var order: [String] = []

        for doc in docs {
            let keyVal = computeGroupKey(doc)
            let keyStr = keyVal.description
            if groups[keyStr] == nil {
                groups[keyStr] = GroupState(id: keyVal, values: [:])
                order.append(keyStr)
            }

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

        var result: [BSONDocument] = []
        result.reserveCapacity(groups.count)

        for keyStr in order {
            guard let state = groups[keyStr] else { continue }
            var out = BSONDocument([("_id", state.id)])

            for (fieldName, specVal) in accumulators {
                guard case .document(let accSpec) = specVal, accSpec.count == 1, let accFirst = accSpec.first else {
                    continue
                }
                let op = accFirst.key
                let values = state.values["\(fieldName)_\(op)"] ?? []
                let finalVal: BSONValue

                switch op {
                case "$sum":
                    finalVal = .double(computeSum(values))
                case "$avg":
                    finalVal = .double(computeAvg(values))
                case "$min":
                    finalVal = computeMin(values)
                case "$max":
                    finalVal = computeMax(values)
                case "$first":
                    finalVal = values.first ?? .null
                case "$last":
                    finalVal = values.last ?? .null
                case "$count":
                    finalVal = .int64(Int64(values.count))
                case "$push":
                    finalVal = .array(BSONArray(values))
                case "$addToSet":
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

    private func computeGroupKey(_ doc: BSONDocument) -> BSONValue {
        guard let idExpr else { return .null }
        switch idExpr {
        case .string(let s) where s.hasPrefix("$"):
            return doc.getValue(forPath: String(s.dropFirst())) ?? .null
        case .document(let d):
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
            return idExpr
        }
    }

    private func evaluateExpression(_ expr: BSONValue, doc: BSONDocument) -> BSONValue {
        if case .string(let s) = expr, s.hasPrefix("$") {
            return doc.getValue(forPath: String(s.dropFirst())) ?? .null
        }
        return expr
    }

    private func computeSum(_ values: [BSONValue]) -> Double {
        values.reduce(0.0) { $0 + (toDouble($1) ?? 0.0) }
    }

    private func computeAvg(_ values: [BSONValue]) -> Double {
        guard !values.isEmpty else { return 0.0 }
        return computeSum(values) / Double(values.count)
    }

    private func computeMin(_ values: [BSONValue]) -> BSONValue {
        guard var m = values.first else { return .null }
        for v in values.dropFirst() {
            if BSONCompare.compare(v, m) < 0 { m = v }
        }
        return m
    }

    private func computeMax(_ values: [BSONValue]) -> BSONValue {
        guard var m = values.first else { return .null }
        for v in values.dropFirst() {
            if BSONCompare.compare(v, m) > 0 { m = v }
        }
        return m
    }

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

// MARK: - $unwind

public struct UnwindStage: PipelineStage {
    public let name: String = "$unwind"
    private let path: String
    private let preserveNullAndEmptyArrays: Bool

    public init(spec: BSONValue) throws {
        switch spec {
        case .string(let s):
            guard s.hasPrefix("$") else { throw MonoError.badValue("$unwind path must start with $") }
            self.path = String(s.dropFirst())
            self.preserveNullAndEmptyArrays = false
        case .document(let d):
            guard let p = d["path"]?.stringValue, p.hasPrefix("$") else {
                throw MonoError.badValue("$unwind requires a path")
            }
            self.path = String(p.dropFirst())
            self.preserveNullAndEmptyArrays = d["preserveNullAndEmptyArrays"]?.boolValue ?? false
        default:
            throw MonoError.badValue("$unwind requires a string or document")
        }
    }

    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        var result: [BSONDocument] = []
        for doc in docs {
            let fieldVal = doc.getValue(forPath: path)
            guard case .array(let arr)? = fieldVal else {
                if preserveNullAndEmptyArrays { result.append(doc) }
                continue
            }
            if arr.isEmpty {
                if preserveNullAndEmptyArrays { result.append(doc) }
                continue
            }
            for elem in arr {
                var newDoc = doc
                newDoc.setValue(elem, forPath: path)
                result.append(newDoc)
            }
        }
        return result
    }
}

// MARK: - $addFields / $set

public struct AddFieldsStage: PipelineStage {
    public let name: String
    private let fields: BSONDocument

    public init(fields: BSONDocument, stageName: String) {
        self.fields = fields
        self.name = stageName
    }

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

    private func evaluateExpression(_ expr: BSONValue, doc: BSONDocument) -> BSONValue {
        if case .string(let s) = expr, s.hasPrefix("$") {
            return doc.getValue(forPath: String(s.dropFirst())) ?? .null
        }
        return expr
    }
}

// MARK: - $unset

public struct UnsetStage: PipelineStage {
    public let name: String = "$unset"
    private let fields: [String]

    public init(spec: BSONValue) throws {
        switch spec {
        case .string(let s):
            self.fields = [s]
        case .array(let arr):
            self.fields = arr.compactMap { $0.stringValue }
        default:
            throw MonoError.badValue("$unset requires a string or array of strings")
        }
    }

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

// MARK: - $replaceRoot

public struct ReplaceRootStage: PipelineStage {
    public let name: String = "$replaceRoot"
    private let newRoot: BSONValue

    public init(spec: BSONDocument) throws {
        guard let nr = spec["newRoot"] else {
            throw MonoError.badValue("$replaceRoot requires newRoot field")
        }
        self.newRoot = nr
    }

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

    private func evaluateNewRoot(_ doc: BSONDocument) -> BSONDocument? {
        switch newRoot {
        case .string(let s) where s.hasPrefix("$"):
            if case .document(let d) = doc.getValue(forPath: String(s.dropFirst())) {
                return d
            }
            return nil
        case .document(let d):
            var out = BSONDocument()
            for (k, v) in d {
                out[k] = evaluateExpr(v, doc: doc)
            }
            return out
        default:
            return nil
        }
    }

    private func evaluateExpr(_ expr: BSONValue, doc: BSONDocument) -> BSONValue {
        if case .string(let s) = expr, s.hasPrefix("$") {
            return doc.getValue(forPath: String(s.dropFirst())) ?? .null
        }
        return expr
    }
}

// MARK: - $lookup

public struct LookupStage: PipelineStage {
    public let name: String = "$lookup"
    private let from: String
    private let localField: String
    private let foreignField: String
    private let asField: String
    private let db: Database?

    public init(spec: BSONDocument, db: Database?) {
        self.from = spec["from"]?.stringValue ?? ""
        self.localField = spec["localField"]?.stringValue ?? ""
        self.foreignField = spec["foreignField"]?.stringValue ?? ""
        self.asField = spec["as"]?.stringValue ?? ""
        self.db = db
    }

    public func execute(_ docs: [BSONDocument]) async throws -> [BSONDocument] {
        guard let db else {
            throw MonoError.badValue("$lookup requires database context")
        }
        if from.isEmpty || asField.isEmpty {
            throw MonoError.badValue("$lookup requires 'from' and 'as' fields")
        }

        // Fetch foreign collection documents using async/await
        var foreignDocs: [BSONDocument] = []
        if let foreign = await db.getCollection(from) {
            foreignDocs = try await foreign.find(BSONDocument())
        }

        var out: [BSONDocument] = []
        out.reserveCapacity(docs.count)

        for doc in docs {
            let localVal = doc.getValue(forPath: localField) ?? .null
            var matches: [BSONValue] = []
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

// MARK: - Helpers

private func unsetField(_ doc: inout BSONDocument, path: String) {
    // Simple nested unset by path components.
    let parts = path.split(separator: ".").map(String.init)
    guard let first = parts.first else { return }
    if parts.count == 1 {
        doc[first] = nil
        return
    }
    guard case .document(var sub)? = doc[first] else { return }
    let rest = parts.dropFirst().joined(separator: ".")
    unsetField(&sub, path: rest)
    doc[first] = .document(sub)
}


