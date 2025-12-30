// Created by Yanjunhui
//
// A minimal runner for MongoDB specifications (Unified CRUD Test Format),
// aligned with Go's `tests/mongo_spec/crud_unified_test.go` strategy:
// - Disabled by default (set MONOLITE_RUN_MONGO_SPECS=1 to enable)
// - Runs a small subset of CRUD spec files (find/insert/delete/distinct)
// - Skips cases requiring command monitoring (expectEvents) or expectError assertions.

import Foundation
import XCTest
@testable import MonoLiteSwift

final class MongoSpecUnifiedCRUDTests: XCTestCase {
    private static let envRunSpecs = "MONOLITE_RUN_MONGO_SPECS"
    private static let envSpecsGlob = "MONOLITE_MONGO_SPECS_GLOB"
    private static let envSpecsFile = "MONOLITE_MONGO_SPECS_FILENAME"
    private static let envSpecsDir = "MONOLITE_MONGO_SPECS_DIR"

    func testMongoDBSpecificationsUnifiedCRUDSubset() async throws {
        if ProcessInfo.processInfo.environment[Self.envRunSpecs] != "1" {
            throw XCTSkip("MongoDB specifications tests disabled (set \(Self.envRunSpecs)=1 to enable)")
        }

        let unifiedDir = try resolveUnifiedDir()
        let files = try selectSpecFiles(unifiedDir: unifiedDir)
        if files.isEmpty {
            throw XCTSkip("no spec files selected (dir=\(unifiedDir.path))")
        }

        for file in files {
            let spec = try loadJSONDocument(fileURL: file)
            let createEntities = try requireArray(spec["createEntities"], "createEntities")
            let initialData = (spec["initialData"] as? [Any]) ?? []
            let tests = try requireArray(spec["tests"], "tests")

            let entities = try buildEntities(createEntities)

            for testAny in tests {
                guard let test = testAny as? [String: Any] else { continue }
                let desc = (test["description"] as? String) ?? "(no description)"

                // Skip command monitoring assertions for now (Go parity).
                if let expectEvents = test["expectEvents"] as? [Any], !expectEvents.isEmpty {
                    continue
                }

                // Skip test cases that require expectError assertions (Go parity).
                if let opsAny = test["operations"] as? [Any] {
                    var hasExpectError = false
                    for opAny in opsAny {
                        if let op = opAny as? [String: Any], op["expectError"] != nil {
                            hasExpectError = true
                            break
                        }
                    }
                    if hasExpectError {
                        continue
                    }
                }

                // Each test case runs in its own DB instance for isolation (Go parity).
                let (db, baseURL) = try await makeTempDB()
                do {
                    try await applyInitialData(initialData, db: db)

                    let operations = try requireArray(test["operations"], "operations")
                    for opAny in operations {
                        guard let op = opAny as? [String: Any] else { continue }

                        if op["expectError"] != nil {
                            // Skip expectError assertions for now (Go parity).
                            continue
                        }

                        let opName = try requireString(op["name"], "operation.name")
                        let objId = try requireString(op["object"], "operation.object")
                        let args = (op["arguments"] as? [String: Any]) ?? [:]

                        guard let target = entities.collections[objId] else {
                            XCTFail("unknown collection entity id: \(objId) (\(desc))")
                            continue
                        }

                        let actual = try await executeOperation(
                            name: opName,
                            dbName: target.dbName,
                            collName: target.collName,
                            args: args,
                            db: db
                        )

                        if let expect = op["expectResult"] {
                            try assertExpectResult(opName: opName, actual: actual, expect: expect, context: "\(file.lastPathComponent):\(desc)")
                        }
                    }

                    if let outcome = test["outcome"] as? [Any], !outcome.isEmpty {
                        try await assertOutcome(outcome, db: db, context: "\(file.lastPathComponent):\(desc)")
                    }
                } catch {
                    try? await db.close()
                    try? FileManager.default.removeItem(at: baseURL)
                    throw error
                }

                try? await db.close()
                try? FileManager.default.removeItem(at: baseURL)
            }
        }
    }

    // MARK: - Spec discovery

    private func resolveUnifiedDir() throws -> URL {
        if let dir = ProcessInfo.processInfo.environment[Self.envSpecsDir], !dir.isEmpty {
            let url = URL(fileURLWithPath: dir, isDirectory: true)
            if FileManager.default.fileExists(atPath: url.path) { return url }
            throw XCTSkip("spec dir does not exist: \(url.path)")
        }

        // Default: sibling Go repo path (read-only) to reuse the same upstream fixtures.
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let candidate = cwd
            .deletingLastPathComponent()
            .appendingPathComponent("MonoLite/third_party/mongodb/specifications/source/crud/tests/unified", isDirectory: true)
        if FileManager.default.fileExists(atPath: candidate.path) {
            return candidate
        }
        throw XCTSkip("cannot locate unified CRUD spec dir; set \(Self.envSpecsDir) to override")
    }

    private func selectSpecFiles(unifiedDir: URL) throws -> [URL] {
        let fm = FileManager.default

        if let name = ProcessInfo.processInfo.environment[Self.envSpecsFile], !name.isEmpty {
            let f = unifiedDir.appendingPathComponent(name)
            if fm.fileExists(atPath: f.path) { return [f] }
            throw XCTSkip("spec file not found: \(f.path)")
        }

        let all = (try? fm.contentsOfDirectory(at: unifiedDir, includingPropertiesForKeys: nil))
            ?? []
        let jsonFiles = all.filter { $0.pathExtension.lowercased() == "json" }

        if let glob = ProcessInfo.processInfo.environment[Self.envSpecsGlob], !glob.isEmpty {
            // very small glob support: "*" only, applied to filename
            let regex = "^" + NSRegularExpression.escapedPattern(for: glob).replacingOccurrences(of: "\\*", with: ".*") + "$"
            return jsonFiles.filter { $0.lastPathComponent.range(of: regex, options: .regularExpression) != nil }.sorted { $0.lastPathComponent < $1.lastPathComponent }
        }

        // Default subset (Go parity)
        let wanted = Set(["find.json", "insertOne.json", "insertMany.json", "deleteOne.json", "deleteMany.json", "distinct.json"])
        return jsonFiles.filter { wanted.contains($0.lastPathComponent) }.sorted { $0.lastPathComponent < $1.lastPathComponent }
    }

    // MARK: - Model + parsing

    private struct EntityBindings {
        var dbById: [String: String]
        var collections: [String: (dbName: String, collName: String)]
    }

    private func buildEntities(_ createEntities: [Any]) throws -> EntityBindings {
        var dbById: [String: String] = [:]
        var collections: [String: (dbName: String, collName: String)] = [:]

        // databases
        for eAny in createEntities {
            guard let e = eAny as? [String: Any] else { continue }
            if let db = e["database"] as? [String: Any] {
                let id = try requireString(db["id"], "database.id")
                let name = try requireString(db["databaseName"], "database.databaseName")
                dbById[id] = name
            }
        }
        // collections
        for eAny in createEntities {
            guard let e = eAny as? [String: Any] else { continue }
            if let c = e["collection"] as? [String: Any] {
                let id = try requireString(c["id"], "collection.id")
                let dbId = try requireString(c["database"], "collection.database")
                let collName = try requireString(c["collectionName"], "collection.collectionName")
                let dbName = dbById[dbId] ?? ""
                collections[id] = (dbName: dbName, collName: collName)
            }
        }

        return EntityBindings(dbById: dbById, collections: collections)
    }

    private func loadJSONDocument(fileURL: URL) throws -> [String: Any] {
        let data = try Data(contentsOf: fileURL)
        let obj = try JSONSerialization.jsonObject(with: data, options: [])
        guard let dict = obj as? [String: Any] else {
            throw XCTSkip("invalid JSON root (expected object): \(fileURL.lastPathComponent)")
        }
        return dict
    }

    // MARK: - Execution

    private enum OperationResult {
        case document(BSONDocument)
        case documents([BSONDocument])
        case values([BSONValue])
        case none
    }

    private func executeOperation(name: String, dbName: String, collName: String, args: [String: Any], db: Database) async throws -> OperationResult {
        let col = try await db.collection(collName)

        switch name {
        case "insertOne":
            let docAny = args["document"]
            let doc = try jsonToBSONDocument(docAny)
            let id = try await col.insertOne(doc)
            return .document(BSONDocument([("insertedId", id)]))

        case "insertMany":
            let docsAny = args["documents"]
            let docs = try jsonToBSONDocumentsArray(docsAny)
            let ids = try await col.insert(docs)
            var insertedIds = BSONDocument()
            for (i, id) in ids.enumerated() {
                insertedIds[String(i)] = id
            }
            return .document(BSONDocument([("insertedIds", .document(insertedIds))]))

        case "find":
            let filter = try jsonToBSONDocument(args["filter"]) // default {}

            var opts = QueryOptions()
            if let sortAny = args["sort"] { opts.sort = try jsonToBSONDocument(sortAny) }
            if let projAny = args["projection"] { opts.projection = try jsonToBSONDocument(projAny) }
            if let limitAny = args["limit"] { opts.limit = Int64((try jsonToBSONValue(limitAny)).intValue ?? 0) }
            if let skipAny = args["skip"] { opts.skip = Int64((try jsonToBSONValue(skipAny)).intValue ?? 0) }

            let docs: [BSONDocument]
            if !opts.sort.isEmpty || opts.limit > 0 || opts.skip > 0 || !opts.projection.isEmpty {
                docs = try await col.findWithOptions(filter, options: opts)
            } else {
                docs = try await col.find(filter)
            }
            return .documents(docs)

        case "deleteOne":
            let filter = try jsonToBSONDocument(args["filter"])
            let deleted = try await col.deleteOne(filter)
            return .document(BSONDocument([("deletedCount", .int64(deleted))]))

        case "deleteMany":
            let filter = try jsonToBSONDocument(args["filter"])
            let deleted = try await col.delete(filter)
            return .document(BSONDocument([("deletedCount", .int64(deleted))]))

        case "distinct":
            let field = (args["fieldName"] as? String) ?? ""
            let filter = try jsonToBSONDocument(args["filter"])
            let vals = try await col.distinct(field, filter: filter)
            return .values(vals)

        default:
            // Unsupported operations are explicitly skipped (no false green).
            throw XCTSkip("operation not supported yet: \(name)")
        }
    }

    // MARK: - Assertions

    private func assertExpectResult(opName: String, actual: OperationResult, expect: Any, context: String) throws {
        switch (opName, actual, expect) {
        case ("find", .documents(let docs), let expAny as [Any]):
            let expectedDocs = try expAny.map { try jsonToBSONDocument($0) }
            try assertDocumentsEqual(docs, expectedDocs, context: context)

        case ("distinct", .values(let vals), let expAny as [Any]):
            let expectedVals = try expAny.map { try jsonToBSONValue($0) }
            try assertValuesEqualUnordered(vals, expectedVals, context: context)

        case (_, .document(let doc), let expAny as [String: Any]):
            // Supports the common "$$unsetOrMatches" wrapper used by insertOne/insertMany in the spec subset.
            try assertDocumentMatchesExpectation(actual: doc, expectation: expAny, context: context)

        default:
            // If spec expects something we don't model yet, skip rather than fake-pass.
            throw XCTSkip("unsupported expectResult shape for \(opName) (\(context))")
        }
    }

    private func assertDocumentMatchesExpectation(actual: BSONDocument, expectation: [String: Any], context: String) throws {
        if let wrapper = expectation["$$unsetOrMatches"] as? [String: Any] {
            for (k, v) in wrapper {
                if let nested = v as? [String: Any], nested.keys.contains("$$unsetOrMatches") {
                    // field can be unset OR matches nested expectation
                    if actual[k] == nil {
                        continue
                    }
                    guard case .document(let actualSub)? = actual[k] else {
                        // allow scalar too
                        let actualVal = actual[k]!
                        let expectedVal = try jsonToBSONValue((nested["$$unsetOrMatches"] as Any))
                        if BSONCompare.compare(actualVal, expectedVal) != 0 {
                            XCTFail("expect mismatch (field=\(k)) \(context)")
                        }
                        continue
                    }
                    if let subExp = nested["$$unsetOrMatches"] as? [String: Any] {
                        try assertDocumentMatchesExpectation(actual: actualSub, expectation: ["$$unsetOrMatches": subExp], context: context)
                    }
                } else {
                    guard let actualVal = actual[k] else {
                        XCTFail("missing expected field \(k) \(context)")
                        continue
                    }
                    let expectedVal = try jsonToBSONValue(v)
                    XCTAssertEqual(BSONCompare.compare(actualVal, expectedVal), 0, "field \(k) mismatch \(context)")
                }
            }
            return
        }

        // Plain exact-match document (field subset)
        for (k, v) in expectation {
            guard let actualVal = actual[k] else {
                XCTFail("missing expected field \(k) \(context)")
                continue
            }
            let expectedVal = try jsonToBSONValue(v)
            XCTAssertEqual(BSONCompare.compare(actualVal, expectedVal), 0, "field \(k) mismatch \(context)")
        }
    }

    private func assertOutcome(_ outcome: [Any], db: Database, context: String) async throws {
        for oAny in outcome {
            guard let o = oAny as? [String: Any] else { continue }
            let collName = try requireString(o["collectionName"], "outcome.collectionName")
            let expectedDocsAny = try requireArray(o["documents"], "outcome.documents")
            let expectedDocs = try expectedDocsAny.map { try jsonToBSONDocument($0) }

            guard let col = await db.getCollection(collName) else {
                XCTFail("outcome collection not found: \(collName) \(context)")
                continue
            }
            let actualDocs = try await col.find(BSONDocument())
            try assertDocumentsEqual(actualDocs, expectedDocs, context: context)
        }
    }

    private func assertDocumentsEqual(_ actual: [BSONDocument], _ expected: [BSONDocument], context: String) throws {
        func sortKey(_ doc: BSONDocument) -> BSONValue {
            doc.getValue(forPath: "_id") ?? .null
        }
        let a = actual.sorted { BSONCompare.compare(sortKey($0), sortKey($1)) < 0 }
        let e = expected.sorted { BSONCompare.compare(sortKey($0), sortKey($1)) < 0 }
        XCTAssertEqual(a.count, e.count, "doc count mismatch \(context)")
        for (ad, ed) in zip(a, e) {
            XCTAssertEqual(BSONCompare.compare(.document(ad), .document(ed)), 0, "doc mismatch \(context)\nactual=\(ad)\nexpect=\(ed)")
        }
    }

    private func assertValuesEqualUnordered(_ actual: [BSONValue], _ expected: [BSONValue], context: String) throws {
        func sortVal(_ v: BSONValue) -> BSONValue { v }
        let a = actual.sorted { BSONCompare.compare(sortVal($0), sortVal($1)) < 0 }
        let e = expected.sorted { BSONCompare.compare(sortVal($0), sortVal($1)) < 0 }
        XCTAssertEqual(a.count, e.count, "value count mismatch \(context)")
        for (av, ev) in zip(a, e) {
            XCTAssertEqual(BSONCompare.compare(av, ev), 0, "value mismatch \(context)")
        }
    }

    // MARK: - Initial data

    private func applyInitialData(_ initialData: [Any], db: Database) async throws {
        for itemAny in initialData {
            guard let item = itemAny as? [String: Any] else { continue }
            let collName = try requireString(item["collectionName"], "initialData.collectionName")
            let docsAny = (item["documents"] as? [Any]) ?? []
            let docs = try docsAny.map { try jsonToBSONDocument($0) }
            let col = try await db.collection(collName)
            _ = try await col.insert(docs)
        }
    }

    // MARK: - JSON -> BSON (minimal)

    private func jsonToBSONDocumentsArray(_ any: Any?) throws -> [BSONDocument] {
        guard let arr = any as? [Any] else { return [] }
        return try arr.map { try jsonToBSONDocument($0) }
    }

    private func jsonToBSONDocument(_ any: Any?) throws -> BSONDocument {
        guard let dict = any as? [String: Any] else { return BSONDocument() }
        // Deterministic order: sort keys.
        var doc = BSONDocument()
        for k in dict.keys.sorted() {
            doc[k] = try jsonToBSONValue(dict[k] as Any)
        }
        return doc
    }

    private func jsonToBSONValue(_ any: Any) throws -> BSONValue {
        if any is NSNull { return .null }

        // JSONSerialization returns NSNumber for both booleans and numbers.
        if let n = any as? NSNumber {
            if CFGetTypeID(n) == CFBooleanGetTypeID() {
                return .bool(n.boolValue)
            }
            let d = n.doubleValue
            let i = Int64(d)
            if Double(i) == d {
                if i >= Int64(Int32.min), i <= Int64(Int32.max) { return .int32(Int32(i)) }
                return .int64(i)
            }
            return .double(d)
        }

        if let v = any as? Bool { return .bool(v) }
        if let v = any as? String { return .string(v) }
        if let arr = any as? [Any] {
            return .array(BSONArray(try arr.map { try jsonToBSONValue($0) }))
        }
        if let dict = any as? [String: Any] {
            return .document(try jsonToBSONDocument(dict))
        }
        return .null
    }

    // MARK: - Required JSON helpers

    private func requireString(_ any: Any?, _ field: String) throws -> String {
        guard let s = any as? String else { throw XCTSkip("missing/invalid string field: \(field)") }
        return s
    }

    private func requireArray(_ any: Any?, _ field: String) throws -> [Any] {
        guard let a = any as? [Any] else { throw XCTSkip("missing/invalid array field: \(field)") }
        return a
    }

    // MARK: - Temp DB

    private func makeTempDB() async throws -> (Database, URL) {
        let base = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
            .appendingPathComponent(".build/monolite_test_tmp/mongo_spec/\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: base, withIntermediateDirectories: true)
        let path = base.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)
        return (db, base)
    }
}


