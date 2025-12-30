// Created by Yanjunhui

import Foundation
import XCTest
@testable import MonoLiteSwift

/// BSON round-trip tests: verify consistency of data import/export (Go parity: engine/roundtrip_test.go)
final class BSONRoundtripTests: XCTestCase {
    private func makeWorkDir(_ name: String) throws -> URL {
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        let dir = cwd.appendingPathComponent(".build/monolite_test_tmp/\(name)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    func testBSONTypesPreservation() async throws {
        let dir = try makeWorkDir("bson-roundtrip-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)

        let col = try await db.collection("types")

        // Test String (including Unicode)
        _ = try await col.insertOne(BSONDocument([("type", .string("string")), ("value", .string("hello 你好"))]))

        // Test Int32
        _ = try await col.insertOne(BSONDocument([("type", .string("int32")), ("value", .int32(42))]))

        // Test Int64
        _ = try await col.insertOne(BSONDocument([("type", .string("int64")), ("value", .int64(9223372036854775807))]))

        // Test Double
        _ = try await col.insertOne(BSONDocument([("type", .string("double")), ("value", .double(3.14159265358979))]))

        // Test Boolean
        _ = try await col.insertOne(BSONDocument([("type", .string("bool_true")), ("value", .bool(true))]))
        _ = try await col.insertOne(BSONDocument([("type", .string("bool_false")), ("value", .bool(false))]))

        // Test Null
        _ = try await col.insertOne(BSONDocument([("type", .string("null")), ("value", .null)]))

        // Test ObjectID
        let oid = ObjectID.generate()
        _ = try await col.insertOne(BSONDocument([("type", .string("objectid")), ("value", .objectId(oid))]))

        // Test DateTime
        let now = Date()
        _ = try await col.insertOne(BSONDocument([("type", .string("datetime")), ("value", .dateTime(now))]))

        // Test Binary
        let binData = Data([0x01, 0x02, 0x03, 0x04])
        _ = try await col.insertOne(BSONDocument([("type", .string("binary")), ("value", .binary(BSONBinary(subtype: .generic, data: binData)))]))

        // Test Array
        _ = try await col.insertOne(BSONDocument([("type", .string("array")), ("value", .array(BSONArray([.int32(1), .int32(2), .int32(3)])))]))

        // Test Nested Document
        _ = try await col.insertOne(BSONDocument([("type", .string("nested")), ("value", .document(BSONDocument([("a", .int32(1)), ("b", .string("two"))])))]))

        // Verify each type
        let stringDoc = try await col.findOne(BSONDocument([("type", .string("string"))]))
        XCTAssertEqual(stringDoc?["value"]?.stringValue, "hello 你好")

        let int32Doc = try await col.findOne(BSONDocument([("type", .string("int32"))]))
        XCTAssertEqual(int32Doc?["value"]?.intValue, 42)

        let int64Doc = try await col.findOne(BSONDocument([("type", .string("int64"))]))
        XCTAssertEqual(int64Doc?["value"]?.int64Value, 9223372036854775807)

        let doubleDoc = try await col.findOne(BSONDocument([("type", .string("double"))]))
        XCTAssertEqual(doubleDoc?["value"]?.doubleValue ?? 0, 3.14159265358979, accuracy: 0.0000001)

        let boolTrueDoc = try await col.findOne(BSONDocument([("type", .string("bool_true"))]))
        XCTAssertEqual(boolTrueDoc?["value"]?.boolValue, true)

        let boolFalseDoc = try await col.findOne(BSONDocument([("type", .string("bool_false"))]))
        XCTAssertEqual(boolFalseDoc?["value"]?.boolValue, false)

        let nullDoc = try await col.findOne(BSONDocument([("type", .string("null"))]))
        XCTAssertNotNil(nullDoc)
        if case .null = nullDoc?["value"] {
            // OK
        } else {
            XCTFail("expected null value")
        }

        let oidDoc = try await col.findOne(BSONDocument([("type", .string("objectid"))]))
        if case .objectId(let retrievedOid) = oidDoc?["value"] {
            XCTAssertEqual(retrievedOid.hexString, oid.hexString)
        } else {
            XCTFail("expected objectid value")
        }

        let dateDoc = try await col.findOne(BSONDocument([("type", .string("datetime"))]))
        if case .dateTime(let retrievedDate) = dateDoc?["value"] {
            // Allow small time difference due to serialization
            XCTAssertEqual(retrievedDate.timeIntervalSince1970, now.timeIntervalSince1970, accuracy: 0.001)
        } else {
            XCTFail("expected datetime value")
        }

        let binDoc = try await col.findOne(BSONDocument([("type", .string("binary"))]))
        if case .binary(let bin) = binDoc?["value"] {
            XCTAssertEqual(bin.data, binData)
        } else {
            XCTFail("expected binary value")
        }

        let arrDoc = try await col.findOne(BSONDocument([("type", .string("array"))]))
        if case .array(let arr) = arrDoc?["value"] {
            XCTAssertEqual(arr.count, 3)
        } else {
            XCTFail("expected array value")
        }

        let nestedDoc = try await col.findOne(BSONDocument([("type", .string("nested"))]))
        if case .document(let nested) = nestedDoc?["value"] {
            XCTAssertEqual(nested["a"]?.intValue, 1)
            XCTAssertEqual(nested["b"]?.stringValue, "two")
        } else {
            XCTFail("expected nested document value")
        }

        try await db.close()
    }

    func testDocumentUpdateRoundtrip() async throws {
        let dir = try makeWorkDir("update-roundtrip-\(UUID().uuidString)")
        let path = dir.appendingPathComponent("data.monodb").path
        let db = try await Database(path: path, enableWAL: true)

        let col = try await db.collection("update_test")

        // Insert
        _ = try await col.insertOne(BSONDocument([("x", .int32(1)), ("y", .string("original"))]))

        // Update
        _ = try await col.updateOne(
            BSONDocument([("x", .int32(1))]),
            update: BSONDocument([("$set", .document(BSONDocument([("y", .string("modified"))])))]),
            upsert: false
        )

        // Verify
        let doc = try await col.findOne(BSONDocument([("x", .int32(1))]))
        XCTAssertEqual(doc?["y"]?.stringValue, "modified")

        try await db.close()
    }
}

