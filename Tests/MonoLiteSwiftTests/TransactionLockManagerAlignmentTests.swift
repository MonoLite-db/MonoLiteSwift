// Created by Yanjunhui

import XCTest
@testable import MonoLiteSwift

final class TransactionLockManagerAlignmentTests: XCTestCase {
    func testLockManagerWriteThenReadWaitsAndSucceedsAfterRelease() async throws {
        let lm = LockManager()

        let txn1 = TxnID(1)
        let txn2 = TxnID(2)

        // txn1 acquires write lock.
        _ = try await lm.acquire("resource1", type: .write, txnId: txn1, timeout: 1.0)

        // txn2 attempts read lock (should wait).
        let t = Task {
            try await lm.acquire("resource1", type: .read, txnId: txn2, timeout: 0.2)
        }

        // Release after a short delay.
        try await Task.sleep(nanoseconds: 50_000_000)
        await lm.release(resource: "resource1", txnId: txn1)

        _ = try await t.value
        await lm.release(resource: "resource1", txnId: txn2)
    }

    func testSharedReadLocksCanCoexist() async throws {
        let lm = LockManager()

        let txn1 = TxnID(1)
        let txn2 = TxnID(2)
        let txn3 = TxnID(3)

        _ = try await lm.acquire("shared_resource", type: .read, txnId: txn1, timeout: 0.2)
        _ = try await lm.acquire("shared_resource", type: .read, txnId: txn2, timeout: 0.2)
        _ = try await lm.acquire("shared_resource", type: .read, txnId: txn3, timeout: 0.2)

        await lm.release(resource: "shared_resource", txnId: txn1)
        await lm.release(resource: "shared_resource", txnId: txn2)
        await lm.release(resource: "shared_resource", txnId: txn3)
    }

    func testDeadlockDetectionAbortsOneWaiter() async throws {
        let lm = LockManager()

        let txn1 = TxnID(1)
        let txn2 = TxnID(2)

        // txn1 holds r1, txn2 holds r2
        _ = try await lm.acquire("resource1", type: .write, txnId: txn1, timeout: 1.0)
        _ = try await lm.acquire("resource2", type: .write, txnId: txn2, timeout: 1.0)

        // txn1 waits for r2
        let w1 = Task {
            try await lm.acquire("resource2", type: .write, txnId: txn1, timeout: 0.5)
        }

        // small delay to ensure w1 registers in waitGraph
        try await Task.sleep(nanoseconds: 50_000_000)

        // txn2 waits for r1 -> should detect deadlock and abort one side
        let w2 = Task {
            try await lm.acquire("resource1", type: .write, txnId: txn2, timeout: 0.5)
        }

        var abortedCount = 0
        do { _ = try await w1.value } catch { abortedCount += 1 }
        do { _ = try await w2.value } catch { abortedCount += 1 }

        XCTAssertGreaterThanOrEqual(abortedCount, 1)

        // cleanup
        await lm.release(resource: "resource1", txnId: txn1)
        await lm.release(resource: "resource2", txnId: txn2)
    }
}


