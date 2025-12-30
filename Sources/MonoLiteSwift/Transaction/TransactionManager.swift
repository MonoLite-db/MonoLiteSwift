// Created by Yanjunhui

import Foundation

/// 事务管理器（对齐 Go：Begin/Commit/Abort + undo 回滚）
public actor TransactionManager {
    private var nextId: UInt64 = 1
    private var active: [TxnID: Transaction] = [:]

    private let lockManager: LockManager
    private unowned let db: Database

    public init(db: Database) {
        self.db = db
        self.lockManager = LockManager()
    }

    public func begin(isolation: IsolationLevel = .readCommitted, timeout: TimeInterval = 30) -> Transaction {
        nextId += 1
        let txn = Transaction(id: TxnID(nextId), isolationLevel: isolation, timeout: timeout, lockManager: lockManager)
        active[TxnID(nextId)] = txn
        return txn
    }

    public func commit(_ txn: Transaction) async throws {
        let state = await txn.state
        guard state == .active else {
            // Go parity: committing a non-active txn reports TransactionCommitted.
            throw MonoError(code: .transactionCommitted, message: "transaction is not active")
        }

        await txn.setState(.committed)
        await txn.releaseAllLocks()
        active.removeValue(forKey: txn.id)

        // Go：commit 后 flush
        try await db.flush()
    }

    public func abort(_ txn: Transaction) async throws {
        let state = await txn.state
        guard state == .active else {
            // Go parity: aborting a non-active txn reports TransactionAborted.
            throw MonoError(code: .transactionAborted, message: "transaction is not active")
        }

        // rollback
        let undo = await txn.drainUndoLogReversed()
        for rec in undo {
            guard let col = await db.getCollection(rec.collection) else { continue }

            switch rec.operation {
            case "insert":
                _ = try? await col.deleteOne(BSONDocument([("_id", rec.docId)]))
            case "update":
                if let old = rec.oldDoc {
                    _ = try? await col.replaceOne(BSONDocument([("_id", rec.docId)]), replacement: old)
                }
            case "delete":
                if let old = rec.oldDoc {
                    _ = try? await col.insertOne(old)
                }
            default:
                break
            }
        }

        await txn.setState(.aborted)
        await txn.releaseAllLocks()
        active.removeValue(forKey: txn.id)
    }
}


