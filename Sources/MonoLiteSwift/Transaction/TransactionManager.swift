// Created by Yanjunhui

import Foundation

// MARK: - 事务管理器 / Transaction Manager

/// 事务管理器（对齐 Go：Begin/Commit/Abort + undo 回滚）
/// EN: Transaction manager (Go alignment: Begin/Commit/Abort + undo rollback)
public actor TransactionManager {
    /// 下一个事务 ID / EN: Next transaction ID
    private var nextId: UInt64 = 1
    /// 活跃事务映射表 / EN: Active transactions map
    private var active: [TxnID: Transaction] = [:]

    /// 锁管理器 / EN: Lock manager
    private let lockManager: LockManager
    /// 数据库引用（弱引用避免循环引用）
    /// EN: Database reference (unowned to avoid retain cycle)
    private unowned let db: Database

    /// 初始化事务管理器
    /// EN: Initialize transaction manager
    /// - Parameter db: 数据库实例 / EN: Database instance
    public init(db: Database) {
        self.db = db
        self.lockManager = LockManager()
    }

    /// 开始新事务
    /// EN: Begin a new transaction
    /// - Parameters:
    ///   - isolation: 隔离级别，默认为读已提交 / EN: Isolation level, defaults to read committed
    ///   - timeout: 超时时间，默认 30 秒 / EN: Timeout duration, defaults to 30 seconds
    /// - Returns: 新创建的事务对象 / EN: Newly created transaction object
    public func begin(isolation: IsolationLevel = .readCommitted, timeout: TimeInterval = 30) -> Transaction {
        nextId += 1
        let txn = Transaction(id: TxnID(nextId), isolationLevel: isolation, timeout: timeout, lockManager: lockManager)
        active[TxnID(nextId)] = txn
        return txn
    }

    /// 提交事务
    /// EN: Commit transaction
    /// - Parameter txn: 要提交的事务 / EN: Transaction to commit
    /// - Throws: 如果事务不处于活跃状态则抛出错误 / EN: Throws error if transaction is not active
    public func commit(_ txn: Transaction) async throws {
        let state = await txn.state
        guard state == .active else {
            // Go 对齐：提交非活跃事务报告 TransactionCommitted 错误
            // EN: Go parity: committing a non-active txn reports TransactionCommitted
            throw MonoError(code: .transactionCommitted, message: "transaction is not active")
        }

        await txn.setState(.committed)
        await txn.releaseAllLocks()
        active.removeValue(forKey: txn.id)

        // Go 对齐：提交后刷新到磁盘
        // EN: Go parity: flush after commit
        try await db.flush()
    }

    /// 中止事务
    /// EN: Abort transaction
    /// - Parameter txn: 要中止的事务 / EN: Transaction to abort
    /// - Throws: 如果事务不处于活跃状态则抛出错误 / EN: Throws error if transaction is not active
    public func abort(_ txn: Transaction) async throws {
        let state = await txn.state
        guard state == .active else {
            // Go 对齐：中止非活跃事务报告 TransactionAborted 错误
            // EN: Go parity: aborting a non-active txn reports TransactionAborted
            throw MonoError(code: .transactionAborted, message: "transaction is not active")
        }

        // 回滚操作 / EN: Rollback operations
        let undo = await txn.drainUndoLogReversed()
        for rec in undo {
            guard let col = await db.getCollection(rec.collection) else { continue }

            switch rec.operation {
            case "insert":
                // 回滚插入：删除文档 / EN: Rollback insert: delete document
                _ = try? await col.deleteOne(BSONDocument([("_id", rec.docId)]))
            case "update":
                // 回滚更新：恢复旧文档 / EN: Rollback update: restore old document
                if let old = rec.oldDoc {
                    _ = try? await col.replaceOne(BSONDocument([("_id", rec.docId)]), replacement: old)
                }
            case "delete":
                // 回滚删除：重新插入旧文档 / EN: Rollback delete: re-insert old document
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
