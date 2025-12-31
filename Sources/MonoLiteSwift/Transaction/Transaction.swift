// Created by Yanjunhui

import Foundation

// MARK: - 事务状态枚举 / Transaction State Enum

/// 事务状态枚举
/// EN: Transaction state enumeration
public enum TxnState: Int, Sendable {
    /// 活跃状态 / EN: Active state
    case active = 0
    /// 已提交状态 / EN: Committed state
    case committed = 1
    /// 已中止状态 / EN: Aborted state
    case aborted = 2
}

// MARK: - 隔离级别枚举 / Isolation Level Enum

/// 事务隔离级别枚举
/// EN: Transaction isolation level enumeration
public enum IsolationLevel: Int, Sendable {
    /// 读已提交 / EN: Read committed
    case readCommitted = 0
    /// 可重复读 / EN: Repeatable read
    case repeatableRead = 1
    /// 快照隔离 / EN: Snapshot isolation
    case snapshot = 2
}

// MARK: - 撤销记录结构体 / Undo Record Struct

/// 撤销记录结构体，用于事务回滚
/// EN: Undo record struct for transaction rollback
public struct UndoRecord: Sendable {
    /// 操作类型：insert/update/delete
    /// EN: Operation type: insert/update/delete
    public let operation: String
    /// 集合名称
    /// EN: Collection name
    public let collection: String
    /// 文档 ID
    /// EN: Document ID
    public let docId: BSONValue
    /// 旧文档（用于回滚）
    /// EN: Old document (for rollback)
    public let oldDoc: BSONDocument?
}

// MARK: - 事务对象 / Transaction Object

/// 事务对象（简化对齐 Go：锁 + undo log）
/// EN: Transaction object (simplified Go alignment: locks + undo log)
///
/// 注意：使用 actor 避免在 async 上下文中使用 NSLock（Swift 6 会收紧）。
/// EN: Note: Using actor to avoid NSLock in async context (Swift 6 will tighten restrictions).
public actor Transaction {
    /// 事务 ID / EN: Transaction ID
    public let id: TxnID
    /// 事务状态 / EN: Transaction state
    public private(set) var state: TxnState
    /// 隔离级别 / EN: Isolation level
    public let isolationLevel: IsolationLevel
    /// 开始时间 / EN: Start time
    public let startTime: Date
    /// 超时时间 / EN: Timeout duration
    public let timeout: TimeInterval

    /// 锁管理器 / EN: Lock manager
    private let lockManager: LockManager
    /// 已持有的锁映射表 / EN: Map of held locks
    private var heldLocks: [String: Lock] = [:]
    /// 撤销日志 / EN: Undo log
    private var undoLog: [UndoRecord] = []

    /// 初始化事务
    /// EN: Initialize transaction
    /// - Parameters:
    ///   - id: 事务 ID / EN: Transaction ID
    ///   - isolationLevel: 隔离级别 / EN: Isolation level
    ///   - timeout: 超时时间 / EN: Timeout duration
    ///   - lockManager: 锁管理器 / EN: Lock manager
    init(id: TxnID, isolationLevel: IsolationLevel, timeout: TimeInterval, lockManager: LockManager) {
        self.id = id
        self.state = .active
        self.isolationLevel = isolationLevel
        self.startTime = Date()
        self.timeout = timeout
        self.lockManager = lockManager
    }

    /// 设置事务状态
    /// EN: Set transaction state
    /// - Parameter s: 新状态 / EN: New state
    public func setState(_ s: TxnState) {
        state = s
    }

    /// 获取资源锁
    /// EN: Acquire resource lock
    /// - Parameters:
    ///   - resource: 资源标识符 / EN: Resource identifier
    ///   - type: 锁类型 / EN: Lock type
    public func acquireLock(resource: String, type: LockType) async throws {
        let lock = try await lockManager.acquire(resource, type: type, txnId: id, timeout: timeout)
        heldLocks[resource] = lock
    }

    /// 释放所有已持有的锁
    /// EN: Release all held locks
    public func releaseAllLocks() async {
        let locks = Array(heldLocks.values)
        heldLocks.removeAll()
        for l in locks {
            await lockManager.release(l)
        }
    }

    /// 追加撤销记录
    /// EN: Append undo record
    /// - Parameter record: 撤销记录 / EN: Undo record
    public func appendUndo(_ record: UndoRecord) {
        undoLog.append(record)
    }

    /// 逆序获取并清空撤销日志
    /// EN: Get and clear undo log in reverse order
    /// - Returns: 逆序的撤销记录数组 / EN: Array of undo records in reverse order
    public func drainUndoLogReversed() -> [UndoRecord] {
        let records = undoLog.reversed()
        undoLog.removeAll()
        return Array(records)
    }
}
