// Created by Yanjunhui

import Foundation

public enum TxnState: Int, Sendable {
    case active = 0
    case committed = 1
    case aborted = 2
}

public enum IsolationLevel: Int, Sendable {
    case readCommitted = 0
    case repeatableRead = 1
    case snapshot = 2
}

public struct UndoRecord: Sendable {
    public let operation: String // insert/update/delete
    public let collection: String
    public let docId: BSONValue
    public let oldDoc: BSONDocument?
}

/// 事务对象（简化对齐 Go：锁 + undo log）
///
/// 注意：使用 actor 避免在 async 上下文中使用 NSLock（Swift 6 会收紧）。
public actor Transaction {
    public let id: TxnID
    public private(set) var state: TxnState
    public let isolationLevel: IsolationLevel
    public let startTime: Date
    public let timeout: TimeInterval

    private let lockManager: LockManager
    private var heldLocks: [String: Lock] = [:]
    private var undoLog: [UndoRecord] = []

    init(id: TxnID, isolationLevel: IsolationLevel, timeout: TimeInterval, lockManager: LockManager) {
        self.id = id
        self.state = .active
        self.isolationLevel = isolationLevel
        self.startTime = Date()
        self.timeout = timeout
        self.lockManager = lockManager
    }

    public func setState(_ s: TxnState) {
        state = s
    }

    public func acquireLock(resource: String, type: LockType) async throws {
        let lock = try await lockManager.acquire(resource, type: type, txnId: id, timeout: timeout)
        heldLocks[resource] = lock
    }

    public func releaseAllLocks() async {
        let locks = Array(heldLocks.values)
        heldLocks.removeAll()
        for l in locks {
            await lockManager.release(l)
        }
    }

    public func appendUndo(_ record: UndoRecord) {
        undoLog.append(record)
    }

    public func drainUndoLogReversed() -> [UndoRecord] {
        let records = undoLog.reversed()
        undoLog.removeAll()
        return Array(records)
    }
}


