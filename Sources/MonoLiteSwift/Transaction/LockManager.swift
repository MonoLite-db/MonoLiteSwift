// Created by Yanjunhui

import Foundation

public enum LockType: Int, Sendable {
    case read = 0
    case write = 1
}

public struct TxnID: Hashable, Sendable {
    public let rawValue: UInt64
    public init(_ rawValue: UInt64) { self.rawValue = rawValue }
}

public struct Lock: Sendable {
    public let resource: String
    public let type: LockType
    public let txnId: TxnID
    public let heldAt: Date
}

/// Go-aligned lock manager:
/// - shared (read) + exclusive (write)
/// - wait queue
/// - wait-for graph + deadlock detection
public actor LockManager {
    private struct Waiter {
        let id: UInt64
        let txnId: TxnID
        let type: LockType
        let startedAt: Date
        let continuation: CheckedContinuation<Lock, Error>
    }

    private struct Entry {
        var exclusive: TxnID?
        var shared: Set<TxnID>
        var waiters: [Waiter]
    }

    private var locks: [String: Entry] = [:]
    private var waitGraph: [TxnID: [TxnID]] = [:]
    private var nextWaiterId: UInt64 = 1
    /// Track timeout tasks by waiterId so we can cancel them when waiter is woken.
    private var timeoutTasks: [UInt64: Task<Void, Never>] = [:]

    public init() {}

    public func acquire(_ resource: String, type: LockType, txnId: TxnID, timeout: TimeInterval) async throws -> Lock {
        if let lock = try tryAcquire(resource, type: type, txnId: txnId) {
            return lock
        }

        let now = Date()
        let deadline = now.addingTimeInterval(timeout)
        let waiterId = nextWaiterId
        nextWaiterId &+= 1

        return try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Lock, Error>) in
            var entry = self.locks[resource] ?? Entry(exclusive: nil, shared: [], waiters: [])
            entry.waiters.append(Waiter(id: waiterId, txnId: txnId, type: type, startedAt: now, continuation: cont))
            self.locks[resource] = entry

            // Build wait-for edges from this txn to current holders.
            let waitFor = currentHolders(entry: entry, excluding: txnId)
            self.waitGraph[txnId] = waitFor

            // Deadlock detection (Go parity).
            if detectDeadlockLocked(start: txnId) {
                // Remove waiter + graph edge and fail fast.
                self.removeWaiterLocked(resource: resource, waiterId: waiterId, txnId: txnId)
                cont.resume(throwing: MonoError.transactionAborted())
                return
            }

            // Timeout task: remove waiter if still waiting. Store handle so we can cancel on wake.
            let task = Task.detached(priority: .background) { [weak self] in
                let nanos = UInt64(max(0, deadline.timeIntervalSinceNow) * 1_000_000_000)
                do {
                    try await Task.sleep(nanoseconds: nanos)
                } catch {
                    // Task was cancelled; exit without timing out.
                    return
                }
                await self?.timeoutWaiter(resource: resource, waiterId: waiterId, txnId: txnId)
            }
            self.timeoutTasks[waiterId] = task
        }
    }

    public func release(_ lock: Lock) async {
        release(resource: lock.resource, txnId: lock.txnId)
    }

    public func release(resource: String, txnId: TxnID) {
        guard var entry = locks[resource] else { return }
        if entry.exclusive == txnId { entry.exclusive = nil }
        entry.shared.remove(txnId)
        locks[resource] = entry
        wakeWaiters(resource: resource)

        // BUG-013 parity: cleanup empty entry to avoid leaks.
        if let e = locks[resource], e.exclusive == nil, e.shared.isEmpty, e.waiters.isEmpty {
            locks.removeValue(forKey: resource)
        }
    }

    private func tryAcquire(_ resource: String, type: LockType, txnId: TxnID) throws -> Lock? {
        var entry = locks[resource] ?? Entry(exclusive: nil, shared: [], waiters: [])

        // 已持有同类型锁 -> 复用
        if type == .write, entry.exclusive == txnId {
            return Lock(resource: resource, type: .write, txnId: txnId, heldAt: Date())
        }
        if type == .read, entry.shared.contains(txnId) {
            return Lock(resource: resource, type: .read, txnId: txnId, heldAt: Date())
        }

        switch type {
        case .read:
            // 无排他锁即可授予读锁
            if entry.exclusive == nil || entry.exclusive == txnId {
                entry.shared.insert(txnId)
                locks[resource] = entry
                return Lock(resource: resource, type: .read, txnId: txnId, heldAt: Date())
            }
            return nil

        case .write:
            // Go parity: write lock allowed if no other txn holds shared/exclusive; allow "upgrade" when only self holds shared.
            if entry.exclusive == nil || entry.exclusive == txnId {
                let othersShared = entry.shared.subtracting([txnId])
                if !othersShared.isEmpty {
                    return nil
                }
                entry.exclusive = txnId
                locks[resource] = entry
                return Lock(resource: resource, type: .write, txnId: txnId, heldAt: Date())
            }
            return nil
        }
    }

    private func wakeWaiters(resource: String) {
        guard var entry = locks[resource] else { return }
        if entry.waiters.isEmpty { return }

        // FIFO：只要能授予就依次唤醒（读锁可批量）
        var remaining: [Waiter] = []
        for w in entry.waiters {
            if let lock = try? tryAcquire(resource, type: w.type, txnId: w.txnId) {
                // Cancel timeout task so it doesn't linger in background.
                timeoutTasks[w.id]?.cancel()
                timeoutTasks.removeValue(forKey: w.id)
                // remove waitGraph edge before resuming
                waitGraph.removeValue(forKey: w.txnId)
                w.continuation.resume(returning: lock)
            } else {
                remaining.append(w)
            }
        }
        entry.waiters = remaining
        locks[resource] = entry
    }

    // MARK: - Deadlock detection + timeout

    private func currentHolders(entry: Entry, excluding txnId: TxnID) -> [TxnID] {
        var waitFor: [TxnID] = []
        if let ex = entry.exclusive, ex != txnId {
            waitFor.append(ex)
        }
        for tid in entry.shared where tid != txnId {
            waitFor.append(tid)
        }
        return waitFor
    }

    private func detectDeadlockLocked(start: TxnID) -> Bool {
        var visited: Set<TxnID> = []
        var inStack: Set<TxnID> = []

        func dfs(_ t: TxnID) -> Bool {
            if inStack.contains(t) { return true }
            if visited.contains(t) { return false }
            visited.insert(t)
            inStack.insert(t)
            for next in waitGraph[t] ?? [] {
                if dfs(next) { return true }
            }
            inStack.remove(t)
            return false
        }

        return dfs(start)
    }

    private func removeWaiterLocked(resource: String, waiterId: UInt64, txnId: TxnID) {
        // Cancel timeout task.
        timeoutTasks[waiterId]?.cancel()
        timeoutTasks.removeValue(forKey: waiterId)

        if var entry = locks[resource] {
            entry.waiters.removeAll { $0.id == waiterId }
            locks[resource] = entry
        }
        waitGraph.removeValue(forKey: txnId)
    }

    private func timeoutWaiter(resource: String, waiterId: UInt64, txnId: TxnID) {
        // Clean up task handle first.
        timeoutTasks.removeValue(forKey: waiterId)

        guard var entry = locks[resource] else { return }
        guard let idx = entry.waiters.firstIndex(where: { $0.id == waiterId }) else { return }
        let waiter = entry.waiters.remove(at: idx)
        locks[resource] = entry
        waitGraph.removeValue(forKey: txnId)
        waiter.continuation.resume(throwing: MonoError.operationFailed("lock acquisition timeout"))
    }
}
