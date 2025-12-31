// Created by Yanjunhui

import Foundation

// MARK: - 锁类型枚举 / Lock Type Enum

/// 锁类型枚举
/// EN: Lock type enumeration
public enum LockType: Int, Sendable {
    /// 读锁（共享锁）/ EN: Read lock (shared lock)
    case read = 0
    /// 写锁（排他锁）/ EN: Write lock (exclusive lock)
    case write = 1
}

// MARK: - 事务 ID 结构体 / Transaction ID Struct

/// 事务 ID 结构体
/// EN: Transaction ID struct
public struct TxnID: Hashable, Sendable {
    /// 原始值 / EN: Raw value
    public let rawValue: UInt64
    /// 初始化事务 ID / EN: Initialize transaction ID
    public init(_ rawValue: UInt64) { self.rawValue = rawValue }
}

// MARK: - 锁结构体 / Lock Struct

/// 锁结构体
/// EN: Lock struct
public struct Lock: Sendable {
    /// 资源标识符 / EN: Resource identifier
    public let resource: String
    /// 锁类型 / EN: Lock type
    public let type: LockType
    /// 持有锁的事务 ID / EN: Transaction ID holding the lock
    public let txnId: TxnID
    /// 获取锁的时间 / EN: Time when lock was acquired
    public let heldAt: Date
}

// MARK: - 锁管理器 / Lock Manager

/// Go 对齐的锁管理器：
/// - 共享锁（读）+ 排他锁（写）
/// - 等待队列
/// - 等待图 + 死锁检测
///
/// EN: Go-aligned lock manager:
/// - shared (read) + exclusive (write)
/// - wait queue
/// - wait-for graph + deadlock detection
public actor LockManager {
    // MARK: - 等待者结构体 / Waiter Struct

    /// 等待获取锁的请求者
    /// EN: Waiter requesting lock acquisition
    private struct Waiter {
        /// 等待者 ID / EN: Waiter ID
        let id: UInt64
        /// 事务 ID / EN: Transaction ID
        let txnId: TxnID
        /// 请求的锁类型 / EN: Requested lock type
        let type: LockType
        /// 开始等待时间 / EN: Time when waiting started
        let startedAt: Date
        /// 用于恢复异步等待的 continuation
        /// EN: Continuation for resuming async wait
        let continuation: CheckedContinuation<Lock, Error>
    }

    // MARK: - 锁条目结构体 / Lock Entry Struct

    /// 资源锁条目
    /// EN: Resource lock entry
    private struct Entry {
        /// 持有排他锁的事务 ID / EN: Transaction ID holding exclusive lock
        var exclusive: TxnID?
        /// 持有共享锁的事务 ID 集合 / EN: Set of transaction IDs holding shared locks
        var shared: Set<TxnID>
        /// 等待队列 / EN: Wait queue
        var waiters: [Waiter]
    }

    // MARK: - 属性 / Properties

    /// 锁映射表：资源 -> 锁条目
    /// EN: Lock map: resource -> lock entry
    private var locks: [String: Entry] = [:]
    /// 等待图：用于死锁检测
    /// EN: Wait graph: for deadlock detection
    private var waitGraph: [TxnID: [TxnID]] = [:]
    /// 下一个等待者 ID / EN: Next waiter ID
    private var nextWaiterId: UInt64 = 1
    /// 超时任务映射：等待者 ID -> 超时任务
    /// EN: Timeout tasks map: waiter ID -> timeout task
    /// 用于在等待者被唤醒时取消超时任务
    /// EN: Used to cancel timeout tasks when waiter is woken
    private var timeoutTasks: [UInt64: Task<Void, Never>] = [:]

    /// 初始化锁管理器
    /// EN: Initialize lock manager
    public init() {}

    // MARK: - 锁获取 / Lock Acquisition

    /// 获取资源锁
    /// EN: Acquire resource lock
    /// - Parameters:
    ///   - resource: 资源标识符 / EN: Resource identifier
    ///   - type: 锁类型 / EN: Lock type
    ///   - txnId: 事务 ID / EN: Transaction ID
    ///   - timeout: 超时时间 / EN: Timeout duration
    /// - Returns: 获取到的锁 / EN: Acquired lock
    /// - Throws: 超时或死锁时抛出错误 / EN: Throws error on timeout or deadlock
    public func acquire(_ resource: String, type: LockType, txnId: TxnID, timeout: TimeInterval) async throws -> Lock {
        // 尝试立即获取锁 / EN: Try to acquire lock immediately
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

            // 构建等待图边：从当前事务指向当前持有者
            // EN: Build wait-for edges from this txn to current holders
            let waitFor = currentHolders(entry: entry, excluding: txnId)
            self.waitGraph[txnId] = waitFor

            // 死锁检测（Go 对齐）
            // EN: Deadlock detection (Go parity)
            if detectDeadlockLocked(start: txnId) {
                // 移除等待者和图边，快速失败
                // EN: Remove waiter + graph edge and fail fast
                self.removeWaiterLocked(resource: resource, waiterId: waiterId, txnId: txnId)
                cont.resume(throwing: MonoError.transactionAborted())
                return
            }

            // 超时任务：如果仍在等待则移除等待者
            // EN: Timeout task: remove waiter if still waiting
            // 保存任务句柄以便在唤醒时取消
            // EN: Store handle so we can cancel on wake
            let task = Task.detached(priority: .background) { [weak self] in
                let nanos = UInt64(max(0, deadline.timeIntervalSinceNow) * 1_000_000_000)
                do {
                    try await Task.sleep(nanoseconds: nanos)
                } catch {
                    // 任务被取消，退出而不超时
                    // EN: Task was cancelled; exit without timing out
                    return
                }
                await self?.timeoutWaiter(resource: resource, waiterId: waiterId, txnId: txnId)
            }
            self.timeoutTasks[waiterId] = task
        }
    }

    // MARK: - 锁释放 / Lock Release

    /// 释放锁
    /// EN: Release lock
    /// - Parameter lock: 要释放的锁 / EN: Lock to release
    public func release(_ lock: Lock) async {
        release(resource: lock.resource, txnId: lock.txnId)
    }

    /// 释放指定资源和事务的锁
    /// EN: Release lock for specified resource and transaction
    /// - Parameters:
    ///   - resource: 资源标识符 / EN: Resource identifier
    ///   - txnId: 事务 ID / EN: Transaction ID
    public func release(resource: String, txnId: TxnID) {
        guard var entry = locks[resource] else { return }
        // 释放排他锁 / EN: Release exclusive lock
        if entry.exclusive == txnId { entry.exclusive = nil }
        // 释放共享锁 / EN: Release shared lock
        entry.shared.remove(txnId)
        locks[resource] = entry
        // 唤醒等待者 / EN: Wake up waiters
        wakeWaiters(resource: resource)

        // BUG-013 对齐：清理空条目避免内存泄漏
        // EN: BUG-013 parity: cleanup empty entry to avoid leaks
        if let e = locks[resource], e.exclusive == nil, e.shared.isEmpty, e.waiters.isEmpty {
            locks.removeValue(forKey: resource)
        }
    }

    // MARK: - 尝试获取锁 / Try Acquire Lock

    /// 尝试立即获取锁（不阻塞）
    /// EN: Try to acquire lock immediately (non-blocking)
    /// - Parameters:
    ///   - resource: 资源标识符 / EN: Resource identifier
    ///   - type: 锁类型 / EN: Lock type
    ///   - txnId: 事务 ID / EN: Transaction ID
    /// - Returns: 获取到的锁，如果无法立即获取则返回 nil
    /// EN: Acquired lock, or nil if cannot acquire immediately
    private func tryAcquire(_ resource: String, type: LockType, txnId: TxnID) throws -> Lock? {
        var entry = locks[resource] ?? Entry(exclusive: nil, shared: [], waiters: [])

        // 已持有同类型锁 -> 复用
        // EN: Already holding same type lock -> reuse
        if type == .write, entry.exclusive == txnId {
            return Lock(resource: resource, type: .write, txnId: txnId, heldAt: Date())
        }
        if type == .read, entry.shared.contains(txnId) {
            return Lock(resource: resource, type: .read, txnId: txnId, heldAt: Date())
        }

        switch type {
        case .read:
            // 无排他锁即可授予读锁
            // EN: Grant read lock if no exclusive lock
            if entry.exclusive == nil || entry.exclusive == txnId {
                entry.shared.insert(txnId)
                locks[resource] = entry
                return Lock(resource: resource, type: .read, txnId: txnId, heldAt: Date())
            }
            return nil

        case .write:
            // Go 对齐：无其他事务持有共享/排他锁时可授予写锁
            // 允许"升级"：仅自己持有共享锁时可升级为写锁
            // EN: Go parity: write lock allowed if no other txn holds shared/exclusive
            // Allow "upgrade" when only self holds shared
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

    // MARK: - 唤醒等待者 / Wake Waiters

    /// 唤醒等待队列中的等待者
    /// EN: Wake up waiters in the wait queue
    /// - Parameter resource: 资源标识符 / EN: Resource identifier
    private func wakeWaiters(resource: String) {
        guard var entry = locks[resource] else { return }
        if entry.waiters.isEmpty { return }

        // FIFO：只要能授予就依次唤醒（读锁可批量）
        // EN: FIFO: wake up sequentially as long as lock can be granted (read locks can batch)
        var remaining: [Waiter] = []
        for w in entry.waiters {
            if let lock = try? tryAcquire(resource, type: w.type, txnId: w.txnId) {
                // 取消超时任务，避免后台残留
                // EN: Cancel timeout task so it doesn't linger in background
                timeoutTasks[w.id]?.cancel()
                timeoutTasks.removeValue(forKey: w.id)
                // 在恢复前移除等待图边
                // EN: Remove waitGraph edge before resuming
                waitGraph.removeValue(forKey: w.txnId)
                w.continuation.resume(returning: lock)
            } else {
                remaining.append(w)
            }
        }
        entry.waiters = remaining
        locks[resource] = entry
    }

    // MARK: - 死锁检测 + 超时 / Deadlock Detection + Timeout

    /// 获取当前锁持有者（排除指定事务）
    /// EN: Get current lock holders (excluding specified transaction)
    /// - Parameters:
    ///   - entry: 锁条目 / EN: Lock entry
    ///   - txnId: 要排除的事务 ID / EN: Transaction ID to exclude
    /// - Returns: 持有者事务 ID 数组 / EN: Array of holder transaction IDs
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

    /// 检测死锁（使用 DFS 检测等待图中的环）
    /// EN: Detect deadlock (use DFS to detect cycle in wait graph)
    /// - Parameter start: 起始事务 ID / EN: Starting transaction ID
    /// - Returns: 是否存在死锁 / EN: Whether deadlock exists
    private func detectDeadlockLocked(start: TxnID) -> Bool {
        var visited: Set<TxnID> = []
        var inStack: Set<TxnID> = []

        func dfs(_ t: TxnID) -> Bool {
            // 在栈中发现则存在环 / EN: Found in stack means cycle exists
            if inStack.contains(t) { return true }
            // 已访问过则无需再访问 / EN: Already visited, no need to revisit
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

    /// 移除等待者（内部方法，需在锁保护下调用）
    /// EN: Remove waiter (internal method, must be called under lock)
    /// - Parameters:
    ///   - resource: 资源标识符 / EN: Resource identifier
    ///   - waiterId: 等待者 ID / EN: Waiter ID
    ///   - txnId: 事务 ID / EN: Transaction ID
    private func removeWaiterLocked(resource: String, waiterId: UInt64, txnId: TxnID) {
        // 取消超时任务 / EN: Cancel timeout task
        timeoutTasks[waiterId]?.cancel()
        timeoutTasks.removeValue(forKey: waiterId)

        if var entry = locks[resource] {
            entry.waiters.removeAll { $0.id == waiterId }
            locks[resource] = entry
        }
        waitGraph.removeValue(forKey: txnId)
    }

    /// 处理等待者超时
    /// EN: Handle waiter timeout
    /// - Parameters:
    ///   - resource: 资源标识符 / EN: Resource identifier
    ///   - waiterId: 等待者 ID / EN: Waiter ID
    ///   - txnId: 事务 ID / EN: Transaction ID
    private func timeoutWaiter(resource: String, waiterId: UInt64, txnId: TxnID) {
        // 先清理任务句柄 / EN: Clean up task handle first
        timeoutTasks.removeValue(forKey: waiterId)

        guard var entry = locks[resource] else { return }
        guard let idx = entry.waiters.firstIndex(where: { $0.id == waiterId }) else { return }
        let waiter = entry.waiters.remove(at: idx)
        locks[resource] = entry
        waitGraph.removeValue(forKey: txnId)
        waiter.continuation.resume(throwing: MonoError.operationFailed("lock acquisition timeout"))
    }
}
