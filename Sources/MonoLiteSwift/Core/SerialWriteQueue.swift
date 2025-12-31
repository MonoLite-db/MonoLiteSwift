// Created by Yanjunhui

import Foundation

/// SerialWriteQueue 确保写操作按顺序串行执行，避免 actor 重入导致的竞态问题。
/// EN: SerialWriteQueue ensures write operations are executed serially in order, avoiding race conditions caused by actor reentrancy.
/// 类似于 Go 的 sync.Mutex，但适用于 Swift async/await 环境。
/// EN: Similar to Go's sync.Mutex, but designed for Swift async/await environment.
public actor SerialWriteQueue {
    /// 等待中的继续体队列
    /// EN: Queue of pending continuations
    private var pending: [CheckedContinuation<Void, Never>] = []

    /// 是否正在处理中
    /// EN: Whether processing is in progress
    private var isProcessing: Bool = false

    /// 初始化串行写队列
    /// EN: Initialize serial write queue
    public init() {}

    /// 获取写锁，确保在 closure 执行期间没有其他写操作
    /// EN: Acquire write lock, ensuring no other write operations during closure execution
    /// 用法：await queue.withLock { ... }
    /// EN: Usage: await queue.withLock { ... }
    public func withLock<T>(_ operation: @Sendable () async throws -> T) async rethrows -> T {
        await acquireLock()
        defer { releaseLock() }
        return try await operation()
    }

    /// 获取锁
    /// EN: Acquire lock
    private func acquireLock() async {
        if !isProcessing {
            isProcessing = true
            return
        }

        // 已有操作在执行，等待
        // EN: Another operation is executing, wait
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            pending.append(cont)
        }
    }

    /// 释放锁
    /// EN: Release lock
    private func releaseLock() {
        if let next = pending.first {
            pending.removeFirst()
            next.resume()
        } else {
            isProcessing = false
        }
    }
}
