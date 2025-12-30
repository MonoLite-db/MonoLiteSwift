// Created by Yanjunhui

import Foundation

/// SerialWriteQueue 确保写操作按顺序串行执行，避免 actor 重入导致的竞态问题。
/// 类似于 Go 的 sync.Mutex，但适用于 Swift async/await 环境。
public actor SerialWriteQueue {
    private var pending: [CheckedContinuation<Void, Never>] = []
    private var isProcessing: Bool = false

    public init() {}

    /// 获取写锁，确保在 closure 执行期间没有其他写操作
    /// 用法：await queue.withLock { ... }
    public func withLock<T>(_ operation: @Sendable () async throws -> T) async rethrows -> T {
        await acquireLock()
        defer { releaseLock() }
        return try await operation()
    }

    private func acquireLock() async {
        if !isProcessing {
            isProcessing = true
            return
        }

        // 已有操作在执行，等待
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            pending.append(cont)
        }
    }

    private func releaseLock() {
        if let next = pending.first {
            pending.removeFirst()
            next.resume()
        } else {
            isProcessing = false
        }
    }
}

