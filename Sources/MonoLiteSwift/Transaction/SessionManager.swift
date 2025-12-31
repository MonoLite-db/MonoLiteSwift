// Created by Yanjunhui

import Foundation

// MARK: - 会话状态枚举 / Session State Enum

/// 会话状态枚举
/// EN: Session state enumeration
public enum SessionState: Int, Sendable {
    /// 活跃状态 / EN: Active state
    case active = 0
    /// 已结束状态 / EN: Ended state
    case ended = 1
}

// MARK: - 会话事务结构体 / Session Transaction Struct

/// 会话事务结构体，表示会话中的事务信息
/// EN: Session transaction struct, represents transaction info in a session
public struct SessionTransaction: Sendable {
    /// 事务编号 / EN: Transaction number
    public let txnNumber: Int64
    /// 事务状态 / EN: Transaction state
    public var state: TxnState
    /// 开始时间 / EN: Start time
    public let startTime: Date
    /// 是否自动提交 / EN: Whether auto-commit is enabled
    public var autoCommit: Bool
    /// 操作计数 / EN: Operation count
    public var operations: Int
    /// 读关注级别 / EN: Read concern level
    public var readConcern: String
    /// 写关注级别 / EN: Write concern level
    public var writeConcern: String
    /// 底层事务对象 / EN: Underlying transaction object
    public var txn: Transaction?
}

// MARK: - 会话结构体 / Session Struct

/// 会话结构体，表示逻辑会话
/// EN: Session struct, represents a logical session
public struct Session: Sendable {
    /// 会话 ID（lsid.id）/ EN: Session ID (lsid.id)
    public let id: BSONBinary
    /// 最后使用时间 / EN: Last used time
    public var lastUsed: Date
    /// 会话状态 / EN: Session state
    public var state: SessionState
    /// 已使用的事务编号 / EN: Used transaction number
    public var txnNumberUsed: Int64
    /// 当前事务 / EN: Current transaction
    public var currentTxn: SessionTransaction?
}

// MARK: - 命令上下文结构体 / Command Context Struct

/// 命令上下文结构体，用于传递会话和事务信息
/// EN: Command context struct for passing session and transaction info
public struct CommandContext: Sendable {
    /// 会话 / EN: Session
    public var session: Session?
    /// 会话事务 / EN: Session transaction
    public var sessionTxn: SessionTransaction?
    /// 事务编号 / EN: Transaction number
    public var txnNumber: Int64
    /// 是否自动提交 / EN: Whether auto-commit is enabled
    public var autoCommit: Bool?
    /// 是否开始新事务 / EN: Whether to start a new transaction
    public var startTxn: Bool
    /// 读关注级别 / EN: Read concern level
    public var readConcern: String
    /// 写关注级别 / EN: Write concern level
    public var writeConcern: String

    /// 初始化命令上下文
    /// EN: Initialize command context
    public init() {
        self.session = nil
        self.sessionTxn = nil
        self.txnNumber = 0
        self.autoCommit = nil
        self.startTxn = false
        self.readConcern = ""
        self.writeConcern = ""
    }

    /// 是否在事务中
    /// EN: Whether in a transaction
    public var isInTransaction: Bool {
        sessionTxn?.state == .active && sessionTxn?.txn != nil
    }

    /// 获取当前事务
    /// EN: Get current transaction
    public var transaction: Transaction? {
        sessionTxn?.txn
    }
}

// MARK: - 会话管理器 / Session Manager

/// MongoDB 逻辑会话管理器（对齐 Go: engine/session.go，简化实现）
/// EN: MongoDB logical session manager (Go alignment: engine/session.go, simplified implementation)
public actor SessionManager {
    /// 会话映射表：lsid.id hex -> Session
    /// EN: Session map: lsid.id hex -> Session
    private var sessions: [String: Session] = [:]
    /// 数据库引用（弱引用避免循环引用）
    /// EN: Database reference (unowned to avoid retain cycle)
    private unowned let db: Database
    /// 会话 TTL（30 分钟）/ EN: Session TTL (30 minutes)
    private let sessionTTL: TimeInterval = 30 * 60
    /// 清理任务 / EN: Cleanup task
    private var cleanupTask: Task<Void, Never>?

    /// 初始化会话管理器
    /// EN: Initialize session manager
    /// - Parameter db: 数据库实例 / EN: Database instance
    public init(db: Database) {
        self.db = db
        // 在 actor 初始化后启动清理任务（Swift 6：避免在 init 中访问隔离状态）
        // EN: Start cleanup task after actor initialization (Swift 6: avoid touching isolated state in init)
        Task { [weak self] in
            await self?.startCleanupTaskIfNeeded()
        }
    }

    // MARK: - 关闭 / Shutdown

    /// 停止后台清理任务（由 Database.close() 调用）
    /// EN: Stop background cleanup task (used by Database.close())
    /// Go 对齐：在删除会话前中止所有活跃事务
    /// EN: Go parity: abort all active transactions before dropping sessions
    public func shutdown() async {
        cleanupTask?.cancel()
        cleanupTask = nil

        // 中止所有活跃事务（尽力而为）
        // EN: Abort all active transactions (best-effort)
        for (_, session) in sessions {
            if let st = session.currentTxn, st.state == .active, let txn = st.txn {
                try? await db.txnManager.abort(txn)
            }
        }

        sessions.removeAll()
    }

    deinit {
        cleanupTask?.cancel()
    }

    // MARK: - 清理任务 / Cleanup Task

    /// 如果需要则启动清理任务
    /// EN: Start cleanup task if needed
    private func startCleanupTaskIfNeeded() {
        if cleanupTask != nil { return }
        cleanupTask = Task.detached(priority: .background) { [weak self] in
            // 后台清理（Go 对齐）：定期清理过期会话
            // EN: Background cleanup (Go parity): periodic cleanup for expired sessions
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 5 * 60 * 1_000_000_000)
                guard let self else { return }
                await self.cleanupExpiredSessions()
            }
        }
    }

    // MARK: - 会话 ID 处理 / Session ID Handling

    /// 从二进制数据生成会话键
    /// EN: Generate session key from binary data
    /// - Parameter bin: 二进制数据 / EN: Binary data
    /// - Returns: 十六进制字符串键 / EN: Hexadecimal string key
    private func sessionKey(from bin: BSONBinary) -> String {
        bin.data.map { String(format: "%02x", $0) }.joined()
    }

    /// 从 lsid 文档提取会话 ID
    /// EN: Extract session ID from lsid document
    /// - Parameter lsid: lsid 文档 / EN: lsid document
    /// - Returns: 会话 ID 二进制数据 / EN: Session ID binary data
    /// - Throws: 如果 lsid.id 不存在则抛出错误 / EN: Throws error if lsid.id is missing
    private func extractSessionId(lsid: BSONDocument) throws -> BSONBinary {
        guard let idVal = lsid["id"], case .binary(let bin) = idVal else {
            throw MonoError.badValue("lsid.id is required")
        }
        return bin
    }

    // MARK: - 会话操作 / Session Operations

    /// 获取或创建会话
    /// EN: Get or create session
    /// - Parameter lsid: lsid 文档 / EN: lsid document
    /// - Returns: 会话对象 / EN: Session object
    /// - Throws: 如果 lsid 无效则抛出错误 / EN: Throws error if lsid is invalid
    public func getOrCreateSession(lsid: BSONDocument) async throws -> Session {
        let bin = try extractSessionId(lsid: lsid)
        let key = sessionKey(from: bin)

        // 如果会话已存在，更新最后使用时间并返回
        // EN: If session exists, update last used time and return
        if var s = sessions[key] {
            s.lastUsed = Date()
            sessions[key] = s
            return s
        }

        // 创建新会话 / EN: Create new session
        let s = Session(id: bin, lastUsed: Date(), state: .active, txnNumberUsed: -1, currentTxn: nil)
        sessions[key] = s
        return s
    }

    /// 结束会话
    /// EN: End session
    /// - Parameter lsid: lsid 文档 / EN: lsid document
    /// - Throws: 如果 lsid 无效则抛出错误 / EN: Throws error if lsid is invalid
    public func endSession(lsid: BSONDocument) async throws {
        let bin = try extractSessionId(lsid: lsid)
        let key = sessionKey(from: bin)
        guard var session = sessions[key] else { return }

        // 若有活跃事务，隐式中止（对齐 Go 行为）
        // EN: If there's an active transaction, implicitly abort (Go parity)
        if let st = session.currentTxn, st.state == .active, let txn = st.txn {
            try? await db.txnManager.abort(txn)
            var st2 = st
            st2.state = .aborted
            session.currentTxn = st2
        }
        session.state = .ended
        sessions.removeValue(forKey: key)
    }

    /// 刷新会话（更新最后使用时间）
    /// EN: Refresh session (update last used time)
    /// - Parameter lsid: lsid 文档 / EN: lsid document
    /// - Throws: 如果会话不存在则抛出错误 / EN: Throws error if session not found
    public func refreshSession(lsid: BSONDocument) async throws {
        let bin = try extractSessionId(lsid: lsid)
        let key = sessionKey(from: bin)
        guard var session = sessions[key] else {
            throw MonoError(code: .noSuchSession, message: "session not found")
        }
        session.lastUsed = Date()
        sessions[key] = session
    }

    // MARK: - 事务操作 / Transaction Operations

    /// 开始事务
    /// EN: Start transaction
    /// - Parameters:
    ///   - lsid: lsid 文档 / EN: lsid document
    ///   - txnNumber: 事务编号 / EN: Transaction number
    ///   - readConcern: 读关注级别 / EN: Read concern level
    ///   - writeConcern: 写关注级别 / EN: Write concern level
    /// - Throws: 如果会话无效或事务编号过旧则抛出错误
    /// EN: Throws error if session is invalid or transaction number is too old
    public func startTransaction(lsid: BSONDocument, txnNumber: Int64, readConcern: String = "local", writeConcern: String = "majority") async throws {
        var session = try await getOrCreateSession(lsid: lsid)
        guard session.state == .active else {
            throw MonoError(code: .noSuchSession, message: "session has ended")
        }

        // 检查事务编号是否过旧 / EN: Check if transaction number is too old
        if txnNumber <= session.txnNumberUsed {
            throw MonoError.transactionTooOld()
        }

        // 若有正在进行的事务，先隐式中止（对齐 Go 行为）
        // EN: If there's an ongoing transaction, implicitly abort first (Go parity)
        if let prev = session.currentTxn, prev.state == .active, let prevTxn = prev.txn {
            try? await db.txnManager.abort(prevTxn)
        }

        // 创建底层事务 / EN: Create underlying transaction
        let txn = await db.txnManager.begin()
        session.currentTxn = SessionTransaction(
            txnNumber: txnNumber,
            state: .active,
            startTime: Date(),
            autoCommit: false,
            operations: 0,
            readConcern: readConcern,
            writeConcern: writeConcern,
            txn: txn
        )
        session.txnNumberUsed = txnNumber
        session.lastUsed = Date()

        let key = sessionKey(from: session.id)
        sessions[key] = session
    }

    /// 获取活跃事务
    /// EN: Get active transaction
    /// - Parameters:
    ///   - lsid: lsid 文档 / EN: lsid document
    ///   - txnNumber: 事务编号 / EN: Transaction number
    /// - Returns: 会话事务对象 / EN: Session transaction object
    /// - Throws: 如果会话或事务不存在则抛出错误
    /// EN: Throws error if session or transaction not found
    public func getActiveTransaction(lsid: BSONDocument, txnNumber: Int64) async throws -> SessionTransaction {
        let bin = try extractSessionId(lsid: lsid)
        let key = sessionKey(from: bin)
        guard let session = sessions[key] else {
            throw MonoError(code: .noSuchSession, message: "session not found")
        }
        guard let st = session.currentTxn else {
            throw MonoError(code: .noSuchTransaction, message: "no transaction in progress")
        }
        guard st.txnNumber == txnNumber else {
            throw MonoError(code: .noSuchTransaction, message: "transaction number mismatch")
        }
        switch st.state {
        case .active:
            return st
        case .committed:
            throw MonoError(code: .transactionCommitted, message: "transaction has been committed")
        case .aborted:
            throw MonoError(code: .transactionAborted, message: "transaction has been aborted")
        }
    }

    /// 提交事务
    /// EN: Commit transaction
    /// - Parameters:
    ///   - lsid: lsid 文档 / EN: lsid document
    ///   - txnNumber: 事务编号 / EN: Transaction number
    /// - Throws: 如果会话或事务不存在则抛出错误
    /// EN: Throws error if session or transaction not found
    public func commitTransaction(lsid: BSONDocument, txnNumber: Int64) async throws {
        let bin = try extractSessionId(lsid: lsid)
        let key = sessionKey(from: bin)
        guard var session = sessions[key] else { throw MonoError(code: .noSuchSession, message: "session not found") }
        guard var st = session.currentTxn else { throw MonoError(code: .noSuchTransaction, message: "no transaction in progress") }
        guard st.txnNumber == txnNumber else { throw MonoError(code: .noSuchTransaction, message: "transaction number mismatch") }

        if st.state != .active {
            if st.state == .committed {
                // Go 对齐：重复提交是允许的（幂等）
                // EN: Go parity: repeated commit is allowed (idempotent)
                session.lastUsed = Date()
                sessions[key] = session
                return
            }
            throw MonoError(code: .transactionAborted, message: "transaction has been aborted")
        }

        // 提交底层事务 / EN: Commit underlying transaction
        if let txn = st.txn {
            try await db.txnManager.commit(txn)
        }
        st.state = .committed
        session.currentTxn = st
        session.lastUsed = Date()
        sessions[key] = session
    }

    /// 中止事务
    /// EN: Abort transaction
    /// - Parameters:
    ///   - lsid: lsid 文档 / EN: lsid document
    ///   - txnNumber: 事务编号 / EN: Transaction number
    /// - Throws: 如果会话或事务不存在则抛出错误
    /// EN: Throws error if session or transaction not found
    public func abortTransaction(lsid: BSONDocument, txnNumber: Int64) async throws {
        let bin = try extractSessionId(lsid: lsid)
        let key = sessionKey(from: bin)
        guard var session = sessions[key] else { throw MonoError(code: .noSuchSession, message: "session not found") }
        guard var st = session.currentTxn else { throw MonoError(code: .noSuchTransaction, message: "no transaction in progress") }
        guard st.txnNumber == txnNumber else { throw MonoError(code: .noSuchTransaction, message: "transaction number mismatch") }

        if st.state != .active {
            if st.state == .aborted {
                // Go 对齐：重复中止是允许的（幂等）
                // EN: Go parity: repeated abort is allowed (idempotent)
                session.lastUsed = Date()
                sessions[key] = session
                return
            }
            throw MonoError(code: .transactionCommitted, message: "transaction has been committed")
        }

        // 中止底层事务 / EN: Abort underlying transaction
        if let txn = st.txn {
            try await db.txnManager.abort(txn)
        }
        st.state = .aborted
        session.currentTxn = st
        session.lastUsed = Date()
        sessions[key] = session
    }

    // MARK: - 过期会话清理 / Expired Session Cleanup

    /// 清理过期会话
    /// EN: Cleanup expired sessions
    private func cleanupExpiredSessions() async {
        let now = Date()
        var toRemove: [String] = []
        for (k, s) in sessions {
            // 已结束的会话直接移除 / EN: Remove ended sessions directly
            if s.state == .ended {
                toRemove.append(k)
                continue
            }
            // 检查是否超过 TTL / EN: Check if TTL exceeded
            if now.timeIntervalSince(s.lastUsed) > sessionTTL {
                // Go 对齐：过期前中止活跃事务
                // EN: Go parity: abort active txn before expiring
                if let st = s.currentTxn, st.state == .active, let txn = st.txn {
                    try? await db.txnManager.abort(txn)
                }
                toRemove.append(k)
            }
        }
        // 移除过期会话 / EN: Remove expired sessions
        for k in toRemove {
            sessions.removeValue(forKey: k)
        }
    }

    // MARK: - 命令上下文提取 / Command Context Extraction

    /// 对齐 Go: ExtractCommandContext
    /// EN: Go alignment: ExtractCommandContext
    /// 从命令文档提取会话和事务上下文
    /// EN: Extract session and transaction context from command document
    /// - Parameter cmd: 命令文档 / EN: Command document
    /// - Returns: 命令上下文 / EN: Command context
    /// - Throws: 如果参数无效则抛出错误 / EN: Throws error if parameters are invalid
    public func extractCommandContext(cmd: BSONDocument) async throws -> CommandContext {
        var ctx = CommandContext()

        var lsidDoc: BSONDocument? = nil
        var hasTxnNumber = false

        // 解析命令参数 / EN: Parse command parameters
        for (k, v) in cmd {
            switch k {
            case "lsid":
                // 提取 lsid 文档 / EN: Extract lsid document
                if case .document(let d) = v { lsidDoc = d }
            case "txnNumber":
                // 提取事务编号 / EN: Extract transaction number
                if let n = v.int64Value { ctx.txnNumber = n; hasTxnNumber = true }
                else if let n = v.intValue { ctx.txnNumber = Int64(n); hasTxnNumber = true }
            case "autocommit":
                // 提取自动提交标志 / EN: Extract auto-commit flag
                if case .bool(let b) = v { ctx.autoCommit = b }
            case "startTransaction":
                // 提取开始事务标志 / EN: Extract start transaction flag
                if case .bool(let b) = v { ctx.startTxn = b }
            case "readConcern":
                // 提取读关注级别 / EN: Extract read concern level
                if case .document(let d) = v, let level = d["level"]?.stringValue {
                    ctx.readConcern = level
                }
            case "writeConcern":
                // 提取写关注级别 / EN: Extract write concern level
                if case .document(let d) = v {
                    if let w = d["w"]?.stringValue { ctx.writeConcern = w }
                }
            default:
                break
            }
        }

        // 处理会话和事务 / EN: Handle session and transaction
        if let lsid = lsidDoc {
            let session = try await getOrCreateSession(lsid: lsid)
            ctx.session = session

            if hasTxnNumber {
                if ctx.startTxn {
                    // 开始新事务时，autocommit 必须为 false
                    // EN: When starting new transaction, autocommit must be false
                    guard let ac = ctx.autoCommit, ac == false else {
                        throw MonoError.badValue("autocommit must be false for multi-document transactions")
                    }
                    try await startTransaction(lsid: lsid, txnNumber: ctx.txnNumber, readConcern: ctx.readConcern, writeConcern: ctx.writeConcern)
                }

                // 获取活跃事务 / EN: Get active transaction
                if let ac = ctx.autoCommit, ac == false {
                    let st = try await getActiveTransaction(lsid: lsid, txnNumber: ctx.txnNumber)
                    ctx.sessionTxn = st
                }
            }
        }

        return ctx
    }

}
