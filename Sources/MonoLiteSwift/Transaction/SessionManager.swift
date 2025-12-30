// Created by Yanjunhui

import Foundation

public enum SessionState: Int, Sendable {
    case active = 0
    case ended = 1
}

public struct SessionTransaction: Sendable {
    public let txnNumber: Int64
    public var state: TxnState
    public let startTime: Date
    public var autoCommit: Bool
    public var operations: Int
    public var readConcern: String
    public var writeConcern: String
    public var txn: Transaction?
}

public struct Session: Sendable {
    public let id: BSONBinary // lsid.id
    public var lastUsed: Date
    public var state: SessionState
    public var txnNumberUsed: Int64
    public var currentTxn: SessionTransaction?
}

public struct CommandContext: Sendable {
    public var session: Session?
    public var sessionTxn: SessionTransaction?
    public var txnNumber: Int64
    public var autoCommit: Bool?
    public var startTxn: Bool
    public var readConcern: String
    public var writeConcern: String

    public init() {
        self.session = nil
        self.sessionTxn = nil
        self.txnNumber = 0
        self.autoCommit = nil
        self.startTxn = false
        self.readConcern = ""
        self.writeConcern = ""
    }

    public var isInTransaction: Bool {
        sessionTxn?.state == .active && sessionTxn?.txn != nil
    }

    public var transaction: Transaction? {
        sessionTxn?.txn
    }
}

/// MongoDB logical session manager（对齐 Go: engine/session.go，简化实现）
public actor SessionManager {
    private var sessions: [String: Session] = [:] // lsid.id hex -> Session
    private unowned let db: Database
    private let sessionTTL: TimeInterval = 30 * 60
    private var cleanupTask: Task<Void, Never>?

    public init(db: Database) {
        self.db = db
        // Start cleanup task after actor initialization (Swift 6: avoid touching isolated state in init).
        Task { [weak self] in
            await self?.startCleanupTaskIfNeeded()
        }
    }

    /// Stop background cleanup task (used by Database.close()).
    /// Go parity: abort all active transactions before dropping sessions.
    public func shutdown() async {
        cleanupTask?.cancel()
        cleanupTask = nil

        // Abort all active transactions (best-effort).
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

    private func startCleanupTaskIfNeeded() {
        if cleanupTask != nil { return }
        cleanupTask = Task.detached(priority: .background) { [weak self] in
            // Background cleanup (Go parity): periodic cleanup for expired sessions.
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 5 * 60 * 1_000_000_000)
                guard let self else { return }
                await self.cleanupExpiredSessions()
            }
        }
    }

    private func sessionKey(from bin: BSONBinary) -> String {
        bin.data.map { String(format: "%02x", $0) }.joined()
    }

    private func extractSessionId(lsid: BSONDocument) throws -> BSONBinary {
        guard let idVal = lsid["id"], case .binary(let bin) = idVal else {
            throw MonoError.badValue("lsid.id is required")
        }
        return bin
    }

    public func getOrCreateSession(lsid: BSONDocument) async throws -> Session {
        let bin = try extractSessionId(lsid: lsid)
        let key = sessionKey(from: bin)

        if var s = sessions[key] {
            s.lastUsed = Date()
            sessions[key] = s
            return s
        }

        let s = Session(id: bin, lastUsed: Date(), state: .active, txnNumberUsed: -1, currentTxn: nil)
        sessions[key] = s
        return s
    }

    public func endSession(lsid: BSONDocument) async throws {
        let bin = try extractSessionId(lsid: lsid)
        let key = sessionKey(from: bin)
        guard var session = sessions[key] else { return }

        // 若有活跃事务，隐式中止（对齐 Go 行为）
        if let st = session.currentTxn, st.state == .active, let txn = st.txn {
            try? await db.txnManager.abort(txn)
            var st2 = st
            st2.state = .aborted
            session.currentTxn = st2
        }
        session.state = .ended
        sessions.removeValue(forKey: key)
    }

    public func refreshSession(lsid: BSONDocument) async throws {
        let bin = try extractSessionId(lsid: lsid)
        let key = sessionKey(from: bin)
        guard var session = sessions[key] else {
            throw MonoError(code: .noSuchSession, message: "session not found")
        }
        session.lastUsed = Date()
        sessions[key] = session
    }

    public func startTransaction(lsid: BSONDocument, txnNumber: Int64, readConcern: String = "local", writeConcern: String = "majority") async throws {
        var session = try await getOrCreateSession(lsid: lsid)
        guard session.state == .active else {
            throw MonoError(code: .noSuchSession, message: "session has ended")
        }

        if txnNumber <= session.txnNumberUsed {
            throw MonoError.transactionTooOld()
        }

        // 若有正在进行的事务，先隐式中止（对齐 Go 行为）
        if let prev = session.currentTxn, prev.state == .active, let prevTxn = prev.txn {
            try? await db.txnManager.abort(prevTxn)
        }

        // 创建底层事务
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

    public func commitTransaction(lsid: BSONDocument, txnNumber: Int64) async throws {
        let bin = try extractSessionId(lsid: lsid)
        let key = sessionKey(from: bin)
        guard var session = sessions[key] else { throw MonoError(code: .noSuchSession, message: "session not found") }
        guard var st = session.currentTxn else { throw MonoError(code: .noSuchTransaction, message: "no transaction in progress") }
        guard st.txnNumber == txnNumber else { throw MonoError(code: .noSuchTransaction, message: "transaction number mismatch") }

        if st.state != .active {
            if st.state == .committed {
                // Go parity: repeated commit is allowed (idempotent).
                session.lastUsed = Date()
                sessions[key] = session
                return
            }
            throw MonoError(code: .transactionAborted, message: "transaction has been aborted")
        }

        if let txn = st.txn {
            try await db.txnManager.commit(txn)
        }
        st.state = .committed
        session.currentTxn = st
        session.lastUsed = Date()
        sessions[key] = session
    }

    public func abortTransaction(lsid: BSONDocument, txnNumber: Int64) async throws {
        let bin = try extractSessionId(lsid: lsid)
        let key = sessionKey(from: bin)
        guard var session = sessions[key] else { throw MonoError(code: .noSuchSession, message: "session not found") }
        guard var st = session.currentTxn else { throw MonoError(code: .noSuchTransaction, message: "no transaction in progress") }
        guard st.txnNumber == txnNumber else { throw MonoError(code: .noSuchTransaction, message: "transaction number mismatch") }

        if st.state != .active {
            if st.state == .aborted {
                // Go parity: repeated abort is allowed (idempotent).
                session.lastUsed = Date()
                sessions[key] = session
                return
            }
            throw MonoError(code: .transactionCommitted, message: "transaction has been committed")
        }

        if let txn = st.txn {
            try await db.txnManager.abort(txn)
        }
        st.state = .aborted
        session.currentTxn = st
        session.lastUsed = Date()
        sessions[key] = session
    }

    private func cleanupExpiredSessions() async {
        let now = Date()
        var toRemove: [String] = []
        for (k, s) in sessions {
            if s.state == .ended {
                toRemove.append(k)
                continue
            }
            if now.timeIntervalSince(s.lastUsed) > sessionTTL {
                // Go parity: abort active txn before expiring.
                if let st = s.currentTxn, st.state == .active, let txn = st.txn {
                    try? await db.txnManager.abort(txn)
                }
                toRemove.append(k)
            }
        }
        for k in toRemove {
            sessions.removeValue(forKey: k)
        }
    }

    /// 对齐 Go: ExtractCommandContext
    public func extractCommandContext(cmd: BSONDocument) async throws -> CommandContext {
        var ctx = CommandContext()

        var lsidDoc: BSONDocument? = nil
        var hasTxnNumber = false

        for (k, v) in cmd {
            switch k {
            case "lsid":
                if case .document(let d) = v { lsidDoc = d }
            case "txnNumber":
                if let n = v.int64Value { ctx.txnNumber = n; hasTxnNumber = true }
                else if let n = v.intValue { ctx.txnNumber = Int64(n); hasTxnNumber = true }
            case "autocommit":
                if case .bool(let b) = v { ctx.autoCommit = b }
            case "startTransaction":
                if case .bool(let b) = v { ctx.startTxn = b }
            case "readConcern":
                if case .document(let d) = v, let level = d["level"]?.stringValue {
                    ctx.readConcern = level
                }
            case "writeConcern":
                if case .document(let d) = v {
                    if let w = d["w"]?.stringValue { ctx.writeConcern = w }
                }
            default:
                break
            }
        }

        if let lsid = lsidDoc {
            let session = try await getOrCreateSession(lsid: lsid)
            ctx.session = session

            if hasTxnNumber {
                if ctx.startTxn {
                    guard let ac = ctx.autoCommit, ac == false else {
                        throw MonoError.badValue("autocommit must be false for multi-document transactions")
                    }
                    try await startTransaction(lsid: lsid, txnNumber: ctx.txnNumber, readConcern: ctx.readConcern, writeConcern: ctx.writeConcern)
                }

                if let ac = ctx.autoCommit, ac == false {
                    let st = try await getActiveTransaction(lsid: lsid, txnNumber: ctx.txnNumber)
                    ctx.sessionTxn = st
                }
            }
        }

        return ctx
    }

}


