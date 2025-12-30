// Created by Yanjunhui

import Foundation

/// Log levels (Go parity: engine/logger.go)
public enum LogLevel: Int, Sendable, Comparable {
    case debug = 0
    case info = 1
    case warn = 2
    case error = 3

    public var name: String {
        switch self {
        case .debug: return "DEBUG"
        case .info: return "INFO"
        case .warn: return "WARN"
        case .error: return "ERROR"
        }
    }

    public static func < (lhs: LogLevel, rhs: LogLevel) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

/// Structured log entry (Go parity: LogEntry)
public struct LogEntry: Sendable, Encodable {
    public let ts: Date
    public let level: String
    public let component: String?
    public let msg: String
    public let ctx: [String: String]?
    public let durationMs: Int64?
}

/// Structured JSON logger (Go parity: Logger)
public final class MonoLogger: @unchecked Sendable {
    private let lock = NSLock()
    private var output: TextOutputStream
    private var level: LogLevel
    private let component: String
    private var slowThreshold: TimeInterval

    public static let `default` = MonoLogger(output: FileHandleOutputStream.standardOutput)

    public init(output: TextOutputStream, component: String = "MONODB", level: LogLevel = .info, slowThreshold: TimeInterval = 0.1) {
        self.output = output
        self.component = component
        self.level = level
        self.slowThreshold = slowThreshold
    }

    public func setLevel(_ level: LogLevel) {
        lock.lock()
        defer { lock.unlock() }
        self.level = level
    }

    public func setSlowThreshold(_ d: TimeInterval) {
        lock.lock()
        defer { lock.unlock() }
        self.slowThreshold = d
    }

    public func withComponent(_ name: String) -> MonoLogger {
        MonoLogger(output: output, component: name, level: level, slowThreshold: slowThreshold)
    }

    private func log(_ lvl: LogLevel, _ msg: String, ctx: [String: String]? = nil, duration: TimeInterval? = nil) {
        lock.lock()
        defer { lock.unlock() }

        guard lvl >= level else { return }

        let entry = LogEntry(
            ts: Date(),
            level: lvl.name,
            component: component,
            msg: msg,
            ctx: ctx,
            durationMs: duration.map { Int64($0 * 1000) }
        )

        if let data = try? JSONEncoder().encode(entry), let str = String(data: data, encoding: .utf8) {
            output.write(str + "\n")
        }
    }

    public func debug(_ msg: String, ctx: [String: String]? = nil) {
        log(.debug, msg, ctx: ctx)
    }

    public func info(_ msg: String, ctx: [String: String]? = nil) {
        log(.info, msg, ctx: ctx)
    }

    public func warn(_ msg: String, ctx: [String: String]? = nil) {
        log(.warn, msg, ctx: ctx)
    }

    public func error(_ msg: String, ctx: [String: String]? = nil) {
        log(.error, msg, ctx: ctx)
    }

    /// Log slow operation if duration exceeds threshold
    public func slowOp(_ msg: String, duration: TimeInterval, ctx: [String: String]? = nil) {
        if duration > slowThreshold {
            log(.warn, msg, ctx: ctx, duration: duration)
        }
    }
}

/// TextOutputStream wrapper for FileHandle
public struct FileHandleOutputStream: TextOutputStream {
    private let handle: FileHandle

    public static let standardOutput = FileHandleOutputStream(handle: .standardOutput)
    public static let standardError = FileHandleOutputStream(handle: .standardError)

    public init(handle: FileHandle) {
        self.handle = handle
    }

    public mutating func write(_ string: String) {
        if let data = string.data(using: .utf8) {
            handle.write(data)
        }
    }
}

