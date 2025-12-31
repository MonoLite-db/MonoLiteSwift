// Created by Yanjunhui

import Foundation

/// 日志级别（Go 对齐：engine/logger.go）
/// EN: Log levels (Go parity: engine/logger.go)
public enum LogLevel: Int, Sendable, Comparable {
    case debug = 0
    case info = 1
    case warn = 2
    case error = 3

    /// 日志级别名称
    /// EN: Log level name
    public var name: String {
        switch self {
        case .debug: return "DEBUG"
        case .info: return "INFO"
        case .warn: return "WARN"
        case .error: return "ERROR"
        }
    }

    /// 比较日志级别
    /// EN: Compare log levels
    public static func < (lhs: LogLevel, rhs: LogLevel) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

/// 结构化日志条目（Go 对齐：LogEntry）
/// EN: Structured log entry (Go parity: LogEntry)
public struct LogEntry: Sendable, Encodable {
    /// 时间戳 / EN: Timestamp
    public let ts: Date
    /// 日志级别 / EN: Log level
    public let level: String
    /// 组件名 / EN: Component name
    public let component: String?
    /// 日志消息 / EN: Log message
    public let msg: String
    /// 上下文信息 / EN: Context information
    public let ctx: [String: String]?
    /// 操作耗时（毫秒）/ EN: Operation duration (milliseconds)
    public let durationMs: Int64?
}

/// 结构化 JSON 日志记录器（Go 对齐：Logger）
/// EN: Structured JSON logger (Go parity: Logger)
public final class MonoLogger: @unchecked Sendable {
    /// 线程锁 / EN: Thread lock
    private let lock = NSLock()
    /// 输出流 / EN: Output stream
    private var output: TextOutputStream
    /// 当前日志级别 / EN: Current log level
    private var level: LogLevel
    /// 组件名称 / EN: Component name
    private let component: String
    /// 慢操作阈值 / EN: Slow operation threshold
    private var slowThreshold: TimeInterval

    /// 默认日志记录器实例
    /// EN: Default logger instance
    public static let `default` = MonoLogger(output: FileHandleOutputStream.standardOutput)

    /// 初始化日志记录器
    /// EN: Initialize logger
    /// - Parameters:
    ///   - output: 输出流 / EN: Output stream
    ///   - component: 组件名称 / EN: Component name
    ///   - level: 日志级别 / EN: Log level
    ///   - slowThreshold: 慢操作阈值（秒）/ EN: Slow operation threshold (seconds)
    public init(output: TextOutputStream, component: String = "MONODB", level: LogLevel = .info, slowThreshold: TimeInterval = 0.1) {
        self.output = output
        self.component = component
        self.level = level
        self.slowThreshold = slowThreshold
    }

    /// 设置日志级别
    /// EN: Set log level
    public func setLevel(_ level: LogLevel) {
        lock.lock()
        defer { lock.unlock() }
        self.level = level
    }

    /// 设置慢操作阈值
    /// EN: Set slow operation threshold
    public func setSlowThreshold(_ d: TimeInterval) {
        lock.lock()
        defer { lock.unlock() }
        self.slowThreshold = d
    }

    /// 创建子组件日志记录器
    /// EN: Create sub-component logger
    public func withComponent(_ name: String) -> MonoLogger {
        MonoLogger(output: output, component: name, level: level, slowThreshold: slowThreshold)
    }

    /// 内部日志记录方法
    /// EN: Internal log method
    private func log(_ lvl: LogLevel, _ msg: String, ctx: [String: String]? = nil, duration: TimeInterval? = nil) {
        lock.lock()
        defer { lock.unlock() }

        // 检查日志级别 / EN: Check log level
        guard lvl >= level else { return }

        let entry = LogEntry(
            ts: Date(),
            level: lvl.name,
            component: component,
            msg: msg,
            ctx: ctx,
            durationMs: duration.map { Int64($0 * 1000) }
        )

        // 编码并输出 JSON / EN: Encode and output JSON
        if let data = try? JSONEncoder().encode(entry), let str = String(data: data, encoding: .utf8) {
            output.write(str + "\n")
        }
    }

    /// 记录调试日志
    /// EN: Log debug message
    public func debug(_ msg: String, ctx: [String: String]? = nil) {
        log(.debug, msg, ctx: ctx)
    }

    /// 记录信息日志
    /// EN: Log info message
    public func info(_ msg: String, ctx: [String: String]? = nil) {
        log(.info, msg, ctx: ctx)
    }

    /// 记录警告日志
    /// EN: Log warning message
    public func warn(_ msg: String, ctx: [String: String]? = nil) {
        log(.warn, msg, ctx: ctx)
    }

    /// 记录错误日志
    /// EN: Log error message
    public func error(_ msg: String, ctx: [String: String]? = nil) {
        log(.error, msg, ctx: ctx)
    }

    /// 记录慢操作（如果耗时超过阈值）
    /// EN: Log slow operation if duration exceeds threshold
    public func slowOp(_ msg: String, duration: TimeInterval, ctx: [String: String]? = nil) {
        if duration > slowThreshold {
            log(.warn, msg, ctx: ctx, duration: duration)
        }
    }
}

/// FileHandle 的 TextOutputStream 包装器
/// EN: TextOutputStream wrapper for FileHandle
public struct FileHandleOutputStream: TextOutputStream {
    /// 文件句柄 / EN: File handle
    private let handle: FileHandle

    /// 标准输出流
    /// EN: Standard output stream
    public static let standardOutput = FileHandleOutputStream(handle: .standardOutput)

    /// 标准错误流
    /// EN: Standard error stream
    public static let standardError = FileHandleOutputStream(handle: .standardError)

    /// 初始化输出流
    /// EN: Initialize output stream
    public init(handle: FileHandle) {
        self.handle = handle
    }

    /// 写入字符串
    /// EN: Write string
    public mutating func write(_ string: String) {
        if let data = string.data(using: .utf8) {
            handle.write(data)
        }
    }
}
