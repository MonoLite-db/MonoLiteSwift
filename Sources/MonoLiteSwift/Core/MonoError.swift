// Created by Yanjunhui

import Foundation

/// MonoLite 错误类型（MongoDB 兼容）
/// EN: MonoLite error type (MongoDB compatible)
public struct MonoError: Error, Sendable, CustomStringConvertible {
    /// 错误码
    /// EN: Error code
    public let code: ErrorCode

    /// 错误消息
    /// EN: Error message
    public let message: String

    /// 错误码名称
    /// EN: Error code name
    public var codeName: String {
        code.name
    }

    /// 错误码数值
    /// EN: Error code value
    public var codeValue: Int {
        code.rawValue
    }

    /// 错误描述
    /// EN: Error description
    public var description: String {
        "\(codeName) (\(codeValue)): \(message)"
    }

    /// 初始化错误
    /// EN: Initialize error
    public init(code: ErrorCode, message: String) {
        self.code = code
        self.message = message
    }
}

// MARK: - 常用错误构造函数 / Common Error Constructors

extension MonoError {
    /// 内部错误
    /// EN: Internal error
    public static func internalError(_ message: String) -> MonoError {
        MonoError(code: .internalError, message: message)
    }

    /// 参数值错误
    /// EN: Bad value error
    public static func badValue(_ message: String) -> MonoError {
        MonoError(code: .badValue, message: message)
    }

    /// 协议错误（Wire Protocol）
    /// EN: Protocol error (Wire Protocol)
    public static func protocolError(_ message: String) -> MonoError {
        MonoError(code: .protocolError, message: message)
    }

    /// 类型不匹配
    /// EN: Type mismatch
    public static func typeMismatch(_ message: String) -> MonoError {
        MonoError(code: .typeMismatch, message: message)
    }

    /// 命名空间不存在
    /// EN: Namespace not found
    public static func namespaceNotFound(_ namespace: String) -> MonoError {
        MonoError(code: .namespaceNotFound, message: "ns not found: \(namespace)")
    }

    /// 游标不存在
    /// EN: Cursor not found
    public static func cursorNotFound(_ cursorId: Int64) -> MonoError {
        MonoError(code: .cursorNotFound, message: "cursor id \(cursorId) not found")
    }

    /// 重复键错误
    /// EN: Duplicate key error
    public static func duplicateKey(keyPattern: String, keyValue: Any) -> MonoError {
        MonoError(
            code: .duplicateKey,
            message: "E11000 duplicate key error collection: \(keyPattern) dup key: \(keyValue)"
        )
    }

    /// 索引不存在
    /// EN: Index not found
    public static func indexNotFound(_ indexName: String) -> MonoError {
        MonoError(code: .indexNotFound, message: "index not found with name [\(indexName)]")
    }

    /// 命令不存在
    /// EN: Command not found
    public static func commandNotFound(_ cmdName: String) -> MonoError {
        MonoError(code: .commandNotFound, message: "no such command: '\(cmdName)'")
    }

    /// 无效命名空间
    /// EN: Invalid namespace
    public static func invalidNamespace(_ namespace: String) -> MonoError {
        MonoError(code: .invalidNamespace, message: "Invalid namespace specified '\(namespace)'")
    }

    /// 文档过大
    /// EN: Document too large
    public static func documentTooLarge(size: Int, maxSize: Int) -> MonoError {
        MonoError(
            code: .documentTooLarge,
            message: "document is too large: \(size) bytes, max size is \(maxSize) bytes"
        )
    }

    /// 解析失败
    /// EN: Failed to parse
    public static func failedToParse(_ message: String) -> MonoError {
        MonoError(code: .failedToParse, message: message)
    }

    /// 无效选项
    /// EN: Invalid options
    public static func invalidOptions(_ message: String) -> MonoError {
        MonoError(code: .invalidOptions, message: message)
    }

    /// 写关注失败
    /// EN: Write concern failed
    public static func writeConcernFailed(_ message: String) -> MonoError {
        MonoError(code: .writeConcernFailed, message: message)
    }

    /// 操作失败（Go 对齐：lock acquisition timeout 等）
    /// EN: Operation failed (Go parity: lock acquisition timeout, etc.)
    public static func operationFailed(_ message: String) -> MonoError {
        MonoError(code: .operationFailed, message: message)
    }

    /// 非法操作
    /// EN: Illegal operation
    public static func illegalOperation(_ message: String) -> MonoError {
        MonoError(code: .illegalOperation, message: message)
    }

    /// 无效的 _id 字段
    /// EN: Invalid _id field
    public static func invalidIdField(_ message: String) -> MonoError {
        MonoError(code: .invalidIdField, message: message)
    }

    /// 无法创建索引
    /// EN: Cannot create index
    public static func cannotCreateIndex(_ message: String) -> MonoError {
        MonoError(code: .cannotCreateIndex, message: message)
    }

    /// 空字段名
    /// EN: Empty field name
    public static func emptyFieldName() -> MonoError {
        MonoError(code: .emptyFieldName, message: "field name cannot be empty")
    }

    /// $ 前缀字段名错误
    /// EN: Dollar-prefixed field name error
    public static func dollarPrefixedFieldName(_ name: String) -> MonoError {
        MonoError(
            code: .dollarPrefixedFieldName,
            message: "field name '\(name)' cannot start with '$' in stored documents"
        )
    }

    /// 无效 BSON
    /// EN: Invalid BSON
    public static func invalidBSON(_ message: String) -> MonoError {
        MonoError(code: .invalidBSON, message: message)
    }

    /// 事务不存在
    /// EN: No such transaction
    public static func noSuchTransaction() -> MonoError {
        MonoError(code: .noSuchTransaction, message: "no such transaction")
    }

    /// 事务已提交
    /// EN: Transaction already committed
    public static func transactionCommitted() -> MonoError {
        MonoError(code: .transactionCommitted, message: "transaction already committed")
    }

    /// 事务已中止
    /// EN: Transaction already aborted
    public static func transactionAborted() -> MonoError {
        MonoError(code: .transactionAborted, message: "transaction aborted")
    }

    /// 会话不存在
    /// EN: No such session
    public static func noSuchSession() -> MonoError {
        MonoError(code: .noSuchSession, message: "no such session")
    }

    /// 事务序号过旧
    /// EN: Transaction number too old
    public static func transactionTooOld() -> MonoError {
        MonoError(code: .transactionTooOld, message: "txnNumber is too old")
    }
}

// MARK: - 存储层错误 / Storage Layer Errors

/// 存储层错误
/// EN: Storage layer errors
public enum StorageError: Error, Sendable {
    /// 文件未找到 / EN: File not found
    case fileNotFound(String)
    /// 文件未打开 / EN: File not open
    case fileNotOpen(String)
    /// 文件损坏 / EN: File corrupted
    case fileCorrupted(String)
    /// 无效的魔数 / EN: Invalid magic number
    case invalidMagic
    /// 无效的版本 / EN: Invalid version
    case invalidVersion(UInt16)
    /// 页面未找到 / EN: Page not found
    case pageNotFound(UInt32)
    /// 页面损坏 / EN: Page corrupted
    case pageCorrupted(UInt32)
    /// WAL 损坏 / EN: WAL corrupted
    case walCorrupted(String)
    /// IO 错误 / EN: IO error
    case ioError(String)
    /// 空间不足 / EN: Out of space
    case outOfSpace
    /// 校验和不匹配 / EN: Checksum mismatch
    case checksumMismatch
}

// MARK: - BSON 错误 / BSON Errors

/// BSON 编解码错误
/// EN: BSON encoding/decoding errors
public enum BSONError: Error, Sendable {
    /// 无效文档 / EN: Invalid document
    case invalidDocument(String)
    /// 无效类型 / EN: Invalid type
    case invalidType(UInt8)
    /// 无效 ObjectId / EN: Invalid ObjectId
    case invalidObjectId(String)
    /// 无效 UTF-8 / EN: Invalid UTF-8
    case invalidUtf8
    /// 意外的数据结束 / EN: Unexpected end of data
    case unexpectedEndOfData
    /// 文档过大 / EN: Document too large
    case documentTooLarge(Int)
    /// 嵌套层级过深 / EN: Nesting too deep
    case nestingTooDeep(Int)
    /// 无效字段名 / EN: Invalid field name
    case invalidFieldName(String)
}

// MARK: - 事务错误 / Transaction Errors

/// 事务错误
/// EN: Transaction errors
public enum TransactionError: Error, Sendable {
    /// 死锁 / EN: Deadlock
    case deadlock
    /// 锁超时 / EN: Lock timeout
    case lockTimeout
    /// 事务未激活 / EN: Transaction not active
    case transactionNotActive
    /// 事务已提交 / EN: Transaction already committed
    case transactionAlreadyCommitted
    /// 事务已中止 / EN: Transaction already aborted
    case transactionAlreadyAborted
    /// 无效状态 / EN: Invalid state
    case invalidState(String)
}
