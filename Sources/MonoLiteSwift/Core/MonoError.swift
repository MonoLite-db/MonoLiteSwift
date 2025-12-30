// Created by Yanjunhui

import Foundation

/// MonoLite 错误类型（MongoDB 兼容）
public struct MonoError: Error, Sendable, CustomStringConvertible {
    /// 错误码
    public let code: ErrorCode

    /// 错误消息
    public let message: String

    /// 错误码名称
    public var codeName: String {
        code.name
    }

    /// 错误码数值
    public var codeValue: Int {
        code.rawValue
    }

    public var description: String {
        "\(codeName) (\(codeValue)): \(message)"
    }

    public init(code: ErrorCode, message: String) {
        self.code = code
        self.message = message
    }
}

// MARK: - 常用错误构造函数

extension MonoError {
    /// 内部错误
    public static func internalError(_ message: String) -> MonoError {
        MonoError(code: .internalError, message: message)
    }

    /// 参数值错误
    public static func badValue(_ message: String) -> MonoError {
        MonoError(code: .badValue, message: message)
    }

    /// 协议错误（Wire Protocol）
    public static func protocolError(_ message: String) -> MonoError {
        MonoError(code: .protocolError, message: message)
    }

    /// 类型不匹配
    public static func typeMismatch(_ message: String) -> MonoError {
        MonoError(code: .typeMismatch, message: message)
    }

    /// 命名空间不存在
    public static func namespaceNotFound(_ namespace: String) -> MonoError {
        MonoError(code: .namespaceNotFound, message: "ns not found: \(namespace)")
    }

    /// 游标不存在
    public static func cursorNotFound(_ cursorId: Int64) -> MonoError {
        MonoError(code: .cursorNotFound, message: "cursor id \(cursorId) not found")
    }

    /// 重复键错误
    public static func duplicateKey(keyPattern: String, keyValue: Any) -> MonoError {
        MonoError(
            code: .duplicateKey,
            message: "E11000 duplicate key error collection: \(keyPattern) dup key: \(keyValue)"
        )
    }

    /// 索引不存在
    public static func indexNotFound(_ indexName: String) -> MonoError {
        MonoError(code: .indexNotFound, message: "index not found with name [\(indexName)]")
    }

    /// 命令不存在
    public static func commandNotFound(_ cmdName: String) -> MonoError {
        MonoError(code: .commandNotFound, message: "no such command: '\(cmdName)'")
    }

    /// 无效命名空间
    public static func invalidNamespace(_ namespace: String) -> MonoError {
        MonoError(code: .invalidNamespace, message: "Invalid namespace specified '\(namespace)'")
    }

    /// 文档过大
    public static func documentTooLarge(size: Int, maxSize: Int) -> MonoError {
        MonoError(
            code: .documentTooLarge,
            message: "document is too large: \(size) bytes, max size is \(maxSize) bytes"
        )
    }

    /// 解析失败
    public static func failedToParse(_ message: String) -> MonoError {
        MonoError(code: .failedToParse, message: message)
    }

    /// 无效选项
    public static func invalidOptions(_ message: String) -> MonoError {
        MonoError(code: .invalidOptions, message: message)
    }

    /// 写关注失败
    public static func writeConcernFailed(_ message: String) -> MonoError {
        MonoError(code: .writeConcernFailed, message: message)
    }

    /// 操作失败（Go 对齐：lock acquisition timeout 等）
    public static func operationFailed(_ message: String) -> MonoError {
        MonoError(code: .operationFailed, message: message)
    }

    /// 非法操作
    public static func illegalOperation(_ message: String) -> MonoError {
        MonoError(code: .illegalOperation, message: message)
    }

    /// 无效的 _id 字段
    public static func invalidIdField(_ message: String) -> MonoError {
        MonoError(code: .invalidIdField, message: message)
    }

    /// 无法创建索引
    public static func cannotCreateIndex(_ message: String) -> MonoError {
        MonoError(code: .cannotCreateIndex, message: message)
    }

    /// 空字段名
    public static func emptyFieldName() -> MonoError {
        MonoError(code: .emptyFieldName, message: "field name cannot be empty")
    }

    /// $ 前缀字段名错误
    public static func dollarPrefixedFieldName(_ name: String) -> MonoError {
        MonoError(
            code: .dollarPrefixedFieldName,
            message: "field name '\(name)' cannot start with '$' in stored documents"
        )
    }

    /// 无效 BSON
    public static func invalidBSON(_ message: String) -> MonoError {
        MonoError(code: .invalidBSON, message: message)
    }

    /// 事务不存在
    public static func noSuchTransaction() -> MonoError {
        MonoError(code: .noSuchTransaction, message: "no such transaction")
    }

    /// 事务已提交
    public static func transactionCommitted() -> MonoError {
        MonoError(code: .transactionCommitted, message: "transaction already committed")
    }

    /// 事务已中止
    public static func transactionAborted() -> MonoError {
        MonoError(code: .transactionAborted, message: "transaction aborted")
    }

    /// 会话不存在
    public static func noSuchSession() -> MonoError {
        MonoError(code: .noSuchSession, message: "no such session")
    }

    /// 事务序号过旧
    public static func transactionTooOld() -> MonoError {
        MonoError(code: .transactionTooOld, message: "txnNumber is too old")
    }
}

// MARK: - 存储层错误

/// 存储层错误
public enum StorageError: Error, Sendable {
    case fileNotFound(String)
    case fileNotOpen(String)
    case fileCorrupted(String)
    case invalidMagic
    case invalidVersion(UInt16)
    case pageNotFound(UInt32)
    case pageCorrupted(UInt32)
    case walCorrupted(String)
    case ioError(String)
    case outOfSpace
    case checksumMismatch
}

// MARK: - BSON 错误

/// BSON 编解码错误
public enum BSONError: Error, Sendable {
    case invalidDocument(String)
    case invalidType(UInt8)
    case invalidObjectId(String)
    case invalidUtf8
    case unexpectedEndOfData
    case documentTooLarge(Int)
    case nestingTooDeep(Int)
    case invalidFieldName(String)
}

// MARK: - 事务错误

/// 事务错误
public enum TransactionError: Error, Sendable {
    case deadlock
    case lockTimeout
    case transactionNotActive
    case transactionAlreadyCommitted
    case transactionAlreadyAborted
    case invalidState(String)
}
