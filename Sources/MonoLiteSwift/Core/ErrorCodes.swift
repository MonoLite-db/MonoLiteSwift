// Created by Yanjunhui

import Foundation

/// MongoDB 兼容错误码定义
/// EN: MongoDB-compatible error code definitions
/// 参考 / Reference: https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml
public enum ErrorCode: Int, Sendable {
    // MARK: - 通用错误 (1-99) / General Errors (1-99)
    case ok = 0
    case internalError = 1
    case badValue = 2
    case noSuchKey = 4
    case graphContainsCycle = 5
    case hostUnreachable = 6
    case hostNotFound = 7
    case unknownError = 8
    case failedToParse = 9
    case cannotMutateObject = 10
    case userNotFound = 11
    case unsupportedFormat = 12
    case unauthorized = 13
    case typeMismatch = 14
    case overflow = 15
    case invalidLength = 16
    case protocolError = 17
    case authenticationFailed = 18
    case cannotReuseObject = 19
    case illegalOperation = 20
    case emptyArrayOperation = 21
    case invalidBSON = 22

    // MARK: - 命令错误 (26-50) / Command Errors (26-50)
    case namespaceNotFound = 26
    case indexNotFound = 27
    case pathNotViable = 28
    case nonExistentPath = 29
    case invalidPath = 30
    case roleNotFound = 31
    case rolesNotRelated = 32
    case privilegeNotFound = 33
    case cannotBackfillArray = 34
    case userModificationFailed = 35
    case remoteChangeDetected = 36
    case fileRenameFailed = 37
    case fileNotOpen = 38
    case fileStreamFailed = 39
    case conflictingUpdateOperators = 40
    case fileAlreadyOpen = 41
    case logWriteFailed = 42
    case cursorNotFound = 43

    // MARK: - 查询和操作错误 (52-70) / Query and Operation Errors (52-70)
    case dollarPrefixedFieldName = 52
    case invalidIdField = 53
    case notSingleValueField = 54
    case invalidDBRef = 55
    case emptyFieldName = 56
    case dottedFieldName = 57
    case roleDataInconsistent = 58
    case commandNotFound = 59
    case noProgressMade = 60
    case remoteResultsUnavailable = 61
    case writeConcernFailed = 64
    case multipleErrorsOccurred = 65
    case cannotCreateIndex = 67
    case indexBuildAborted = 71
    case invalidOptions = 72
    case invalidNamespace = 73
    case indexOptionsConflict = 85
    case indexKeySpecsConflict = 86
    case networkTimeout = 89
    case operationFailed = 96

    // MARK: - 事务/MVCC 错误 / Transaction/MVCC Errors
    case noSuchSession = 206
    case transactionTooOld = 225
    case noSuchTransaction = 251
    case transactionCommitted = 256
    case transactionAborted = 263

    // MARK: - 写错误 / Write Errors
    case duplicateKey = 11000
    case notWritablePrimary = 10107

    // MARK: - 文档相关 / Document Related
    case documentTooLarge = 17419
    case documentValidationFailure = 121

    /// 错误码名称
    /// EN: Error code name
    public var name: String {
        switch self {
        case .ok: return "OK"
        case .internalError: return "InternalError"
        case .badValue: return "BadValue"
        case .noSuchKey: return "NoSuchKey"
        case .graphContainsCycle: return "GraphContainsCycle"
        case .hostUnreachable: return "HostUnreachable"
        case .hostNotFound: return "HostNotFound"
        case .unknownError: return "UnknownError"
        case .failedToParse: return "FailedToParse"
        case .cannotMutateObject: return "CannotMutateObject"
        case .userNotFound: return "UserNotFound"
        case .unsupportedFormat: return "UnsupportedFormat"
        case .unauthorized: return "Unauthorized"
        case .typeMismatch: return "TypeMismatch"
        case .overflow: return "Overflow"
        case .invalidLength: return "InvalidLength"
        case .protocolError: return "ProtocolError"
        case .authenticationFailed: return "AuthenticationFailed"
        case .cannotReuseObject: return "CannotReuseObject"
        case .illegalOperation: return "IllegalOperation"
        case .emptyArrayOperation: return "EmptyArrayOperation"
        case .invalidBSON: return "InvalidBSON"
        case .namespaceNotFound: return "NamespaceNotFound"
        case .indexNotFound: return "IndexNotFound"
        case .pathNotViable: return "PathNotViable"
        case .nonExistentPath: return "NonExistentPath"
        case .invalidPath: return "InvalidPath"
        case .roleNotFound: return "RoleNotFound"
        case .rolesNotRelated: return "RolesNotRelated"
        case .privilegeNotFound: return "PrivilegeNotFound"
        case .cannotBackfillArray: return "CannotBackfillArray"
        case .userModificationFailed: return "UserModificationFailed"
        case .remoteChangeDetected: return "RemoteChangeDetected"
        case .fileRenameFailed: return "FileRenameFailed"
        case .fileNotOpen: return "FileNotOpen"
        case .fileStreamFailed: return "FileStreamFailed"
        case .conflictingUpdateOperators: return "ConflictingUpdateOperators"
        case .fileAlreadyOpen: return "FileAlreadyOpen"
        case .logWriteFailed: return "LogWriteFailed"
        case .cursorNotFound: return "CursorNotFound"
        case .dollarPrefixedFieldName: return "DollarPrefixedFieldName"
        case .invalidIdField: return "InvalidIdField"
        case .notSingleValueField: return "NotSingleValueField"
        case .invalidDBRef: return "InvalidDBRef"
        case .emptyFieldName: return "EmptyFieldName"
        case .dottedFieldName: return "DottedFieldName"
        case .roleDataInconsistent: return "RoleDataInconsistent"
        case .commandNotFound: return "CommandNotFound"
        case .noProgressMade: return "NoProgressMade"
        case .remoteResultsUnavailable: return "RemoteResultsUnavailable"
        case .writeConcernFailed: return "WriteConcernFailed"
        case .multipleErrorsOccurred: return "MultipleErrorsOccurred"
        case .cannotCreateIndex: return "CannotCreateIndex"
        case .indexBuildAborted: return "IndexBuildAborted"
        case .invalidOptions: return "InvalidOptions"
        case .invalidNamespace: return "InvalidNamespace"
        case .indexOptionsConflict: return "IndexOptionsConflict"
        case .indexKeySpecsConflict: return "IndexKeySpecsConflict"
        case .networkTimeout: return "NetworkTimeout"
        case .operationFailed: return "OperationFailed"
        case .noSuchSession: return "NoSuchSession"
        case .transactionTooOld: return "TransactionTooOld"
        case .noSuchTransaction: return "NoSuchTransaction"
        case .transactionCommitted: return "TransactionCommitted"
        case .transactionAborted: return "TransactionAborted"
        case .duplicateKey: return "DuplicateKey"
        case .notWritablePrimary: return "NotWritablePrimary"
        case .documentTooLarge: return "DocumentTooLarge"
        case .documentValidationFailure: return "DocumentValidationFailure"
        }
    }
}
