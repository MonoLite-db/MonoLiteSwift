// Created by Yanjunhui

import Foundation

/// 数据库文件头（64 字节）
/// 存储在文件的开头，包含数据库元信息
/// EN: Database file header (64 bytes).
/// Stored at the beginning of the file, contains database metadata.
public struct FileHeader: Sendable {
    /// 文件魔数
    /// EN: File magic number.
    public var magic: UInt32

    /// 文件格式版本
    /// EN: File format version.
    public var version: UInt16

    /// 页面大小
    /// EN: Page size.
    public var pageSize: UInt16

    /// 总页面数
    /// EN: Total page count.
    public var pageCount: UInt32

    /// 空闲页链表头
    /// EN: Free list head.
    public var freeListHead: PageID

    /// 元数据页 ID
    /// EN: Metadata page ID.
    public var metaPageId: PageID

    /// 目录页 ID
    /// EN: Catalog page ID.
    public var catalogPageId: PageID

    /// 创建时间（Unix 毫秒）
    /// EN: Creation time (Unix milliseconds).
    public var createTime: Int64

    /// 修改时间（Unix 毫秒）
    /// EN: Modification time (Unix milliseconds).
    public var modifyTime: Int64

    /// 预留字段（24 字节）
    /// EN: Reserved fields (24 bytes).
    private var reserved: (UInt64, UInt64, UInt64)

    // MARK: - 初始化 / Initialization

    /// 创建新的文件头
    /// EN: Create a new file header.
    public init() {
        self.magic = StorageConstants.fileMagic
        self.version = StorageConstants.fileVersion
        self.pageSize = UInt16(StorageConstants.pageSize)
        self.pageCount = 1  // 初始只有一个页面（元数据页） / EN: Initially only one page (metadata page)
        self.freeListHead = StorageConstants.invalidPageId
        self.metaPageId = 0
        self.catalogPageId = StorageConstants.invalidPageId
        let now = Int64(Date().timeIntervalSince1970 * 1000)
        self.createTime = now
        self.modifyTime = now
        self.reserved = (0, 0, 0)
    }

    // MARK: - 序列化 / Serialization

    /// 序列化为 Data（64 字节）
    /// EN: Serialize to Data (64 bytes).
    public func marshal() -> Data {
        var data = Data(count: StorageConstants.fileHeaderSize)

        DataEndian.writeUInt32LE(magic, to: &data, at: 0)
        DataEndian.writeUInt16LE(version, to: &data, at: 4)
        DataEndian.writeUInt16LE(pageSize, to: &data, at: 6)
        DataEndian.writeUInt32LE(pageCount, to: &data, at: 8)
        DataEndian.writeUInt32LE(freeListHead, to: &data, at: 12)
        DataEndian.writeUInt32LE(metaPageId, to: &data, at: 16)
        DataEndian.writeUInt32LE(catalogPageId, to: &data, at: 20)
        DataEndian.writeInt64LE(createTime, to: &data, at: 24)
        DataEndian.writeInt64LE(modifyTime, to: &data, at: 32)

        return data
    }

    /// 从 Data 反序列化
    /// EN: Deserialize from Data.
    public static func unmarshal(_ data: Data) throws -> FileHeader {
        guard data.count >= StorageConstants.fileHeaderSize else {
            throw StorageError.fileCorrupted("File header too short: \(data.count) bytes")
        }

        var header = FileHeader()

        header.magic = DataEndian.readUInt32LE(data, at: 0)
        header.version = DataEndian.readUInt16LE(data, at: 4)
        header.pageSize = DataEndian.readUInt16LE(data, at: 6)
        header.pageCount = DataEndian.readUInt32LE(data, at: 8)
        header.freeListHead = DataEndian.readUInt32LE(data, at: 12)
        header.metaPageId = DataEndian.readUInt32LE(data, at: 16)
        header.catalogPageId = DataEndian.readUInt32LE(data, at: 20)
        header.createTime = DataEndian.readInt64LE(data, at: 24)
        header.modifyTime = DataEndian.readInt64LE(data, at: 32)

        return header
    }

    // MARK: - 验证 / Validation

    /// 验证文件头
    /// EN: Validate file header.
    public func validate() throws {
        guard magic == StorageConstants.fileMagic else {
            throw StorageError.invalidMagic
        }

        guard version == StorageConstants.fileVersion else {
            throw StorageError.invalidVersion(version)
        }

        guard pageSize == UInt16(StorageConstants.pageSize) else {
            throw StorageError.fileCorrupted("Invalid page size: \(pageSize)")
        }
    }

    // MARK: - 修改 / Modification

    /// 更新修改时间
    /// EN: Update modification time.
    public mutating func touch() {
        modifyTime = Int64(Date().timeIntervalSince1970 * 1000)
    }
}
