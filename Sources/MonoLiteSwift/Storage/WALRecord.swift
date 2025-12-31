// Created by Yanjunhui

import Foundation

/// WAL 记录类型
/// EN: WAL record type.
public enum WALRecordType: UInt8, Sendable {
    /// 页面写入（完整页面数据）
    /// EN: Page write (complete page data).
    case pageWrite = 1

    /// 分配页面
    /// EN: Allocate page.
    case allocPage = 2

    /// 释放页面
    /// EN: Free page.
    case freePage = 3

    /// 事务提交
    /// EN: Transaction commit.
    case commit = 4

    /// 检查点
    /// EN: Checkpoint.
    case checkpoint = 5

    /// 元数据更新
    /// EN: Metadata update.
    case metaUpdate = 6
}

/// 元数据更新类型
/// EN: Metadata update type.
public enum MetaUpdateType: UInt8, Sendable {
    /// 空闲列表头
    /// EN: Free list head.
    case freeListHead = 1

    /// 页面计数
    /// EN: Page count.
    case pageCount = 2

    /// 目录页 ID
    /// EN: Catalog page ID.
    case catalogPageId = 3
}

/// WAL 记录头大小：20 字节
/// LSN(8) + Type(1) + Flags(1) + DataLen(2) + PageId(4) + Checksum(4)
/// EN: WAL record header size: 20 bytes.
/// LSN(8) + Type(1) + Flags(1) + DataLen(2) + PageId(4) + Checksum(4)
public let walRecordHeaderSize = 20

/// WAL 记录
/// EN: WAL record.
public struct WALRecord: Sendable {
    /// 日志序列号
    /// EN: Log sequence number.
    public var lsn: LSN

    /// 记录类型
    /// EN: Record type.
    public var type: WALRecordType

    /// 标志位
    /// EN: Flags.
    public var flags: UInt8

    /// 数据长度
    /// EN: Data length.
    public var dataLen: UInt16

    /// 页面 ID（对于页面操作）
    /// EN: Page ID (for page operations).
    public var pageId: PageID

    /// 校验和
    /// EN: Checksum.
    public var checksum: UInt32

    /// 数据负载
    /// EN: Data payload.
    public var data: Data

    // MARK: - 初始化 / Initialization

    public init(
        lsn: LSN,
        type: WALRecordType,
        pageId: PageID = StorageConstants.invalidPageId,
        data: Data = Data()
    ) {
        self.lsn = lsn
        self.type = type
        self.flags = 0
        self.dataLen = UInt16(data.count)
        self.pageId = pageId
        self.data = data
        self.checksum = 0
        self.checksum = calculateChecksum()
    }

    // MARK: - 序列化 / Serialization

    /// 序列化为 Data
    /// EN: Serialize to Data.
    public func marshal() -> Data {
        var buffer = Data(capacity: walRecordHeaderSize + Int(dataLen))

        buffer.append(contentsOf: withUnsafeBytes(of: lsn.littleEndian) { Data($0) })
        buffer.append(type.rawValue)
        buffer.append(flags)
        buffer.append(contentsOf: withUnsafeBytes(of: dataLen.littleEndian) { Data($0) })
        buffer.append(contentsOf: withUnsafeBytes(of: pageId.littleEndian) { Data($0) })
        buffer.append(contentsOf: withUnsafeBytes(of: checksum.littleEndian) { Data($0) })
        buffer.append(data)

        // 对齐到 8 字节
        // EN: Align to 8 bytes
        let padding = (StorageConstants.walRecordAlign - (buffer.count % StorageConstants.walRecordAlign))
            % StorageConstants.walRecordAlign
        if padding > 0 {
            buffer.append(Data(count: padding))
        }

        return buffer
    }

    /// 从 Data 反序列化
    /// EN: Deserialize from Data.
    public static func unmarshal(_ buffer: Data, at offset: Int = 0) throws -> WALRecord {
        guard buffer.count >= offset + walRecordHeaderSize else {
            throw StorageError.walCorrupted("Record too short")
        }

        var record = WALRecord(lsn: 0, type: .pageWrite)
        let base = offset
        record.lsn = DataEndian.readUInt64LE(buffer, at: base)
        record.type = WALRecordType(rawValue: buffer[base + 8]) ?? .pageWrite
        record.flags = buffer[base + 9]
        record.dataLen = DataEndian.readUInt16LE(buffer, at: base + 10)
        record.pageId = DataEndian.readUInt32LE(buffer, at: base + 12)
        record.checksum = DataEndian.readUInt32LE(buffer, at: base + 16)

        // 读取数据
        // EN: Read data
        let dataStart = offset + walRecordHeaderSize
        let dataEnd = dataStart + Int(record.dataLen)
        guard buffer.count >= dataEnd else {
            throw StorageError.walCorrupted("Data truncated")
        }
        record.data = Data(buffer[dataStart..<dataEnd])

        // 验证校验和
        // EN: Verify checksum
        let expectedChecksum = record.calculateChecksum()
        guard record.checksum == expectedChecksum else {
            throw StorageError.checksumMismatch
        }

        return record
    }

    // MARK: - 校验和 / Checksum

    /// 计算校验和（CRC32 of header[0:16] + data）
    /// EN: Calculate checksum (CRC32 of header[0:16] + data).
    public func calculateChecksum() -> UInt32 {
        var buffer = Data(capacity: 16 + data.count)

        // 头部前 16 字节（不包括 checksum）
        // EN: First 16 bytes of header (excluding checksum)
        buffer.append(contentsOf: withUnsafeBytes(of: lsn.littleEndian) { Data($0) })
        buffer.append(type.rawValue)
        buffer.append(flags)
        buffer.append(contentsOf: withUnsafeBytes(of: dataLen.littleEndian) { Data($0) })
        buffer.append(contentsOf: withUnsafeBytes(of: pageId.littleEndian) { Data($0) })

        // 数据
        // EN: Data
        buffer.append(data)

        return crc32(buffer)
    }

    /// 记录总大小（包括对齐）
    /// EN: Total record size (including alignment).
    public var totalSize: Int {
        let size = walRecordHeaderSize + Int(dataLen)
        let padding = (StorageConstants.walRecordAlign - (size % StorageConstants.walRecordAlign))
            % StorageConstants.walRecordAlign
        return size + padding
    }
}

// MARK: - CRC32 计算 / CRC32 Calculation

/// 计算 CRC32（使用 IEEE 多项式）
/// EN: Calculate CRC32 (using IEEE polynomial).
private func crc32(_ data: Data) -> UInt32 {
    var crc: UInt32 = 0xFFFFFFFF

    for byte in data {
        crc ^= UInt32(byte)
        for _ in 0..<8 {
            crc = (crc >> 1) ^ (crc & 1 != 0 ? 0xEDB88320 : 0)
        }
    }

    return ~crc
}

// MARK: - WAL 头 / WAL Header

/// WAL 文件头（32 字节）
/// EN: WAL file header (32 bytes).
public struct WALHeader: Sendable {
    /// 魔数
    /// EN: Magic number.
    public var magic: UInt32

    /// 版本
    /// EN: Version.
    public var version: UInt16

    /// 保留字段
    /// EN: Reserved field.
    public var reserved1: UInt16

    /// 检查点 LSN
    /// EN: Checkpoint LSN.
    public var checkpointLSN: LSN

    /// 文件大小
    /// EN: File size.
    public var fileSize: UInt64

    /// 校验和
    /// EN: Checksum.
    public var checksum: UInt32

    /// 保留字段
    /// EN: Reserved field.
    public var reserved2: UInt32

    // MARK: - 初始化 / Initialization

    public init() {
        self.magic = StorageConstants.walMagic
        self.version = 1
        self.reserved1 = 0
        self.checkpointLSN = 0
        self.fileSize = UInt64(StorageConstants.walHeaderSize)
        self.checksum = 0
        self.reserved2 = 0
    }

    // MARK: - 序列化 / Serialization

    /// 序列化为 Data（32 字节）
    /// EN: Serialize to Data (32 bytes).
    public func marshal() -> Data {
        var data = Data(count: StorageConstants.walHeaderSize)
        DataEndian.writeUInt32LE(magic, to: &data, at: 0)
        DataEndian.writeUInt16LE(version, to: &data, at: 4)
        DataEndian.writeUInt16LE(reserved1, to: &data, at: 6)
        DataEndian.writeUInt64LE(checkpointLSN, to: &data, at: 8)
        DataEndian.writeUInt64LE(fileSize, to: &data, at: 16)
        DataEndian.writeUInt32LE(checksum, to: &data, at: 24)
        DataEndian.writeUInt32LE(reserved2, to: &data, at: 28)
        return data
    }

    /// 从 Data 反序列化
    /// EN: Deserialize from Data.
    public static func unmarshal(_ data: Data) throws -> WALHeader {
        guard data.count >= StorageConstants.walHeaderSize else {
            throw StorageError.walCorrupted("Header too short")
        }

        var header = WALHeader()
        let rawChecksum = DataEndian.readUInt32LE(data, at: 24)

        header.magic = DataEndian.readUInt32LE(data, at: 0)
        header.version = DataEndian.readUInt16LE(data, at: 4)
        header.reserved1 = DataEndian.readUInt16LE(data, at: 6)
        header.checkpointLSN = DataEndian.readUInt64LE(data, at: 8)
        header.fileSize = DataEndian.readUInt64LE(data, at: 16)
        header.checksum = DataEndian.readUInt32LE(data, at: 24)
        header.reserved2 = DataEndian.readUInt32LE(data, at: 28)

        // 验证魔数
        // EN: Validate magic number
        guard header.magic == StorageConstants.walMagic else {
            throw StorageError.walCorrupted("Invalid magic number")
        }

        // 验证版本（以 Go 参考实现为准：WALVersion = 1）
        // EN: Validate version (aligned with Go reference implementation: WALVersion = 1)
        guard header.version == 1 else {
            throw StorageError.walCorrupted("Unsupported WAL version: \(header.version)")
        }

        // 验证头部校验和：CRC32(header[0:24])
        // EN: Validate header checksum: CRC32(header[0:24])
        let checksumBuf = Data(data[0..<24])
        let expected = crc32(checksumBuf)
        guard rawChecksum == expected else {
            throw StorageError.walCorrupted("WAL header checksum mismatch")
        }

        return header
    }

    /// 计算并更新校验和
    /// EN: Calculate and update checksum.
    public mutating func updateChecksum() {
        var buffer = Data(count: 24)
        DataEndian.writeUInt32LE(magic, to: &buffer, at: 0)
        DataEndian.writeUInt16LE(version, to: &buffer, at: 4)
        DataEndian.writeUInt16LE(reserved1, to: &buffer, at: 6)
        DataEndian.writeUInt64LE(checkpointLSN, to: &buffer, at: 8)
        DataEndian.writeUInt64LE(fileSize, to: &buffer, at: 16)
        checksum = crc32(buffer)
    }
}
