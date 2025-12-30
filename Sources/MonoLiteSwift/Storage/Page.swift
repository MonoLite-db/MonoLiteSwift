// Created by Yanjunhui

import Foundation

/// 页面结构（4096 字节）
/// 页面头：24 字节
/// 数据区：4072 字节
public final class Page: @unchecked Sendable {
    /// 页面 ID
    public let id: PageID

    /// 页面类型
    public var pageType: PageType

    /// 标志位
    public var flags: UInt8

    /// 项目数量
    public var itemCount: UInt16

    /// 剩余空间
    public var freeSpace: UInt16

    /// 下一页 ID（用于链表）
    public var nextPageId: PageID

    /// 上一页 ID（用于双向链表）
    public var prevPageId: PageID

    /// 校验和
    public var checksum: UInt32

    /// 数据区（4072 字节）
    public var data: Data

    /// 是否脏页
    public var isDirty: Bool = false

    /// 并发保护锁
    private let _lock = NSLock()

    // MARK: - 初始化

    /// 创建新页面
    public init(id: PageID, type: PageType) {
        self.id = id
        self.pageType = type
        self.flags = 0
        self.itemCount = 0
        self.freeSpace = UInt16(StorageConstants.maxPageData)
        // 以 Go 参考实现为准：0 表示“无/空指针”
        self.nextPageId = StorageConstants.invalidPageId
        self.prevPageId = StorageConstants.invalidPageId
        self.checksum = 0
        self.data = Data(count: StorageConstants.maxPageData)
    }

    /// 从原始数据创建（反序列化）
    public init(id: PageID, rawData: Data) throws {
        guard rawData.count == StorageConstants.pageSize else {
            throw StorageError.pageCorrupted(id)
        }

        self.id = id

        // 解析页面头（先提取所有值，再赋值）
        let onDiskPageId = DataEndian.readUInt32LE(rawData, at: 0)
        let typeValue = rawData[4]
        let pType = PageType(rawValue: typeValue) ?? .free
        let pFlags = rawData[5]
        let pItemCount = DataEndian.readUInt16LE(rawData, at: 6)
        let pFreeSpace = DataEndian.readUInt16LE(rawData, at: 8)
        let pNextPageId = DataEndian.readUInt32LE(rawData, at: 10)
        let pPrevPageId = DataEndian.readUInt32LE(rawData, at: 14)
        let pChecksum = DataEndian.readUInt32LE(rawData, at: 18)

        // Go 参考实现会从页头读取 pageId；这里要求读到的 pageId 必须与调用方期望一致
        guard onDiskPageId == id else {
            throw StorageError.pageCorrupted(id)
        }

        self.pageType = pType
        self.flags = pFlags
        self.itemCount = pItemCount
        self.freeSpace = pFreeSpace
        self.nextPageId = pNextPageId
        self.prevPageId = pPrevPageId
        self.checksum = pChecksum

        // 复制数据区
        self.data = Data(rawData[StorageConstants.pageHeaderSize...])

        // Go 参考实现会校验 checksum；不匹配视为页损坏
        guard verifyChecksum() else {
            throw StorageError.checksumMismatch
        }
    }

    // MARK: - 序列化

    /// 序列化为原始数据（4096 字节）
    public func marshal() -> Data {
        var rawData = Data(count: StorageConstants.pageSize)
        DataEndian.writeUInt32LE(id, to: &rawData, at: 0)
        rawData[4] = pageType.rawValue
        rawData[5] = flags
        DataEndian.writeUInt16LE(itemCount, to: &rawData, at: 6)
        DataEndian.writeUInt16LE(freeSpace, to: &rawData, at: 8)
        DataEndian.writeUInt32LE(nextPageId, to: &rawData, at: 10)
        DataEndian.writeUInt32LE(prevPageId, to: &rawData, at: 14)
        DataEndian.writeUInt32LE(0, to: &rawData, at: 18)

        // 复制数据区
        rawData.replaceSubrange(StorageConstants.pageHeaderSize..<StorageConstants.pageSize,
                                with: data.prefix(StorageConstants.maxPageData))

        // 计算并写入 checksum（以 Go 参考实现为准：对 data 区做 XOR）
        let cs = Page.calculateChecksum(for: rawData, dataOffset: StorageConstants.pageHeaderSize)
        DataEndian.writeUInt32LE(cs, to: &rawData, at: 18)

        return rawData
    }

    // MARK: - 数据访问

    /// 获取指定偏移的数据
    public func getData(at offset: Int, length: Int) -> Data? {
        guard offset >= 0, length > 0, offset + length <= data.count else {
            return nil
        }
        return Data(data[offset..<offset + length])
    }

    /// 设置指定偏移的数据
    public func setData(_ newData: Data, at offset: Int) {
        guard offset >= 0, offset + newData.count <= data.count else {
            return
        }
        data.replaceSubrange(offset..<offset + newData.count, with: newData)
        isDirty = true
    }

    /// 清空数据区
    public func clearData() {
        data = Data(count: StorageConstants.maxPageData)
        itemCount = 0
        freeSpace = UInt16(StorageConstants.maxPageData)
        isDirty = true
    }

    // MARK: - 校验和

    /// 计算校验和
    public func calculateChecksum() -> UInt32 {
        // 以 Go 参考实现为准：按 little-endian u32 分组 XOR（尾部不足 4B 也参与）
        return Page.calculateChecksum(for: data, dataOffset: 0)
    }

    /// 更新校验和
    public func updateChecksum() {
        checksum = calculateChecksum()
    }

    /// 验证校验和
    public func verifyChecksum() -> Bool {
        return checksum == calculateChecksum()
    }

    // MARK: - 锁操作

    /// 加锁
    public func lock() {
        _lock.lock()
    }

    /// 解锁
    public func unlock() {
        _lock.unlock()
    }

    /// 在锁保护下执行操作
    public func withLock<T>(_ body: () throws -> T) rethrows -> T {
        _lock.lock()
        defer { _lock.unlock() }
        return try body()
    }
}

// MARK: - Checksum helpers (Go-compatible)

extension Page {
    /// 以 Go 参考实现为准的 XOR 校验和（对 Data 的指定区间按 little-endian u32 XOR）
    fileprivate static func calculateChecksum(for raw: Data, dataOffset: Int) -> UInt32 {
        var sum: UInt32 = 0
        let bytes = [UInt8](raw)
        var i = dataOffset
        while i < bytes.count {
            var word: UInt32 = 0
            let remaining = bytes.count - i
            let take = min(4, remaining)
            // little-endian 组装
            if take >= 1 { word |= UInt32(bytes[i]) }
            if take >= 2 { word |= UInt32(bytes[i + 1]) << 8 }
            if take >= 3 { word |= UInt32(bytes[i + 2]) << 16 }
            if take >= 4 { word |= UInt32(bytes[i + 3]) << 24 }
            sum ^= word
            i += 4
        }
        return sum
    }
}

// MARK: - Equatable

extension Page: Equatable {
    public static func == (lhs: Page, rhs: Page) -> Bool {
        lhs.id == rhs.id
    }
}

// MARK: - Hashable

extension Page: Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }
}
