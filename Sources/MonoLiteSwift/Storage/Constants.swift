// Created by Yanjunhui

import Foundation

/// 存储层常量
public enum StorageConstants {
    // MARK: - 页面常量

    /// 页面大小：4KB
    public static let pageSize: Int = 4096

    /// 页面头大小：24 字节
    public static let pageHeaderSize: Int = 24

    /// 页面最大数据区大小
    public static let maxPageData: Int = pageSize - pageHeaderSize

    // MARK: - 文件头常量

    /// 文件头大小：64 字节
    public static let fileHeaderSize: Int = 64

    /// 文件魔数："MONO"（小端序）
    /// 注意：以 Go 参考实现为准（`MonoLite/storage/pager.go`）
    /// - Go 使用 LittleEndian 读写 uint32，并与常量 `0x4D4F4E4F` 比较。
    /// - 为了与 Go 的磁盘格式字节级一致，这里必须使用同一个数值常量。
    public static let fileMagic: UInt32 = 0x4D4F4E4F

    /// 文件格式版本
    public static let fileVersion: UInt16 = 1

    // MARK: - 页面 ID

    /// 空页面 ID（链表/指针的 nil 语义）
    /// 注意：以 Go 参考实现为准：0 表示“无/空指针”，并非 0xFFFFFFFF。
    public static let invalidPageId: UInt32 = 0

    /// 元数据页面 ID（通常为 0）
    public static let metaPageId: UInt32 = 0

    // MARK: - 缓存配置

    /// 默认页面缓存大小
    public static let defaultCacheSize: Int = 1000

    /// 最大页面缓存大小
    public static let maxCacheSize: Int = 10000

    // MARK: - WAL 常量

    /// WAL 文件魔数："WALM"
    /// 注意：以 Go 参考实现为准（`MonoLite/storage/wal.go`）
    public static let walMagic: UInt32 = 0x57414C4D

    /// WAL 头大小：32 字节
    public static let walHeaderSize: Int = 32

    /// WAL 记录对齐：8 字节
    public static let walRecordAlign: Int = 8

    /// WAL 自动截断阈值：64 MB
    public static let walTruncateThreshold: Int64 = 64 * 1024 * 1024

    /// WAL 截断后最小保留大小：4 MB
    public static let walMinRetainSize: Int64 = 4 * 1024 * 1024

    // MARK: - B+Tree 常量

    /// B+Tree 阶数
    public static let btreeOrder: Int = 50

    /// 最大索引键大小
    public static let maxIndexKeyBytes: Int = maxPageData / 4  // ~1KB

    /// 最大索引值大小
    public static let maxIndexValueBytes: Int = 256

    /// B+Tree 节点头大小：11 字节
    public static let btreeNodeHeaderSize: Int = 11

    /// B+Tree 节点最大数据大小
    public static let btreeNodeMaxBytes: Int = maxPageData - 64

    /// B+Tree 节点分裂阈值（3/4 满）
    public static let btreeNodeSplitThreshold: Int = btreeNodeMaxBytes * 3 / 4
}

/// 页面 ID 类型
public typealias PageID = UInt32

/// 日志序列号类型
public typealias LSN = UInt64

/// 页面类型
public enum PageType: UInt8, Sendable {
    /// 空闲页
    case free = 0x00

    /// 元数据页
    case meta = 0x01

    /// 目录页
    case catalog = 0x02

    /// 数据页
    case data = 0x03

    /// 索引页
    case index = 0x04

    /// 溢出页（用于大文档）
    case overflow = 0x05

    /// 空闲列表页
    case freeList = 0x06
}

/// 槽标志
public struct SlotFlags: OptionSet, Sendable {
    public let rawValue: UInt16

    public init(rawValue: UInt16) {
        self.rawValue = rawValue
    }

    /// 已删除
    public static let deleted = SlotFlags(rawValue: 0x01)

    /// 压缩标记
    public static let compressed = SlotFlags(rawValue: 0x02)
}
