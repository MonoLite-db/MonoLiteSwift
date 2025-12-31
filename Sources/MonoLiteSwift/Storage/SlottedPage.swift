// Created by Yanjunhui

import Foundation

/// 槽结构（6 字节）
/// EN: Slot structure (6 bytes).
public struct Slot: Sendable {
    /// 记录在页中的偏移
    /// EN: Record offset within the page.
    public var offset: UInt16

    /// 记录长度
    /// EN: Record length.
    public var length: UInt16

    /// 标志位
    /// EN: Flags.
    public var flags: SlotFlags

    /// 槽大小（字节）
    /// EN: Slot size (bytes).
    public static let size = 6

    /// 是否已删除
    /// EN: Whether this slot is deleted.
    public var isDeleted: Bool {
        flags.contains(.deleted)
    }

    public init(offset: UInt16, length: UInt16, flags: SlotFlags = []) {
        self.offset = offset
        self.length = length
        self.flags = flags
    }
}

/// 变长记录页面管理
/// 使用槽目录管理变长记录
/// 布局：[槽目录从前向后增长] ... [记录从后向前增长]
/// EN: Variable-length record page management.
/// Uses slot directory to manage variable-length records.
/// Layout: [Slot directory grows forward] ... [Records grow backward]
public final class SlottedPage: @unchecked Sendable {
    /// 底层页面
    /// EN: Underlying page.
    public let page: Page

    /// 槽目录（内存缓存）
    /// EN: Slot directory (in-memory cache).
    private var slots: [Slot]

    /// 下一条记录的写入偏移（从后向前）
    /// EN: Write offset for next record (grows backward).
    private var nextRecordOffset: UInt16

    // MARK: - 初始化 / Initialization

    /// 包装现有页面
    /// EN: Wrap existing page.
    public init(page: Page) {
        self.page = page
        self.slots = []
        self.nextRecordOffset = UInt16(StorageConstants.maxPageData)

        // 加载槽目录
        // EN: Load slot directory
        loadSlots()
    }

    /// 创建新的槽页面
    /// EN: Create new slotted page.
    public init(id: PageID, type: PageType) {
        self.page = Page(id: id, type: type)
        self.slots = []
        self.nextRecordOffset = UInt16(StorageConstants.maxPageData)
    }

    // MARK: - 槽管理 / Slot Management

    /// 槽数量
    /// EN: Slot count.
    public var slotCount: Int {
        slots.count
    }

    /// 活跃槽数量（未删除）
    /// EN: Active slot count (not deleted).
    public var liveCount: Int {
        slots.filter { !$0.isDeleted }.count
    }

    /// 已用空间
    /// EN: Used space.
    public var usedSpace: Int {
        let slotsSpace = slots.count * Slot.size
        let recordsSpace = Int(StorageConstants.maxPageData) - Int(nextRecordOffset)
        return slotsSpace + recordsSpace
    }

    /// 可用空间
    /// EN: Available space.
    public var availableSpace: Int {
        let slotsEnd = slots.count * Slot.size
        return Int(nextRecordOffset) - slotsEnd
    }

    // MARK: - 记录操作 / Record Operations

    /// 插入记录
    /// - Returns: 槽索引
    /// EN: Insert record.
    /// - Returns: Slot index.
    public func insertRecord(_ data: Data) throws -> Int {
        let recordLen = data.count

        // 检查空间是否足够
        // EN: Check if there's enough space
        let requiredSpace = recordLen + Slot.size
        guard availableSpace >= requiredSpace else {
            throw StorageError.outOfSpace
        }

        // 计算新记录的偏移（从后向前）
        // EN: Calculate new record offset (grows backward)
        let recordOffset = nextRecordOffset - UInt16(recordLen)

        // 写入记录数据
        // EN: Write record data
        page.setData(data, at: Int(recordOffset))

        // 创建新槽
        // EN: Create new slot
        let slot = Slot(offset: recordOffset, length: UInt16(recordLen))
        slots.append(slot)

        // 更新偏移
        // EN: Update offset
        nextRecordOffset = recordOffset

        // 更新页面元数据
        // EN: Update page metadata
        page.itemCount = UInt16(slots.count)
        page.freeSpace = UInt16(availableSpace)
        page.isDirty = true

        // 保存槽目录
        // EN: Save slot directory
        saveSlots()

        return slots.count - 1
    }

    /// 获取记录
    /// EN: Get record.
    public func getRecord(at slotIndex: Int) throws -> Data {
        guard slotIndex >= 0, slotIndex < slots.count else {
            throw StorageError.pageCorrupted(page.id)
        }

        let slot = slots[slotIndex]

        guard !slot.isDeleted else {
            throw StorageError.pageCorrupted(page.id)
        }

        guard let data = page.getData(at: Int(slot.offset), length: Int(slot.length)) else {
            throw StorageError.pageCorrupted(page.id)
        }

        return data
    }

    /// 删除记录（标记删除）
    /// EN: Delete record (mark as deleted).
    public func deleteRecord(at slotIndex: Int) throws {
        guard slotIndex >= 0, slotIndex < slots.count else {
            throw StorageError.pageCorrupted(page.id)
        }

        slots[slotIndex].flags.insert(.deleted)
        page.isDirty = true

        saveSlots()
    }

    /// 更新记录
    /// EN: Update record.
    public func updateRecord(at slotIndex: Int, _ data: Data) throws {
        guard slotIndex >= 0, slotIndex < slots.count else {
            throw StorageError.pageCorrupted(page.id)
        }

        let slot = slots[slotIndex]
        guard !slot.isDeleted else {
            throw StorageError.pageCorrupted(page.id)
        }

        // 如果新数据长度相同或更小，就地更新
        // EN: If new data length is same or smaller, update in place
        if data.count <= slot.length {
            page.setData(data, at: Int(slot.offset))
            slots[slotIndex].length = UInt16(data.count)
            saveSlots()
            return
        }

        // 以 Go 参考实现为准：扩容时复用同一个 slotIndex（保持 RecordID 稳定）
        // EN: Aligned with Go reference implementation: reuse same slotIndex when expanding (keep RecordID stable)
        let newLen = UInt16(data.count)
        let extra = newLen &- slot.length

        // 注意：这里使用 page.freeSpace（与 Go 的 sp.freeSpace 同语义：delete 不回收）
        // EN: Note: use page.freeSpace here (same semantics as Go's sp.freeSpace: delete doesn't reclaim)
        guard extra <= page.freeSpace else {
            throw StorageError.outOfSpace
        }

        // 计算 minOffset（排除当前槽位，相当于先把旧记录标记删除）
        // EN: Calculate minOffset (exclude current slot, equivalent to marking old record as deleted first)
        var minOffset = UInt16(StorageConstants.maxPageData)
        for (i, s) in slots.enumerated() {
            if i == slotIndex { continue }
            if !s.isDeleted && s.offset < minOffset {
                minOffset = s.offset
            }
        }

        // slot 目录从前往后增长，记录从后往前增长
        // EN: Slot directory grows forward, records grow backward
        let slotDirEnd = UInt16(slots.count * Slot.size)
        guard slotDirEnd + newLen <= minOffset else {
            throw StorageError.outOfSpace
        }

        let newOffset = minOffset &- newLen

        // 写入新记录数据
        // EN: Write new record data
        page.setData(data, at: Int(newOffset))

        // 更新槽位：保持同一个 slotIndex
        // EN: Update slot: keep same slotIndex
        slots[slotIndex] = Slot(offset: newOffset, length: newLen, flags: [])

        // 重新计算 nextRecordOffset（以 Go 的 min live offset 语义为准）
        // EN: Recalculate nextRecordOffset (aligned with Go's min live offset semantics)
        recomputeNextRecordOffset()

        // 更新页面元数据（与当前 Swift 的空间计算保持一致）
        // EN: Update page metadata (consistent with current Swift space calculation)
        page.freeSpace = UInt16(availableSpace)
        page.isDirty = true

        saveSlots()
    }

    private func recomputeNextRecordOffset() {
        if let min = slots.filter({ !$0.isDeleted }).map({ $0.offset }).min() {
            nextRecordOffset = min
        } else {
            nextRecordOffset = UInt16(StorageConstants.maxPageData)
        }
    }

    // MARK: - 压缩 / Compaction

    /// 压缩页面（回收已删除记录的空间）
    /// - Returns: 旧索引 -> 新索引的映射
    /// EN: Compact page (reclaim space from deleted records).
    /// - Returns: Old index -> new index mapping.
    @discardableResult
    public func compact() throws -> [Int: Int] {
        var mapping = [Int: Int]()
        var liveSlots: [(oldIndex: Int, slot: Slot, data: Data)] = []

        // 收集所有活跃记录
        // EN: Collect all active records
        for (index, slot) in slots.enumerated() {
            if !slot.isDeleted {
                if let data = page.getData(at: Int(slot.offset), length: Int(slot.length)) {
                    liveSlots.append((index, slot, data))
                }
            }
        }

        // 清空数据区
        // EN: Clear data area
        page.clearData()

        // 重新写入记录
        // EN: Rewrite records
        slots.removeAll()
        nextRecordOffset = UInt16(StorageConstants.maxPageData)

        for (oldIndex, _, data) in liveSlots {
            let newIndex = try insertRecord(data)
            mapping[oldIndex] = newIndex
        }

        return mapping
    }

    // MARK: - 迭代 / Iteration

    /// 遍历所有活跃记录
    /// EN: Iterate over all active records.
    public func forEachRecord(_ body: (Int, Data) throws -> Void) rethrows {
        for (index, slot) in slots.enumerated() {
            if !slot.isDeleted {
                if let data = page.getData(at: Int(slot.offset), length: Int(slot.length)) {
                    try body(index, data)
                }
            }
        }
    }

    /// 获取所有活跃记录
    /// EN: Get all active records.
    public func allRecords() -> [(index: Int, data: Data)] {
        var results: [(Int, Data)] = []
        forEachRecord { index, data in
            results.append((index, data))
        }
        return results
    }

    // MARK: - 槽目录持久化 / Slot Directory Persistence

    /// 加载槽目录
    /// EN: Load slot directory.
    private func loadSlots() {
        slots.removeAll()

        let slotCount = Int(page.itemCount)
        guard slotCount > 0 else { return }

        for i in 0..<slotCount {
            let offset = i * Slot.size
            guard offset + Slot.size <= page.data.count else { break }

            // 注意：不要对 Data 先切片再用 0-based offset 读。
            // Data 的 slice 可能保留原始 index（类似 ArraySlice），会导致 data[0] 越界并触发 SIGTRAP。
            // EN: Note: Don't slice Data first then read with 0-based offset.
            // Data's slice may preserve original index (like ArraySlice), causing data[0] to be out of bounds and trigger SIGTRAP.
            let slotOffset = DataEndian.readUInt16LE(page.data, at: offset)
            let slotLength = DataEndian.readUInt16LE(page.data, at: offset + 2)
            let slotFlags = DataEndian.readUInt16LE(page.data, at: offset + 4)

            let slot = Slot(
                offset: slotOffset,
                length: slotLength,
                flags: SlotFlags(rawValue: slotFlags)
            )
            slots.append(slot)
        }

        // 计算 nextRecordOffset
        // EN: Calculate nextRecordOffset
        recomputeNextRecordOffset()
    }

    /// 保存槽目录
    /// EN: Save slot directory.
    private func saveSlots() {
        var slotData = Data(count: slots.count * Slot.size)

        for (i, slot) in slots.enumerated() {
            let offset = i * Slot.size
            DataEndian.writeUInt16LE(slot.offset, to: &slotData, at: offset)
            DataEndian.writeUInt16LE(slot.length, to: &slotData, at: offset + 2)
            DataEndian.writeUInt16LE(slot.flags.rawValue, to: &slotData, at: offset + 4)
        }

        // 注意：这里只更新槽目录区域，不覆盖记录区域
        // 槽目录从 data 的开头开始
        // EN: Note: Only update slot directory area, don't overwrite record area
        // Slot directory starts from the beginning of data
        page.data.replaceSubrange(0..<slotData.count, with: slotData)
        page.itemCount = UInt16(slots.count)
    }
}
