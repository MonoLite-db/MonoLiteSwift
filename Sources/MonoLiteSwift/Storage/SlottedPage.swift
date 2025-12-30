// Created by Yanjunhui

import Foundation

/// 槽结构（6 字节）
public struct Slot: Sendable {
    /// 记录在页中的偏移
    public var offset: UInt16

    /// 记录长度
    public var length: UInt16

    /// 标志位
    public var flags: SlotFlags

    /// 槽大小（字节）
    public static let size = 6

    /// 是否已删除
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
public final class SlottedPage: @unchecked Sendable {
    /// 底层页面
    public let page: Page

    /// 槽目录（内存缓存）
    private var slots: [Slot]

    /// 下一条记录的写入偏移（从后向前）
    private var nextRecordOffset: UInt16

    // MARK: - 初始化

    /// 包装现有页面
    public init(page: Page) {
        self.page = page
        self.slots = []
        self.nextRecordOffset = UInt16(StorageConstants.maxPageData)

        // 加载槽目录
        loadSlots()
    }

    /// 创建新的槽页面
    public init(id: PageID, type: PageType) {
        self.page = Page(id: id, type: type)
        self.slots = []
        self.nextRecordOffset = UInt16(StorageConstants.maxPageData)
    }

    // MARK: - 槽管理

    /// 槽数量
    public var slotCount: Int {
        slots.count
    }

    /// 活跃槽数量（未删除）
    public var liveCount: Int {
        slots.filter { !$0.isDeleted }.count
    }

    /// 已用空间
    public var usedSpace: Int {
        let slotsSpace = slots.count * Slot.size
        let recordsSpace = Int(StorageConstants.maxPageData) - Int(nextRecordOffset)
        return slotsSpace + recordsSpace
    }

    /// 可用空间
    public var availableSpace: Int {
        let slotsEnd = slots.count * Slot.size
        return Int(nextRecordOffset) - slotsEnd
    }

    // MARK: - 记录操作

    /// 插入记录
    /// - Returns: 槽索引
    public func insertRecord(_ data: Data) throws -> Int {
        let recordLen = data.count

        // 检查空间是否足够
        let requiredSpace = recordLen + Slot.size
        guard availableSpace >= requiredSpace else {
            throw StorageError.outOfSpace
        }

        // 计算新记录的偏移（从后向前）
        let recordOffset = nextRecordOffset - UInt16(recordLen)

        // 写入记录数据
        page.setData(data, at: Int(recordOffset))

        // 创建新槽
        let slot = Slot(offset: recordOffset, length: UInt16(recordLen))
        slots.append(slot)

        // 更新偏移
        nextRecordOffset = recordOffset

        // 更新页面元数据
        page.itemCount = UInt16(slots.count)
        page.freeSpace = UInt16(availableSpace)
        page.isDirty = true

        // 保存槽目录
        saveSlots()

        return slots.count - 1
    }

    /// 获取记录
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
    public func deleteRecord(at slotIndex: Int) throws {
        guard slotIndex >= 0, slotIndex < slots.count else {
            throw StorageError.pageCorrupted(page.id)
        }

        slots[slotIndex].flags.insert(.deleted)
        page.isDirty = true

        saveSlots()
    }

    /// 更新记录
    public func updateRecord(at slotIndex: Int, _ data: Data) throws {
        guard slotIndex >= 0, slotIndex < slots.count else {
            throw StorageError.pageCorrupted(page.id)
        }

        let slot = slots[slotIndex]
        guard !slot.isDeleted else {
            throw StorageError.pageCorrupted(page.id)
        }

        // 如果新数据长度相同或更小，就地更新
        if data.count <= slot.length {
            page.setData(data, at: Int(slot.offset))
            slots[slotIndex].length = UInt16(data.count)
            saveSlots()
            return
        }

        // 以 Go 参考实现为准：扩容时复用同一个 slotIndex（保持 RecordID 稳定）
        let newLen = UInt16(data.count)
        let extra = newLen &- slot.length

        // 注意：这里使用 page.freeSpace（与 Go 的 sp.freeSpace 同语义：delete 不回收）
        guard extra <= page.freeSpace else {
            throw StorageError.outOfSpace
        }

        // 计算 minOffset（排除当前槽位，相当于先把旧记录标记删除）
        var minOffset = UInt16(StorageConstants.maxPageData)
        for (i, s) in slots.enumerated() {
            if i == slotIndex { continue }
            if !s.isDeleted && s.offset < minOffset {
                minOffset = s.offset
            }
        }

        // slot 目录从前往后增长，记录从后往前增长
        let slotDirEnd = UInt16(slots.count * Slot.size)
        guard slotDirEnd + newLen <= minOffset else {
            throw StorageError.outOfSpace
        }

        let newOffset = minOffset &- newLen

        // 写入新记录数据
        page.setData(data, at: Int(newOffset))

        // 更新槽位：保持同一个 slotIndex
        slots[slotIndex] = Slot(offset: newOffset, length: newLen, flags: [])

        // 重新计算 nextRecordOffset（以 Go 的 min live offset 语义为准）
        recomputeNextRecordOffset()

        // 更新页面元数据（与当前 Swift 的空间计算保持一致）
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

    // MARK: - 压缩

    /// 压缩页面（回收已删除记录的空间）
    /// - Returns: 旧索引 -> 新索引的映射
    @discardableResult
    public func compact() throws -> [Int: Int] {
        var mapping = [Int: Int]()
        var liveSlots: [(oldIndex: Int, slot: Slot, data: Data)] = []

        // 收集所有活跃记录
        for (index, slot) in slots.enumerated() {
            if !slot.isDeleted {
                if let data = page.getData(at: Int(slot.offset), length: Int(slot.length)) {
                    liveSlots.append((index, slot, data))
                }
            }
        }

        // 清空数据区
        page.clearData()

        // 重新写入记录
        slots.removeAll()
        nextRecordOffset = UInt16(StorageConstants.maxPageData)

        for (oldIndex, _, data) in liveSlots {
            let newIndex = try insertRecord(data)
            mapping[oldIndex] = newIndex
        }

        return mapping
    }

    // MARK: - 迭代

    /// 遍历所有活跃记录
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
    public func allRecords() -> [(index: Int, data: Data)] {
        var results: [(Int, Data)] = []
        forEachRecord { index, data in
            results.append((index, data))
        }
        return results
    }

    // MARK: - 槽目录持久化

    /// 加载槽目录
    private func loadSlots() {
        slots.removeAll()

        let slotCount = Int(page.itemCount)
        guard slotCount > 0 else { return }

        for i in 0..<slotCount {
            let offset = i * Slot.size
            guard offset + Slot.size <= page.data.count else { break }

            // 注意：不要对 Data 先切片再用 0-based offset 读。
            // Data 的 slice 可能保留原始 index（类似 ArraySlice），会导致 data[0] 越界并触发 SIGTRAP。
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
        recomputeNextRecordOffset()
    }

    /// 保存槽目录
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
        page.data.replaceSubrange(0..<slotData.count, with: slotData)
        page.itemCount = UInt16(slots.count)
    }
}
