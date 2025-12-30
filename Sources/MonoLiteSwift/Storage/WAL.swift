// Created by Yanjunhui

import Foundation

/// Write-Ahead Log
/// 提供崩溃恢复能力
public actor WAL {
    /// 日志文件句柄
    private var fileHandle: FileHandle?

    /// 文件路径
    private let path: String

    /// 文件头
    private var header: WALHeader

    /// 当前 LSN（下一条记录的 LSN）
    private var currentLSN: LSN

    /// 写入偏移
    private var writeOffset: Int64

    /// 检查点 LSN
    private var checkpointLSN: LSN

    /// 是否启用自动截断
    public var autoTruncate: Bool = true

    // MARK: - 初始化

    /// 创建或打开 WAL
    public init(path: String) async throws {
        self.path = path
        self.header = WALHeader()
        self.currentLSN = 1
        self.writeOffset = Int64(StorageConstants.walHeaderSize)
        self.checkpointLSN = 0

        try await open()
    }

    /// 打开 WAL 文件
    private func open() async throws {
        let fileManager = FileManager.default

        if fileManager.fileExists(atPath: path) {
            // 打开现有文件
            fileHandle = try FileHandle(forUpdating: URL(fileURLWithPath: path))

            // 读取头部
            try fileHandle?.seek(toOffset: 0)
            guard let headerData = try fileHandle?.read(upToCount: StorageConstants.walHeaderSize) else {
                throw StorageError.walCorrupted("Cannot read header")
            }
            // Go 参考实现：空文件视为新 WAL
            if headerData.isEmpty {
                header = WALHeader()
                header.updateChecksum()
                try writeHeader()
            } else {
                header = try WALHeader.unmarshal(headerData)
            }
            checkpointLSN = header.checkpointLSN

            // 扫描找到最大 LSN 和正确的写入偏移
            try await scanForMaxLSN()
        } else {
            // 创建新文件
            fileManager.createFile(atPath: path, contents: nil)
            fileHandle = try FileHandle(forUpdating: URL(fileURLWithPath: path))

            // 写入初始头部
            header = WALHeader()
            header.updateChecksum()
            try writeHeader()
        }
    }

    /// 扫描找到最大 LSN
    private func scanForMaxLSN() async throws {
        guard let handle = fileHandle else { return }

        // 获取文件大小
        try handle.seek(toOffset: 0)
        let endOffset = try handle.seekToEnd()

        var offset: UInt64 = UInt64(StorageConstants.walHeaderSize)
        var lastValidOffset: UInt64 = offset
        // Go 参考实现语义：即使 WAL 被截断为仅 header，LSN 也必须从 checkpointLSN+1 继续
        var maxLSN: LSN = checkpointLSN

        while offset < endOffset {
            try handle.seek(toOffset: offset)

            // 尝试读取记录头
            guard let headerData = try handle.read(upToCount: walRecordHeaderSize),
                  headerData.count == walRecordHeaderSize else {
                break
            }

            // 解析记录头获取数据长度
            let dataLen = DataEndian.readUInt16LE(headerData, at: 10)

            // 读取完整记录
            try handle.seek(toOffset: offset)
            let recordSize = walRecordHeaderSize + Int(dataLen)
            guard let recordData = try handle.read(upToCount: recordSize),
                  recordData.count == recordSize else {
                break
            }

            // 解析记录
            do {
                let record = try WALRecord.unmarshal(recordData)
                if record.lsn > maxLSN {
                    maxLSN = record.lsn
                }

                // 计算下一条记录的偏移（包括对齐）
                lastValidOffset = offset + UInt64(record.totalSize)
                offset = lastValidOffset
            } catch {
                // 记录损坏，停止扫描
                break
            }
        }

        currentLSN = maxLSN + 1
        // Go 参考实现：writeOffset 必须指向最后一条有效记录末尾，而不是损坏记录起始处
        writeOffset = Int64(lastValidOffset)
        header.fileSize = UInt64(lastValidOffset)
    }

    // MARK: - 写入记录

    /// 写入页面记录
    @discardableResult
    public func writePageRecord(_ pageId: PageID, _ pageData: Data) async throws -> LSN {
        let record = WALRecord(lsn: currentLSN, type: .pageWrite, pageId: pageId, data: pageData)
        try await writeRecord(record)
        let lsn = currentLSN
        currentLSN += 1
        return lsn
    }

    /// 写入分配页面记录
    @discardableResult
    public func writeAllocRecord(_ pageId: PageID, pageType: PageType) async throws -> LSN {
        var data = Data(count: 1)
        data[0] = pageType.rawValue
        let record = WALRecord(lsn: currentLSN, type: .allocPage, pageId: pageId, data: data)
        try await writeRecord(record)
        let lsn = currentLSN
        currentLSN += 1
        return lsn
    }

    /// 写入释放页面记录
    @discardableResult
    public func writeFreeRecord(_ pageId: PageID) async throws -> LSN {
        let record = WALRecord(lsn: currentLSN, type: .freePage, pageId: pageId)
        try await writeRecord(record)
        let lsn = currentLSN
        currentLSN += 1
        return lsn
    }

    /// 写入元数据更新记录
    @discardableResult
    public func writeMetaRecord(_ metaType: MetaUpdateType, oldValue: UInt32, newValue: UInt32) async throws -> LSN {
        var data = Data(count: 9)
        data[0] = metaType.rawValue
        DataEndian.writeUInt32LE(oldValue, to: &data, at: 1)
        DataEndian.writeUInt32LE(newValue, to: &data, at: 5)
        let record = WALRecord(lsn: currentLSN, type: .metaUpdate, data: data)
        try await writeRecord(record)
        let lsn = currentLSN
        currentLSN += 1
        return lsn
    }

    /// 写入提交记录
    @discardableResult
    public func writeCommitRecord() async throws -> LSN {
        let record = WALRecord(lsn: currentLSN, type: .commit)
        try await writeRecord(record)
        let lsn = currentLSN
        currentLSN += 1
        return lsn
    }

    /// 写入记录（底层实现）
    private func writeRecord(_ record: WALRecord) async throws {
        guard let handle = fileHandle else {
            throw StorageError.fileNotOpen(path)
        }

        let data = record.marshal()
        try handle.seek(toOffset: UInt64(writeOffset))
        try handle.write(contentsOf: data)
        writeOffset += Int64(data.count)

        // 更新头部文件大小
        header.fileSize = UInt64(writeOffset)
    }

    // MARK: - 检查点

    /// 创建检查点
    public func checkpoint(_ lsn: LSN) async throws {
        // 写入检查点记录
        var data = Data(count: 8)
        DataEndian.writeUInt64LE(lsn, to: &data, at: 0)
        let record = WALRecord(lsn: currentLSN, type: .checkpoint, data: data)
        try await writeRecord(record)
        currentLSN += 1

        // 更新头部
        header.checkpointLSN = lsn
        checkpointLSN = lsn
        try await sync()

        // 检查是否需要截断
        if autoTruncate && writeOffset > StorageConstants.walTruncateThreshold {
            try await truncate()
        }
    }

    /// 截断 WAL（保留最近的检查点之后的记录）
    public func truncate() async throws {
        guard let handle = fileHandle else { return }

        // 重置文件到只有头部
        header.fileSize = UInt64(StorageConstants.walHeaderSize)
        header.updateChecksum()

        try writeHeader()
        try handle.truncate(atOffset: UInt64(StorageConstants.walHeaderSize))
        try handle.synchronize()

        writeOffset = Int64(StorageConstants.walHeaderSize)
    }

    // MARK: - 读取记录

    /// 从指定 LSN 开始读取记录
    public func readRecordsFrom(_ startLSN: LSN) async throws -> [WALRecord] {
        guard let handle = fileHandle else {
            throw StorageError.fileNotOpen(path)
        }

        var records: [WALRecord] = []
        var offset: UInt64 = UInt64(StorageConstants.walHeaderSize)

        // 只读取到“最后一条有效记录末尾”（由 scanForMaxLSN 校正）
        let endOffset = UInt64(max(writeOffset, Int64(StorageConstants.walHeaderSize)))

        while offset < endOffset {
            try handle.seek(toOffset: offset)

            // 读取记录头
            guard let headerData = try handle.read(upToCount: walRecordHeaderSize),
                  headerData.count == walRecordHeaderSize else {
                break
            }

            // 获取数据长度
            let dataLen = DataEndian.readUInt16LE(headerData, at: 10)

            // 读取完整记录
            try handle.seek(toOffset: offset)
            let recordSize = walRecordHeaderSize + Int(dataLen)
            guard let recordData = try handle.read(upToCount: recordSize) else {
                break
            }

            // 解析记录
            do {
                let record = try WALRecord.unmarshal(recordData)

                // 只返回 >= startLSN 的记录
                if record.lsn >= startLSN {
                    records.append(record)
                }

                // 移动到下一条记录
                offset += UInt64(record.totalSize)
            } catch {
                // 记录损坏，停止读取
                break
            }
        }

        return records
    }

    // MARK: - 同步

    /// 同步到磁盘
    public func sync() async throws {
        guard let handle = fileHandle else { return }
        try writeHeader()
        try handle.synchronize()
    }

    /// 写入头部
    private func writeHeader() throws {
        guard let handle = fileHandle else { return }
        header.updateChecksum()
        try handle.seek(toOffset: 0)
        try handle.write(contentsOf: header.marshal())
    }

    // MARK: - 关闭

    /// 关闭 WAL
    public func close() async throws {
        try await sync()
        try fileHandle?.close()
        fileHandle = nil
    }

    /// 模拟崩溃：直接关闭底层句柄，不写 header、不 sync（仅测试使用）
    ///
    /// 用途：复刻 Go 测试里“直接 close 文件描述符”的崩溃窗口，验证恢复逻辑不依赖正常关闭流程。
    func simulateCrashCloseForTesting() {
        try? fileHandle?.close()
        fileHandle = nil
    }

    // MARK: - 属性访问

    /// 获取当前 LSN
    public var currentLogSequenceNumber: LSN {
        currentLSN
    }

    /// 获取检查点 LSN
    public var lastCheckpointLSN: LSN {
        checkpointLSN
    }

    /// 获取文件大小
    public var fileSize: Int64 {
        writeOffset
    }
}
