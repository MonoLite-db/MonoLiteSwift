// Created by Yanjunhui

import Foundation

/// Write-Ahead Log
/// 提供崩溃恢复能力
/// EN: Write-Ahead Log.
/// Provides crash recovery capability.
public actor WAL {
    /// 日志文件句柄
    /// EN: Log file handle.
    private var fileHandle: FileHandle?

    /// 文件路径
    /// EN: File path.
    private let path: String

    /// 文件头
    /// EN: File header.
    private var header: WALHeader

    /// 当前 LSN（下一条记录的 LSN）
    /// EN: Current LSN (LSN of next record).
    private var currentLSN: LSN

    /// 写入偏移
    /// EN: Write offset.
    private var writeOffset: Int64

    /// 检查点 LSN
    /// EN: Checkpoint LSN.
    private var checkpointLSN: LSN

    /// 是否启用自动截断
    /// EN: Whether auto-truncation is enabled.
    public var autoTruncate: Bool = true

    // MARK: - 初始化 / Initialization

    /// 创建或打开 WAL
    /// EN: Create or open WAL.
    public init(path: String) async throws {
        self.path = path
        self.header = WALHeader()
        self.currentLSN = 1
        self.writeOffset = Int64(StorageConstants.walHeaderSize)
        self.checkpointLSN = 0

        try await open()
    }

    /// 打开 WAL 文件
    /// EN: Open WAL file.
    private func open() async throws {
        let fileManager = FileManager.default

        if fileManager.fileExists(atPath: path) {
            // 打开现有文件
            // EN: Open existing file
            fileHandle = try FileHandle(forUpdating: URL(fileURLWithPath: path))

            // 读取头部
            // EN: Read header
            try fileHandle?.seek(toOffset: 0)
            guard let headerData = try fileHandle?.read(upToCount: StorageConstants.walHeaderSize) else {
                throw StorageError.walCorrupted("Cannot read header")
            }
            // Go 参考实现：空文件视为新 WAL
            // EN: Go reference implementation: empty file is treated as new WAL
            if headerData.isEmpty {
                header = WALHeader()
                header.updateChecksum()
                try writeHeader()
            } else {
                header = try WALHeader.unmarshal(headerData)
            }
            checkpointLSN = header.checkpointLSN

            // 扫描找到最大 LSN 和正确的写入偏移
            // EN: Scan to find maximum LSN and correct write offset
            try await scanForMaxLSN()
        } else {
            // 创建新文件
            // EN: Create new file
            fileManager.createFile(atPath: path, contents: nil)
            fileHandle = try FileHandle(forUpdating: URL(fileURLWithPath: path))

            // 写入初始头部
            // EN: Write initial header
            header = WALHeader()
            header.updateChecksum()
            try writeHeader()
        }
    }

    /// 扫描找到最大 LSN
    /// EN: Scan to find maximum LSN.
    private func scanForMaxLSN() async throws {
        guard let handle = fileHandle else { return }

        // 获取文件大小
        // EN: Get file size
        try handle.seek(toOffset: 0)
        let endOffset = try handle.seekToEnd()

        var offset: UInt64 = UInt64(StorageConstants.walHeaderSize)
        var lastValidOffset: UInt64 = offset
        // Go 参考实现语义：即使 WAL 被截断为仅 header，LSN 也必须从 checkpointLSN+1 继续
        // EN: Go reference implementation semantics: even if WAL is truncated to header only, LSN must continue from checkpointLSN+1
        var maxLSN: LSN = checkpointLSN

        while offset < endOffset {
            try handle.seek(toOffset: offset)

            // 尝试读取记录头
            // EN: Try to read record header
            guard let headerData = try handle.read(upToCount: walRecordHeaderSize),
                  headerData.count == walRecordHeaderSize else {
                break
            }

            // 解析记录头获取数据长度
            // EN: Parse record header to get data length
            let dataLen = DataEndian.readUInt16LE(headerData, at: 10)

            // 读取完整记录
            // EN: Read complete record
            try handle.seek(toOffset: offset)
            let recordSize = walRecordHeaderSize + Int(dataLen)
            guard let recordData = try handle.read(upToCount: recordSize),
                  recordData.count == recordSize else {
                break
            }

            // 解析记录
            // EN: Parse record
            do {
                let record = try WALRecord.unmarshal(recordData)
                if record.lsn > maxLSN {
                    maxLSN = record.lsn
                }

                // 计算下一条记录的偏移（包括对齐）
                // EN: Calculate next record offset (including alignment)
                lastValidOffset = offset + UInt64(record.totalSize)
                offset = lastValidOffset
            } catch {
                // 记录损坏，停止扫描
                // EN: Record corrupted, stop scanning
                break
            }
        }

        currentLSN = maxLSN + 1
        // Go 参考实现：writeOffset 必须指向最后一条有效记录末尾，而不是损坏记录起始处
        // EN: Go reference implementation: writeOffset must point to the end of last valid record, not the start of corrupted record
        writeOffset = Int64(lastValidOffset)
        header.fileSize = UInt64(lastValidOffset)
    }

    // MARK: - 写入记录 / Write Records

    /// 写入页面记录
    /// EN: Write page record.
    @discardableResult
    public func writePageRecord(_ pageId: PageID, _ pageData: Data) async throws -> LSN {
        let record = WALRecord(lsn: currentLSN, type: .pageWrite, pageId: pageId, data: pageData)
        try await writeRecord(record)
        let lsn = currentLSN
        currentLSN += 1
        return lsn
    }

    /// 写入分配页面记录
    /// EN: Write allocate page record.
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
    /// EN: Write free page record.
    @discardableResult
    public func writeFreeRecord(_ pageId: PageID) async throws -> LSN {
        let record = WALRecord(lsn: currentLSN, type: .freePage, pageId: pageId)
        try await writeRecord(record)
        let lsn = currentLSN
        currentLSN += 1
        return lsn
    }

    /// 写入元数据更新记录
    /// EN: Write metadata update record.
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
    /// EN: Write commit record.
    @discardableResult
    public func writeCommitRecord() async throws -> LSN {
        let record = WALRecord(lsn: currentLSN, type: .commit)
        try await writeRecord(record)
        let lsn = currentLSN
        currentLSN += 1
        return lsn
    }

    /// 写入记录（底层实现）
    /// EN: Write record (underlying implementation).
    private func writeRecord(_ record: WALRecord) async throws {
        guard let handle = fileHandle else {
            throw StorageError.fileNotOpen(path)
        }

        let data = record.marshal()
        try handle.seek(toOffset: UInt64(writeOffset))
        try handle.write(contentsOf: data)
        writeOffset += Int64(data.count)

        // 更新头部文件大小
        // EN: Update header file size
        header.fileSize = UInt64(writeOffset)
    }

    // MARK: - 检查点 / Checkpoint

    /// 创建检查点
    /// EN: Create checkpoint.
    public func checkpoint(_ lsn: LSN) async throws {
        // 写入检查点记录
        // EN: Write checkpoint record
        var data = Data(count: 8)
        DataEndian.writeUInt64LE(lsn, to: &data, at: 0)
        let record = WALRecord(lsn: currentLSN, type: .checkpoint, data: data)
        try await writeRecord(record)
        currentLSN += 1

        // 更新头部
        // EN: Update header
        header.checkpointLSN = lsn
        checkpointLSN = lsn
        try await sync()

        // 检查是否需要截断
        // EN: Check if truncation is needed
        if autoTruncate && writeOffset > StorageConstants.walTruncateThreshold {
            try await truncate()
        }
    }

    /// 截断 WAL（保留最近的检查点之后的记录）
    /// EN: Truncate WAL (keep records after the most recent checkpoint).
    public func truncate() async throws {
        guard let handle = fileHandle else { return }

        // 重置文件到只有头部
        // EN: Reset file to header only
        header.fileSize = UInt64(StorageConstants.walHeaderSize)
        header.updateChecksum()

        try writeHeader()
        try handle.truncate(atOffset: UInt64(StorageConstants.walHeaderSize))
        try handle.synchronize()

        writeOffset = Int64(StorageConstants.walHeaderSize)
    }

    // MARK: - 读取记录 / Read Records

    /// 从指定 LSN 开始读取记录
    /// EN: Read records starting from specified LSN.
    public func readRecordsFrom(_ startLSN: LSN) async throws -> [WALRecord] {
        guard let handle = fileHandle else {
            throw StorageError.fileNotOpen(path)
        }

        var records: [WALRecord] = []
        var offset: UInt64 = UInt64(StorageConstants.walHeaderSize)

        // 只读取到"最后一条有效记录末尾"（由 scanForMaxLSN 校正）
        // EN: Only read to "end of last valid record" (corrected by scanForMaxLSN)
        let endOffset = UInt64(max(writeOffset, Int64(StorageConstants.walHeaderSize)))

        while offset < endOffset {
            try handle.seek(toOffset: offset)

            // 读取记录头
            // EN: Read record header
            guard let headerData = try handle.read(upToCount: walRecordHeaderSize),
                  headerData.count == walRecordHeaderSize else {
                break
            }

            // 获取数据长度
            // EN: Get data length
            let dataLen = DataEndian.readUInt16LE(headerData, at: 10)

            // 读取完整记录
            // EN: Read complete record
            try handle.seek(toOffset: offset)
            let recordSize = walRecordHeaderSize + Int(dataLen)
            guard let recordData = try handle.read(upToCount: recordSize) else {
                break
            }

            // 解析记录
            // EN: Parse record
            do {
                let record = try WALRecord.unmarshal(recordData)

                // 只返回 >= startLSN 的记录
                // EN: Only return records >= startLSN
                if record.lsn >= startLSN {
                    records.append(record)
                }

                // 移动到下一条记录
                // EN: Move to next record
                offset += UInt64(record.totalSize)
            } catch {
                // 记录损坏，停止读取
                // EN: Record corrupted, stop reading
                break
            }
        }

        return records
    }

    // MARK: - 同步 / Sync

    /// 同步到磁盘
    /// EN: Sync to disk.
    public func sync() async throws {
        guard let handle = fileHandle else { return }
        try writeHeader()
        try handle.synchronize()
    }

    /// 写入头部
    /// EN: Write header.
    private func writeHeader() throws {
        guard let handle = fileHandle else { return }
        header.updateChecksum()
        try handle.seek(toOffset: 0)
        try handle.write(contentsOf: header.marshal())
    }

    // MARK: - 关闭 / Close

    /// 关闭 WAL
    /// EN: Close WAL.
    public func close() async throws {
        try await sync()
        try fileHandle?.close()
        fileHandle = nil
    }

    /// 模拟崩溃：直接关闭底层句柄，不写 header、不 sync（仅测试使用）
    /// EN: Simulate crash: close underlying handle directly, no header write, no sync (for testing only).
    ///
    /// 用途：复刻 Go 测试里"直接 close 文件描述符"的崩溃窗口，验证恢复逻辑不依赖正常关闭流程。
    /// EN: Purpose: Replicate Go test's "direct close file descriptor" crash window, verify recovery logic doesn't depend on normal close process.
    func simulateCrashCloseForTesting() {
        try? fileHandle?.close()
        fileHandle = nil
    }

    // MARK: - 属性访问 / Property Access

    /// 获取当前 LSN
    /// EN: Get current LSN.
    public var currentLogSequenceNumber: LSN {
        currentLSN
    }

    /// 获取检查点 LSN
    /// EN: Get checkpoint LSN.
    public var lastCheckpointLSN: LSN {
        checkpointLSN
    }

    /// 获取文件大小
    /// EN: Get file size.
    public var fileSize: Int64 {
        writeOffset
    }
}
