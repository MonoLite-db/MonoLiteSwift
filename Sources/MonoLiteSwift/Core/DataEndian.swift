// Created by Yanjunhui

import Foundation

/// Data 的无对齐字节序读写工具
/// EN: Unaligned byte order read/write utilities for Data
///
/// 说明：
/// EN: Description:
/// - 在 arm64 上，`UnsafeRawPointer.load(fromByteOffset:as:)` 可能因为未对齐而直接崩溃（fatal error: load from misaligned raw pointer）。
/// - EN: On arm64, `UnsafeRawPointer.load(fromByteOffset:as:)` may crash due to misalignment (fatal error: load from misaligned raw pointer).
/// - 存储层（Page/WAL/BTree 等）大量按字节布局读写，需要**严格避免未对齐 load/store**。
/// - EN: Storage layers (Page/WAL/BTree, etc.) heavily rely on byte-layout read/write, requiring **strict avoidance of misaligned load/store**.
public enum DataEndian {
    /// 小端读取 UInt16
    /// EN: Read UInt16 in little-endian
    public static func readUInt16LE(_ data: Data, at offset: Int) -> UInt16 {
        let b0 = UInt16(data[offset])
        let b1 = UInt16(data[offset + 1]) << 8
        return b0 | b1
    }

    /// 小端读取 UInt32
    /// EN: Read UInt32 in little-endian
    public static func readUInt32LE(_ data: Data, at offset: Int) -> UInt32 {
        let b0 = UInt32(data[offset])
        let b1 = UInt32(data[offset + 1]) << 8
        let b2 = UInt32(data[offset + 2]) << 16
        let b3 = UInt32(data[offset + 3]) << 24
        return b0 | b1 | b2 | b3
    }

    /// 小端读取 UInt64
    /// EN: Read UInt64 in little-endian
    public static func readUInt64LE(_ data: Data, at offset: Int) -> UInt64 {
        var v: UInt64 = 0
        v |= UInt64(data[offset])
        v |= UInt64(data[offset + 1]) << 8
        v |= UInt64(data[offset + 2]) << 16
        v |= UInt64(data[offset + 3]) << 24
        v |= UInt64(data[offset + 4]) << 32
        v |= UInt64(data[offset + 5]) << 40
        v |= UInt64(data[offset + 6]) << 48
        v |= UInt64(data[offset + 7]) << 56
        return v
    }

    /// 小端读取 Int64
    /// EN: Read Int64 in little-endian
    public static func readInt64LE(_ data: Data, at offset: Int) -> Int64 {
        Int64(bitPattern: readUInt64LE(data, at: offset))
    }

    /// 小端写入 UInt16
    /// EN: Write UInt16 in little-endian
    public static func writeUInt16LE(_ value: UInt16, to data: inout Data, at offset: Int) {
        data[offset] = UInt8(value & 0xFF)
        data[offset + 1] = UInt8((value >> 8) & 0xFF)
    }

    /// 小端写入 UInt32
    /// EN: Write UInt32 in little-endian
    public static func writeUInt32LE(_ value: UInt32, to data: inout Data, at offset: Int) {
        data[offset] = UInt8(value & 0xFF)
        data[offset + 1] = UInt8((value >> 8) & 0xFF)
        data[offset + 2] = UInt8((value >> 16) & 0xFF)
        data[offset + 3] = UInt8((value >> 24) & 0xFF)
    }

    /// 小端写入 UInt64
    /// EN: Write UInt64 in little-endian
    public static func writeUInt64LE(_ value: UInt64, to data: inout Data, at offset: Int) {
        data[offset] = UInt8(value & 0xFF)
        data[offset + 1] = UInt8((value >> 8) & 0xFF)
        data[offset + 2] = UInt8((value >> 16) & 0xFF)
        data[offset + 3] = UInt8((value >> 24) & 0xFF)
        data[offset + 4] = UInt8((value >> 32) & 0xFF)
        data[offset + 5] = UInt8((value >> 40) & 0xFF)
        data[offset + 6] = UInt8((value >> 48) & 0xFF)
        data[offset + 7] = UInt8((value >> 56) & 0xFF)
    }

    /// 小端写入 Int64
    /// EN: Write Int64 in little-endian
    public static func writeInt64LE(_ value: Int64, to data: inout Data, at offset: Int) {
        writeUInt64LE(UInt64(bitPattern: value), to: &data, at: offset)
    }
}
