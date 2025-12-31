// Created by Yanjunhui

import Foundation

// MARK: - CRC32C 校验和 / CRC32C Checksum

/// CRC32C (Castagnoli) 校验和实现，用于 MongoDB OP_MSG 消息校验
/// EN: CRC32C (Castagnoli) checksum implementation used by MongoDB OP_MSG checksum.
public enum CRC32C {
    /// 反转的 Castagnoli 多项式: 0x82F63B78
    /// EN: Reversed Castagnoli polynomial: 0x82F63B78
    private static let table: [UInt32] = {
        var t = [UInt32](repeating: 0, count: 256)
        for i in 0..<256 {
            var c = UInt32(i)
            for _ in 0..<8 {
                if (c & 1) != 0 {
                    c = 0x82F63B78 ^ (c >> 1)
                } else {
                    c >>= 1
                }
            }
            t[i] = c
        }
        return t
    }()

    /// 计算数据的 CRC32C 校验和
    /// EN: Calculate CRC32C checksum for the given data.
    /// - Parameter data: 要计算校验和的数据 / EN: Data to calculate checksum for
    /// - Returns: 32位校验和值 / EN: 32-bit checksum value
    public static func checksum<D: DataProtocol>(_ data: D) -> UInt32 {
        var crc: UInt32 = 0xFFFFFFFF  // 初始值 / EN: Initial value
        for b in data {
            let idx = Int((crc ^ UInt32(b)) & 0xFF)
            crc = table[idx] ^ (crc >> 8)
        }
        return ~crc  // 最终取反 / EN: Final complement
    }
}
