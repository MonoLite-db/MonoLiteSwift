// Created by Yanjunhui

import Foundation

/// CRC32C (Castagnoli) implementation used by MongoDB OP_MSG checksum.
public enum CRC32C {
    // reversed Castagnoli polynomial: 0x82F63B78
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

    public static func checksum<D: DataProtocol>(_ data: D) -> UInt32 {
        var crc: UInt32 = 0xFFFFFFFF
        for b in data {
            let idx = Int((crc ^ UInt32(b)) & 0xFF)
            crc = table[idx] ^ (crc >> 8)
        }
        return ~crc
    }
}


