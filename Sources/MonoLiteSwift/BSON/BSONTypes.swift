// Created by Yanjunhui

import Foundation

// MARK: - ObjectID

/// MongoDB ObjectID（12 字节）
/// 格式：4字节时间戳 + 5字节随机值 + 3字节计数器
public struct ObjectID: Hashable, Sendable, CustomStringConvertible, Comparable {
    /// 原始字节数据
    public let bytes: Data

    /// ObjectID 的字节长度
    public static let length = 12

    /// 随机值（进程生命周期内固定）
    private static let randomBytes: Data = {
        var bytes = Data(count: 5)
        _ = bytes.withUnsafeMutableBytes { ptr in
            SecRandomCopyBytes(kSecRandomDefault, 5, ptr.baseAddress!)
        }
        return bytes
    }()

    /// 计数器（原子递增）
    private static var counter = UInt32.random(in: 0..<0xFFFFFF)
    private static let counterLock = NSLock()

    /// 生成新的 ObjectID
    public static func generate() -> ObjectID {
        var data = Data(count: 12)

        // 4 字节时间戳（大端序）
        let timestamp = UInt32(Date().timeIntervalSince1970)
        data[0] = UInt8((timestamp >> 24) & 0xFF)
        data[1] = UInt8((timestamp >> 16) & 0xFF)
        data[2] = UInt8((timestamp >> 8) & 0xFF)
        data[3] = UInt8(timestamp & 0xFF)

        // 5 字节随机值
        data[4] = randomBytes[0]
        data[5] = randomBytes[1]
        data[6] = randomBytes[2]
        data[7] = randomBytes[3]
        data[8] = randomBytes[4]

        // 3 字节计数器（大端序）
        counterLock.lock()
        let count = counter
        counter = (counter + 1) & 0xFFFFFF
        counterLock.unlock()

        data[9] = UInt8((count >> 16) & 0xFF)
        data[10] = UInt8((count >> 8) & 0xFF)
        data[11] = UInt8(count & 0xFF)

        return ObjectID(bytes: data)
    }

    /// 从字节数据创建
    public init(bytes: Data) {
        precondition(bytes.count == 12, "ObjectID must be 12 bytes")
        self.bytes = bytes
    }

    /// 从十六进制字符串创建
    public init?(hexString: String) {
        guard hexString.count == 24 else { return nil }

        var data = Data(capacity: 12)
        var index = hexString.startIndex

        for _ in 0..<12 {
            let nextIndex = hexString.index(index, offsetBy: 2)
            guard let byte = UInt8(hexString[index..<nextIndex], radix: 16) else {
                return nil
            }
            data.append(byte)
            index = nextIndex
        }

        self.bytes = data
    }

    /// 十六进制字符串表示
    public var hexString: String {
        bytes.map { String(format: "%02x", $0) }.joined()
    }

    public var description: String {
        "ObjectId(\"\(hexString)\")"
    }

    /// 时间戳
    public var timestamp: Date {
        let ts = UInt32(bytes[0]) << 24 | UInt32(bytes[1]) << 16 |
                 UInt32(bytes[2]) << 8 | UInt32(bytes[3])
        return Date(timeIntervalSince1970: TimeInterval(ts))
    }

    public static func < (lhs: ObjectID, rhs: ObjectID) -> Bool {
        lhs.bytes.lexicographicallyPrecedes(rhs.bytes)
    }
}

// MARK: - BSONTimestamp

/// MongoDB 时间戳（8 字节）
/// 格式：4字节秒 + 4字节序号
public struct BSONTimestamp: Hashable, Sendable, Comparable, CustomStringConvertible {
    /// 秒（Unix 时间戳）
    public let t: UInt32

    /// 序号
    public let i: UInt32

    public init(t: UInt32, i: UInt32) {
        self.t = t
        self.i = i
    }

    public init(timestamp: UInt64) {
        self.t = UInt32((timestamp >> 32) & 0xFFFFFFFF)
        self.i = UInt32(timestamp & 0xFFFFFFFF)
    }

    public var value: UInt64 {
        (UInt64(t) << 32) | UInt64(i)
    }

    public var description: String {
        "Timestamp(\(t), \(i))"
    }

    public static func < (lhs: BSONTimestamp, rhs: BSONTimestamp) -> Bool {
        if lhs.t != rhs.t {
            return lhs.t < rhs.t
        }
        return lhs.i < rhs.i
    }
}

// MARK: - BSONBinary

/// BSON 二进制数据
public struct BSONBinary: Hashable, Sendable {
    /// 二进制子类型
    public enum Subtype: UInt8, Sendable {
        case generic = 0x00
        case function = 0x01
        case binaryOld = 0x02
        case uuidOld = 0x03
        case uuid = 0x04
        case md5 = 0x05
        case encrypted = 0x06
        case column = 0x07
        case userDefined = 0x80
    }

    public let subtype: Subtype
    public let data: Data

    public init(subtype: Subtype = .generic, data: Data) {
        self.subtype = subtype
        self.data = data
    }

    /// 从 UUID 创建
    public init(uuid: UUID) {
        self.subtype = .uuid
        self.data = withUnsafeBytes(of: uuid.uuid) { Data($0) }
    }

    /// 转换为 UUID（仅当 subtype 为 uuid 时）
    public var uuid: UUID? {
        guard subtype == .uuid, data.count == 16 else { return nil }
        return data.withUnsafeBytes { ptr in
            UUID(uuid: ptr.load(as: uuid_t.self))
        }
    }
}

// MARK: - BSONRegex

/// BSON 正则表达式
public struct BSONRegex: Hashable, Sendable, CustomStringConvertible {
    public let pattern: String
    public let options: String

    public init(pattern: String, options: String = "") {
        self.pattern = pattern
        // 选项必须按字母顺序排列
        self.options = String(options.sorted())
    }

    public var description: String {
        "/\(pattern)/\(options)"
    }

    /// 转换为 NSRegularExpression
    public func toNSRegularExpression() throws -> NSRegularExpression {
        var nsOptions: NSRegularExpression.Options = []

        for char in options {
            switch char {
            case "i": nsOptions.insert(.caseInsensitive)
            case "m": nsOptions.insert(.anchorsMatchLines)
            case "s": nsOptions.insert(.dotMatchesLineSeparators)
            case "x": nsOptions.insert(.allowCommentsAndWhitespace)
            default: break
            }
        }

        return try NSRegularExpression(pattern: pattern, options: nsOptions)
    }
}

// MARK: - Decimal128

/// BSON Decimal128（IEEE 754-2008 128位十进制浮点数）
public struct Decimal128: Hashable, Sendable, CustomStringConvertible {
    /// 高64位
    public let high: UInt64

    /// 低64位
    public let low: UInt64

    public init(high: UInt64, low: UInt64) {
        self.high = high
        self.low = low
    }

    // MongoDB Decimal128 is IEEE 754-2008 decimal128 in BID encoding.
    // We primarily treat (high, low) as on-wire bytes and decode for comparison / display.
    // NOTE: We intentionally do NOT provide a string->decimal128 encoder here yet; values are expected to come from BSON bytes.

    public var description: String {
        toString()
    }

    /// 转换为 Double（可能损失精度）
    public var doubleValue: Double {
        switch decodedKind() {
        case .nan:
            return .nan
        case .infinity(let sign):
            return sign >= 0 ? .infinity : -.infinity
        case .finite(let sign, let coeffHigh, let coeffLow, let exp):
            if coeffHigh == 0 && coeffLow == 0 {
                return sign >= 0 ? 0.0 : -0.0
            }
            let coeffStr = Decimal128.coefficientString(high: coeffHigh, low: coeffLow)
            guard let coeff = Double(coeffStr) else {
                return sign >= 0 ? 0.0 : -0.0
            }
            let v = coeff * pow(10.0, Double(exp))
            return sign >= 0 ? v : -v
        }
    }

    // MARK: - Go-aligned decoding helpers (primitive.Decimal128)

    // Exponent range constants (Go driver)
    private static let minExp = -6176

    private enum DecodedKind {
        case nan
        case infinity(sign: Int) // +1 or -1
        case finite(sign: Int, coeffHigh: UInt64, coeffLow: UInt64, exp: Int)
    }

    private func decodedKind() -> DecodedKind {
        // Special values are encoded in the "combination field" (bits 58..62).
        let comb = (high >> 58) & 0x1F
        let sign = ((high >> 63) & 1) == 0 ? 1 : -1
        if comb == 0x1F {
            return .nan
        }
        if comb == 0x1E {
            return .infinity(sign: sign)
        }

        var expBits: UInt64
        var coeffHigh = high
        var coeffLow = low

        if ((high >> 61) & 0x3) == 0x3 {
            // Bits: 1*sign 2*ignored 14*exponent 111*significand.
            expBits = (high >> 47) & 0x3FFF
            // Spec says all of these values are out of range; treat as 0 significand.
            coeffHigh = 0
            coeffLow = 0
        } else {
            // Bits: 1*sign 14*exponent 113*significand.
            expBits = (high >> 49) & 0x3FFF
            coeffHigh = high & ((1 << 49) - 1)
        }

        let exp = Int(expBits) + Decimal128.minExp
        return .finite(sign: sign, coeffHigh: coeffHigh, coeffLow: coeffLow, exp: exp)
    }

    /// Go-aligned Decimal128 comparison: compare numeric value exactly for Decimal128 vs Decimal128.
    public func compare(to other: Decimal128) -> Int {
        let a = decodedKind()
        let b = other.decodedKind()

        // Mirror Go engine behavior: treat NaN/Inf as "invalid" (ordered before valid).
        switch (a, b) {
        case (.nan, .nan), (.infinity, .infinity):
            return 0
        case (.nan, _), (.infinity, _):
            return -1
        case (_, .nan), (_, .infinity):
            return 1
        case (.finite(let signA, let ah, let al, let expA), .finite(let signB, let bh, let bl, let expB)):
            if signA != signB {
                return signA < signB ? -1 : 1
            }
            if ah == 0 && al == 0 && bh == 0 && bl == 0 {
                return 0
            }

            let aDigits = Decimal128.coefficientDigits(high: ah, low: al)
            let bDigits = Decimal128.coefficientDigits(high: bh, low: bl)

            // Compare absolute value first, then apply sign.
            let absCmp = Decimal128.compareAbs(aDigits: aDigits, aExp: expA, bDigits: bDigits, bExp: expB)
            return signA >= 0 ? absCmp : -absCmp
        }
    }

    private static func compareAbs(aDigits: [UInt8], aExp: Int, bDigits: [UInt8], bExp: Int) -> Int {
        let aMag = aDigits.count + aExp
        let bMag = bDigits.count + bExp
        if aMag != bMag {
            return aMag < bMag ? -1 : 1
        }

        if aExp > bExp {
            return compareDigits(aDigits, appendingZeros: aExp - bExp, to: bDigits)
        } else if bExp > aExp {
            return -compareDigits(bDigits, appendingZeros: bExp - aExp, to: aDigits)
        } else {
            // Same exponent; compare coefficient digits directly.
            let n = min(aDigits.count, bDigits.count)
            for i in 0..<n {
                if aDigits[i] != bDigits[i] {
                    return aDigits[i] < bDigits[i] ? -1 : 1
                }
            }
            if aDigits.count == bDigits.count { return 0 }
            return aDigits.count < bDigits.count ? -1 : 1
        }
    }

    /// Compare `aDigits * 10^zeros` with `bDigits` (assuming they have the same effective digit length).
    private static func compareDigits(_ aDigits: [UInt8], appendingZeros zeros: Int, to bDigits: [UInt8]) -> Int {
        // When magnitudes match, aDigits.count + zeros == bDigits.count.
        let total = bDigits.count
        for i in 0..<total {
            let ad: UInt8 = i < aDigits.count ? aDigits[i] : 0
            let bd: UInt8 = bDigits[i]
            if ad != bd {
                return ad < bd ? -1 : 1
            }
        }
        return 0
    }

    // MARK: - Formatting

    private func toString() -> String {
        switch decodedKind() {
        case .nan:
            return "NaN"
        case .infinity(let sign):
            return sign >= 0 ? "Infinity" : "-Infinity"
        case .finite(let sign, let coeffHigh, let coeffLow, let exp):
            // For debugging/display only; we keep this lightweight and Go-aligned.
            if coeffHigh == 0 && coeffLow == 0 {
                if sign < 0 { return "-0" }
                return "0"
            }
            let coeffStr = Decimal128.coefficientString(high: coeffHigh, low: coeffLow)
            if exp == 0 {
                return sign < 0 ? "-\(coeffStr)" : coeffStr
            }
            // Scientific form (simple): coeff * 10^exp
            let base = sign < 0 ? "-\(coeffStr)" : coeffStr
            return exp > 0 ? "\(base)E+\(exp)" : "\(base)E\(exp)"
        }
    }

    // MARK: - 128-bit coefficient helpers (BID)

    /// Convert the (up to 113-bit) coefficient into a decimal string.
    private static func coefficientString(high: UInt64, low: UInt64) -> String {
        let digits = coefficientDigits(high: high, low: low)
        if digits.isEmpty { return "0" }
        return digits.map { String($0) }.joined()
    }

    /// Convert the coefficient into decimal digit array (each 0...9).
    private static func coefficientDigits(high: UInt64, low: UInt64) -> [UInt8] {
        if high == 0 && low == 0 { return [0] }
        var h = high
        var l = low
        var parts: [UInt32] = []
        parts.reserveCapacity(6)
        while h != 0 || l != 0 {
            let (qh, ql, rem) = divmod(h, l, 1_000_000_000)
            parts.append(rem)
            h = qh
            l = ql
        }
        // Build digits from most-significant chunk to least.
        var out: [UInt8] = []
        out.reserveCapacity(parts.count * 9)
        for idx in parts.indices.reversed() {
            let v = parts[idx]
            if idx == parts.indices.last {
                // Most-significant: no zero padding.
                out.append(contentsOf: String(v).utf8.map { $0 - 48 })
            } else {
                let s = String(format: "%09u", v)
                out.append(contentsOf: s.utf8.map { $0 - 48 })
            }
        }
        // Strip leading zeros if any.
        while out.count > 1, out.first == 0 { out.removeFirst() }
        return out
    }

    private static func divmod(_ h: UInt64, _ l: UInt64, _ div: UInt32) -> (qh: UInt64, ql: UInt64, rem: UInt32) {
        let div64 = UInt64(div)
        let a = h >> 32
        let aq = a / div64
        let ar = a % div64
        let b = (ar << 32) + (h & ((1 << 32) - 1))
        let bq = b / div64
        let br = b % div64
        let c = (br << 32) + (l >> 32)
        let cq = c / div64
        let cr = c % div64
        let d = (cr << 32) + (l & ((1 << 32) - 1))
        let dq = d / div64
        let dr = d % div64
        return ((aq << 32) | bq, (cq << 32) | dq, UInt32(dr))
    }
}

// MARK: - MinKey / MaxKey

/// BSON MinKey（最小值）
public struct MinKey: Hashable, Sendable, CustomStringConvertible {
    public static let shared = MinKey()

    private init() {}

    public var description: String { "MinKey" }
}

/// BSON MaxKey（最大值）
public struct MaxKey: Hashable, Sendable, CustomStringConvertible {
    public static let shared = MaxKey()

    private init() {}

    public var description: String { "MaxKey" }
}

// MARK: - BSONNull

/// BSON Null
public struct BSONNull: Hashable, Sendable, CustomStringConvertible {
    public static let shared = BSONNull()

    private init() {}

    public var description: String { "null" }
}

// MARK: - BSONUndefined

/// BSON Undefined（已废弃，但仍需支持）
public struct BSONUndefined: Hashable, Sendable, CustomStringConvertible {
    public static let shared = BSONUndefined()

    private init() {}

    public var description: String { "undefined" }
}

// MARK: - JavaScript

/// BSON JavaScript 代码
public struct BSONJavaScript: Hashable, Sendable, CustomStringConvertible {
    public let code: String

    public init(code: String) {
        self.code = code
    }

    public var description: String {
        "JavaScript(\"\(code)\")"
    }
}

/// BSON JavaScript 代码（带作用域）
public struct BSONJavaScriptWithScope: Hashable, Sendable {
    public let code: String
    public let scope: BSONDocument

    public init(code: String, scope: BSONDocument) {
        self.code = code
        self.scope = scope
    }
}

// MARK: - DBPointer（已废弃）

/// BSON DBPointer（已废弃）
public struct BSONDBPointer: Hashable, Sendable {
    public let ref: String
    public let id: ObjectID

    public init(ref: String, id: ObjectID) {
        self.ref = ref
        self.id = id
    }
}

// MARK: - Symbol（已废弃）

/// BSON Symbol（已废弃）
public struct BSONSymbol: Hashable, Sendable, CustomStringConvertible {
    public let value: String

    public init(_ value: String) {
        self.value = value
    }

    public var description: String {
        "Symbol(\"\(value)\")"
    }
}
