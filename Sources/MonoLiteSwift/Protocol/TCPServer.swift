// Created by Yanjunhui
//
// Go parity: protocol/server.go (TCP listener + accept loop + per-connection handler).
// This file intentionally uses POSIX sockets (Darwin/Glibc) to avoid external dependencies.

import Foundation

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

private actor MongoWireTCPServerState {
    var stopped: Bool = false
    var openConns: Set<Int32> = []

    func addConn(_ fd: Int32) {
        openConns.insert(fd)
    }

    func removeConn(_ fd: Int32) {
        openConns.remove(fd)
    }

    func stopAndDrainConns() -> [Int32] {
        stopped = true
        let conns = Array(openConns)
        openConns.removeAll()
        return conns
    }
}

public final class MongoWireTCPServer: @unchecked Sendable {
    private let bindHost: String
    private let bindPort: UInt16
    private let db: Database

    private var listenFD: Int32 = -1
    private var acceptThread: Thread?
    private let state = MongoWireTCPServerState()

    private let connGroup = DispatchGroup()

    /// `addr` format: "host:port" (e.g., "127.0.0.1:27017") or ":0".
    public init(addr: String, db: Database) throws {
        let (h, p) = try MongoWireTCPServer.parseAddr(addr)
        self.bindHost = h
        self.bindPort = p
        self.db = db
    }

    deinit {
        // Best-effort: close listener fd.
        if listenFD >= 0 {
            shutdown(listenFD, SHUT_RDWR)
            close(listenFD)
        }
    }

    /// Returns the actual listening address after `start()` (useful when binding to port 0).
    public var addr: String {
        if listenFD < 0 {
            return "\(bindHost):\(bindPort)"
        }
        if let p = try? Self.getSockNamePort(listenFD) {
            return "\(bindHost):\(p)"
        }
        return "\(bindHost):\(bindPort)"
    }

    public func start() async throws {
        if listenFD >= 0 { return }
        if await state.stopped {
            throw MonoError.illegalOperation("server already stopped")
        }

        let fd = socket(AF_INET, Int32(SOCK_STREAM), 0)
        guard fd >= 0 else {
            throw MonoError.internalError("socket() failed: \(errno)")
        }

        var one: Int32 = 1
        _ = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, socklen_t(MemoryLayout.size(ofValue: one)))

        var addrIn = sockaddr_in()
        addrIn.sin_family = sa_family_t(AF_INET)
        addrIn.sin_port = bindPort.bigEndian
        addrIn.sin_addr = in_addr(s_addr: 0)

        let host = bindHost.isEmpty ? "0.0.0.0" : bindHost
        if host == "localhost" {
            addrIn.sin_addr = in_addr(s_addr: inet_addr("127.0.0.1"))
        } else {
            let res = host.withCString { cs in
                inet_pton(AF_INET, cs, &addrIn.sin_addr)
            }
            guard res == 1 else {
                close(fd)
                throw MonoError.badValue("invalid listen host: \(bindHost)")
            }
        }

        var addrSock = sockaddr()
        memcpy(&addrSock, &addrIn, MemoryLayout<sockaddr_in>.size)
        let bindRes = withUnsafePointer(to: &addrSock) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { ptr in
                bind(fd, ptr, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard bindRes == 0 else {
            let e = errno
            close(fd)
            throw MonoError.internalError("bind() failed: \(e)")
        }

        guard listen(fd, 128) == 0 else {
            let e = errno
            close(fd)
            throw MonoError.internalError("listen() failed: \(e)")
        }

        listenFD = fd

        let thread = Thread { [weak self] in
            self?.acceptLoop(listenFD: fd)
        }
        thread.name = "MongoWireTCPServer.acceptLoop"
        acceptThread = thread
        thread.start()
    }

    public func stop() async {
        let conns = await state.stopAndDrainConns()

        let fd = listenFD
        listenFD = -1

        if fd >= 0 {
            shutdown(fd, SHUT_RDWR)
            close(fd)
        }

        // Close all open connections so handlers can exit.
        for c in conns {
            shutdown(c, SHUT_RDWR)
            close(c)
        }

        // Wait for all connection handlers to finish (Swift 6: don't block in async context).
        await waitForConnections()
    }

    private func waitForConnections() async {
        await withCheckedContinuation { cont in
            DispatchQueue.global(qos: .background).async { [connGroup] in
                connGroup.wait()
                cont.resume()
            }
        }
    }

    // MARK: - accept loop

    private func acceptLoop(listenFD: Int32) {
        while true {
            var addr = sockaddr()
            var len: socklen_t = socklen_t(MemoryLayout<sockaddr>.size)
            let cfd = withUnsafeMutablePointer(to: &addr) { ptr in
                ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sa in
                    accept(listenFD, sa, &len)
                }
            }
            if cfd < 0 {
                // If we closed the listener, accept will fail; exit.
                return
            }

            Task { await state.addConn(cfd) }

            connGroup.enter()
            Task.detached(priority: .background) { [weak self] in
                defer { self?.connGroup.leave() }
                await self?.handleConnection(connFD: cfd)
            }
        }
    }

    private func handleConnection(connFD: Int32) async {
        defer {
            Task { await state.removeConn(connFD) }
            shutdown(connFD, SHUT_RDWR)
            close(connFD)
        }

        while true {
            if await state.stopped { return }

            let msg: WireMessage
            do {
                msg = try readWireMessage(fd: connFD)
            } catch {
                return
            }

            do {
                if let resp = try await ProtocolServer.handle(message: msg, db: db) {
                    try writeAll(fd: connFD, data: resp.bytes)
                }
            } catch {
                // Go parity: attempt to send a structured OP_MSG error response, then keep reading.
                do {
                    let errDoc = ProtocolServer.structuredErrorResponse(error)
                    let errMsg = try OpMsgParser.buildReply(requestId: msg.header.requestId, responseDoc: errDoc)
                    try writeAll(fd: connFD, data: errMsg.bytes)
                } catch {
                    return
                }
            }
        }
    }

    // MARK: - Wire I/O

    private func readWireMessage(fd: Int32) throws -> WireMessage {
        let headerBytes = try readExact(fd: fd, count: 16)
        let msgLen = Int(Int32(bitPattern: DataEndian.readUInt32LE(headerBytes, at: 0)))
        if msgLen < 16 || msgLen > 48 * 1024 * 1024 {
            throw MonoError.protocolError("invalid messageLength: \(msgLen)")
        }
        let body = try readExact(fd: fd, count: msgLen - 16)
        var full = Data()
        full.reserveCapacity(msgLen)
        full.append(headerBytes)
        full.append(body)
        return try WireMessage.parse(full)
    }

    private func readExact(fd: Int32, count: Int) throws -> Data {
        var buf = Data(count: count)
        var readTotal = 0
        while readTotal < count {
            let n = buf.withUnsafeMutableBytes { ptr in
                let base = ptr.baseAddress!.assumingMemoryBound(to: UInt8.self).advanced(by: readTotal)
                return recv(fd, base, count - readTotal, 0)
            }
            if n == 0 {
                throw MonoError.protocolError("unexpected EOF")
            }
            if n < 0 {
                throw MonoError.protocolError("recv failed: \(errno)")
            }
            readTotal += n
        }
        return buf
    }

    private func writeAll(fd: Int32, data: Data) throws {
        var sent = 0
        while sent < data.count {
            let n = data.withUnsafeBytes { ptr in
                let base = ptr.baseAddress!.assumingMemoryBound(to: UInt8.self).advanced(by: sent)
                return send(fd, base, data.count - sent, 0)
            }
            if n <= 0 {
                throw MonoError.protocolError("send failed: \(errno)")
            }
            sent += n
        }
    }

    // MARK: - Addr parsing

    private static func parseAddr(_ addr: String) throws -> (host: String, port: UInt16) {
        // Very small parser: split on last ":".
        guard let idx = addr.lastIndex(of: ":") else {
            throw MonoError.badValue("invalid addr (expected host:port): \(addr)")
        }
        let host = String(addr[..<idx])
        let portStr = String(addr[addr.index(after: idx)...])
        guard let p = UInt16(portStr) else {
            throw MonoError.badValue("invalid port: \(portStr)")
        }
        return (host, p)
    }

    private static func getSockNamePort(_ fd: Int32) throws -> UInt16 {
        var addr = sockaddr_in()
        var len: socklen_t = socklen_t(MemoryLayout<sockaddr_in>.size)
        let res = withUnsafeMutablePointer(to: &addr) { ptr in
            ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sa in
                getsockname(fd, sa, &len)
            }
        }
        guard res == 0 else {
            throw MonoError.internalError("getsockname failed: \(errno)")
        }
        return UInt16(bigEndian: addr.sin_port)
    }
}


