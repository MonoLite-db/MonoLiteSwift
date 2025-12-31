// Created by Yanjunhui
//
// Go 兼容性：protocol/server.go（TCP 监听器 + accept 循环 + 每连接处理器）
// EN: Go parity: protocol/server.go (TCP listener + accept loop + per-connection handler).
// 此文件特意使用 POSIX sockets (Darwin/Glibc) 以避免外部依赖
// EN: This file intentionally uses POSIX sockets (Darwin/Glibc) to avoid external dependencies.

import Foundation

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

// MARK: - TCP 服务器状态 / TCP Server State

/// TCP 服务器内部状态管理器（Actor 保证线程安全）
/// EN: TCP server internal state manager (Actor ensures thread safety).
private actor MongoWireTCPServerState {
    /// 服务器是否已停止
    /// EN: Whether the server has stopped
    var stopped: Bool = false
    /// 当前打开的连接文件描述符集合
    /// EN: Set of currently open connection file descriptors
    var openConns: Set<Int32> = []

    /// 添加连接
    /// EN: Add connection.
    func addConn(_ fd: Int32) {
        openConns.insert(fd)
    }

    /// 移除连接
    /// EN: Remove connection.
    func removeConn(_ fd: Int32) {
        openConns.remove(fd)
    }

    /// 停止服务器并返回所有打开的连接以便关闭
    /// EN: Stop the server and return all open connections for closing.
    func stopAndDrainConns() -> [Int32] {
        stopped = true
        let conns = Array(openConns)
        openConns.removeAll()
        return conns
    }
}

// MARK: - MongoDB Wire Protocol TCP 服务器 / MongoDB Wire Protocol TCP Server

/// MongoDB Wire Protocol TCP 服务器
/// EN: MongoDB Wire Protocol TCP Server.
public final class MongoWireTCPServer: @unchecked Sendable {
    /// 绑定主机地址
    /// EN: Bind host address
    private let bindHost: String
    /// 绑定端口
    /// EN: Bind port
    private let bindPort: UInt16
    /// 数据库实例
    /// EN: Database instance
    private let db: Database

    /// 监听套接字文件描述符
    /// EN: Listen socket file descriptor
    private var listenFD: Int32 = -1
    /// accept 循环线程
    /// EN: Accept loop thread
    private var acceptThread: Thread?
    /// 服务器状态
    /// EN: Server state
    private let state = MongoWireTCPServerState()

    /// 连接等待组（用于优雅关闭）
    /// EN: Connection wait group (for graceful shutdown)
    private let connGroup = DispatchGroup()

    /// 初始化 TCP 服务器
    /// EN: Initialize TCP server.
    /// - Parameters:
    ///   - addr: 地址格式："host:port"（如 "127.0.0.1:27017"）或 ":0" / EN: Address format: "host:port" (e.g., "127.0.0.1:27017") or ":0"
    ///   - db: 数据库实例 / EN: Database instance
    public init(addr: String, db: Database) throws {
        let (h, p) = try MongoWireTCPServer.parseAddr(addr)
        self.bindHost = h
        self.bindPort = p
        self.db = db
    }

    deinit {
        // 尽力关闭监听套接字 / EN: Best-effort: close listener fd.
        if listenFD >= 0 {
            shutdown(listenFD, SHUT_RDWR)
            close(listenFD)
        }
    }

    /// 获取实际监听地址（在调用 start() 后，对绑定端口 0 的情况很有用）
    /// EN: Returns the actual listening address after start() (useful when binding to port 0).
    public var addr: String {
        if listenFD < 0 {
            return "\(bindHost):\(bindPort)"
        }
        if let p = try? Self.getSockNamePort(listenFD) {
            return "\(bindHost):\(p)"
        }
        return "\(bindHost):\(bindPort)"
    }

    // MARK: - 服务器生命周期 / Server Lifecycle

    /// 启动 TCP 服务器
    /// EN: Start the TCP server.
    public func start() async throws {
        if listenFD >= 0 { return }
        if await state.stopped {
            throw MonoError.illegalOperation("server already stopped")
        }

        // 创建套接字 / EN: Create socket
        let fd = socket(AF_INET, Int32(SOCK_STREAM), 0)
        guard fd >= 0 else {
            throw MonoError.internalError("socket() failed: \(errno)")
        }

        // 设置 SO_REUSEADDR 选项 / EN: Set SO_REUSEADDR option
        var one: Int32 = 1
        _ = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, socklen_t(MemoryLayout.size(ofValue: one)))

        // 配置地址结构 / EN: Configure address structure
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

        // 绑定套接字 / EN: Bind socket
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

        // 开始监听 / EN: Start listening
        guard listen(fd, 128) == 0 else {
            let e = errno
            close(fd)
            throw MonoError.internalError("listen() failed: \(e)")
        }

        listenFD = fd

        // 启动 accept 循环线程 / EN: Start accept loop thread
        let thread = Thread { [weak self] in
            self?.acceptLoop(listenFD: fd)
        }
        thread.name = "MongoWireTCPServer.acceptLoop"
        acceptThread = thread
        thread.start()
    }

    /// 停止 TCP 服务器
    /// EN: Stop the TCP server.
    public func stop() async {
        let conns = await state.stopAndDrainConns()

        let fd = listenFD
        listenFD = -1

        // 关闭监听套接字 / EN: Close listening socket
        if fd >= 0 {
            shutdown(fd, SHUT_RDWR)
            close(fd)
        }

        // 关闭所有打开的连接以便处理器可以退出
        // EN: Close all open connections so handlers can exit.
        for c in conns {
            shutdown(c, SHUT_RDWR)
            close(c)
        }

        // 等待所有连接处理器完成（Swift 6：不在 async 上下文中阻塞）
        // EN: Wait for all connection handlers to finish (Swift 6: don't block in async context).
        await waitForConnections()
    }

    /// 等待所有连接处理完成
    /// EN: Wait for all connections to finish processing.
    private func waitForConnections() async {
        await withCheckedContinuation { cont in
            DispatchQueue.global(qos: .background).async { [connGroup] in
                connGroup.wait()
                cont.resume()
            }
        }
    }

    // MARK: - Accept 循环 / Accept Loop

    /// Accept 循环：接受新连接并为每个连接启动处理任务
    /// EN: Accept loop: accept new connections and start handler task for each.
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
                // 如果关闭了监听器，accept 会失败；退出循环
                // EN: If we closed the listener, accept will fail; exit.
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

    /// 读取超时（5 分钟，与 Go 版本对齐）
    /// EN: Read timeout (5 minutes, aligned with Go version)
    private static let readTimeoutSeconds: Int = 5 * 60

    // MARK: - 连接处理 / Connection Handling

    /// 处理单个客户端连接
    /// EN: Handle a single client connection.
    private func handleConnection(connFD: Int32) async {
        defer {
            Task { await state.removeConn(connFD) }
            shutdown(connFD, SHUT_RDWR)
            close(connFD)
        }

        // 设置读取超时（Go 兼容性：5 分钟）
        // EN: Set read timeout (Go parity: 5 minutes)
        Self.setReadTimeout(fd: connFD, seconds: Self.readTimeoutSeconds)

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
                // Go 兼容性：尝试发送结构化 OP_MSG 错误响应，然后继续读取
                // EN: Go parity: attempt to send a structured OP_MSG error response, then keep reading.
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

    // MARK: - Wire 协议 I/O / Wire Protocol I/O

    /// 从套接字读取完整的 Wire 消息
    /// EN: Read a complete Wire message from the socket.
    private func readWireMessage(fd: Int32) throws -> WireMessage {
        // 读取 16 字节消息头 / EN: Read 16-byte message header
        let headerBytes = try readExact(fd: fd, count: 16)
        let msgLen = Int(Int32(bitPattern: DataEndian.readUInt32LE(headerBytes, at: 0)))
        // 消息长度校验 / EN: Message length validation
        if msgLen < 16 || msgLen > 48 * 1024 * 1024 {
            throw MonoError.protocolError("invalid messageLength: \(msgLen)")
        }
        // 读取消息体 / EN: Read message body
        let body = try readExact(fd: fd, count: msgLen - 16)
        var full = Data()
        full.reserveCapacity(msgLen)
        full.append(headerBytes)
        full.append(body)
        return try WireMessage.parse(full)
    }

    /// 精确读取指定字节数
    /// EN: Read exact number of bytes.
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

    /// 写入所有数据到套接字
    /// EN: Write all data to the socket.
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

    // MARK: - 地址解析 / Address Parsing

    /// 解析地址字符串为主机和端口
    /// EN: Parse address string into host and port.
    private static func parseAddr(_ addr: String) throws -> (host: String, port: UInt16) {
        // 简单解析器：在最后一个 ":" 处分割
        // EN: Very small parser: split on last ":"
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

    /// 获取套接字绑定的实际端口
    /// EN: Get the actual port bound to the socket.
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

    /// 设置读取超时（与 Go 的 SetReadDeadline 对齐）
    /// EN: Set read timeout (aligned with Go's SetReadDeadline).
    private static func setReadTimeout(fd: Int32, seconds: Int) {
        var tv = timeval()
        tv.tv_sec = seconds
        tv.tv_usec = 0
        _ = setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, socklen_t(MemoryLayout<timeval>.size))
    }
}
