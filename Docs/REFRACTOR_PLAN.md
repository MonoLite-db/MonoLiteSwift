Created by Yanjunhui

## 目标

以 Go 版 MonoLite（`/Users/hui/Documents/GoProject/MonoLite`）为唯一权威实现，将 MonoLiteSwift 逐步重构到 **行为一致 + 内部结构尽量一致（C 级别对齐）**。

该文档是“分阶段实施计划 + 验收标准”，用于推动长期重构，而不是一次性大爆改。

---

## 已完成里程碑（P0：存储层可用且与 Go 对齐）

- **文件格式/常量**：Magic、PageId=0 哨兵、PageHeader 布局与 checksum
- **SlottedPage**：ItemCount/slotIndex 稳定、updateRecord 语义对齐
- **KeyString**：类型标记、bool 编码、binary、字符串 escape、降序 invert 语义对齐 + 单测
- **BTree**：split/leaf 链表/delete/fixUnderflow/search 路由语义对齐 + 单测
- **WAL/Pager**：scanForMaxLSN、writeOffset、redo 起点、allocPageTypes + alloc-init、ensureFileSize（半页修复）、以及“free list 复用崩溃窗口”恢复单测
- **arm64 稳定性**：消除存储层 misaligned raw pointer 崩溃（统一 DataEndian）

验收：`swift test` 通过，且包含对齐单测（BTree/KeyString/Recovery）。

---

## P1：Engine 基础对齐（让 Swift 拥有 Go 同款数据库对象与 catalog 生命周期）

### 要做什么

- **新增 `Database`（Go: `engine/database.go`）**
  - open/close
  - catalog 读写（page1 catalog / catalog BSON）
  - collections 管理（create/drop/list、namespace 规则）
  - 与 Pager 的生命周期绑定（flush/checkpoint/close）
- **补齐 Validate（Go: `engine/validate.go`）**
  - free list 无环/无重复/页类型一致
  - catalog BSON 可解析
  - 数据页链表 prev/next 一致
  - index BTree 结构基本不变量
- **补齐索引元数据持久化语义**
  - `listIndexes` 输出结构与 Mongo 兼容（包含用户索引信息，不仅仅 `_id_`）

### 验收标准

- Swift 有 `Database` 入口，能 open/create collection，并可在重启后通过 catalog 恢复 collections + indexes
- `Database.validate()` 能返回类似 Go 的结构校验结果（至少 errors/warnings/stats）

---

## P2：事务/会话对齐（Go: `engine/transaction.go` / `engine/session.go`）

### 要做什么

- Session/Transaction 类型
- begin/commit/abort 语义与 WAL 的提交点对齐
- 并发控制：至少保证单进程并发下的可重复读/写原子性（先做最小闭环，再逐步增强）

### 验收标准

- 基础事务用例通过（insert+index+abort 回滚；commit 后重启可恢复）
- 并发下无明显一致性破坏（通过小型 stress 测试）

---

## P3：MongoDB Wire Protocol（Go: `protocol/`）

### 要做什么

- TCP server + handshake（hello/isMaster）
- OP_MSG 编解码
- 命令路由层：find/insert/update/delete/getMore/killCursors/createIndexes/dropIndexes/listIndexes
- 错误/响应格式与 Go 行为对齐

### 验收标准

- 使用官方 MongoDB driver 能连接并执行基本 CRUD + index
- 关键错误码/消息对齐（duplicate key, ns not found 等）

---

## P4：对齐回归测试体系（对标 Go tests）

### 要做什么

- 引入“高价值 goldstandard”用例（数值/KeyString/协议响应）
- 引入部分 Mongo CRUD unified tests（优先 insert/find/update/delete + index）

### 验收标准

- Swift 能运行一组稳定的对齐回归测试，作为后续重构的安全网


