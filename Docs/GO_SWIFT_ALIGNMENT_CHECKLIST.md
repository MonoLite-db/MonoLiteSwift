Created by Yanjunhui

## 目标

以 Go 版 MonoLite（`/Users/hui/Documents/GoProject/MonoLite`）为**唯一权威实现**，对当前 Swift 包 MonoLiteSwift 做 **C 级别对齐**：

- **行为一致**（CRUD/索引/事务/WAL/恢复/查询）
- **实现尽量一致**（模块结构、算法、关键数据结构）
- **存储格式尽量字节级一致**（`.monodb` 数据文件 + `.wal`）

---

## 模块映射（Go → Swift）

- **`storage/` → `Sources/MonoLiteSwift/Storage/`**
  - `page.go` → `Page.swift`、`SlottedPage.swift`
  - `pager.go` → `Pager.swift`、`FileHeader.swift`
  - `wal.go` → `WAL.swift`、`WALRecord.swift`
  - `btree.go` → `BTree.swift`、`BTreeNode.swift`
  - `keystring.go` → `KeyString.swift`
- **`engine/` → `Sources/MonoLiteSwift/Engine/`**
  - `collection.go` → `MonoCollection.swift`（已实现：基础 CRUD/索引更新，但与 Go 的语义仍有差距）
  - `index.go` → `Index.swift`、`IndexManager.swift`（已实现：BTree + KeyString；需继续对齐 listIndexes / index meta 持久化语义）
  - `cursor.go` → `Cursor.swift`（已实现：支持 cursor/getMore/killCursors 基本语义；仍需继续对齐更多边界行为）
  - `bson_compare.go` → `BSONCompare.swift`（已实现：基础类型排序/比较）
  - `limits.go` → `Limits.swift`（已实现：部分限制项）
  - `errors.go` → `MonoError.swift`、`ErrorCodes.swift`（已实现：错误码/消息框架；仍需按 Go 校准错误场景与消息细节）
  - `database.go` → `Database.swift` + `DatabaseCommands.swift` + `DatabaseValidate.swift`（已实现：catalog/collection 生命周期/validate/命令路由骨架）
  - `transaction.go` / `session.go` → `Transaction/`（已实现：LockManager/Transaction/TransactionManager/SessionManager + runCommand 集成；已对齐：txnNumber 校验、commit/abort 幂等性、过期/关闭时中止活跃事务、LockManager 超时 Task 可取消避免测试挂起）
  - **并发写入串行化**：`SerialWriteQueue` 解决 actor 重入导致的竞态问题；`MonoCollection` 和 `IndexManager` 的写操作（insert/update/delete/createIndex）均使用 writeQueue 保护，确保数据页+索引操作原子性，对齐 Go 的 `sync.RWMutex` 保护策略
  - `aggregate.go` → `Aggregate/Pipeline.swift`（已实现：$match/$project/$sort/$limit/$skip/$group/$count/$unwind/$addFields/$unset/$replaceRoot/$lookup 等，基本对齐 Go）
  - `explain.go` → `Engine/Explain.swift`（已实现：ExplainResult + Collection.explain + explainCommand；COLLSCAN 阶段，与 Go 一致）
  - `logger.go` → `Core/Logger.swift`（已实现：MonoLogger 结构化 JSON 日志、分级输出、慢操作检测）
  - `roundtrip_test.go` → `BSONRoundtripTests.swift`（新增：BSON 类型保持、document update roundtrip）
  - `index_nonunique_test.go` → `IndexNonUniqueTests.swift`（新增：非唯一索引允许重复键、查询验证）
  - `aggregate_test.go` → `AggregateAccumulatorTests.swift`（新增：$avg/$min/$max/$push/$addToSet 累加器）
- **`protocol/` → `Sources/MonoLiteSwift/Protocol/`（进行中）**
  - 已实现：`WireMessage`、`OP_MSG` 解析/CRC32C（required bits + docSequence strict）、`OP_QUERY` 解析/`OP_REPLY` 构建、`ProtocolServer`（分发核心）、`MongoWireTCPServer`（POSIX TCP listener/accept/connection loop）
  - 已对齐：OP_COMPRESSED 结构化拒绝；OP_QUERY 仅允许 `*.$cmd`（非 `$cmd` 返回 “OP_QUERY is deprecated, use OP_MSG”）
  - 仍需：更多协议级边界行为/连接级细节（超时/关闭语义等）继续对齐 Go

---

## 存储格式（Go 版权威规范）

### 数据文件 `.monodb`

- **FileHeader（64B，小端）**（`storage/pager.go`）
  - `MagicNumber uint32 = 0x4D4F4E4F`（以 Go 常量为准；注意它在磁盘上的字节序与直观看到的 ASCII 不一致）
  - `Version uint16 = 1`
  - `PageSize uint16 = 4096`
  - `PageCount uint32`
  - `FreeListHead PageId (uint32)`：**0 表示空**
  - `MetaPageId PageId`：初始化为 0
  - `CatalogPageId PageId`：初始化为 0（Go 新库只创建 meta 页，catalog 在更高层创建/绑定）
  - `CreateTime/ModifyTime int64`：Unix 毫秒
  - `Reserved[24]`

- **Page（4096B）**（`storage/page.go`）
  - 头部 24B：
    - `PageId u32`
    - `Type u8`
    - `Flags u8`
    - `ItemCount u16`
    - `FreeSpace u16`
    - `NextPageId u32`：链表指针，**0 表示空**
    - `PrevPageId u32`：链表指针，**0 表示空**
    - `Checksum u32`：对 data 区做 XOR 校验
    - `Reserved u16`
  - data 区 4072B
  - **Checksum 算法**：对 data 区按 4 字节 little-endian u32 做 XOR（Go 实现对尾部不足 4 字节也有处理）

- **SlottedPage（数据页）**（`storage/page.go`）
  - slot 目录存放在 page.data 头部，记录从尾部向前增长
  - slot（6B）：`offset u16 + length u16 + flags u16`
  - **ItemCount 语义**：表示“槽总数”，单调递增；delete 只打标记，不减少 ItemCount
  - **UpdateRecord 语义**：需要扩容时，仍然复用同一个 slotIndex（保持 RecordId 稳定）
  - Compact 是唯一会减少 slot 数量的操作，并返回 old→new slotIndex 映射

### WAL 文件 `.wal`

- **WALHeader（32B，小端）**（`storage/wal.go`）
  - `Magic uint32 = 0x57414C4D`
  - `Version uint16 = 1`
  - `CheckpointLSN u64`
  - `FileSize u64`
  - `Checksum u32 = CRC32( header[0:24] )`

- **WALRecord（对齐 8B）**（`storage/wal.go`）
  - 头 20B：`LSN u64 + Type u8 + Flags u8 + DataLen u16 + PageId u32 + Checksum u32`
  - `Checksum = CRC32( header[0:16] + data )`
  - 记录体按 8 字节补齐 padding（padding 不参与校验）

---

## Swift 当前实现差异（必须修复）

### P0：会导致格式不兼容/数据错误/崩溃恢复错误

- **（已修复）Magic 常量不一致（字节级不兼容）**
  - Go：`FileHeader.Magic = 0x4D4F4E4F`，`WALHeader.Magic = 0x57414C4D`
  - Swift：已改为与 Go 一致（见 `Storage/Constants.swift`）

- **（已修复）“无效 PageId”哨兵值不一致**
  - Go：普遍用 **0 表示 nil/空指针**（FreeListHead、Next/Prev、叶子链表等）
  - Swift：已改为 0（见 `Storage/Constants.swift`）

- **（已修复）Page 校验和语义不一致**
  - Go：Marshal 时始终计算并写入 checksum；Unmarshal 时校验 checksum，失败即报错
  - Swift：已按 Go XOR 算法对齐（见 `Storage/Page.swift`）

- **（已修复）Pager.flush/WAL 先行原则不一致（WAL 形同虚设）**
  - Go：写脏页必须 **WAL 先写入并 fsync**，再写 data file，最后 checkpoint
  - Swift：已改为脏页走 `writePage()`（写 pageWrite WAL + 写 data file），并按 Go 语义 checkpoint（见 `Storage/Pager.swift`）

- **（已修复）freePage 没有把 Free 页写回磁盘**
  - Go：freePage 必须把 pageType=Free + next 指针写回，再更新 header
  - Swift：已按 Go 语义落盘（见 `Storage/Pager.swift`）

- **（已修复）recover 不完整（缺 Go 版关键修复点）**
  - Swift 已补齐：allocPageTypes、alloc-init、ensureFileSize（含半页修复）、恢复后 header 持久化与 free list 重载（见 `Storage/Pager.swift`、`Storage/WAL.swift`）
  - **单测**：已新增 “allocate from free list -> crash -> recover” 对齐测试（`Tests/StoragePagerRecoveryAlignmentTests.swift`）

- **SlottedPage.updateRecord 语义不一致（会破坏索引 RecordId）**
  - Go：扩容时复用原 slotIndex（RecordId 不变）
  - Swift：已按 Go 语义修复（优先原地更新；必要时迁移但保持 slotIndex 语义），并加入 `liveCount`（见 `Storage/SlottedPage.swift`）

- **（已修复）SlottedPage.loadSlots 使用 Data slice 导致 SIGTRAP**
  - Swift 之前对 `page.data[offset..<offset+size]` 切片后再用 `DataEndian(..., at: 0)` 读取，会在 slotCount>=2 时触发越界 SIGTRAP
  - 已修复为直接对 `page.data` 用“绝对 offset”读取（见 `Storage/SlottedPage.swift`），并增加复现/回归测试

- **KeyString 编码不一致（索引排序/唯一性判断会错）**
  - bool 编码：Go `true=0x02 false=0x01`；Swift `true=0x01 false=0x00`
  - binary 编码：Go 无 subtype；Swift 写入 subtype
  - 对象/正则字符串编码：Go 使用“转义 + 双 0 终止”；Swift 用简单 null-terminated
  - 降序处理：Go 仅对当前字段 valueBuf 做逐字节取反；Swift 当前实现会错误反转整段 buffer
  - **状态**：Swift 已按 Go 参考实现对齐上述 4 点（见 `Storage/KeyString.swift`）；后续会补充针对性单测验证字节级输出

- **BTree 结构不一致（树形状/删除修复/最小键数）**
  - `minKeys`：Go = `(order-1)/2`；Swift = `order/2`
  - `NeedsSplit`：Go 用 `ByteSize() > threshold`（以及根节点 keyCount 上限）；Swift 用 `>=`
  - `writeNode`：Go 每次写满 MaxPageData（清零尾部）；Swift 只覆盖前缀，尾部可能残留旧数据
  - **状态**：Swift 已对齐 `minKeys`、分裂触发（按 `keyCount >= order-1`）、`writeNode` 写满页与 `unmarshal` 一致性校验，并将 `delete/fixAfterDelete/fixUnderflow/borrow/merge` 与 `search/searchRange` 路由语义对齐到 Go；已加入单测锁定借键/合并/叶子链表一致性（见 `Storage/BTree.swift`、`Tests/StorageBTreeAlignmentTests.swift`）

### P1：行为可能不一致（但不一定立刻损坏数据）

- **allocatePage 行为**：
  - Go：新页分配只写 alloc/meta WAL，再直接写初始化页到 data file（不额外写 pageWrite WAL）
  - Swift：已对齐为 Go 语义（非 freeList 写初始化页但不写 pageWrite WAL；freeList 复用不立即写初始化页，依赖 recover.alloc-init 兜底；并在 allocate 时立即持久化 header）

---

## 未实现/缺口清单（Go → Swift）

### P1（Engine 核心缺口）

- **`engine/aggregate.go`（已实现基础版本）**
  - Swift：`Aggregate/Pipeline.swift` 已支持 `$match/$project/$sort/$limit/$skip/$group/$count/$unwind/$addFields/$unset/$replaceRoot/$lookup`
  - 仍需：更完整的表达式/边界语义（与 Go 的 `aggregate_extra_test.go` 继续对齐）

### P2（MongoDB Wire Protocol 缺口：无法被 driver 连接）

- **`protocol/server.go`（已实现基本版本）**
  - `Sources/MonoLiteSwift/Protocol/TCPServer.swift`：TCP listener/accept loop/connection handler（与 Go 结构对齐）
  - 集成测试：`Tests/ProtocolTCPServerIntegrationTests.swift`（OP_QUERY hello 往返）
- **协议覆盖不完整（进行中）**
  - 已实现：OP_MSG / OP_QUERY（握手）/OP_REPLY
  - 已对齐：OP_COMPRESSED 返回 **结构化 ProtocolError**（且 hello/isMaster 不宣称 compression）
  - 仍需：更多命令响应细节、连接级并发/关闭/超时语义进一步对齐

### P3（兼容性/测试基础设施缺口）

- **goldstandard/mongo_spec 测试体系（已补齐基础子集）**
  - goldstandard：
    - `Tests/GoldstandardProtocolAlignmentTests.swift`（required bits / docSequence strict）
    - `Tests/GoldstandardNumericAlignmentTests.swift`（int64>2^53、Decimal128 精确比较、类型排序）
  - mongo_spec（Unified CRUD 子集 runner）：
    - `Tests/MongoSpecUnifiedCRUDTests.swift`（默认跳过；`MONOLITE_RUN_MONGO_SPECS=1` 启用；复用 Go repo 的上游 fixtures 路径只读读取）

## 修复优先级（执行顺序）

1. **对齐常量与哨兵值**：Magic、nil PageId=0、FileHeader 默认值
2. **对齐 Page 校验和**：marshal 自动更新 + unmarshal 验证
3. **修复 SlottedPage.updateRecord**：保持 slotIndex 稳定，精确对齐 Go 插入/删除/压缩语义
4. **补齐 WAL-first + flush + freePage 落盘**
5. **实现 Go 同款 recover + ensureFileSize（含半页修复/alloc-init）**
6. **对齐 KeyString 编码（逐类型逐字节对齐）**
7. **对齐 BTree：minKeys、split 条件、节点页写入方式、校验逻辑**

---

## 备注

- 本文档是“对齐规范 + 差异清单”，会随着实现推进持续更新。

