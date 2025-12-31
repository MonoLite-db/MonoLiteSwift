# MonoLite

MonoLite 是一个**单文件、可嵌入的文档数据库**，专为 Swift 设计，兼容 MongoDB Wire Protocol。纯 Swift 实现，采用 Actor 并发模型。

<div align="center">

![Swift Version](https://img.shields.io/badge/Swift-5.9+-F05138?style=flat&logo=swift)
![Platform](https://img.shields.io/badge/Platform-macOS%2013%2B%20%7C%20iOS%2016%2B-blue?style=flat)
![MongoDB Compatible](https://img.shields.io/badge/MongoDB-Wire%20Protocol-47A248?style=flat&logo=mongodb)
![License](https://img.shields.io/badge/License-MIT-blue?style=flat)

**[README (EN)](README.md)** · **[README (中文)](README_CN.md)**

</div>

## 项目愿景

> **像 SQLite 一样简单，像 MongoDB 一样思考和工作。**

- **单文件存储** — 一个 `.monodb` 文件即为完整数据库
- **零依赖** — 纯 Swift 实现，无需外部库
- **Actor 并发模型** — 使用 Swift Actor 实现天然线程安全
- **嵌入优先** — 库优先设计，直接嵌入到 Swift 应用中
- **MongoDB 驱动兼容** — 通过 Wire Protocol 支持标准 MongoDB 驱动和工具

## 为什么选择 MonoLite？我们解决的痛点

### SQLite 的困境

SQLite 是一个优秀的嵌入式数据库，但当你的 Swift 应用处理**文档型数据**时，会遇到这些困扰：

| 痛点 | SQLite 现状 | MonoLite 方案 |
|------|-------------|-------------------|
| **僵化的 Schema** | 必须用 `CREATE TABLE` 预定义表结构，修改需要 `ALTER TABLE` 迁移 | Schema-free — 文档可以有不同字段，自然演进 |
| **嵌套数据** | 需要 JSON1 扩展或序列化，查询笨拙 | 原生嵌套文档，支持点号路径查询（`address.city`） |
| **数组操作** | 无原生数组类型，需序列化或使用关联表 | 原生数组，支持 `$push`、`$pull`、`$elemMatch` 等操作符 |
| **对象-关系阻抗不匹配** | Swift 结构体 ↔ 关系表需要映射层 | BSON 文档直接映射 Swift 类型 |
| **查询复杂性** | 层级数据需要复杂 JOIN，SQL 冗长 | 直观的查询操作符（`$gt`、`$in`、`$or`）和聚合管道 |
| **并发处理** | 需要手动加锁保证线程安全 | Actor 并发模型 — 天然线程安全 |

### 何时选择 MonoLite 而非 SQLite

✅ **选择 MonoLite 当：**
- 你的数据天然是层级或文档形态（类 JSON）
- 文档结构多变（可选字段、演进中的 Schema）
- 你需要强大的数组操作
- 你想要原生 Swift async/await 与 Actor 隔离
- 你的团队已经熟悉 MongoDB
- 你想用 MongoDB 兼容的方式原型开发

✅ **继续使用 SQLite 当：**
- 你的数据高度关系化，有大量多对多关系
- 你需要复杂的多表 JOIN
- 你需要严格的 Schema 约束
- 你使用现有的 Core Data 或 GRDB 工具链

### MonoLite vs SQLite：功能对比

| 特性 | MonoLite | SQLite |
|------|---------------|--------|
| **数据模型** | 文档（BSON） | 关系型（表） |
| **Schema** | 灵活，无 Schema 约束 | 固定，需要迁移 |
| **嵌套数据** | 原生支持 | JSON1 扩展 |
| **数组** | 原生支持，丰富操作符 | 需要序列化 |
| **查询语言** | MongoDB 查询语言 | SQL |
| **Swift 并发** | Actor 隔离的 async/await | 手动线程安全 |
| **事务** | ✅ 多文档 ACID | ✅ ACID |
| **索引** | B+Tree（单字段、复合、唯一） | B-Tree（多种类型） |
| **文件格式** | 单个 `.monodb` 文件 | 单个 `.db` 文件 |
| **崩溃恢复** | WAL | WAL/回滚日志 |
| **成熟度** | 新项目 | 20+ 年久经考验 |

## 快速开始

### 安装

在 `Package.swift` 中添加 MonoLiteSwift：

```swift
dependencies: [
    .package(path: "../MonoLiteSwift")  // 本地路径
    // 或使用 git URL（可用时）:
    // .package(url: "https://github.com/user/MonoLiteSwift.git", from: "1.0.0")
]
```

### 基本使用（库 API）

```swift
import MonoLiteSwift

// 打开数据库
let db = try await Database.open(path: "data.monodb")

// 获取集合
let users = try await db.collection("users")

// 插入文档
let doc: BSONDocument = [
    "name": "Alice",
    "age": .int32(25),
    "email": "alice@example.com"
]
let insertResult = try await users.insertOne(doc)

// 批量插入
let docs: [BSONDocument] = [
    ["name": "Bob", "age": .int32(30), "tags": .array([.string("dev"), .string("swift")])],
    ["name": "Carol", "age": .int32(28), "address": .document(["city": "Beijing"])]
]
let insertManyResult = try await users.insertMany(docs)

// 查询文档
let results = try await users.find(["age": ["$gt": .int32(20)]])
for doc in results {
    print(doc)
}

// 查找单个文档
if let alice = try await users.findOne(["name": "Alice"]) {
    print("找到: \(alice)")
}

// 更新文档
let updateResult = try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$set": ["age": .int32(26)]]
)

// 删除文档
let deleteResult = try await users.deleteOne(["name": "Alice"])

// 关闭数据库
try await db.close()
```

### Wire Protocol 服务器

```swift
import MonoLiteSwift

// 启动 MongoDB 兼容服务器
let db = try await Database.open(path: "data.monodb")
let server = try MongoWireTCPServer(database: db, port: 27017)
try await server.start()

// 现在可以用 mongosh 连接：
// mongosh mongodb://localhost:27017
```

### 使用事务

```swift
// 开启事务
let session = try await db.startSession()
try await session.startTransaction()

do {
    let users = try await db.collection("users")
    let accounts = try await db.collection("accounts")

    // 转账操作
    try await users.updateOne(
        filter: ["name": "Alice"],
        update: ["$inc": ["balance": .int32(-100)]],
        session: session
    )
    try await users.updateOne(
        filter: ["name": "Bob"],
        update: ["$inc": ["balance": .int32(100)]],
        session: session
    )

    try await session.commitTransaction()
} catch {
    try await session.abortTransaction()
    throw error
}
```

### 聚合管道

```swift
let orders = try await db.collection("orders")

let pipeline: [BSONDocument] = [
    ["$match": ["status": "completed"]],
    ["$group": [
        "_id": "$customerId",
        "total": ["$sum": "$amount"]
    ]],
    ["$sort": ["total": .int32(-1)]],
    ["$limit": .int32(10)]
]

let results = try await orders.aggregate(pipeline)
```

### 索引管理

```swift
let users = try await db.collection("users")

// 创建唯一索引
try await users.createIndex(
    keys: ["email": .int32(1)],
    options: ["unique": true]
)

// 创建复合索引
try await users.createIndex(
    keys: ["name": .int32(1), "age": .int32(-1)]
)

// 列出索引
let indexes = try await users.listIndexes()

// 删除索引
try await users.dropIndex("email_1")
```

## 核心特性

### Actor 并发模型

- **线程安全** — 所有数据库操作都是 Actor 隔离的
- **Async/Await** — 原生 Swift 并发支持
- **无数据竞争** — 编译器强制的线程安全

### 崩溃一致性（WAL）

- **预写日志** — 所有写操作在写入数据文件前先写入 WAL
- **自动崩溃恢复** — 启动时 WAL 重放，恢复到一致状态
- **检查点机制** — 定期检查点加速恢复并控制 WAL 大小
- **原子写入** — 保证单个写操作的原子性

### 完整事务支持

- **多文档事务** — 支持跨多个集合的事务
- **事务 API** — startTransaction / commitTransaction / abortTransaction
- **锁管理** — 文档级和集合级锁粒度
- **死锁检测** — 基于等待图的死锁检测，自动中止事务
- **事务回滚** — 完整的 Undo Log 支持事务回滚

### B+Tree 索引

- **高效查找** — O(log n) 查找复杂度
- **多种索引类型** — 单字段、复合、唯一索引
- **点号标记支持** — 支持嵌套字段索引（如 `address.city`）
- **叶节点链表** — 高效的范围查询和排序

### 资源限制与安全

| 限制项 | 值 |
|--------|-----|
| 最大文档大小 | 16 MB |
| 最大嵌套深度 | 100 层 |
| 每集合最大索引数 | 64 |
| 最大批量写入 | 100,000 文档 |
| 最大字段名长度 | 1,024 字符 |

## 功能支持状态

### 已支持的核心功能

| 分类 | 支持 |
|------|------|
| **CRUD** | insert, find, update, delete, findAndModify, replaceOne, distinct |
| **查询操作符** | $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $and, $or, $not, $nor, $exists, $type, $all, $elemMatch, $size, $regex |
| **更新操作符** | $set, $unset, $inc, $min, $max, $mul, $rename, $push, $pop, $pull, $pullAll, $addToSet, $setOnInsert |
| **聚合阶段** | $match, $project, $sort, $limit, $skip, $group, $count, $unwind, $addFields, $set, $unset, $lookup, $replaceRoot |
| **$group 累加器** | $sum, $avg, $min, $max, $count, $push, $addToSet, $first, $last |
| **索引** | 单字段、复合、唯一索引，点号标记（嵌套字段） |
| **游标** | getMore, killCursors, batchSize |
| **命令** | dbStats, collStats, listCollections, listIndexes, serverStatus, validate, explain |
| **事务** | startTransaction, commitTransaction, abortTransaction |

### 查询操作符详情

| 分类 | 操作符 |
|------|--------|
| 比较 | `$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$in` `$nin` |
| 逻辑 | `$and` `$or` `$not` `$nor` |
| 元素 | `$exists` `$type` |
| 数组 | `$all` `$elemMatch` `$size` |
| 求值 | `$regex` |

### 更新操作符详情

| 分类 | 操作符 |
|------|--------|
| 字段 | `$set` `$unset` `$inc` `$min` `$max` `$mul` `$rename` `$setOnInsert` |
| 数组 | `$push` `$pop` `$pull` `$pullAll` `$addToSet` |

### 聚合管道阶段详情

| 阶段 | 描述 |
|------|------|
| `$match` | 文档过滤（支持所有查询操作符） |
| `$project` | 字段投影（包含/排除模式） |
| `$sort` | 排序（支持复合排序） |
| `$limit` | 限制结果数量 |
| `$skip` | 跳过指定数量 |
| `$group` | 分组聚合（支持 9 种累加器） |
| `$count` | 文档计数 |
| `$unwind` | 数组展开（支持 preserveNullAndEmptyArrays） |
| `$addFields` / `$set` | 添加/设置字段 |
| `$unset` | 移除字段 |
| `$lookup` | 集合关联（左外连接） |
| `$replaceRoot` | 替换根文档 |

### 不支持的功能（非目标）

- 副本集 / 分片（分布式）
- 认证与授权
- Change Streams
- 地理空间功能
- 全文搜索
- GridFS

## 存储引擎架构

```
┌────────────────────────────────────────────────────────────────┐
│                      Wire Protocol                              │
│              (OP_MSG / OP_QUERY / OP_REPLY)                    │
├────────────────────────────────────────────────────────────────┤
│                        查询引擎                                  │
│        ┌─────────────┬─────────────┬─────────────┐             │
│        │   解析器    │   执行器    │   优化器    │             │
│        │  (BSON)     │  (Pipeline) │  (Index)    │             │
│        └─────────────┴─────────────┴─────────────┘             │
├────────────────────────────────────────────────────────────────┤
│                       事务管理器                                 │
│        ┌─────────────┬─────────────┬─────────────┐             │
│        │    锁       │   死锁      │    Undo     │             │
│        │   管理器    │   检测器    │    Log      │             │
│        └─────────────┴─────────────┴─────────────┘             │
├────────────────────────────────────────────────────────────────┤
│                       存储引擎                                   │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│   │   B+Tree     │  │    Pager     │  │     WAL      │        │
│   │   索引       │  │    缓存      │  │    恢复      │        │
│   └──────────────┘  └──────────────┘  └──────────────┘        │
├────────────────────────────────────────────────────────────────┤
│                        单文件                                    │
│                    (.monodb 文件)                                │
└────────────────────────────────────────────────────────────────┘
```

## 项目结构

```
MonoLiteSwift/
├── Package.swift              # Swift Package Manager 配置
├── Sources/MonoLiteSwift/
│   ├── BSON/                  # BSON 编解码
│   │   ├── BSONValue.swift    # BSON 值类型
│   │   ├── BSONDocument.swift # 文档类型
│   │   ├── BSONEncoder.swift  # BSON 序列化
│   │   ├── BSONDecoder.swift  # BSON 反序列化
│   │   └── BSONCompare.swift  # 值比较（MongoDB 标准）
│   │
│   ├── Engine/                # 数据库引擎
│   │   ├── Database.swift     # 数据库核心（actor）
│   │   ├── MonoCollection.swift # 集合操作（actor）
│   │   ├── DatabaseCommands.swift # 命令处理器
│   │   ├── Index.swift        # 索引管理
│   │   ├── UpdateOperator.swift # 更新操作符
│   │   ├── Cursor.swift       # 游标管理
│   │   └── Explain.swift      # 查询计划解释
│   │
│   ├── Aggregate/             # 聚合管道
│   │   └── Pipeline.swift     # 管道阶段实现
│   │
│   ├── Transaction/           # 事务管理
│   │   ├── Transaction.swift  # 事务状态
│   │   ├── TransactionManager.swift # 事务协调
│   │   ├── LockManager.swift  # 锁管理与死锁检测
│   │   └── SessionManager.swift # 会话管理
│   │
│   ├── Storage/               # 存储引擎
│   │   ├── Pager.swift        # 页面管理器（缓存、读写）
│   │   ├── Page.swift         # 页面结构
│   │   ├── SlottedPage.swift  # 槽页面（存储文档）
│   │   ├── BTree.swift        # B+Tree 实现
│   │   ├── BTreeNode.swift    # B+Tree 节点
│   │   ├── WAL.swift          # 预写日志
│   │   ├── KeyString.swift    # 索引键编码
│   │   └── FileHeader.swift   # 文件头结构
│   │
│   ├── Protocol/              # MongoDB Wire Protocol
│   │   ├── TCPServer.swift    # TCP 服务器
│   │   ├── ProtocolServer.swift # 协议处理器
│   │   ├── WireMessage.swift  # 消息解析
│   │   ├── OpMsg.swift        # OP_MSG 处理
│   │   └── OpQuery.swift      # OP_QUERY 处理
│   │
│   └── Core/                  # 核心工具
│       ├── MonoError.swift    # 错误类型
│       ├── ErrorCodes.swift   # MongoDB 错误码
│       ├── Limits.swift       # 资源限制
│       ├── Validation.swift   # 文档验证
│       └── Logger.swift       # 结构化日志
│
└── Tests/MonoLiteSwiftTests/  # 单元测试
```

## 技术规格

| 项目 | 规格 |
|------|------|
| 最大文档大小 | 16 MB |
| 最大嵌套深度 | 100 层 |
| 每集合最大索引数 | 64 |
| 最大批量写入 | 100,000 文档 |
| 页面大小 | 4 KB |
| 默认游标批量大小 | 101 文档 |
| 游标超时 | 10 分钟 |
| 事务锁超时 | 30 秒 |
| WAL 格式版本 | 1 |
| 文件格式版本 | 1 |
| Wire Protocol 版本 | 13 (MongoDB 5.0) |

## 跨语言兼容性

MonoLiteSwift 是 MonoLite 家族的一部分，拥有以下相同实现：

| 语言 | 仓库 | 状态 |
|------|------|------|
| Go | MonoLite | 参考实现 |
| Swift | MonoLiteSwift | Actor 化 Swift 移植 |
| TypeScript | MonoLiteTS | Node.js/Bun 实现 |

三种实现：
- 共享相同的 `.monodb` 文件格式
- 通过相同的一致性测试（33/33 测试，100%）
- 支持相同的查询/更新操作符
- 兼容 MongoDB Wire Protocol

## 系统要求

- Swift 5.9+
- macOS 13+ 或 iOS 16+

## 开发

```bash
# 构建
swift build

# 运行测试
swift test

# 构建发布版
swift build -c release
```

## 许可证

MIT License

---

<div align="center">

**[README (EN)](README.md)** · **[README (中文)](README_CN.md)**

</div>
