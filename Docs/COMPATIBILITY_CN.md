# MonoLite Swift - MongoDB 兼容性说明

Created by Yanjunhui

本文档说明 **MonoLite Swift** API 与 MongoDB 语义的兼容性。

- **English**：[`docs/COMPATIBILITY.md`](COMPATIBILITY.md)
- **返回中文 README**：[`README_CN.md`](../README_CN.md)

---

## 概述

MonoLite Swift 是一个 **嵌入式文档数据库库**，为 Swift 应用提供 MongoDB 兼容的 API。设计目标：

- 原生 Swift 集成，支持 async/await
- 基于 Actor 的并发模型，确保线程安全
- 单文件存储，BSON 格式
- 本地/嵌入式场景，无网络开销

**注意**：MonoLite Swift 是库而非服务器，不实现 MongoDB Wire Protocol。如需协议级兼容，请使用 Go 版本。

---

## API 兼容性

MonoLite Swift 通过 `Database` 和 `Collection` Actor 提供 MongoDB 风格的 API。

### 数据库操作

| 操作 | 状态 | Swift API |
|------|------|-----------|
| 打开数据库 | ✅ | `Database.open(path:)` |
| 关闭数据库 | ✅ | `database.close()` |
| 刷新到磁盘 | ✅ | `database.flush()` |
| 获取集合 | ✅ | `database.collection(_:)` |
| 创建集合 | ✅ | `database.createCollection(_:)` |
| 删除集合 | ✅ | `database.dropCollection(_:)` |
| 列出集合 | ✅ | `database.listCollections()` |
| 数据库统计 | ✅ | `database.stats()` |

### 集合操作

| 操作 | 状态 | Swift API |
|------|------|-----------|
| 插入单个 | ✅ | `collection.insertOne(_:)` |
| 批量插入 | ✅ | `collection.insertMany(_:)` |
| 查询 | ✅ | `collection.find(filter:options:)` |
| 查询单个 | ✅ | `collection.findOne(filter:)` |
| 更新单个 | ✅ | `collection.updateOne(filter:update:upsert:)` |
| 批量更新 | ✅ | `collection.updateMany(filter:update:)` |
| 删除单个 | ✅ | `collection.deleteOne(filter:)` |
| 批量删除 | ✅ | `collection.deleteMany(filter:)` |
| 替换文档 | ✅ | `collection.replaceOne(filter:replacement:)` |
| 文档计数 | ✅ | `collection.countDocuments(filter:)` |
| 去重 | ✅ | `collection.distinct(field:filter:)` |
| 聚合 | ✅ | `collection.aggregate(pipeline:)` |
| 查找并修改 | ✅ | `collection.findOneAndUpdate(...)` |
| 创建索引 | ✅ | `collection.createIndex(keys:options:)` |
| 删除索引 | ✅ | `collection.dropIndex(name:)` |
| 列出索引 | ✅ | `collection.listIndexes()` |

---

## 查询过滤器操作符

使用 `BSONDocument` 并采用 MongoDB 风格的操作符指定过滤条件。

### 比较操作符

| 操作符 | 状态 | 示例 |
|--------|------|------|
| `$eq` | ✅ | `["age": ["$eq": 25]]` |
| `$ne` | ✅ | `["status": ["$ne": "inactive"]]` |
| `$gt` | ✅ | `["age": ["$gt": 18]]` |
| `$gte` | ✅ | `["age": ["$gte": 21]]` |
| `$lt` | ✅ | `["price": ["$lt": 100]]` |
| `$lte` | ✅ | `["score": ["$lte": 60]]` |
| `$in` | ✅ | `["status": ["$in": ["active", "pending"]]]` |
| `$nin` | ✅ | `["role": ["$nin": ["admin", "root"]]]` |

### 逻辑操作符

| 操作符 | 状态 | 示例 |
|--------|------|------|
| `$and` | ✅ | `["$and": [["age": ["$gte": 18]], ["status": "active"]]]` |
| `$or` | ✅ | `["$or": [["status": "active"], ["premium": true]]]` |
| `$not` | ✅ | `["age": ["$not": ["$lt": 18]]]` |
| `$nor` | ✅ | `["$nor": [["deleted": true], ["banned": true]]]` |

### 元素操作符

| 操作符 | 状态 | 示例 |
|--------|------|------|
| `$exists` | ✅ | `["email": ["$exists": true]]` |
| `$type` | ✅ | `["age": ["$type": "int"]]` |

### 数组操作符

| 操作符 | 状态 | 示例 |
|--------|------|------|
| `$all` | ✅ | `["tags": ["$all": ["swift", "ios"]]]` |
| `$size` | ✅ | `["items": ["$size": 3]]` |
| `$elemMatch` | ✅ | `["scores": ["$elemMatch": ["$gte": 80]]]` |

### 其他操作符

| 操作符 | 状态 | 示例 |
|--------|------|------|
| `$regex` | ✅ | `["email": ["$regex": "@gmail\\.com$"]]` |
| `$mod` | ✅ | `["num": ["$mod": [5, 0]]]` |

---

## 更新操作符

### 字段操作符

| 操作符 | 状态 | 示例 |
|--------|------|------|
| `$set` | ✅ | `["$set": ["name": "Alice", "age": 26]]` |
| `$unset` | ✅ | `["$unset": ["tempField": ""]]` |
| `$inc` | ✅ | `["$inc": ["count": 1, "score": 10]]` |
| `$mul` | ✅ | `["$mul": ["price": 1.1]]` |
| `$min` | ✅ | `["$min": ["lowScore": 50]]` |
| `$max` | ✅ | `["$max": ["highScore": 100]]` |
| `$rename` | ✅ | `["$rename": ["oldName": "newName"]]` |
| `$currentDate` | ✅ | `["$currentDate": ["lastModified": true]]` |
| `$setOnInsert` | ✅ | `["$setOnInsert": ["createdAt": Date()]]` |

### 数组操作符

| 操作符 | 状态 | 示例 |
|--------|------|------|
| `$push` | ✅ | `["$push": ["tags": "newTag"]]` |
| `$push` + `$each` | ✅ | `["$push": ["tags": ["$each": ["a", "b"]]]]` |
| `$pop` | ✅ | `["$pop": ["items": 1]]` |
| `$pull` | ✅ | `["$pull": ["tags": "oldTag"]]` |
| `$pullAll` | ✅ | `["$pullAll": ["tags": ["a", "b"]]]` |
| `$addToSet` | ✅ | `["$addToSet": ["tags": "unique"]]` |
| `$addToSet` + `$each` | ✅ | `["$addToSet": ["tags": ["$each": ["a", "b"]]]]` |

---

## 索引

| 功能 | 状态 | 说明 |
|------|------|------|
| B+Tree 索引 | ✅ | 默认索引结构 |
| 单字段索引 | ✅ | `["email": 1]` |
| 复合索引 | ✅ | `["lastName": 1, "firstName": 1]` |
| 唯一索引 | ✅ | `options: ["unique": true]` |
| 降序索引 | ✅ | `["createdAt": -1]` |
| 稀疏索引 | ❌ | 未实现 |
| TTL 索引 | ❌ | 未实现 |
| 文本索引 | ❌ | 未实现 |
| 地理空间索引 | ❌ | 未实现 |

---

## 聚合管道

MonoLite Swift 通过 `collection.aggregate(pipeline:)` 支持聚合。

### 已支持阶段

| 阶段 | 状态 | 说明 |
|------|------|------|
| `$match` | ✅ | 过滤文档 |
| `$project` | ✅ | 文档投影 |
| `$sort` | ✅ | 排序 |
| `$limit` | ✅ | 限制数量 |
| `$skip` | ✅ | 跳过文档 |
| `$group` | ✅ | 分组聚合 |
| `$count` | ✅ | 计数 |
| `$unwind` | ✅ | 展开数组 |
| `$addFields` / `$set` | ✅ | 添加字段 |
| `$unset` | ✅ | 移除字段 |
| `$replaceRoot` | ✅ | 替换根文档 |
| `$lookup` | ✅ | 左外连接 |

### 分组累加器

| 累加器 | 状态 |
|--------|------|
| `$sum` | ✅ |
| `$avg` | ✅ |
| `$min` | ✅ |
| `$max` | ✅ |
| `$first` | ✅ |
| `$last` | ✅ |
| `$push` | ✅ |
| `$addToSet` | ✅ |

### 未实现阶段

| 阶段 | 状态 |
|------|------|
| `$out` | ❌ |
| `$merge` | ❌ |
| `$facet` | ❌ |
| `$bucket` | ❌ |
| `$graphLookup` | ❌ |
| `$geoNear` | ❌ |

---

## 事务

MonoLite Swift 支持单机事务：

| 功能 | 状态 | 说明 |
|------|------|------|
| 开始事务 | ✅ | `database.startTransaction()` |
| 提交事务 | ✅ | `transaction.commit()` |
| 回滚事务 | ✅ | `transaction.abort()` |
| 锁管理器 | ✅ | 读/写锁 |
| 死锁检测 | ✅ | 等待图分析 |
| 回滚支持 | ✅ | Undo 日志 |

限制：
- 仅支持单机（无分布式事务）
- 不支持因果一致性

---

## BSON 类型支持

| 类型 | 状态 | Swift 类型 |
|------|------|------------|
| Double | ✅ | `Double` |
| String | ✅ | `String` |
| Document | ✅ | `BSONDocument` |
| Array | ✅ | `[BSONValue]` |
| Binary | ✅ | `BSONBinary` |
| ObjectId | ✅ | `ObjectId` |
| Boolean | ✅ | `Bool` |
| Date | ✅ | `Date` |
| Null | ✅ | `BSONNull` |
| Int32 | ✅ | `Int32` |
| Int64 | ✅ | `Int64` |
| Timestamp | ✅ | `BSONTimestamp` |
| Decimal128 | ❌ | 不支持 |
| MinKey/MaxKey | ❌ | 不支持 |
| JavaScript | ❌ | 不支持 |

---

## 并发模型

MonoLite Swift 使用 Swift 的 **Actor 模型** 确保线程安全：

```swift
// Database 和 Collection 都是 Actor
actor Database {
    func collection(_ name: String) async throws -> Collection
}

actor Collection {
    func insertOne(_ document: BSONDocument) async throws -> InsertOneResult
}
```

所有操作特性：
- **线程安全**：Actor 隔离防止数据竞争
- **Async/await**：原生 Swift 并发支持
- **非阻塞**：操作不会阻塞线程

---

## 与 MongoDB 功能对比

| 功能 | MongoDB | MonoLite Swift |
|------|---------|----------------|
| 网络服务器 | ✅ | ❌（嵌入式）|
| 复制集 | ✅ | ❌ |
| 分片 | ✅ | ❌ |
| 认证授权 | ✅ | ❌ |
| Wire Protocol | ✅ | ❌ |
| 单文件存储 | ❌ | ✅ |
| 零配置 | ❌ | ✅ |
| iOS/macOS 原生 | ❌ | ✅ |
| Actor 并发 | ❌ | ✅ |

---

## 平台支持

| 平台 | 状态 |
|------|------|
| macOS 13+ | ✅ |
| iOS 16+ | ✅ |
| watchOS 9+ | ✅ |
| tvOS 16+ | ✅ |
| Linux | 🚧 |
| Windows | ❌ |

---

## 如何反馈问题

建议提供：

- Swift 版本和平台
- 可复现问题的代码片段
- MongoDB 期望行为 vs MonoLite 实际行为
- 如适用：堆栈跟踪信息
