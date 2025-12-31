# MonoLite Swift API 完整示例

Created by Yanjunhui

本文档提供 MonoLite Swift 版本的完整 API 使用示例，便于开发者和大模型理解调用方式。

## 目录

- [数据库操作](#数据库操作)
- [集合操作](#集合操作)
- [文档 CRUD](#文档-crud)
- [查询操作符](#查询操作符)
- [更新操作符](#更新操作符)
- [聚合管道](#聚合管道)
- [索引管理](#索引管理)
- [事务操作](#事务操作)
- [游标操作](#游标操作)
- [数据库命令](#数据库命令)

---

## 数据库操作

### 打开数据库

```swift
import MonoLiteSwift

// 打开或创建数据库
let db = try await Database.open(path: "data.monodb")

// 使用完毕后关闭
try await db.close()
```

### 获取数据库信息

```swift
// 刷新数据到磁盘
try await db.flush()

// 关闭数据库
try await db.close()

// 验证数据库完整性
let result = await db.validate()
if result.valid {
    print("Database is valid")
} else {
    for error in result.errors {
        print("Error: \(error)")
    }
}
```

### 集合管理

```swift
// 获取或创建集合
let users = try await db.collection("users")

// 仅获取集合（不自动创建）
if let users = db.getCollection("users") {
    // 集合存在
}

// 列出所有集合
let collections = db.listCollections()
for name in collections {
    print(name)
}

// 删除集合
try await db.dropCollection("users")
```

---

## 集合操作

### 基本信息

```swift
let users = try await db.collection("users")

// 获取集合信息
let info = await users.catalogInfoForDatabase()
```

---

## 文档 CRUD

### 插入文档

```swift
let users = try await db.collection("users")

// 插入单个文档
let doc: BSONDocument = [
    "name": "Alice",
    "age": .int32(25),
    "email": "alice@example.com"
]
let insertedId = try await users.insertOne(doc)
print("Inserted ID: \(insertedId)")

// 插入多个文档
let docs: [BSONDocument] = [
    [
        "name": "Bob",
        "age": .int32(30),
        "tags": .array([.string("developer"), .string("swift")])
    ],
    [
        "name": "Carol",
        "age": .int32(28),
        "address": .document([
            "city": "Beijing",
            "country": "China"
        ])
    ]
]
let ids = try await users.insert(docs)
print("Inserted \(ids.count) documents")

// 插入带自定义 _id 的文档
let customDoc: BSONDocument = [
    "_id": "user_001",
    "name": "David"
]
let id = try await users.insertOne(customDoc)
```

### 查询文档

```swift
// 查询所有文档
let allDocs = try await users.find([:])

// 按条件查询
let docs = try await users.find([
    "age": ["$gt": .int32(20)]
])

// 查询单个文档
if let alice = try await users.findOne(["name": "Alice"]) {
    print("Found: \(alice)")
}

// 按 ID 查询
if let doc = try await users.findById("user_001") {
    print("Found by ID: \(doc)")
}

// 带选项查询
let options = QueryOptions(
    sort: ["age": .int32(-1)],  // 按年龄降序
    projection: ["name": .int32(1), "age": .int32(1)],
    skip: 0,
    limit: 10
)
let docs = try await users.findWithOptions([:], options: options)
```

### 更新文档

```swift
// 更新匹配的所有文档
let result = try await users.updateMany(
    filter: ["age": ["$lt": .int32(30)]],
    update: ["$inc": ["age": .int32(1)]]
)
print("Matched: \(result.matchedCount), Modified: \(result.modifiedCount)")

// 更新单个文档
let result = try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$set": ["age": .int32(26)]]
)

// Upsert（不存在则插入）
let result = try await users.updateOne(
    filter: ["name": "Eve"],
    update: ["$set": [
        "name": "Eve",
        "age": .int32(22)
    ]],
    upsert: true
)
if let upsertedId = result.upsertedId {
    print("Upserted ID: \(upsertedId)")
}
```

### 替换文档

```swift
// 替换整个文档（保留 _id）
let result = try await users.replaceOne(
    filter: ["name": "Alice"],
    replacement: [
        "name": "Alice",
        "age": .int32(27),
        "status": "active"
    ]
)
```

### 删除文档

```swift
// 删除匹配的所有文档
let result = try await users.deleteMany(
    filter: ["age": ["$lt": .int32(18)]]
)
print("Deleted: \(result.deletedCount)")

// 删除单个文档
let result = try await users.deleteOne(
    filter: ["name": "Alice"]
)
```

### FindAndModify

```swift
// 查找并更新，返回更新前的文档
let (doc, _) = try await users.findAndModify(
    query: ["name": "Alice"],
    update: ["$inc": ["age": .int32(1)]],
    new: false  // 返回更新前的文档
)

// 查找并更新，返回更新后的文档
let (doc, _) = try await users.findAndModify(
    query: ["name": "Alice"],
    update: ["$set": ["status": "active"]],
    new: true,  // 返回更新后的文档
    upsert: true
)

// 查找并删除
let (doc, _) = try await users.findAndModify(
    query: ["name": "Alice"],
    remove: true
)

// 带排序的 FindAndModify
let (doc, _) = try await users.findAndModify(
    query: ["status": "pending"],
    update: ["$set": ["status": "processing"]],
    sort: ["createdAt": .int32(1)],  // 处理最早的
    new: true
)
```

### Distinct

```swift
// 获取字段的不重复值
let cities = try await users.distinct(field: "city", filter: [:])

// 带过滤条件
let statuses = try await users.distinct(
    field: "status",
    filter: ["age": ["$gte": .int32(18)]]
)
```

### 文档计数

```swift
// 计算文档数量
let count = try await users.count(filter: [:])

// 带条件计数
let count = try await users.count(
    filter: ["status": "active"]
)
```

---

## 查询操作符

### 比较操作符

```swift
// $eq - 等于
try await users.find(["age": ["$eq": .int32(25)]])
// 简写
try await users.find(["age": .int32(25)])

// $ne - 不等于
try await users.find(["status": ["$ne": "inactive"]])

// $gt - 大于
try await users.find(["age": ["$gt": .int32(18)]])

// $gte - 大于等于
try await users.find(["age": ["$gte": .int32(18)]])

// $lt - 小于
try await users.find(["age": ["$lt": .int32(65)]])

// $lte - 小于等于
try await users.find(["age": ["$lte": .int32(65)]])

// $in - 在数组中
try await users.find([
    "status": ["$in": .array([.string("active"), .string("pending")])]
])

// $nin - 不在数组中
try await users.find([
    "status": ["$nin": .array([.string("deleted"), .string("banned")])]
])
```

### 逻辑操作符

```swift
// $and - 与
try await users.find([
    "$and": .array([
        .document(["age": ["$gte": .int32(18)]]),
        .document(["age": ["$lte": .int32(65)]])
    ])
])

// 隐式 $and（同一文档中的多个条件）
try await users.find([
    "age": ["$gte": .int32(18)],
    "status": "active"
])

// $or - 或
try await users.find([
    "$or": .array([
        .document(["age": ["$lt": .int32(18)]]),
        .document(["age": ["$gt": .int32(65)]])
    ])
])

// $nor - 都不满足
try await users.find([
    "$nor": .array([
        .document(["status": "deleted"]),
        .document(["status": "banned"])
    ])
])

// $not - 非
try await users.find([
    "age": ["$not": ["$gt": .int32(65)]]
])
```

### 元素操作符

```swift
// $exists - 字段存在
try await users.find(["email": ["$exists": .bool(true)]])

// $type - 字段类型
try await users.find(["age": ["$type": "int"]])
```

### 数组操作符

```swift
// $all - 包含所有元素
try await users.find([
    "tags": ["$all": .array([.string("swift"), .string("mongodb")])]
])

// $elemMatch - 数组元素匹配
try await users.find([
    "scores": ["$elemMatch": [
        "$gte": .int32(80),
        "$lt": .int32(90)
    ]]
])

// $size - 数组长度
try await users.find(["tags": ["$size": .int32(3)]])
```

### 求值操作符

```swift
// $regex - 正则表达式
try await users.find(["email": ["$regex": "@gmail\\.com$"]])

// $mod - 取模
try await users.find([
    "age": ["$mod": .array([.int32(2), .int32(0)])]  // 偶数年龄
])
```

### 点号路径查询（嵌套文档）

```swift
// 查询嵌套字段
try await users.find(["address.city": "Beijing"])

// 嵌套字段比较
try await users.find([
    "address.zipcode": ["$gt": "100000"]
])

// 数组中的嵌套文档
try await users.find(["orders.status": "completed"])
```

---

## 更新操作符

### 字段操作符

```swift
// $set - 设置字段值
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$set": [
        "age": .int32(26),
        "status": "active",
        "address.city": "Shanghai"  // 设置嵌套字段
    ]]
)

// $unset - 删除字段
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$unset": ["tempField": ""]]
)

// $inc - 增加数值
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$inc": [
        "age": .int32(1),
        "score": .int32(-5)  // 也可以减少
    ]]
)

// $mul - 乘以数值
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$mul": ["price": .double(1.1)]]  // 涨价 10%
)

// $min - 更新为较小值
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$min": ["lowScore": .int32(50)]]
)

// $max - 更新为较大值
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$max": ["highScore": .int32(100)]]
)

// $rename - 重命名字段
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$rename": ["oldName": "newName"]]
)

// $currentDate - 设置为当前日期
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$currentDate": [
        "lastModified": .bool(true),
        "lastModifiedTS": ["$type": "timestamp"]
    ]]
)

// $setOnInsert - 仅在 upsert 插入时设置
try await users.updateOne(
    filter: ["name": "NewUser"],
    update: [
        "$set": ["lastLogin": .datetime(Date())],
        "$setOnInsert": ["createdAt": .datetime(Date())]
    ],
    upsert: true
)
```

### 数组操作符

```swift
// $push - 添加元素到数组
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$push": ["tags": "newTag"]]
)

// $push 多个元素
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$push": ["tags": [
        "$each": .array([.string("tag1"), .string("tag2"), .string("tag3")])
    ]]]
)

// $pop - 删除数组首/尾元素
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$pop": ["tags": .int32(1)]]  // 1: 删除尾部, -1: 删除头部
)

// $pull - 删除匹配的元素
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$pull": ["tags": "oldTag"]]
)

// $pull 条件删除
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$pull": ["scores": ["$lt": .int32(60)]]]
)

// $pullAll - 删除多个指定元素
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$pullAll": ["tags": .array([.string("tag1"), .string("tag2")])]]
)

// $addToSet - 添加不重复元素
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$addToSet": ["tags": "uniqueTag"]]
)

// $addToSet 多个元素
try await users.updateOne(
    filter: ["name": "Alice"],
    update: ["$addToSet": ["tags": [
        "$each": .array([.string("tag1"), .string("tag2")])
    ]]]
)
```

---

## 聚合管道

```swift
let orders = try await db.collection("orders")

// 基本聚合
let results = try await orders.aggregate([
    // $match - 过滤
    ["$match": ["status": "completed"]],

    // $group - 分组
    ["$group": [
        "_id": "$customerId",
        "totalAmount": ["$sum": "$amount"],
        "orderCount": ["$count": [:]],
        "avgAmount": ["$avg": "$amount"]
    ]],

    // $sort - 排序
    ["$sort": ["totalAmount": .int32(-1)]],

    // $limit - 限制数量
    ["$limit": .int32(10)]
])

// 所有聚合阶段示例
let pipeline: [BSONDocument] = [
    // $match - 过滤文档
    ["$match": [
        "status": "active",
        "age": ["$gte": .int32(18)]
    ]],

    // $project - 投影（选择字段）
    ["$project": [
        "name": .int32(1),
        "email": .int32(1),
        "age": .int32(1),
        "_id": .int32(0)  // 排除 _id
    ]],

    // $addFields / $set - 添加计算字段
    ["$addFields": [
        "fullName": ["$concat": .array([.string("$firstName"), .string(" "), .string("$lastName")])]
    ]],

    // $unset - 移除字段
    ["$unset": .array([.string("tempField"), .string("internalData")])],

    // $skip - 跳过
    ["$skip": .int32(10)],

    // $limit - 限制
    ["$limit": .int32(20)],

    // $sort - 排序
    ["$sort": [
        "age": .int32(-1),
        "name": .int32(1)
    ]],

    // $count - 计数
    ["$count": "totalCount"]
]

// $unwind - 展开数组
let pipeline: [BSONDocument] = [
    ["$unwind": "$tags"],
    // 或带选项
    ["$unwind": [
        "path": "$tags",
        "preserveNullAndEmptyArrays": .bool(true)
    ]]
]

// $lookup - 关联查询
let pipeline: [BSONDocument] = [
    ["$lookup": [
        "from": "orders",
        "localField": "_id",
        "foreignField": "customerId",
        "as": "customerOrders"
    ]]
]

// $replaceRoot - 替换根文档
let pipeline: [BSONDocument] = [
    ["$replaceRoot": [
        "newRoot": "$address"
    ]]
]

// $group 累加器
let pipeline: [BSONDocument] = [
    ["$group": [
        "_id": "$category",
        "sum": ["$sum": "$amount"],
        "avg": ["$avg": "$amount"],
        "min": ["$min": "$amount"],
        "max": ["$max": "$amount"],
        "count": ["$count": [:]],
        "first": ["$first": "$name"],
        "last": ["$last": "$name"],
        "items": ["$push": "$name"],
        "uniqueItems": ["$addToSet": "$name"]
    ]]
]
```

---

## 索引管理

```swift
let users = try await db.collection("users")

// 创建单字段索引
let indexName = try await users.createIndex(
    keys: ["email": .int32(1)],  // 1: 升序, -1: 降序
    options: [:]
)

// 创建唯一索引
let indexName = try await users.createIndex(
    keys: ["email": .int32(1)],
    options: ["unique": .bool(true)]
)

// 创建复合索引
let indexName = try await users.createIndex(
    keys: [
        "lastName": .int32(1),
        "firstName": .int32(1)
    ],
    options: [:]
)

// 创建嵌套字段索引
let indexName = try await users.createIndex(
    keys: ["address.city": .int32(1)],
    options: [:]
)

// 列出所有索引
let indexes = try await users.listIndexes()
for index in indexes {
    print(index)
}

// 删除索引
try await users.dropIndex("email_1")

// 查询计划分析
let explain = await users.explain(
    filter: ["email": "alice@example.com"]
)
print("Index used: \(explain.indexUsed ?? "none")")
print("Documents scanned: \(explain.documentsScanned)")
```

---

## 事务操作

```swift
// 开启会话
let session = try await db.startSession()

// 开始事务
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

    // 提交事务
    try await session.commitTransaction()
} catch {
    // 回滚事务
    try await session.abortTransaction()
    throw error
}
```

---

## 游标操作

```swift
// 游标管理器
let cursorManager = db.cursorManager

// 创建游标并获取首批数据
let (firstBatch, cursorId) = await cursorManager.getFirstBatch(
    namespace: "test.users",
    documents: allDocuments,
    batchSize: 10
)

// 获取更多数据
let (nextBatch, newCursorId, namespace) = await cursorManager.getNextBatch(
    cursorId: cursorId,
    batchSize: 10
)

// 关闭游标
let killedCursors = await cursorManager.killCursors([cursorId])

// 关闭所有游标
await cursorManager.closeAll()
```

---

## 数据库命令

```swift
// 通用命令执行
let result = try await db.runCommand(["ping": .int32(1)])

// 服务器状态
let result = try await db.runCommand(["serverStatus": .int32(1)])

// 数据库统计
let result = try await db.runCommand(["dbStats": .int32(1)])

// 集合统计
let result = try await db.runCommand(["collStats": "users"])

// 验证数据库
let validationResult = await db.validate()
if validationResult.valid {
    print("Database is valid")
}

// 列出集合
let result = try await db.runCommand(["listCollections": .int32(1)])

// 创建集合
let result = try await db.runCommand(["create": "newCollection"])

// 删除集合
let result = try await db.runCommand(["drop": "oldCollection"])

// isMaster / hello
let result = try await db.runCommand(["isMaster": .int32(1)])
let result = try await db.runCommand(["hello": .int32(1)])

// buildInfo
let result = try await db.runCommand(["buildInfo": .int32(1)])
```

---

## 完整示例：用户管理系统

```swift
import MonoLiteSwift
import Foundation

@main
struct UserManagement {
    static func main() async throws {
        // 打开数据库
        let db = try await Database.open(path: "userdb.monodb")
        defer {
            Task {
                try? await db.close()
            }
        }

        // 获取用户集合
        let users = try await db.collection("users")

        // 创建唯一索引
        _ = try await users.createIndex(
            keys: ["email": .int32(1)],
            options: ["unique": .bool(true)]
        )
        _ = try await users.createIndex(
            keys: ["username": .int32(1)],
            options: ["unique": .bool(true)]
        )

        // 创建用户
        let newUser: BSONDocument = [
            "username": "alice",
            "email": "alice@example.com",
            "password": "hashed_password",
            "profile": .document([
                "firstName": "Alice",
                "lastName": "Smith",
                "age": .int32(25)
            ]),
            "roles": .array([.string("user")]),
            "createdAt": .datetime(Date()),
            "updatedAt": .datetime(Date())
        ]

        do {
            let id = try await users.insertOne(newUser)
            print("Created user with ID: \(id)")
        } catch {
            print("Failed to create user: \(error)")
        }

        // 查找用户
        if let user = try await users.findOne(["username": "alice"]) {
            print("Found user: \(user)")
        }

        // 更新用户资料
        _ = try await users.updateOne(
            filter: ["username": "alice"],
            update: [
                "$set": [
                    "profile.age": .int32(26),
                    "updatedAt": .datetime(Date())
                ],
                "$addToSet": [
                    "roles": "admin"
                ]
            ]
        )

        // 查询活跃成年用户
        let docs = try await users.find([
            "profile.age": ["$gte": .int32(18)]
        ])
        print("Found \(docs.count) adult users")

        // 按年龄分组统计
        let results = try await users.aggregate([
            ["$group": [
                "_id": .null,
                "avgAge": ["$avg": "$profile.age"],
                "totalUsers": ["$count": [:]]
            ]]
        ])
        print("Statistics: \(results)")

        // 验证数据库
        let validation = await db.validate()
        print("Database valid: \(validation.valid)")
    }
}
```
