// Created by Yanjunhui

import Foundation

extension Database {
    /// 执行数据库命令（对齐 Go: Database.RunCommand）
    /// EN: Execute database command (aligned with Go: Database.RunCommand)
    ///
    /// 注意：该接口用于 wire protocol 命令路由层，命令格式为"有序 BSONDocument"（类似 Go bson.D）。
    /// EN: Note: This interface is used by wire protocol command routing layer, command format is "ordered BSONDocument" (similar to Go bson.D).
    public func runCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        // 以第一个 key 作为 command name（Go 行为）
        // EN: Use first key as command name (Go behavior)
        guard let first = cmd.first else {
            throw MonoError.commandNotFound("")
        }

        switch first.key {
        case "ping":
            return BSONDocument([("ok", .int32(1))])

        case "isMaster", "ismaster":
            return isMasterCommand()

        case "hello":
            return helloCommand()

        case "buildInfo", "buildinfo":
            return buildInfoCommand()

        case "listCollections":
            return listCollectionsCommand()

        case "insert":
            return try await insertCommand(cmd)

        case "find":
            return try await findCommand(cmd)

        case "getMore":
            return try await getMoreCommand(cmd)

        case "killCursors":
            return try await killCursorsCommand(cmd)

        case "update":
            return try await updateCommand(cmd)

        case "delete":
            return try await deleteCommand(cmd)

        case "count":
            return try await countCommand(cmd)

        case "drop":
            return try await dropCommand(cmd)

        case "createIndexes":
            return try await createIndexesCommand(cmd)

        case "listIndexes":
            return try await listIndexesCommand(cmd)

        case "dropIndexes":
            return try await dropIndexesCommand(cmd)

        case "aggregate":
            return try await aggregateCommand(cmd)

        case "validate":
            let r = await validate()
            var doc = BSONDocument()
            doc["ok"] = .int32(r.valid ? 1 : 0)
            doc["valid"] = .bool(r.valid)
            doc["errors"] = .array(BSONArray(r.errors.map { .string($0) }))
            doc["warnings"] = .array(BSONArray(r.warnings.map { .string($0) }))
            return doc

        case "distinct":
            return try await distinctCommand(cmd)

        case "findAndModify", "findandmodify":
            return try await findAndModifyCommand(cmd)

        case "dbStats":
            return try await dbStatsCommand(cmd)

        case "collStats", "collstats":
            return try await collStatsCommand(cmd)

        case "explain":
            return try await explainCommand(cmd)

        case "serverStatus":
            return await serverStatusCommand()

        case "connectionStatus":
            return connectionStatusCommand()

        case "startTransaction":
            return try await startTransactionCommand(cmd)

        case "commitTransaction":
            return try await commitTransactionCommand(cmd)

        case "abortTransaction":
            return try await abortTransactionCommand(cmd)

        case "endSessions":
            return try await endSessionsCommand(cmd)

        case "refreshSessions":
            return try await refreshSessionsCommand(cmd)

        default:
            throw MonoError.commandNotFound(first.key)
        }
    }

    /// isMaster 命令
    /// EN: isMaster command
    private func isMasterCommand() -> BSONDocument {
        BSONDocument([
            ("ismaster", .bool(true)),
            ("maxBsonObjectSize", .int32(Int32(16 * 1024 * 1024))),
            ("maxMessageSizeBytes", .int32(Int32(48 * 1024 * 1024))),
            ("maxWriteBatchSize", .int32(Int32(Limits.maxWriteBatchSize))),
            ("localTime", .dateTime(Date())),
            ("minWireVersion", .int32(0)),
            ("maxWireVersion", .int32(13)),
            ("logicalSessionTimeoutMinutes", .int32(30)),
            ("readOnly", .bool(false)),
            ("ok", .int32(1)),
        ])
    }

    /// hello 命令
    /// EN: hello command
    private func helloCommand() -> BSONDocument {
        BSONDocument([
            ("isWritablePrimary", .bool(true)),
            ("ismaster", .bool(true)),
            ("maxBsonObjectSize", .int32(Int32(16 * 1024 * 1024))),
            ("maxMessageSizeBytes", .int32(Int32(48 * 1024 * 1024))),
            ("maxWriteBatchSize", .int32(Int32(Limits.maxWriteBatchSize))),
            ("localTime", .dateTime(Date())),
            ("minWireVersion", .int32(0)),
            ("maxWireVersion", .int32(13)),
            ("logicalSessionTimeoutMinutes", .int32(30)),
            ("readOnly", .bool(false)),
            ("helloOk", .bool(true)),
            ("ok", .int32(1)),
        ])
    }

    /// buildInfo 命令
    /// EN: buildInfo command
    private func buildInfoCommand() -> BSONDocument {
        BSONDocument([
            ("version", .string("0.1.0")),
            ("gitVersion", .string("monodb")),
            ("modules", .array(BSONArray())),
            ("ok", .int32(1)),
        ])
    }

    /// listCollections 命令
    /// EN: listCollections command
    private func listCollectionsCommand() -> BSONDocument {
        var arr = BSONArray()
        for name in listCollections() {
            arr.append(.document(BSONDocument([
                ("name", .string(name)),
                ("type", .string("collection")),
            ])))
        }
        return BSONDocument([
            ("cursor", .document(BSONDocument([
                ("id", .int64(0)),
                ("firstBatch", .array(arr)),
            ]))),
            ("ok", .int32(1)),
        ])
    }
}

// MARK: - 命令实现（子集）/ Command Implementations (Subset)

extension Database {
    /// insert 命令
    /// EN: insert command
    private func insertCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard let colName = cmd[firstKey: "insert"]?.stringValue else {
            throw MonoError.badValue("insert command requires collection name")
        }

        let col = try await collection(colName)

        let docsVal = cmd["documents"]
        guard case .array(let arr)? = docsVal else {
            throw MonoError.badValue("insert command requires documents array")
        }

        var docs: [BSONDocument] = []
        docs.reserveCapacity(arr.count)
        for item in arr {
            guard case .document(let d) = item else { continue }
            docs.append(d)
        }

        // 事务上下文（对齐 Go: ExtractCommandContext）
        // EN: Transaction context (aligned with Go: ExtractCommandContext)
        let txn = try await extractTransactionIfAny(cmd)
        let inserted: Int32
        if let txn {
            var ids: [BSONValue] = []
            ids.reserveCapacity(docs.count)
            for d in docs {
                ids.append(try await col.insertOne(d, in: txn))
            }
            inserted = Int32(ids.count)
        } else {
            let ids = try await col.insert(docs)
            inserted = Int32(ids.count)
        }

        return BSONDocument([
            ("n", .int32(inserted)),
            ("ok", .int32(1)),
        ])
    }

    /// find 命令
    /// EN: find command
    private func findCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard let colName = cmd[firstKey: "find"]?.stringValue else {
            throw MonoError.badValue("find command requires collection name")
        }

        let dbName = cmd["$db"]?.stringValue
        let ns = (dbName?.isEmpty == false) ? "\(dbName!).\(colName)" : colName

        let filter: BSONDocument
        if let v = cmd["filter"], case .document(let d) = v {
            filter = d
        } else {
            filter = BSONDocument()
        }

        var opts = QueryOptions()
        if let v = cmd["sort"], case .document(let d) = v { opts.sort = d }
        if let v = cmd["projection"], case .document(let d) = v { opts.projection = d }
        if let v = cmd["limit"] { opts.limit = Int64(v.intValue ?? 0) }
        if let v = cmd["skip"] { opts.skip = Int64(v.intValue ?? 0) }
        let batchSize = Int64(cmd["batchSize"]?.intValue ?? 0)

        // find 不隐式创建集合（Go 行为）
        // EN: find does not implicitly create collection (Go behavior)
        let col = getCollection(colName)
        let docs: [BSONDocument]
        if let col {
            if !opts.sort.isEmpty || opts.limit > 0 || opts.skip > 0 || !opts.projection.isEmpty {
                docs = try await col.findWithOptions(filter, options: opts)
            } else {
                docs = try await col.find(filter)
            }
        } else {
            docs = []
        }

        let (firstBatch, cursorId) = await cursorManager.getFirstBatch(namespace: ns, documents: docs, batchSize: batchSize)
        let arr = BSONArray(firstBatch.map { .document($0) })

        return BSONDocument([
            ("cursor", .document(BSONDocument([
                ("id", .int64(cursorId)),
                ("ns", .string(ns)),
                ("firstBatch", .array(arr)),
            ]))),
            ("ok", .int32(1)),
        ])
    }

    /// getMore 命令
    /// EN: getMore command
    private func getMoreCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        // getMore: <cursorId>, collection: <name>, batchSize: <int>
        // EN: getMore: <cursorId>, collection: <name>, batchSize: <int>
        guard let cursorIdVal = cmd[firstKey: "getMore"]?.int64Value ?? cmd[firstKey: "getMore"]?.intValue.map(Int64.init) else {
            throw MonoError.badValue("getMore requires cursor id")
        }
        if cursorIdVal == 0 {
            throw MonoError.badValue("cursor id is required")
        }
        let cursorId = CursorID(cursorIdVal)
        let batchSize = Int64(cmd["batchSize"]?.intValue ?? 0)

        let (nextBatch, nextCursorId, ns) = await cursorManager.getNextBatch(cursorId: cursorId, batchSize: batchSize)
        guard let ns else {
            // cursor 不存在
            // EN: cursor does not exist
            throw MonoError.cursorNotFound(cursorId)
        }

        let arr = BSONArray(nextBatch.map { .document($0) })
        return BSONDocument([
            ("cursor", .document(BSONDocument([
                ("id", .int64(nextCursorId)),
                ("ns", .string(ns)),
                ("nextBatch", .array(arr)),
            ]))),
            ("ok", .int32(1)),
        ])
    }

    /// killCursors 命令
    /// EN: killCursors command
    private func killCursorsCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard let colName = cmd["killCursors"]?.stringValue ?? cmd[firstKey: "killCursors"]?.stringValue else {
            throw MonoError.badValue("killCursors requires collection name")
        }
        // 目前不按集合校验 cursor namespace，按 cursorId 直接关闭
        // EN: Currently not validating cursor namespace by collection, closing directly by cursorId
        _ = colName

        guard case .array(let arr)? = cmd["cursors"] else {
            throw MonoError.badValue("killCursors requires cursors array")
        }

        var ids: [CursorID] = []
        ids.reserveCapacity(arr.count)
        for v in arr {
            let id = v.int64Value ?? Int64(v.intValue ?? 0)
            ids.append(CursorID(id))
        }

        let killedIds = await cursorManager.killCursors(ids)
        let killedSet = Set(killedIds)

        var cursorsKilled = BSONArray()
        for id in killedIds { cursorsKilled.append(.int64(id)) }

        var cursorsNotFound = BSONArray()
        for id in ids where !killedSet.contains(id) { cursorsNotFound.append(.int64(id)) }

        // Go 对齐：cursorsAlive/cursorsUnknown 目前返回空数组
        // EN: Go aligned: cursorsAlive/cursorsUnknown currently return empty arrays
        return BSONDocument([
            ("cursorsKilled", .array(cursorsKilled)),
            ("cursorsNotFound", .array(cursorsNotFound)),
            ("cursorsAlive", .array(BSONArray())),
            ("cursorsUnknown", .array(BSONArray())),
            ("ok", .int32(1)),
        ])
    }

    /// update 命令
    /// EN: update command
    private func updateCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard let colName = cmd[firstKey: "update"]?.stringValue else {
            throw MonoError.badValue("update command requires collection name")
        }
        guard case .array(let updates)? = cmd["updates"] else {
            throw MonoError.badValue("update command requires updates array")
        }

        // 简化：依次执行，每条 update 仅支持 q/u/upsert
        // EN: Simplified: execute sequentially, each update only supports q/u/upsert
        var totalMatched: Int64 = 0
        var totalModified: Int64 = 0
        var upserted = BSONArray()

        let txn = try await extractTransactionIfAny(cmd)
        for (idx, item) in updates.enumerated() {
            guard case .document(let udoc) = item else { continue }
            let filter = (udoc["q"]?.documentValue) ?? BSONDocument()
            let updateDoc = (udoc["u"]?.documentValue) ?? BSONDocument()
            let upsertFlag = (udoc["upsert"]?.boolValue) ?? false

            let col: MonoCollection?
            if upsertFlag {
                col = try await collection(colName)
            } else {
                col = getCollection(colName)
            }
            guard let col else {
                // 非 upsert 且集合不存在：返回 0 匹配/修改
                // EN: Non-upsert and collection doesn't exist: return 0 matched/modified
                continue
            }

            let res: UpdateResult
            if let txn {
                res = try await col.updateOne(filter, update: updateDoc, upsert: upsertFlag, in: txn)
            } else {
                res = try await col.updateOne(filter, update: updateDoc, upsert: upsertFlag)
            }
            totalMatched += res.matchedCount
            totalModified += res.modifiedCount
            if res.upsertedCount > 0, let id = res.upsertedID {
                upserted.append(.document(BSONDocument([
                    ("index", .int32(Int32(idx))),
                    ("_id", id),
                ])))
            }
        }

        var out = BSONDocument([
            ("n", .int64(totalMatched)),
            ("nModified", .int64(totalModified)),
            ("ok", .int32(1)),
        ])
        if !upserted.isEmpty {
            out["upserted"] = .array(upserted)
        }
        return out
    }

    /// delete 命令
    /// EN: delete command
    private func deleteCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard let colName = cmd[firstKey: "delete"]?.stringValue else {
            throw MonoError.badValue("delete command requires collection name")
        }
        guard case .array(let deletes)? = cmd["deletes"] else {
            throw MonoError.badValue("delete command requires deletes array")
        }

        let txn = try await extractTransactionIfAny(cmd)
        let col = getCollection(colName)
        guard let col else {
            return BSONDocument([("n", .int32(0)), ("ok", .int32(1))])
        }

        var total: Int32 = 0
        for item in deletes {
            guard case .document(let ddoc) = item else { continue }
            let filter = (ddoc["q"]?.documentValue) ?? BSONDocument()
            let limit = ddoc["limit"]?.intValue ?? 1
            if limit == 1 {
                if let txn {
                    total += Int32(try await col.deleteOne(filter, in: txn))
                } else {
                    total += Int32(try await col.deleteOne(filter))
                }
            } else {
                // 简化：多删用循环 deleteOne
                // EN: Simplified: use deleteOne loop for multi-delete
                while true {
                    let n: Int64
                    if let txn {
                        n = try await col.deleteOne(filter, in: txn)
                    } else {
                        n = try await col.deleteOne(filter)
                    }
                    if n == 0 { break }
                    total += Int32(n)
                }
            }
        }

        return BSONDocument([
            ("n", .int32(total)),
            ("ok", .int32(1)),
        ])
    }

    /// count 命令
    /// EN: count command
    private func countCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard let colName = cmd[firstKey: "count"]?.stringValue else {
            throw MonoError.badValue("count command requires collection name")
        }
        let filter = (cmd["query"]?.documentValue) ?? BSONDocument()
        let col = getCollection(colName)
        let n: Int64
        if let col {
            n = Int64(try await col.find(filter).count)
        } else {
            n = 0
        }
        return BSONDocument([
            ("n", .int64(n)),
            ("ok", .int32(1)),
        ])
    }

    /// drop 命令
    /// EN: drop command
    private func dropCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard let colName = cmd[firstKey: "drop"]?.stringValue else {
            throw MonoError.badValue("drop command requires collection name")
        }
        try await dropCollection(colName)
        return BSONDocument([("ok", .int32(1))])
    }

    /// createIndexes 命令
    /// EN: createIndexes command
    private func createIndexesCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard let colName = cmd[firstKey: "createIndexes"]?.stringValue else {
            throw MonoError.badValue("createIndexes requires collection name")
        }
        guard case .array(let indexes)? = cmd["indexes"] else {
            throw MonoError.badValue("createIndexes requires indexes array")
        }

        let col = try await collection(colName)
        let before = await col.listIndexes().count

        for item in indexes {
            guard case .document(let idxDoc) = item else { continue }
            guard let keys = idxDoc["key"]?.documentValue else { continue }
            var options = BSONDocument()
            if let name = idxDoc["name"]?.stringValue { options["name"] = .string(name) }
            if let unique = idxDoc["unique"]?.boolValue { options["unique"] = .bool(unique) }
            _ = try await col.createIndex(keys: keys, options: options)
        }

        let after = await col.listIndexes().count
        return BSONDocument([
            ("createdCollectionAutomatically", .bool(false)),
            ("numIndexesBefore", .int32(Int32(before))),
            ("numIndexesAfter", .int32(Int32(after))),
            ("ok", .int32(1)),
        ])
    }

    /// listIndexes 命令
    /// EN: listIndexes command
    private func listIndexesCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard let colName = cmd[firstKey: "listIndexes"]?.stringValue else {
            throw MonoError.badValue("listIndexes requires collection name")
        }

        let col = getCollection(colName)
        guard let col else {
            return BSONDocument([
                ("ok", .int32(0)),
                ("errmsg", .string("ns does not exist")),
                ("code", .int32(26)),
            ])
        }
        let docs = await col.listIndexes()

        let dbName = cmd["$db"]?.stringValue ?? ""
        let ns = dbName.isEmpty ? colName : "\(dbName).\(colName)"

        let arr = BSONArray(docs.map { .document($0) })
        return BSONDocument([
            ("cursor", .document(BSONDocument([
                ("id", .int64(0)),
                ("ns", .string(ns)),
                ("firstBatch", .array(arr)),
            ]))),
            ("ok", .int32(1)),
        ])
    }

    /// dropIndexes 命令
    /// EN: dropIndexes command
    private func dropIndexesCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard let colName = cmd[firstKey: "dropIndexes"]?.stringValue else {
            throw MonoError.badValue("dropIndexes requires collection name")
        }
        guard let index = cmd["index"]?.stringValue else {
            throw MonoError.badValue("dropIndexes requires index")
        }

        guard let col = getCollection(colName) else {
            return BSONDocument([
                ("ok", .int32(0)),
                ("errmsg", .string("ns does not exist")),
            ])
        }
        let before = await col.listIndexes().count

        if index == "*" {
            // 删除所有索引（除了 _id_）
            // EN: Drop all indexes (except _id_)
            let idxDocs = await col.listIndexes()
            for d in idxDocs {
                if d["name"]?.stringValue == "_id_" { continue }
                if let name = d["name"]?.stringValue {
                    try await col.dropIndex(name)
                }
            }
        } else {
            try await col.dropIndex(index)
        }

        return BSONDocument([
            ("nIndexesWas", .int32(Int32(before))),
            ("ok", .int32(1)),
        ])
    }

    /// aggregate 命令
    /// EN: aggregate command
    private func aggregateCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard let colName = cmd[firstKey: "aggregate"]?.stringValue else {
            throw MonoError.badValue("aggregate command requires collection name")
        }
        guard case .array(let pipelineArr)? = cmd["pipeline"] else {
            throw MonoError.badValue("aggregate requires pipeline array")
        }

        var stages: [BSONDocument] = []
        stages.reserveCapacity(pipelineArr.count)
        for v in pipelineArr {
            if case .document(let d) = v {
                stages.append(d)
            }
        }

        let pipeline = try Pipeline(stages: stages, db: self)

        // aggregate 不隐式创建集合（Go 行为）
        // EN: aggregate does not implicitly create collection (Go behavior)
        let col = getCollection(colName)
        let inputDocs: [BSONDocument]
        if let col {
            inputDocs = try await col.find(BSONDocument())
        } else {
            inputDocs = []
        }

        let outDocs = try await pipeline.execute(inputDocs)

        let dbName = cmd["$db"]?.stringValue
        let ns = (dbName?.isEmpty == false) ? "\(dbName!).\(colName)" : colName

        let batch = BSONArray(outDocs.map { .document($0) })
        return BSONDocument([
            ("cursor", .document(BSONDocument([
                ("id", .int64(0)),
                ("ns", .string(ns)),
                ("firstBatch", .array(batch)),
            ]))),
            ("ok", .int32(1)),
        ])
    }

    /// distinct 命令
    /// EN: distinct command
    private func distinctCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard let colName = cmd[firstKey: "distinct"]?.stringValue else {
            throw MonoError.badValue("distinct command requires collection name")
        }
        guard let field = cmd["key"]?.stringValue else {
            throw MonoError.badValue("distinct requires key")
        }
        let filter = cmd["query"]?.documentValue ?? BSONDocument()

        let col = getCollection(colName)
        let values: [BSONValue]
        if let col {
            values = try await col.distinct(field, filter: filter)
        } else {
            values = []
        }
        return BSONDocument([
            ("values", .array(BSONArray(values))),
            ("ok", .int32(1)),
        ])
    }

    /// findAndModify 命令
    /// EN: findAndModify command
    private func findAndModifyCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        let colName = cmd[firstKey: "findAndModify"]?.stringValue ?? cmd[firstKey: "findandmodify"]?.stringValue
        guard let colName, !colName.isEmpty else {
            throw MonoError.badValue("findAndModify command requires collection name")
        }

        let query = cmd["query"]?.documentValue ?? BSONDocument()
        let update = cmd["update"]?.documentValue
        let remove = cmd["remove"]?.boolValue ?? false
        let returnNew = cmd["new"]?.boolValue ?? false
        let upsert = cmd["upsert"]?.boolValue ?? false
        let sort = cmd["sort"]?.documentValue ?? BSONDocument()

        // aggregate/findAndModify 与 Go 一致：仅 upsert 时允许创建集合
        // EN: aggregate/findAndModify aligned with Go: only create collection when upsert
        let col: MonoCollection?
        if upsert {
            col = try await collection(colName)
        } else {
            col = getCollection(colName)
        }

        guard let col else {
            return BSONDocument([
                ("lastErrorObject", .document(BSONDocument([
                    ("n", .int32(0)),
                    ("updatedExisting", .bool(false)),
                ]))),
                ("value", .null),
                ("ok", .int32(1)),
            ])
        }

        // 先根据 query + sort 选出目标文档（用 _id 锚定后续 update/delete，避免 updateOne 的扫描顺序偏差）
        // EN: First select target document by query + sort (anchor by _id for subsequent update/delete, avoid updateOne scan order deviation)
        let target: BSONDocument?
        if sort.isEmpty {
            target = try await col.findOne(query)
        } else {
            let opts = QueryOptions(sort: sort, limit: 1, skip: 0, projection: BSONDocument())
            target = try await col.findWithOptions(query, options: opts).first
        }

        if remove {
            guard let old = target else {
                return BSONDocument([
                    ("lastErrorObject", .document(BSONDocument([
                        ("n", .int32(0)),
                        ("updatedExisting", .bool(false)),
                    ]))),
                    ("value", .null),
                    ("ok", .int32(1)),
                ])
            }
            // 按 _id 删除以确保命中正确文档
            // EN: Delete by _id to ensure hitting the correct document
            if let id = old.getValue(forPath: "_id") {
                _ = try await col.deleteOne(BSONDocument([("_id", id)]))
            } else {
                _ = try await col.deleteOne(query)
            }
            return BSONDocument([
                ("lastErrorObject", .document(BSONDocument([
                    ("n", .int32(1)),
                    ("updatedExisting", .bool(false)),
                ]))),
                ("value", .document(old)),
                ("ok", .int32(1)),
            ])
        }

        guard let update else {
            throw MonoError.badValue("findAndModify requires update when remove is false")
        }

        if let old = target, let id = old.getValue(forPath: "_id") {
            _ = try await col.updateOne(BSONDocument([("_id", id)]), update: update, upsert: false)
            let value: BSONDocument?
            if returnNew {
                value = try await col.findById(id)
            } else {
                value = old
            }
            return BSONDocument([
                ("lastErrorObject", .document(BSONDocument([
                    ("n", .int32(1)),
                    ("updatedExisting", .bool(true)),
                ]))),
                ("value", value.map { .document($0) } ?? .null),
                ("ok", .int32(1)),
            ])
        }

        // 未找到
        // EN: Not found
        if !upsert {
            return BSONDocument([
                ("lastErrorObject", .document(BSONDocument([
                    ("n", .int32(0)),
                    ("updatedExisting", .bool(false)),
                ]))),
                ("value", .null),
                ("ok", .int32(1)),
            ])
        }

        // upsert 路径
        // EN: upsert path
        let res = try await col.updateOne(query, update: update, upsert: true)
        let upsertedId = res.upsertedID
        let value: BSONDocument?
        if returnNew, let upsertedId {
            value = try await col.findById(upsertedId)
        } else {
            value = nil
        }
        var leo = BSONDocument([
            ("n", .int32(1)),
            ("updatedExisting", .bool(false)),
        ])
        if let upsertedId {
            leo["upserted"] = upsertedId
        }
        return BSONDocument([
            ("lastErrorObject", .document(leo)),
            ("value", value.map { .document($0) } ?? .null),
            ("ok", .int32(1)),
        ])
    }

    /// dbStats 命令
    /// EN: dbStats command
    private func dbStatsCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        let dbName = cmd["$db"]?.stringValue ?? "db"

        var totalDocs: Int64 = 0
        var totalIndexes: Int32 = 0
        for (_, col) in collections {
            totalDocs += await col.count
            totalIndexes += Int32((await col.listIndexes()).count)
        }

        let pageCount = await pager.pageCount
        let dataSize = Int64(pageCount) * Int64(StorageConstants.pageSize)

        return BSONDocument([
            ("db", .string(dbName)),
            ("collections", .int32(Int32(collections.count))),
            ("views", .int32(0)),
            ("objects", .int64(totalDocs)),
            ("dataSize", .int64(dataSize)),
            ("storageSize", .int64(dataSize)),
            ("indexes", .int32(totalIndexes)),
            ("indexSize", .int64(0)),
            ("totalSize", .int64(dataSize)),
            ("ok", .int32(1)),
        ])
    }

    /// collStats 命令
    /// EN: collStats command
    private func collStatsCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        let colName = cmd[firstKey: "collStats"]?.stringValue ?? cmd[firstKey: "collstats"]?.stringValue
        guard let colName, !colName.isEmpty else {
            throw MonoError.badValue("collStats command requires collection name")
        }
        guard let col = getCollection(colName) else {
            return BSONDocument([
                ("ok", .int32(0)),
                ("errmsg", .string("ns not found")),
            ])
        }

        let count = await col.count
        let nindexes = Int32((await col.listIndexes()).count)
        return BSONDocument([
            ("ns", .string(colName)),
            ("count", .int64(count)),
            ("size", .int64(0)),
            ("avgObjSize", .int64(0)),
            ("storageSize", .int64(0)),
            ("nindexes", .int32(nindexes)),
            ("totalIndexSize", .int64(0)),
            ("ok", .int32(1)),
        ])
    }

    /// serverStatus 命令
    /// EN: serverStatus command
    private func serverStatusCommand() async -> BSONDocument {
        // 统计信息（简化，关键字段对齐 Go）
        // EN: Statistics (simplified, key fields aligned with Go)
        let pageCount = await pager.pageCount
        let freePages = await pager.freePageCount
        let cursorOpen = await cursorManager.count

        var totalDocs: Int64 = 0
        var totalIndexes: Int32 = 0
        for (_, col) in collections {
            totalDocs += await col.count
            totalIndexes += Int32((await col.listIndexes()).count)
        }

        let pid = Int64(ProcessInfo.processInfo.processIdentifier)
        let uptimeMillis = Int64(Date().timeIntervalSince(startTime) * 1000)
        let uptime = uptimeMillis / 1000

        return BSONDocument([
            ("host", .string("localhost")),
            ("version", .string("0.1.0")),
            ("process", .string("monodb")),
            ("pid", .int64(pid)),
            ("uptime", .int64(uptime)),
            ("uptimeMillis", .int64(uptimeMillis)),
            ("collections", .int32(Int32(collections.count))),
            ("documents", .int64(totalDocs)),
            ("indexes", .int32(totalIndexes)),
            ("storageEngine", .document(BSONDocument([
                ("name", .string("monodb")),
                ("pageSize", .int32(Int32(StorageConstants.pageSize))),
                ("pageCount", .int32(Int32(pageCount))),
                ("freePages", .int32(Int32(freePages))),
            ]))),
            ("cursors", .document(BSONDocument([
                ("totalOpen", .int32(Int32(cursorOpen))),
            ]))),
            ("ok", .int32(1)),
        ])
    }

    /// connectionStatus 命令
    /// EN: connectionStatus command
    private func connectionStatusCommand() -> BSONDocument {
        BSONDocument([
            ("current", .int32(1)),
            ("available", .int32(1000)),
            ("totalCreated", .int64(1)),
            ("ok", .int32(1)),
        ])
    }

    /// explain 命令
    /// EN: explain command
    private func explainCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        // 提取内部命令进行解释
        // EN: Extract inner command to explain
        guard let explainVal = cmd["explain"], case .document(let explainCmd) = explainVal else {
            throw MonoError.badValue("explain requires a command document")
        }

        var colName: String = ""
        var filter = BSONDocument()
        var sort: BSONDocument?
        var projection: BSONDocument?

        for (k, v) in explainCmd {
            switch k {
            case "find", "aggregate":
                colName = v.stringValue ?? ""
            case "filter":
                if case .document(let d) = v { filter = d }
            case "sort":
                if case .document(let d) = v { sort = d }
            case "projection":
                if case .document(let d) = v { projection = d }
            default:
                break
            }
        }

        if colName.isEmpty {
            throw MonoError.badValue("explain requires a command with collection name")
        }

        guard let col = getCollection(colName) else {
            // 集合不存在；返回空 explain
            // EN: Collection does not exist; return empty explain
            var result = ExplainResult()
            result.namespace = colName
            result.stage = "EOF"
            result.totalDocsExamined = 0
            result.totalKeysExamined = 0
            return result.toBSON()
        }

        let result = await col.explain(filter: filter, sort: sort, projection: projection)
        return result.toBSON()
    }
}

// MARK: - BSON 辅助 / BSON Helpers

private extension BSONDocument {
    /// 获取第一个匹配 key 的值
    /// EN: Get value for first matching key
    subscript(firstKey key: String) -> BSONValue? {
        for (k, v) in self {
            if k == key { return v }
        }
        return nil
    }
}

// MARK: - 会话/事务辅助 / Session/Transaction Helpers

extension Database {
    /// 从命令中提取事务（如果有）
    /// EN: Extract transaction from command (if any)
    private func extractTransactionIfAny(_ cmd: BSONDocument) async throws -> Transaction? {
        // 无会话字段 -> 非事务
        // EN: No session field -> not a transaction
        let ctx = try await sessionManager.extractCommandContext(cmd: cmd)
        return ctx.transaction
    }
}

// MARK: - 事务/会话命令 / Transaction/Session Commands

extension Database {
    /// startTransaction 命令
    /// EN: startTransaction command
    private func startTransactionCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard let lsidVal = cmd["lsid"], case .document(let lsid) = lsidVal else {
            throw MonoError.badValue("lsid is required for startTransaction")
        }
        guard let txnNumVal = cmd["txnNumber"] else {
            throw MonoError.badValue("txnNumber is required for startTransaction")
        }
        let txnNumber = txnNumVal.int64Value ?? Int64(txnNumVal.intValue ?? 0)
        guard txnNumber > 0 else { throw MonoError.badValue("txnNumber is required for startTransaction") }

        let readConcern = cmd["readConcern"]?.documentValue?["level"]?.stringValue ?? "local"
        let writeConcern = cmd["writeConcern"]?.documentValue?["w"]?.stringValue ?? "majority"

        try await sessionManager.startTransaction(lsid: lsid, txnNumber: txnNumber, readConcern: readConcern, writeConcern: writeConcern)
        return BSONDocument([("ok", .int32(1))])
    }

    /// commitTransaction 命令
    /// EN: commitTransaction command
    private func commitTransactionCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard let lsidVal = cmd["lsid"], case .document(let lsid) = lsidVal else {
            throw MonoError.badValue("lsid is required for commitTransaction")
        }
        guard let txnNumVal = cmd["txnNumber"] else {
            throw MonoError.badValue("txnNumber is required for commitTransaction")
        }
        let txnNumber = txnNumVal.int64Value ?? Int64(txnNumVal.intValue ?? 0)
        guard txnNumber > 0 else { throw MonoError.badValue("txnNumber is required for commitTransaction") }
        try await sessionManager.commitTransaction(lsid: lsid, txnNumber: txnNumber)
        return BSONDocument([("ok", .int32(1))])
    }

    /// abortTransaction 命令
    /// EN: abortTransaction command
    private func abortTransactionCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard let lsidVal = cmd["lsid"], case .document(let lsid) = lsidVal else {
            throw MonoError.badValue("lsid is required for abortTransaction")
        }
        guard let txnNumVal = cmd["txnNumber"] else {
            throw MonoError.badValue("txnNumber is required for abortTransaction")
        }
        let txnNumber = txnNumVal.int64Value ?? Int64(txnNumVal.intValue ?? 0)
        guard txnNumber > 0 else { throw MonoError.badValue("txnNumber is required for abortTransaction") }
        try await sessionManager.abortTransaction(lsid: lsid, txnNumber: txnNumber)
        return BSONDocument([("ok", .int32(1))])
    }

    /// endSessions 命令
    /// EN: endSessions command
    private func endSessionsCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard case .array(let arr)? = cmd[firstKey: "endSessions"] else {
            throw MonoError.badValue("endSessions requires an array of session ids")
        }
        for v in arr {
            if case .document(let lsid) = v {
                try? await sessionManager.endSession(lsid: lsid)
            }
        }
        return BSONDocument([("ok", .int32(1))])
    }

    /// refreshSessions 命令
    /// EN: refreshSessions command
    private func refreshSessionsCommand(_ cmd: BSONDocument) async throws -> BSONDocument {
        guard case .array(let arr)? = cmd[firstKey: "refreshSessions"] else {
            throw MonoError.badValue("refreshSessions requires an array of session ids")
        }
        for v in arr {
            if case .document(let lsid) = v {
                try? await sessionManager.refreshSession(lsid: lsid)
            }
        }
        return BSONDocument([("ok", .int32(1))])
    }
}
