// Created by Yanjunhui

import Foundation

// MARK: - 更新操作

/// 应用更新操作符到文档
public func applyUpdate(_ doc: inout BSONDocument, update: BSONDocument) throws {
    for (key, value) in update {
        switch key {
        case "$set":
            guard case .document(let setDoc) = value else {
                throw MonoError.badValue("$set value must be a document")
            }
            for (setKey, setVal) in setDoc {
                setField(&doc, key: setKey, value: setVal)
            }

        case "$unset":
            guard case .document(let unsetDoc) = value else {
                throw MonoError.badValue("$unset value must be a document")
            }
            for (unsetKey, _) in unsetDoc {
                removeField(&doc, key: unsetKey)
            }

        case "$inc":
            guard case .document(let incDoc) = value else {
                throw MonoError.badValue("$inc value must be a document")
            }
            for (incKey, incVal) in incDoc {
                try incrementField(&doc, key: incKey, value: incVal)
            }

        case "$mul":
            guard case .document(let mulDoc) = value else {
                throw MonoError.badValue("$mul value must be a document")
            }
            for (mulKey, mulVal) in mulDoc {
                try multiplyField(&doc, key: mulKey, value: mulVal)
            }

        case "$min":
            guard case .document(let minDoc) = value else {
                throw MonoError.badValue("$min value must be a document")
            }
            for (minKey, minVal) in minDoc {
                updateFieldMin(&doc, key: minKey, value: minVal)
            }

        case "$max":
            guard case .document(let maxDoc) = value else {
                throw MonoError.badValue("$max value must be a document")
            }
            for (maxKey, maxVal) in maxDoc {
                updateFieldMax(&doc, key: maxKey, value: maxVal)
            }

        case "$rename":
            guard case .document(let renameDoc) = value else {
                throw MonoError.badValue("$rename value must be a document")
            }
            for (oldName, newNameVal) in renameDoc {
                guard case .string(let newName) = newNameVal else {
                    throw MonoError.badValue("$rename target must be a string")
                }
                renameField(&doc, oldName: oldName, newName: newName)
            }

        case "$push":
            guard case .document(let pushDoc) = value else {
                throw MonoError.badValue("$push value must be a document")
            }
            for (pushKey, pushVal) in pushDoc {
                try pushToArray(&doc, key: pushKey, value: pushVal)
            }

        case "$pop":
            guard case .document(let popDoc) = value else {
                throw MonoError.badValue("$pop value must be a document")
            }
            for (popKey, popVal) in popDoc {
                popFromArray(&doc, key: popKey, value: popVal)
            }

        case "$pull":
            guard case .document(let pullDoc) = value else {
                throw MonoError.badValue("$pull value must be a document")
            }
            for (pullKey, pullVal) in pullDoc {
                pullFromArray(&doc, key: pullKey, value: pullVal)
            }

        case "$addToSet":
            guard case .document(let addDoc) = value else {
                throw MonoError.badValue("$addToSet value must be a document")
            }
            for (addKey, addVal) in addDoc {
                addToSet(&doc, key: addKey, value: addVal)
            }

        case "$pullAll":
            guard case .document(let pullAllDoc) = value else {
                throw MonoError.badValue("$pullAll value must be a document")
            }
            for (pullKey, pullVal) in pullAllDoc {
                pullAllFromArray(&doc, key: pullKey, value: pullVal)
            }

        case "$currentDate":
            guard case .document(let dateDoc) = value else {
                throw MonoError.badValue("$currentDate value must be a document")
            }
            for (dateKey, dateVal) in dateDoc {
                setCurrentDate(&doc, key: dateKey, spec: dateVal)
            }

        case "$setOnInsert":
            // $setOnInsert 仅在 upsert 产生新文档时生效，在普通更新中忽略
            break

        default:
            // 非操作符字段，直接设置（替换模式）
            if !key.hasPrefix("$") {
                setField(&doc, key: key, value: value)
            }
        }
    }
}

// MARK: - 字段操作

/// 设置文档字段
public func setField(_ doc: inout BSONDocument, key: String, value: BSONValue) {
    // 处理点号路径
    if key.contains(".") {
        setNestedField(&doc, path: key, value: value)
        return
    }

    doc[key] = value
}

/// 设置嵌套字段
private func setNestedField(_ doc: inout BSONDocument, path: String, value: BSONValue) {
    let parts = path.split(separator: ".").map(String.init)

    if parts.count == 1 {
        doc[path] = value
        return
    }

    var current = doc
    var parents: [(String, BSONDocument)] = []

    // 遍历到最后一级
    for i in 0..<(parts.count - 1) {
        let part = parts[i]
        if let existing = current[part], case .document(let nested) = existing {
            parents.append((part, current))
            current = nested
        } else {
            parents.append((part, current))
            current = BSONDocument()
        }
    }

    // 设置最终值
    current[parts.last!] = value

    // 反向重建文档链
    for i in stride(from: parents.count - 1, through: 0, by: -1) {
        let (key, _) = parents[i]
        var parent = i == 0 ? doc : parents[i - 1].1
        parent[key] = .document(current)
        current = parent
        if i == 0 {
            doc = current
        }
    }
}

/// 移除文档字段
public func removeField(_ doc: inout BSONDocument, key: String) {
    // 处理点号路径
    if key.contains(".") {
        removeNestedField(&doc, path: key)
        return
    }

    _ = doc.removeValue(forKey: key)
}

/// 移除嵌套字段
private func removeNestedField(_ doc: inout BSONDocument, path: String) {
    let parts = path.split(separator: ".").map(String.init)

    if parts.count == 1 {
        _ = doc.removeValue(forKey: path)
        return
    }

    // 获取父文档
    let parentPath = parts.dropLast().joined(separator: ".")
    if let parentVal = doc.getValue(forPath: parentPath),
       case .document(var parentDoc) = parentVal {
        _ = parentDoc.removeValue(forKey: parts.last!)
        setNestedField(&doc, path: parentPath, value: .document(parentDoc))
    }
}

/// 增加字段值
public func incrementField(_ doc: inout BSONDocument, key: String, value: BSONValue) throws {
    let incAmount = toDouble(value)

    if let existing = doc.getValue(forPath: key) {
        let currentVal = toDouble(existing)
        let newVal = currentVal + incAmount

        // 保持类型
        let newValue: BSONValue
        switch existing {
        case .int32:
            newValue = .int32(Int32(newVal))
        case .int64:
            newValue = .int64(Int64(newVal))
        default:
            newValue = .double(newVal)
        }
        setField(&doc, key: key, value: newValue)
    } else {
        // 字段不存在，直接设置
        setField(&doc, key: key, value: value)
    }
}

/// 乘法更新
public func multiplyField(_ doc: inout BSONDocument, key: String, value: BSONValue) throws {
    let mulAmount = toDouble(value)

    if let existing = doc.getValue(forPath: key) {
        let currentVal = toDouble(existing)
        let newVal = currentVal * mulAmount

        // 保持类型
        let newValue: BSONValue
        switch existing {
        case .int32:
            newValue = .int32(Int32(newVal))
        case .int64:
            newValue = .int64(Int64(newVal))
        default:
            newValue = .double(newVal)
        }
        setField(&doc, key: key, value: newValue)
    } else {
        // 字段不存在，设置为 0
        setField(&doc, key: key, value: .double(0))
    }
}

/// 取最小值更新
public func updateFieldMin(_ doc: inout BSONDocument, key: String, value: BSONValue) {
    if let existing = doc.getValue(forPath: key) {
        if compareBSONValues(value, existing) < 0 {
            setField(&doc, key: key, value: value)
        }
    } else {
        // 字段不存在，直接设置
        setField(&doc, key: key, value: value)
    }
}

/// 取最大值更新
public func updateFieldMax(_ doc: inout BSONDocument, key: String, value: BSONValue) {
    if let existing = doc.getValue(forPath: key) {
        if compareBSONValues(value, existing) > 0 {
            setField(&doc, key: key, value: value)
        }
    } else {
        // 字段不存在，直接设置
        setField(&doc, key: key, value: value)
    }
}

/// 重命名字段
public func renameField(_ doc: inout BSONDocument, oldName: String, newName: String) {
    if let value = doc.getValue(forPath: oldName) {
        removeField(&doc, key: oldName)
        setField(&doc, key: newName, value: value)
    }
}

/// 设置当前日期
public func setCurrentDate(_ doc: inout BSONDocument, key: String, spec: BSONValue) {
    let now = Date()

    switch spec {
    case .bool(true):
        setField(&doc, key: key, value: .dateTime(now))
    case .document(let specDoc):
        if let typeVal = specDoc["$type"], case .string(let typeName) = typeVal {
            if typeName == "timestamp" {
                let ts = BSONTimestamp(t: UInt32(now.timeIntervalSince1970), i: 0)
                setField(&doc, key: key, value: .timestamp(ts))
            } else {
                setField(&doc, key: key, value: .dateTime(now))
            }
        } else {
            setField(&doc, key: key, value: .dateTime(now))
        }
    default:
        setField(&doc, key: key, value: .dateTime(now))
    }
}

// MARK: - 数组操作

/// 向数组追加元素
public func pushToArray(_ doc: inout BSONDocument, key: String, value: BSONValue) throws {
    var arr: BSONArray
    if let existing = doc.getValue(forPath: key) {
        guard case .array(let existingArr) = existing else {
            throw MonoError.badValue("field \(key) is not an array")
        }
        arr = existingArr
    } else {
        arr = []
    }

    // 检查是否有 $each 修饰符
    if case .document(let valDoc) = value {
        if let eachVal = valDoc["$each"], case .array(let eachArr) = eachVal {
            arr.append(contentsOf: eachArr)
            setField(&doc, key: key, value: .array(arr))
            return
        }
    }

    arr.append(value)
    setField(&doc, key: key, value: .array(arr))
}

/// 从数组头部或尾部移除元素
public func popFromArray(_ doc: inout BSONDocument, key: String, value: BSONValue) {
    guard let existing = doc.getValue(forPath: key),
          case .array(var arr) = existing,
          !arr.isEmpty else {
        return
    }

    let pos = toDouble(value)
    if pos >= 0 {
        // 移除尾部
        arr.removeLast()
    } else {
        // 移除头部
        arr.removeFirst()
    }

    setField(&doc, key: key, value: .array(arr))
}

/// 从数组移除匹配的元素
public func pullFromArray(_ doc: inout BSONDocument, key: String, value: BSONValue) {
    guard let existing = doc.getValue(forPath: key),
          case .array(let arr) = existing else {
        return
    }

    // 如果 value 是一个包含查询条件的文档，使用 FilterMatcher
    if case .document(let condition) = value {
        let matcher = FilterMatcher(condition)
        let newArr = arr.filter { item in
            if case .document(let itemDoc) = item {
                return !matcher.match(itemDoc)
            }
            return true
        }
        setField(&doc, key: key, value: .array(BSONArray(newArr)))
        return
    }

    // 简单值匹配
    let newArr = arr.filter { item in
        compareBSONValues(item, value) != 0
    }
    setField(&doc, key: key, value: .array(BSONArray(newArr)))
}

/// 从数组移除所有指定的元素
public func pullAllFromArray(_ doc: inout BSONDocument, key: String, value: BSONValue) {
    guard let existing = doc.getValue(forPath: key),
          case .array(let arr) = existing else {
        return
    }

    guard case .array(let valuesToRemove) = value else {
        return
    }

    let newArr = arr.filter { item in
        for v in valuesToRemove {
            if compareBSONValues(item, v) == 0 {
                return false
            }
        }
        return true
    }

    setField(&doc, key: key, value: .array(BSONArray(newArr)))
}

/// 向数组添加唯一元素
public func addToSet(_ doc: inout BSONDocument, key: String, value: BSONValue) {
    var arr: BSONArray
    if let existing = doc.getValue(forPath: key) {
        guard case .array(let existingArr) = existing else {
            return
        }
        arr = existingArr
    } else {
        arr = []
    }

    // 检查是否有 $each 修饰符
    if case .document(let valDoc) = value {
        if let eachVal = valDoc["$each"], case .array(let eachArr) = eachVal {
            for v in eachArr {
                if !arrayContains(arr, v) {
                    arr.append(v)
                }
            }
            setField(&doc, key: key, value: .array(arr))
            return
        }
    }

    // 单个值
    if !arrayContains(arr, value) {
        arr.append(value)
        setField(&doc, key: key, value: .array(arr))
    }
}

/// 检查数组是否包含指定值
private func arrayContains(_ arr: BSONArray, _ value: BSONValue) -> Bool {
    for item in arr {
        if compareBSONValues(item, value) == 0 {
            return true
        }
    }
    return false
}

// MARK: - 辅助函数

/// 转换为 Double
private func toDouble(_ value: BSONValue) -> Double {
    switch value {
    case .int32(let v):
        return Double(v)
    case .int64(let v):
        return Double(v)
    case .double(let v):
        return v
    case .decimal128(let v):
        return v.doubleValue
    default:
        return 0
    }
}

// MARK: - 文档辅助

/// 深拷贝文档
public func copyDocument(_ doc: BSONDocument) -> BSONDocument {
    var copy = BSONDocument()
    for (key, value) in doc {
        copy[key] = copyValue(value)
    }
    return copy
}

/// 深拷贝 BSON 值
private func copyValue(_ value: BSONValue) -> BSONValue {
    switch value {
    case .document(let doc):
        return .document(copyDocument(doc))
    case .array(let arr):
        return .array(BSONArray(arr.map { copyValue($0) }))
    default:
        return value
    }
}

/// 获取文档字段值
public func getDocField(_ doc: BSONDocument, _ key: String) -> BSONValue? {
    return doc.getValue(forPath: key)
}
