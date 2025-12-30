// Created by Yanjunhui
//
// Test timeout utilities:
// - soft timeout: throws (test can fail/skip)
// - hard timeout: cancels the task and terminates the test process (prevents "swift test" hanging forever)

import Foundation
import XCTest

public enum TestTimeoutMode: Sendable {
    case soft
    case hard
}

public struct TestTimeoutError: Error, Sendable, CustomStringConvertible {
    public let seconds: TimeInterval
    public let message: String
    public var description: String { "TestTimeout(\(seconds)s): \(message)" }
}

/// Run an async operation with a timeout.
/// - Note: `hard` mode will terminate the process if the timeout triggers (to avoid hanging test runs).
@discardableResult
public func withTestTimeout<T>(
    seconds: TimeInterval,
    mode: TestTimeoutMode = .hard,
    file: StaticString = #filePath,
    line: UInt = #line,
    _ operation: @escaping @Sendable () async throws -> T
) async throws -> T {
    let opTask = Task<T, Error> {
        try await operation()
    }

    let watchdog = Task<Void, Never> {
        let nanos = UInt64(max(0, seconds) * 1_000_000_000)
        try? await Task.sleep(nanoseconds: nanos)
        if Task.isCancelled { return }

        opTask.cancel()

        let msg = "operation exceeded timeout (\(seconds)s)"
        switch mode {
        case .soft:
            // Can't throw from here; operation task will be awaited by caller and should observe cancellation.
            return
        case .hard:
            XCTFail("Test timeout: \(msg)", file: file, line: line)
            fatalError("Test timeout: \(msg)")
        }
    }

    defer { watchdog.cancel() }

    do {
        return try await opTask.value
    } catch is CancellationError {
        throw TestTimeoutError(seconds: seconds, message: "cancelled due to timeout")
    } catch {
        throw error
    }
}


