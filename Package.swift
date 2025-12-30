// swift-tools-version: 5.9
// Created by Yanjunhui

import PackageDescription

let package = Package(
    name: "MonoLiteSwift",
    platforms: [
        .macOS(.v13),
        .iOS(.v16)
    ],
    products: [
        .library(
            name: "MonoLiteSwift",
            targets: ["MonoLiteSwift"]
        ),
    ],
    targets: [
        .target(
            name: "MonoLiteSwift",
            dependencies: [],
            path: "Sources/MonoLiteSwift"
        ),
        .testTarget(
            name: "MonoLiteSwiftTests",
            dependencies: ["MonoLiteSwift"],
            path: "Tests/MonoLiteSwiftTests"
        ),
    ]
)
