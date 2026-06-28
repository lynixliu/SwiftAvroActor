// swift-tools-version:6.0
import PackageDescription

let package = Package(
    name: "SwiftAvroActor",
    platforms: [
        .macOS(.v15),
    ],
    products: [
        .library(name: "SwiftAvroActor", targets: ["SwiftAvroActor"]),
    ],
    dependencies: [
        .package(url: "https://github.com/lynixliu/SwiftAvroCore", from: "2.2.0"),
        .package(url: "https://github.com/apple/swift-cluster-membership.git", branch: "main"),
        // Test-only: build CA-trusting client TLS configs for the end-to-end gossip TLS test.
        .package(url: "https://github.com/apple/swift-nio-ssl", from: "2.37.0"),
    ],
    targets: [
        .target(
            name: "SwiftAvroActor",
            dependencies: [
                .product(name: "SwiftAvroCore", package: "SwiftAvroCore"),
                .product(name: "SwiftAvroRpc",  package: "SwiftAvroCore"),
                .product(name: "SWIMNIOExample", package: "swift-cluster-membership"),
            ],
            swiftSettings: [
                .swiftLanguageMode(.v6),
            ]
        ),
        .testTarget(
            name: "SwiftAvroActorTests",
            dependencies: [
                "SwiftAvroActor",
                .product(name: "SwiftAvroCore", package: "SwiftAvroCore"),
                .product(name: "SwiftAvroRpc",  package: "SwiftAvroCore"),
                .product(name: "NIOSSL",        package: "swift-nio-ssl"),
            ],
            swiftSettings: [
                .swiftLanguageMode(.v6),
            ]
        ),
    ]
)
