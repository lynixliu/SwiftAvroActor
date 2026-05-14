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
        .package(path: "../SwiftAvroCore"),
        .package(url: "https://github.com/apple/swift-cluster-membership.git", branch: "main"),
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
            ],
            swiftSettings: [
                .swiftLanguageMode(.v6),
            ]
        ),
    ]
)
