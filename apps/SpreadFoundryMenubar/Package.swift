// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "SpreadFoundryMenubar",
    platforms: [.macOS(.v13)],
    products: [
        .executable(name: "SpreadFoundryMenubar", targets: ["SpreadFoundryMenubar"])
    ],
    targets: [
        .executableTarget(name: "SpreadFoundryMenubar")
    ]
)
