import Foundation

enum RootDirectory {
    static func resolve() -> URL {
        if let configured = ProcessInfo.processInfo.environment["SPREAD_ROOT"], !configured.isEmpty {
            return URL(fileURLWithPath: configured)
        }

        var candidate = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
        for _ in 0..<8 {
            if FileManager.default.fileExists(atPath: candidate.appendingPathComponent("Cargo.toml").path),
               FileManager.default.fileExists(atPath: candidate.appendingPathComponent("scripts/canary-service.sh").path) {
                return candidate
            }
            candidate.deleteLastPathComponent()
        }

        return URL(fileURLWithPath: "/Users/1duo/Projects/SpreadFoundry")
    }
}
