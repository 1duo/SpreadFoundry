import Foundation

struct SnapshotService {
    let rootDirectory: URL

    func fetch() throws -> CanarySnapshot {
        let output = try ScriptRunner(rootDirectory: rootDirectory).runServiceCommand("status")
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
        return try decoder.decode(CanarySnapshot.self, from: output)
    }
}
