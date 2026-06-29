import Foundation

struct CanarySnapshot: Decodable {
    let updatedAt: String
    let healthPath: String
    let pidFile: String
    let workerRunning: Bool
    let healthReadable: Bool
    let healthAgeSeconds: Int?
    let healthStale: Bool
    let status: String
    let trayTitle: String
    let trayTooltip: String
    let rows: [SnapshotRow]
    let brokerRows: [SnapshotRow]
    let actionRows: [SnapshotRow]

    static let unavailable = CanarySnapshot(
        updatedAt: "",
        healthPath: "var/canary_worker_health.json",
        pidFile: "var/canary_worker.pid",
        workerRunning: false,
        healthReadable: false,
        healthAgeSeconds: nil,
        healthStale: true,
        status: "unhealthy",
        trayTitle: "SF down",
        trayTooltip: "Snapshot unavailable",
        rows: [],
        brokerRows: [],
        actionRows: []
    )

    var canaryMode: CanaryModeChoice? {
        rows
            .first { $0.label == "Mode" }
            .flatMap { CanaryModeChoice(rawValue: $0.value) }
    }

    var statusTitle: String {
        switch status {
        case "monitor":
            return "Monitoring"
        case "review":
            return "Review"
        case "live":
            return "Live"
        case "blocked":
            return "Blocked"
        case "unhealthy":
            return "Down"
        default:
            return status.replacingOccurrences(of: "_", with: " ").capitalized
        }
    }
}

struct SnapshotRow: Decodable, Identifiable {
    let id = UUID()
    let label: String
    let value: String
    let tone: String

    private enum CodingKeys: String, CodingKey {
        case label
        case value
        case tone
    }
}
