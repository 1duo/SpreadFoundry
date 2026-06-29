import Foundation
import SwiftUI

enum CanaryModeChoice: String, CaseIterable, Identifiable {
    case monitor
    case review
    case live

    var id: String { rawValue }

    var title: String {
        switch self {
        case .monitor:
            return "Monitor"
        case .review:
            return "Review"
        case .live:
            return "Live"
        }
    }
}

@MainActor
final class MenubarViewModel: ObservableObject {
    @Published private(set) var snapshot: CanarySnapshot = .unavailable
    @Published private(set) var errorMessage: String?

    private let snapshotService: SnapshotService
    private let scriptRunner: ScriptRunner
    private var timer: Timer?

    init(snapshotService: SnapshotService, scriptRunner: ScriptRunner) {
        self.snapshotService = snapshotService
        self.scriptRunner = scriptRunner
    }

    var currentMode: CanaryModeChoice? {
        snapshot.canaryMode
    }

    var modeIsKnown: Bool {
        currentMode != nil
    }

    func start() {
        refresh()
        timer = Timer.scheduledTimer(withTimeInterval: 30, repeats: true) { [weak self] _ in
            Task { @MainActor in
                self?.refresh()
            }
        }
    }

    func refresh() {
        do {
            snapshot = try snapshotService.fetch()
            errorMessage = nil
        } catch {
            snapshot = .unavailable
            errorMessage = error.localizedDescription
        }
    }

    func startWorker() {
        runThenRefresh("start")
    }

    func stopWorker() {
        runThenRefresh("stop")
    }

    func restartWorker() {
        runThenRefresh("restart")
    }

    func setMode(_ mode: CanaryModeChoice) {
        guard currentMode != mode else {
            return
        }
        runThenRefresh("set-mode", arguments: [mode.rawValue])
    }

    func openLog() {
        scriptRunner.openLog()
    }

    func openDocs() {
        scriptRunner.openDocs()
    }

    private func runThenRefresh(_ command: String, arguments: [String] = []) {
        do {
            _ = try scriptRunner.runServiceCommand(command, arguments: arguments)
            refresh()
        } catch {
            errorMessage = error.localizedDescription
        }
    }
}
