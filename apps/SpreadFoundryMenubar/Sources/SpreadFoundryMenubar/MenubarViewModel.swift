import Foundation
import SwiftUI

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

    func openLog() {
        scriptRunner.openLog()
    }

    func openDocs() {
        scriptRunner.openDocs()
    }

    private func runThenRefresh(_ command: String) {
        do {
            _ = try scriptRunner.runServiceCommand(command)
            refresh()
        } catch {
            errorMessage = error.localizedDescription
        }
    }
}
