import AppKit
import SwiftUI

@main
struct SpreadFoundryMenubarApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) var appDelegate

    var body: some Scene {
        Settings {
            EmptyView()
        }
    }
}

final class AppDelegate: NSObject, NSApplicationDelegate {
    private var statusBarController: StatusBarController?

    @MainActor
    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApp.setActivationPolicy(.accessory)
        let rootDirectory = RootDirectory.resolve()
        let scriptRunner = ScriptRunner(rootDirectory: rootDirectory)
        let viewModel = MenubarViewModel(
            snapshotService: SnapshotService(rootDirectory: rootDirectory),
            scriptRunner: scriptRunner
        )
        statusBarController = StatusBarController(viewModel: viewModel)
        viewModel.start()
    }
}
