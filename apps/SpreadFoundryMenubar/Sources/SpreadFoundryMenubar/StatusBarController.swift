import AppKit
import Combine
import SwiftUI

@MainActor
final class StatusBarController {
    private let statusItem: NSStatusItem
    private let viewModel: MenubarViewModel
    private var cancellable: AnyCancellable?

    init(viewModel: MenubarViewModel) {
        self.viewModel = viewModel
        statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.squareLength)
        configureButton(for: viewModel.snapshot)
        rebuildMenu()

        cancellable = viewModel.$snapshot.sink { [weak self] snapshot in
            self?.configureButton(for: snapshot)
            self?.rebuildMenu()
        }
    }

    private func configureButton(for snapshot: CanarySnapshot) {
        statusItem.button?.image = MenubarIcon.statusImage(status: snapshot.status)
        statusItem.button?.imagePosition = .imageOnly
        statusItem.button?.title = ""
        statusItem.button?.toolTip = snapshot.trayTooltip
    }

    private func rebuildMenu() {
        let menu = NSMenu()
        let content = MenubarMenuContent(viewModel: viewModel)
        let hostingView = NSHostingView(rootView: content)
        hostingView.frame = NSRect(x: 0, y: 0, width: 320, height: 410)
        let item = NSMenuItem()
        item.view = hostingView
        menu.addItem(item)
        statusItem.menu = menu
    }
}
