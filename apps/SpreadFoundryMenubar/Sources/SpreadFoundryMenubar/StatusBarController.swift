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
        statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        statusItem.button?.title = viewModel.snapshot.trayTitle
        rebuildMenu()

        cancellable = viewModel.$snapshot.sink { [weak self] snapshot in
            self?.statusItem.button?.title = snapshot.trayTitle
            self?.rebuildMenu()
        }
    }

    private func rebuildMenu() {
        let menu = NSMenu()
        let content = MenubarMenuContent(viewModel: viewModel)
        let hostingView = NSHostingView(rootView: content)
        hostingView.frame = NSRect(x: 0, y: 0, width: 320, height: 360)
        let item = NSMenuItem()
        item.view = hostingView
        menu.addItem(item)
        statusItem.menu = menu
    }
}
