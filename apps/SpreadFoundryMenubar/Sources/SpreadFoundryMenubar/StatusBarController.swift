import AppKit
import Combine
import SwiftUI

@MainActor
final class StatusBarController: NSObject {
    private let statusItem: NSStatusItem
    private let viewModel: MenubarViewModel
    private let popover: NSPopover
    private var hostingController: NSHostingController<MenubarMenuContent>?
    private var cancellable: AnyCancellable?

    init(viewModel: MenubarViewModel) {
        self.viewModel = viewModel
        statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.squareLength)
        popover = NSPopover()
        super.init()

        popover.behavior = .transient
        popover.animates = false
        popover.appearance = NSApp.effectiveAppearance

        let hosting = NSHostingController(rootView: MenubarMenuContent(viewModel: viewModel))
        configureHostingView(hosting)
        hostingController = hosting
        popover.contentViewController = hosting
        syncPopoverSize()

        if let button = statusItem.button {
            button.target = self
            button.action = #selector(togglePopover(_:))
            button.sendAction(on: [.leftMouseUp])
        }

        cancellable = Publishers.CombineLatest(viewModel.$snapshot, viewModel.$errorMessage)
            .sink { [weak self] snapshot, _ in
                self?.refreshStatusItem(snapshot)
                self?.refreshPopoverContent()
            }
        refreshStatusItem(viewModel.snapshot)
    }

    @objc private func togglePopover(_ sender: AnyObject?) {
        guard let button = statusItem.button else {
            return
        }
        if popover.isShown {
            popover.performClose(sender)
            return
        }
        refreshPopoverContent()
        popover.show(relativeTo: button.bounds, of: button, preferredEdge: .minY)
    }

    private func refreshStatusItem(_ snapshot: CanarySnapshot) {
        guard let button = statusItem.button else {
            return
        }
        button.image = MenubarIcon.statusImage(status: snapshot.status)
        button.imagePosition = .imageOnly
        button.title = ""
        button.toolTip = snapshot.trayTooltip
    }

    private func refreshPopoverContent() {
        if let hostingController {
            hostingController.rootView = MenubarMenuContent(viewModel: viewModel)
        } else {
            let hosting = NSHostingController(rootView: MenubarMenuContent(viewModel: viewModel))
            configureHostingView(hosting)
            self.hostingController = hosting
            popover.contentViewController = hosting
        }
        syncPopoverSize()
    }

    private func syncPopoverSize() {
        guard let hosting = hostingController else {
            return
        }
        let width = MenuLayoutMetrics.menuWidth
        let height = Self.popoverHeight(for: hosting, width: width)
        popover.contentSize = NSSize(width: width, height: height)
    }

    private static func popoverHeight(
        for hosting: NSHostingController<MenubarMenuContent>,
        width: CGFloat
    ) -> CGFloat {
        let maxHeight = MenuLayoutMetrics.panelMaxHeight
        let measured = hosting.sizeThatFits(in: NSSize(width: width, height: maxHeight)).height
        guard measured.isFinite, measured > 0 else {
            return MenuLayoutMetrics.panelHeight
        }
        return min(max(ceil(measured), 1), maxHeight)
    }

    private func configureHostingView(_ hosting: NSHostingController<MenubarMenuContent>) {
        hosting.sizingOptions = [.intrinsicContentSize]
        hosting.view.wantsLayer = true
        hosting.view.layer?.backgroundColor = NSColor.clear.cgColor
    }
}
