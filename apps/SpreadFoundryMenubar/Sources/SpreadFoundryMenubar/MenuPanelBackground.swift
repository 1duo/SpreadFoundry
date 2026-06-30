import AppKit
import SwiftUI

extension View {
    func menuPanelBackground() -> some View {
        background {
            MenuPanelGlassEffectRepresentable()
        }
    }
}

private struct MenuPanelGlassEffectRepresentable: NSViewRepresentable {
    func makeNSView(context: Context) -> MenuPanelGlassHostView {
        MenuPanelGlassHostView()
    }

    func updateNSView(_ nsView: MenuPanelGlassHostView, context: Context) {
        nsView.syncAppearance()
    }
}

private final class MenuPanelGlassHostView: NSView {
    private let effectView = NSVisualEffectView()
    private let sheenLayer = CAGradientLayer()

    override init(frame frameRect: NSRect) {
        super.init(frame: frameRect)
        wantsLayer = true

        effectView.material = .hudWindow
        effectView.blendingMode = .behindWindow
        effectView.state = .active
        effectView.autoresizingMask = [.width, .height]
        addSubview(effectView)

        sheenLayer.colors = [
            NSColor.white.withAlphaComponent(0.28).cgColor,
            NSColor.white.withAlphaComponent(0.08).cgColor,
            NSColor.clear.cgColor,
        ]
        sheenLayer.locations = [0.0, 0.25, 0.65]
        sheenLayer.startPoint = CGPoint(x: 0.5, y: 1.0)
        sheenLayer.endPoint = CGPoint(x: 0.5, y: 0.0)
        layer?.addSublayer(sheenLayer)

        syncAppearance()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        nil
    }

    override func layout() {
        super.layout()
        effectView.frame = bounds
        sheenLayer.frame = bounds
    }

    func syncAppearance() {
        let isDark = effectiveAppearance.bestMatch(from: [.darkAqua, .aqua]) == .darkAqua
        effectView.material = isDark ? .hudWindow : .popover
        effectView.state = .active
        sheenLayer.opacity = isDark ? 0.85 : 1.0
    }

    override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        syncAppearance()
    }
}
