import SwiftUI

enum MenuIconMetrics {
    static let columnWidth: CGFloat = 18
    static let fontSize: CGFloat = 13
}

enum MenuLayoutMetrics {
    static let menuWidth: CGFloat = 400
    static let panelHeight: CGFloat = 430
    static let panelMaxHeight: CGFloat = 720
    static let horizontalPadding: CGFloat = 14
    static let rowVerticalPadding: CGFloat = 5
    static let dividerVerticalPadding: CGFloat = 3
    static let panelVerticalPadding: CGFloat = 6
    static let columnSpacing: CGFloat = 12
}

extension View {
    func menuPanelRow() -> some View {
        frame(maxWidth: .infinity, alignment: .leading)
            .padding(.horizontal, MenuLayoutMetrics.horizontalPadding)
            .padding(.vertical, MenuLayoutMetrics.rowVerticalPadding)
    }
}

struct MenuRowIcon: View {
    let systemName: String

    var body: some View {
        Image(systemName: systemName)
            .font(.system(size: MenuIconMetrics.fontSize, weight: .regular))
            .imageScale(.medium)
            .frame(width: MenuIconMetrics.columnWidth, height: MenuIconMetrics.columnWidth)
            .contentShape(Rectangle())
    }
}

struct MenuLabeledRow: View {
    let title: String
    let systemImage: String
    var emphasizeTitle: Bool = false

    var body: some View {
        Label {
            Text(title)
                .font(emphasizeTitle ? .body.weight(.medium) : .body)
                .lineLimit(1)
        } icon: {
            MenuRowIcon(systemName: systemImage)
        }
        .labelStyle(MenuIconLabelStyle())
    }
}

private struct MenuIconLabelStyle: LabelStyle {
    func makeBody(configuration: Configuration) -> some View {
        HStack(alignment: .center, spacing: 7) {
            configuration.icon
            configuration.title
        }
    }
}

struct MenuPanelButtonStyle: ButtonStyle {
    @Environment(\.isEnabled) private var isEnabled

    func makeBody(configuration: Configuration) -> some View {
        MenuPanelButtonStyleBody(configuration: configuration, isEnabled: isEnabled)
    }
}

private struct MenuPanelButtonStyleBody: View {
    let configuration: ButtonStyle.Configuration
    let isEnabled: Bool
    @State private var isHovered = false

    private var highlightColor: Color {
        if !isEnabled {
            return .clear
        }
        if configuration.isPressed {
            return Color(nsColor: .selectedContentBackgroundColor).opacity(0.9)
        }
        if isHovered {
            return Color(nsColor: .selectedContentBackgroundColor).opacity(0.55)
        }
        return .clear
    }

    var body: some View {
        configuration.label
            .background(highlightColor)
            .contentShape(Rectangle())
            .onHover { hovering in
                isHovered = isEnabled && hovering
            }
            .animation(.easeOut(duration: 0.12), value: isHovered)
            .animation(.easeOut(duration: 0.08), value: configuration.isPressed)
    }
}
