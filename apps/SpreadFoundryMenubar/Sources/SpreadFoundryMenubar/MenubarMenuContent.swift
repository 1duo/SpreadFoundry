import SwiftUI

struct MenubarMenuContent: View {
    @ObservedObject var viewModel: MenubarViewModel

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            header
            panelDivider()

            SectionHeaderRow(title: "Canary", systemImage: "waveform.path.ecg")
            ForEach(statusRows) { row in
                StatusIndicatorRow(row: row)
            }
            modePicker

            if !viewModel.snapshot.brokerRows.isEmpty {
                panelDivider()

                SectionHeaderRow(title: "Broker", systemImage: "building.columns")
                ForEach(viewModel.snapshot.brokerRows) { row in
                    BrokerValueRow(row: row)
                }
            }

            if !viewModel.snapshot.actionRows.isEmpty {
                panelDivider()

                SectionHeaderRow(title: "Action", systemImage: "scope")
                ForEach(viewModel.snapshot.actionRows) { row in
                    ValueRow(label: row.label, value: row.value, tone: RowTone(row.tone))
                }
            }

            if let errorMessage = viewModel.errorMessage {
                panelDivider()
                ValueRow(label: "Error", value: errorMessage, tone: .negative)
            }

            panelDivider()

            panelButton(title: "Refresh", systemImage: "arrow.clockwise") {
                viewModel.refresh()
            }
            panelButton(title: "Start Worker", systemImage: "play.fill") {
                viewModel.startWorker()
            }
            panelButton(title: "Restart Worker", systemImage: "arrow.triangle.2.circlepath") {
                viewModel.restartWorker()
            }
            panelButton(title: "Stop Worker", systemImage: "stop.fill") {
                viewModel.stopWorker()
            }

            panelDivider()

            panelButton(title: "Open Log", systemImage: "doc.text.magnifyingglass") {
                viewModel.openLog()
            }
            panelButton(title: "Open Runbook", systemImage: "book") {
                viewModel.openDocs()
            }
            panelButton(title: "Quit Menubar", systemImage: "xmark.circle") {
                NSApplication.shared.terminate(nil)
            }
        }
        .padding(.vertical, MenuLayoutMetrics.panelVerticalPadding)
        .frame(width: MenuLayoutMetrics.menuWidth, alignment: .leading)
        .menuPanelBackground()
    }

    private var header: some View {
        HStack(alignment: .center, spacing: 8) {
            Image(nsImage: MenubarIcon.brandImage(status: viewModel.snapshot.status))
                .resizable()
                .frame(width: 28, height: 28)

            VStack(alignment: .leading, spacing: 1) {
                Text("SpreadFoundry")
                    .font(.body.weight(.medium))
                    .lineLimit(1)
                Text(viewModel.snapshot.trayTitle)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }

            Spacer(minLength: MenuLayoutMetrics.columnSpacing)

            Text(viewModel.snapshot.statusTitle)
                .font(.caption.weight(.medium))
                .foregroundStyle(RowTone(statusTone).color)
                .monospacedDigit()
                .lineLimit(1)
        }
        .menuPanelRow()
        .help(viewModel.snapshot.trayTooltip)
    }

    private var statusTone: String {
        switch viewModel.snapshot.status {
        case "monitor", "live":
            return "ok"
        case "review":
            return "warn"
        default:
            return "bad"
        }
    }

    private var statusRows: [SnapshotRow] {
        viewModel.snapshot.rows.filter { $0.label != "Mode" }
    }

    private var modePicker: some View {
        HStack(alignment: .center, spacing: 7) {
            MenuRowIcon(systemName: "switch.2")
            Text("Mode")
                .lineLimit(1)
            if !viewModel.modeIsKnown {
                Text("Unknown")
                    .font(.system(size: 10, weight: .semibold))
                    .foregroundStyle(.secondary)
            }
            Spacer(minLength: MenuLayoutMetrics.columnSpacing)
            Picker("Mode", selection: Binding(
                get: { viewModel.currentMode },
                set: { mode in
                    if let mode {
                        viewModel.setMode(mode)
                    }
                }
            )) {
                ForEach(CanaryModeChoice.allCases) { mode in
                    Text(mode.title).tag(Optional(mode))
                }
            }
            .labelsHidden()
            .pickerStyle(.segmented)
            .frame(width: 184)
        }
        .menuPanelRow()
    }

    private func panelDivider() -> some View {
        Divider()
            .padding(.horizontal, MenuLayoutMetrics.horizontalPadding)
            .padding(.vertical, MenuLayoutMetrics.dividerVerticalPadding)
    }

    private func panelButton(title: String, systemImage: String, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            MenuLabeledRow(title: title, systemImage: systemImage)
                .menuPanelRow()
        }
        .buttonStyle(MenuPanelButtonStyle())
    }
}

private struct BrokerValueRow: View {
    let row: SnapshotRow

    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: MenuLayoutMetrics.columnSpacing) {
            MenuRowIcon(systemName: systemImage)
            Text(row.label)
                .lineLimit(1)
            Spacer(minLength: MenuLayoutMetrics.columnSpacing)
            Text(row.value)
                .foregroundStyle(RowTone(row.tone).valueColor)
                .monospacedDigit()
                .lineLimit(1)
                .truncationMode(.middle)
                .multilineTextAlignment(.trailing)
        }
        .menuPanelRow()
        .allowsHitTesting(false)
    }

    private var systemImage: String {
        switch row.label {
        case "Account":
            return "person.text.rectangle"
        case "Equity":
            return "chart.line.uptrend.xyaxis"
        case "Buy Power":
            return "bolt.circle"
        case "Cash":
            return "dollarsign.circle"
        case "Day P&L":
            return "calendar"
        case "Open P&L":
            return "waveform.path.ecg"
        case "Requirement":
            return "lock"
        default:
            return "info.circle"
        }
    }
}

private struct SectionHeaderRow: View {
    let title: String
    let systemImage: String

    var body: some View {
        MenuLabeledRow(title: title, systemImage: systemImage, emphasizeTitle: true)
            .menuPanelRow()
    }
}

private struct ValueRow: View {
    let label: String?
    let value: String
    let tone: RowTone

    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: MenuLayoutMetrics.columnSpacing) {
            if let label {
                Text(label)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                Spacer(minLength: MenuLayoutMetrics.columnSpacing)
                Text(value)
                    .foregroundStyle(tone.valueColor)
                    .monospacedDigit()
                    .lineLimit(1)
                    .truncationMode(.middle)
                    .multilineTextAlignment(.trailing)
            } else {
                Text(value)
                    .foregroundStyle(tone.valueColor)
                    .monospacedDigit()
                    .lineLimit(1)
                    .truncationMode(.tail)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
        .menuPanelRow()
        .allowsHitTesting(false)
    }
}

private struct StatusIndicatorRow: View {
    let row: SnapshotRow

    var body: some View {
        HStack(alignment: .center, spacing: 7) {
            MenuRowIcon(systemName: systemImage)
            Text(row.label)
                .lineLimit(1)
            Spacer(minLength: MenuLayoutMetrics.columnSpacing)
            Text(row.value)
                .foregroundStyle(.secondary)
                .monospacedDigit()
                .lineLimit(1)
                .truncationMode(.middle)
                .multilineTextAlignment(.trailing)
            statusBadge
        }
        .menuPanelRow()
        .allowsHitTesting(false)
    }

    private var systemImage: String {
        switch row.label {
        case "Worker":
            return "server.rack"
        case "Health":
            return "heart.text.square"
        case "Decision":
            return "checklist"
        case "Broker":
            return "building.columns"
        case "Live":
            return "bolt.horizontal"
        case "Last Check":
            return "clock"
        default:
            return "circle"
        }
    }

    @ViewBuilder
    private var statusBadge: some View {
        switch RowTone(row.tone) {
        case .positive:
            Image(systemName: "checkmark.circle.fill")
                .foregroundStyle(.green)
                .font(.caption)
        case .negative:
            Image(systemName: "xmark.circle.fill")
                .foregroundStyle(.red)
                .font(.caption)
        case .warning:
            Image(systemName: "exclamationmark.circle.fill")
                .foregroundStyle(.orange)
                .font(.caption)
        case .neutral:
            EmptyView()
        }
    }
}

private enum RowTone: Equatable {
    case positive
    case warning
    case negative
    case neutral

    init(_ raw: String?) {
        switch raw {
        case "ok":
            self = .positive
        case "warn":
            self = .warning
        case "bad":
            self = .negative
        default:
            self = .neutral
        }
    }

    var color: Color {
        switch self {
        case .positive:
            return .green
        case .warning:
            return .orange
        case .negative:
            return .red
        case .neutral:
            return .secondary
        }
    }

    var valueColor: Color {
        switch self {
        case .positive, .negative, .warning:
            return color
        case .neutral:
            return .secondary
        }
    }
}
