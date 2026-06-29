import SwiftUI

struct MenubarMenuContent: View {
    @ObservedObject var viewModel: MenubarViewModel

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text(viewModel.snapshot.trayTitle)
                    .font(.headline)
                Spacer()
                Button("Refresh") {
                    viewModel.refresh()
                }
            }

            Text(viewModel.snapshot.trayTooltip)
                .font(.caption)
                .foregroundStyle(.secondary)
                .lineLimit(3)

            Divider()

            ForEach(viewModel.snapshot.rows) { row in
                rowView(row)
            }

            Divider()

            ForEach(viewModel.snapshot.actionRows) { row in
                rowView(row)
            }

            if let errorMessage = viewModel.errorMessage {
                Text(errorMessage)
                    .font(.caption)
                    .foregroundStyle(.red)
                    .lineLimit(3)
            }

            Divider()

            HStack {
                Button("Start") {
                    viewModel.startWorker()
                }
                Button("Stop") {
                    viewModel.stopWorker()
                }
                Button("Log") {
                    viewModel.openLog()
                }
                Button("Docs") {
                    viewModel.openDocs()
                }
                Spacer()
                Button("Quit") {
                    NSApplication.shared.terminate(nil)
                }
            }
        }
        .padding(12)
        .frame(width: 320)
    }

    private func rowView(_ row: SnapshotRow) -> some View {
        HStack(alignment: .firstTextBaseline) {
            Text(row.label)
                .foregroundStyle(.secondary)
            Spacer()
            Text(row.value)
                .foregroundStyle(color(for: row.tone))
                .multilineTextAlignment(.trailing)
        }
        .font(.system(size: 12, weight: .regular, design: .monospaced))
    }

    private func color(for tone: String) -> Color {
        switch tone {
        case "ok":
            return .green
        case "warn":
            return .orange
        case "bad":
            return .red
        default:
            return .primary
        }
    }
}
