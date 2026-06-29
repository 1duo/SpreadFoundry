import AppKit
import Foundation

struct ScriptRunner {
    let rootDirectory: URL

    @discardableResult
    func runServiceCommand(_ command: String, arguments: [String] = []) throws -> Data {
        let script = rootDirectory.appendingPathComponent("scripts/canary-service.sh")
        let process = Process()
        process.executableURL = script
        process.arguments = [command] + arguments
        process.currentDirectoryURL = rootDirectory

        let output = Pipe()
        let error = Pipe()
        process.standardOutput = output
        process.standardError = error
        try process.run()
        process.waitUntilExit()

        let outputData = output.fileHandleForReading.readDataToEndOfFile()
        if process.terminationStatus != 0 {
            let errorData = error.fileHandleForReading.readDataToEndOfFile()
            let message = String(data: errorData, encoding: .utf8) ?? "command failed"
            throw NSError(domain: "SpreadFoundryMenubar", code: Int(process.terminationStatus), userInfo: [
                NSLocalizedDescriptionKey: message
            ])
        }
        return outputData
    }

    func openLog() {
        let logURL = rootDirectory.appendingPathComponent("var/canary_worker.log")
        NSWorkspace.shared.open(logURL)
    }

    func openDocs() {
        let docsURL = rootDirectory.appendingPathComponent("docs/production_architecture.md")
        NSWorkspace.shared.open(docsURL)
    }
}
