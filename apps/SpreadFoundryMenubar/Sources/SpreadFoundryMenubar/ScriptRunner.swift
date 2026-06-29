import Foundation

struct ScriptRunner {
    let rootDirectory: URL

    @discardableResult
    func runServiceCommand(_ command: String, arguments: [String] = []) throws -> Data {
        let script = rootDirectory.appendingPathComponent("scripts/execution-service.sh")
        return try runScript(script, arguments: [command] + arguments)
    }

    @discardableResult
    func shutdownFromMenubar() throws -> Data {
        let script = rootDirectory.appendingPathComponent("scripts/spreadfoundry-service.sh")
        return try runScript(script, arguments: ["shutdown-from-menubar"])
    }

    @discardableResult
    private func runScript(_ script: URL, arguments: [String]) throws -> Data {
        let process = Process()
        process.executableURL = script
        process.arguments = arguments
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
}
