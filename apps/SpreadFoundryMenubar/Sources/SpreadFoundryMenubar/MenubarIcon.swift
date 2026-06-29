import AppKit
import SwiftUI

enum MenubarIcon {
    static func statusImage(status: String) -> NSImage {
        let image = draw(size: CGSize(width: 26, height: 26), status: status, template: true)
        image.isTemplate = true
        return image
    }

    static func brandImage(status: String) -> NSImage {
        draw(size: CGSize(width: 44, height: 44), status: status, template: false)
    }

    private static func draw(size: CGSize, status: String, template: Bool) -> NSImage {
        let image = NSImage(size: size)
        image.lockFocus()
        defer { image.unlockFocus() }

        let rect = CGRect(origin: .zero, size: size)
        let scale = min(size.width, size.height) / 44.0
        let ink = template ? NSColor.black : statusColor(status)
        let background = template ? NSColor.clear : NSColor(red: 0.04, green: 0.07, blue: 0.06, alpha: 1.0)

        let badge = NSBezierPath(roundedRect: rect.insetBy(dx: 2 * scale, dy: 2 * scale), xRadius: 11 * scale, yRadius: 11 * scale)
        background.setFill()
        badge.fill()

        if !template {
            ink.withAlphaComponent(0.18).setFill()
            NSBezierPath(ovalIn: rect.insetBy(dx: 6 * scale, dy: 6 * scale)).fill()
        }

        ink.setStroke()
        let arc = NSBezierPath()
        arc.lineWidth = max(4.0 * scale, 2.0)
        arc.lineCapStyle = .round
        arc.move(to: CGPoint(x: 11 * scale, y: 13 * scale))
        arc.curve(
            to: CGPoint(x: 33 * scale, y: 32 * scale),
            controlPoint1: CGPoint(x: 14 * scale, y: 27 * scale),
            controlPoint2: CGPoint(x: 23 * scale, y: 33 * scale)
        )
        arc.stroke()

        let wing = NSBezierPath()
        wing.lineWidth = max(5.4 * scale, 2.6)
        wing.lineCapStyle = .round
        wing.lineJoinStyle = .round
        wing.move(to: CGPoint(x: 13 * scale, y: 28 * scale))
        wing.line(to: CGPoint(x: 28 * scale, y: 28 * scale))
        wing.line(to: CGPoint(x: 34 * scale, y: 34 * scale))
        wing.stroke()

        ink.setFill()
        let lowerBar = CGRect(x: 16 * scale, y: 16 * scale, width: 13 * scale, height: 3.8 * scale)
        let upperBar = CGRect(x: 19 * scale, y: 21.5 * scale, width: 13 * scale, height: 3.8 * scale)
        NSBezierPath(roundedRect: lowerBar, xRadius: 1.9 * scale, yRadius: 1.9 * scale).fill()
        NSBezierPath(roundedRect: upperBar, xRadius: 1.9 * scale, yRadius: 1.9 * scale).fill()

        return image
    }

    private static func statusColor(_ status: String) -> NSColor {
        switch status {
        case "ready", "live":
            return NSColor(red: 0.03, green: 0.78, blue: 0.36, alpha: 1.0)
        case "shadow":
            return NSColor(red: 0.15, green: 0.72, blue: 0.38, alpha: 1.0)
        default:
            return NSColor(red: 0.95, green: 0.23, blue: 0.27, alpha: 1.0)
        }
    }
}
