import AppKit
import SwiftUI

enum MenubarIcon {
    static func statusImage(status: String) -> NSImage {
        let image = draw(size: CGSize(width: 18, height: 18), status: status, template: true)
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
        arc.lineWidth = max(1.8 * scale, 1.0)
        arc.lineCapStyle = .round
        arc.move(to: CGPoint(x: 12 * scale, y: 14 * scale))
        arc.curve(
            to: CGPoint(x: 31 * scale, y: 31 * scale),
            controlPoint1: CGPoint(x: 14 * scale, y: 25 * scale),
            controlPoint2: CGPoint(x: 22 * scale, y: 31 * scale)
        )
        arc.stroke()

        let wing = NSBezierPath()
        wing.lineWidth = max(2.6 * scale, 1.2)
        wing.lineCapStyle = .round
        wing.lineJoinStyle = .round
        wing.move(to: CGPoint(x: 14 * scale, y: 28 * scale))
        wing.line(to: CGPoint(x: 28 * scale, y: 28 * scale))
        wing.line(to: CGPoint(x: 33 * scale, y: 34 * scale))
        wing.stroke()

        let spread = NSBezierPath()
        spread.lineWidth = max(1.9 * scale, 1.0)
        spread.lineCapStyle = .round
        spread.move(to: CGPoint(x: 17 * scale, y: 18 * scale))
        spread.line(to: CGPoint(x: 27 * scale, y: 18 * scale))
        spread.move(to: CGPoint(x: 20 * scale, y: 23 * scale))
        spread.line(to: CGPoint(x: 30 * scale, y: 23 * scale))
        spread.stroke()

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
