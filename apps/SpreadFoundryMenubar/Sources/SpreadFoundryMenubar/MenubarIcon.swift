import AppKit
import SwiftUI

enum MenubarIcon {
    static func statusImage(status: String) -> NSImage {
        let image = drawStatusGlyph(size: CGSize(width: 22, height: 22))
        image.isTemplate = true
        return image
    }

    static func brandImage(status: String) -> NSImage {
        drawBrandIcon(size: CGSize(width: 48, height: 48), status: status)
    }

    private struct Palette {
        let accent: NSColor
        let glow: NSColor
        let badge: NSColor
    }

    private static func drawStatusGlyph(size: CGSize) -> NSImage {
        let image = NSImage(size: size)
        image.lockFocus()
        defer { image.unlockFocus() }

        let scale = min(size.width, size.height) / 28.0
        let yOffset: CGFloat = 0.35
        NSColor.black.setFill()

        roundedTile(x: 2.4, y: 2.4 + yOffset, width: 23.2, height: 23.2, radius: 6.0, scale: scale).fill()

        NSGraphicsContext.saveGraphicsState()
        NSGraphicsContext.current?.compositingOperation = .clear
        drawSArrowMark(
            in: CGRect(x: 4.9 * scale, y: (2.3 + yOffset) * scale, width: 18.2 * scale, height: 23.0 * scale),
            fontSize: 19.6 * scale,
            weight: .black,
            color: .black
        )
        NSGraphicsContext.restoreGraphicsState()

        return image
    }

    private static func drawBrandIcon(size: CGSize, status: String) -> NSImage {
        let image = NSImage(size: size)
        image.lockFocus()
        defer { image.unlockFocus() }

        let rect = CGRect(origin: .zero, size: size)
        let scale = min(size.width, size.height) / 48.0
        let palette = statusPalette(status)
        let tileRect = rect.insetBy(dx: 3.0 * scale, dy: 3.0 * scale)
        let tile = NSBezierPath(roundedRect: tileRect, xRadius: 11.5 * scale, yRadius: 11.5 * scale)

        NSGraphicsContext.saveGraphicsState()
        let shadow = NSShadow()
        shadow.shadowColor = NSColor.black.withAlphaComponent(0.28)
        shadow.shadowBlurRadius = 9.0 * scale
        shadow.shadowOffset = NSSize(width: 0.0, height: -2.0 * scale)
        shadow.set()
        NSColor.black.withAlphaComponent(0.18).setFill()
        tile.fill()
        NSGraphicsContext.restoreGraphicsState()

        NSGradient(colors: [
            NSColor(red: 0.18, green: 0.29, blue: 0.35, alpha: 1.0),
            NSColor(red: 0.07, green: 0.11, blue: 0.15, alpha: 1.0),
            NSColor(red: 0.02, green: 0.03, blue: 0.05, alpha: 1.0),
        ])?.draw(in: tile, angle: -38.0)

        NSGraphicsContext.saveGraphicsState()
        tile.addClip()

        NSGradient(colors: [
            NSColor.white.withAlphaComponent(0.38),
            NSColor.white.withAlphaComponent(0.10),
            NSColor.clear,
        ])?.draw(in: tileRect, angle: 90.0)

        palette.glow.withAlphaComponent(0.34).setFill()
        NSBezierPath(ovalIn: CGRect(x: 5.0 * scale, y: 24.0 * scale, width: 28.0 * scale, height: 23.0 * scale)).fill()

        palette.accent.withAlphaComponent(0.30).setFill()
        NSBezierPath(ovalIn: CGRect(x: 19.0 * scale, y: 3.5 * scale, width: 25.0 * scale, height: 25.0 * scale)).fill()

        let diagonalSheen = NSBezierPath()
        diagonalSheen.move(to: CGPoint(x: 8.0 * scale, y: 40.0 * scale))
        diagonalSheen.line(to: CGPoint(x: 20.0 * scale, y: 46.0 * scale))
        diagonalSheen.line(to: CGPoint(x: 45.0 * scale, y: 18.0 * scale))
        diagonalSheen.line(to: CGPoint(x: 45.0 * scale, y: 7.0 * scale))
        diagonalSheen.close()
        NSColor.white.withAlphaComponent(0.08).setFill()
        diagonalSheen.fill()

        NSGraphicsContext.restoreGraphicsState()

        drawBrandGlyph(scale: scale, palette: palette)
        drawStatusBadge(scale: scale, palette: palette)

        return image
    }

    private static func drawBrandGlyph(scale: CGFloat, palette: Palette) {
        let shape = roundedTile(x: 10.5, y: 10.5, width: 27.0, height: 27.0, radius: 7.2, scale: scale)

        NSGraphicsContext.saveGraphicsState()
        let shadow = NSShadow()
        shadow.shadowColor = NSColor.black.withAlphaComponent(0.30)
        shadow.shadowBlurRadius = 4.0 * scale
        shadow.shadowOffset = NSSize(width: 0.0, height: -1.4 * scale)
        shadow.set()
        NSColor.black.withAlphaComponent(0.14).setFill()
        shape.fill()
        NSGraphicsContext.restoreGraphicsState()

        NSGradient(colors: [
            palette.accent.blended(withFraction: 0.48, of: .white) ?? palette.accent,
            palette.accent,
            palette.glow.withAlphaComponent(0.85),
        ])?.draw(in: shape, angle: 115.0)

        drawSArrowMark(
            in: CGRect(x: 14.6 * scale, y: 10.4 * scale, width: 18.8 * scale, height: 26.8 * scale),
            fontSize: 22.4 * scale,
            weight: .heavy,
            color: NSColor.white.withAlphaComponent(0.96)
        )
    }

    private static func drawStatusBadge(scale: CGFloat, palette: Palette) {
        let outer = CGRect(x: 31.0 * scale, y: 5.0 * scale, width: 11.0 * scale, height: 11.0 * scale)
        NSColor.black.withAlphaComponent(0.34).setFill()
        NSBezierPath(ovalIn: outer.insetBy(dx: -0.5 * scale, dy: -0.5 * scale)).fill()

        NSGradient(colors: [
            palette.badge.withAlphaComponent(1.0),
            palette.badge.blended(withFraction: 0.28, of: .white) ?? palette.badge,
        ])?.draw(in: NSBezierPath(ovalIn: outer), angle: 90.0)
    }

    private static func roundedTile(
        x: CGFloat,
        y: CGFloat,
        width: CGFloat,
        height: CGFloat,
        radius: CGFloat,
        scale: CGFloat
    ) -> NSBezierPath {
        let rect = CGRect(x: x * scale, y: y * scale, width: width * scale, height: height * scale)
        return NSBezierPath(roundedRect: rect, xRadius: radius * scale, yRadius: radius * scale)
    }

    private static func drawSArrowMark(in rect: CGRect, fontSize: CGFloat, weight: NSFont.Weight, color: NSColor) {
        drawLetterS(in: rect, fontSize: fontSize, weight: weight, color: color)
        drawVerticalArrow(in: rect, color: color)
    }

    private static func drawLetterS(in rect: CGRect, fontSize: CGFloat, weight: NSFont.Weight, color: NSColor) {
        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = .center
        let attributes: [NSAttributedString.Key: Any] = [
            .font: NSFont.systemFont(ofSize: fontSize, weight: weight),
            .foregroundColor: color,
            .paragraphStyle: paragraph,
        ]
        NSAttributedString(string: "S", attributes: attributes).draw(in: rect)
    }

    private static func drawVerticalArrow(in rect: CGRect, color: NSColor) {
        let shaftWidth = max(rect.width * 0.15, 1.2)
        let shaftHeight = rect.height * 0.56
        let shaftX = rect.midX - shaftWidth / 2.0
        let shaftY = rect.minY + rect.height * 0.16
        let radius = shaftWidth / 2.0

        color.setFill()
        NSBezierPath(
            roundedRect: CGRect(x: shaftX, y: shaftY, width: shaftWidth, height: shaftHeight),
            xRadius: radius,
            yRadius: radius
        ).fill()

        let head = NSBezierPath()
        head.move(to: CGPoint(x: rect.midX, y: rect.maxY - rect.height * 0.07))
        head.line(to: CGPoint(x: rect.midX - rect.width * 0.26, y: rect.maxY - rect.height * 0.31))
        head.line(to: CGPoint(x: rect.midX + rect.width * 0.26, y: rect.maxY - rect.height * 0.31))
        head.close()
        head.fill()
    }

    private static func statusPalette(_ status: String) -> Palette {
        switch status {
        case "monitor", "live":
            return Palette(
                accent: NSColor(red: 0.17, green: 0.86, blue: 0.52, alpha: 1.0),
                glow: NSColor(red: 0.18, green: 0.55, blue: 0.96, alpha: 1.0),
                badge: NSColor(red: 0.17, green: 0.86, blue: 0.52, alpha: 1.0)
            )
        case "review":
            return Palette(
                accent: NSColor(red: 1.0, green: 0.64, blue: 0.18, alpha: 1.0),
                glow: NSColor(red: 0.94, green: 0.36, blue: 0.68, alpha: 1.0),
                badge: NSColor(red: 1.0, green: 0.64, blue: 0.18, alpha: 1.0)
            )
        default:
            return Palette(
                accent: NSColor(red: 0.98, green: 0.29, blue: 0.36, alpha: 1.0),
                glow: NSColor(red: 0.72, green: 0.36, blue: 1.0, alpha: 1.0),
                badge: NSColor(red: 0.98, green: 0.29, blue: 0.36, alpha: 1.0)
            )
        }
    }
}
