# SysMon UI Design Spec

Version: 1.0
Owner: Design + Apps Team
Scope: Desktop app and Banner overlay

## Goals

- Use orange and near‑black hues as primary brand colors
- Make graphs “pop” with clear contrast and subtle depth
- Deliver a slick, minimal look that’s easy to read at a glance
- Add small icons for each metric in both Desktop and Banner
- Keep the banner slim with a tasteful, minimal border

## Brand & Palette

Primary and surfaces
- Primary Orange: #FF7A18
- Orange 700: #FF8C1A
- Orange 500: #FF9B40
- Amber: #FFC24B
- Surface: #15171C
- Background (BG): #0E0F12
- Border: #222832
- Text on dark: #F5F7FA

Data series & states
- CPU: #FF7A18
- RAM: #FFC24B
- GPU: #38BDF8
- Temp: #FF3B30
- State OK: #22C55E
- State Warn: #FF8A00
- State Crit: #FF3B30

Accessibility notes
- Maintain 4.5:1 contrast for body text on Surface/BG; use Text (#F5F7FA).
- For color‑blind safety, rely on icons + labels; avoid red/green only.

## Typography

- Primary font: Segoe UI (Windows default). Fallbacks: Inter, Arial, sans‑serif.
- Sizes
  - Banner values: 16–18 px, semi‑bold (600)
  - Desktop stat labels: 18 px, regular to semi‑bold
  - Chart axes/legend: 10–11 px
  - Table cells: 12–13 px
- Letter spacing: default; keep high legibility, no condensed styles.

## Iconography

- Simple, line‑based 24×24 px SVGs, stroke 2px, rounded joins/caps.
- Use white on dark backgrounds; tint to series colors for context as needed.
- Assets (SVG):
  - CPU: `prototype/ui/assets/icons/cpu.svg`
  - RAM: `prototype/ui/assets/icons/ram.svg`
  - GPU: `prototype/ui/assets/icons/gpu.svg`
  - Temp: `prototype/ui/assets/icons/temp.svg`

Usage (PySide6)
```
icon_cpu = QtGui.QIcon("prototype/ui/assets/icons/cpu.svg")
label = QtWidgets.QLabel()
label.setPixmap(icon_cpu.pixmap(18, 18))
```

## Banner (Overlay) Guidelines

- Position: default Top‑Right; allow TL/BR/BL options.
- Size: 320–360 × 48–56 px; horizontal layout, 12 px spacing.
- Border: 1.5 px in Primary Orange (#FF7A18), 12–16 px radius.
- Background: rgba(30,30,30,0.92) on show; auto‑fade opacity to 0.25 after 3–5 min inactivity.
- Content order: icon • CPU • RAM • GPU • Temp • controls (hide/close).
- Controls: minimal system icons; hover background rgba(255,255,255,0.12) with 4 px radius.
- Don’t steal focus; remain below active windows when possible.

Example QSS for banner container
```
QWidget#BannerRoot {
  background-color: rgba(30,30,30,235);
  border: 1.5px solid #FF7A18;
  border-radius: 16px;
}
QLabel { color: #F5F7FA; font-weight: 600; }
QToolButton { color: #F5F7FA; background: transparent; border: none; }
QToolButton:hover { background: rgba(255,255,255,0.12); border-radius: 4px; }
```

## Desktop App Guidelines

Layout
- Two‑column: stats + charts + details panel; bottom dock for Top Processes.
- 12 px outer padding, 16 px inter‑panel spacing.

Stats row
- Each metric shows icon + label + value; align baselines; 18 px text.
- Use series color for the icon; keep text color #F5F7FA.

Charts (QtCharts)
- Line width: 2.5 px; anti‑aliased.
- Series colors: CPU #FF7A18, RAM #FFC24B; add Glow/Shadow for pop.
- Gridlines: very subtle (#222832 at 40% alpha). No major grid clutter.
- Axis: labels #AAB2C0 at 80% alpha; tick width 1 px.
- Fill: optional soft area to 0 with 12–18% alpha of series color.

QtCharts snippet (styling)
```
cpu_pen = QtGui.QPen(QtGui.QColor('#FF7A18'))
cpu_pen.setWidthF(2.5)
cpu_pen.setCapStyle(QtCore.Qt.RoundCap)
cpu_pen.setJoinStyle(QtCore.Qt.RoundJoin)
cpu_series.setPen(cpu_pen)

ram_pen = QtGui.QPen(QtGui.QColor('#FFC24B'))
ram_pen.setWidthF(2.5)
ram_pen.setCapStyle(QtCore.Qt.RoundCap)
ram_pen.setJoinStyle(QtCore.Qt.RoundJoin)
ram_series.setPen(ram_pen)

# Subtle gridlines
axis_pen = QtGui.QPen(QtGui.QColor(34,40,50,120))
chart.axisX().setGridLinePen(axis_pen)
chart.axisY().setGridLinePen(axis_pen)
```

Thresholds
- Optional translucent bands for Temp: Warn 75–85°C (#FF8A00, 12% alpha), Crit >85°C (#FF3B30, 12% alpha).

Tables
- Header: #AAB2C0 on Surface; row hover: rgba(255,255,255,0.06).
- Align numeric columns right; 1‑decimal precision for %.

## Motion & Feedback

- Fade‑in 120 ms when showing banner; fade‑out 800 ms when auto‑dim.
- Live updates no more than ~10 Hz UI refresh to avoid flicker; smooth chart history.

## Implementation Notes (PySide6)

- Use `style.qss` to centralize theme: `prototype/ui/style.qss`.
- Package `prototype/ui` as data in PyInstaller (already set in .spec files).
- Respect High‑DPI: enable `QtCore.QCoreApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling)` on startup if needed.

## Assets

- Icons live under `prototype/ui/assets/icons/`.
- Theme file: `prototype/ui/style.qss` (optional; see sample below).

## Open Items / Final Touches

- Decide exact banner width for localized text lengths.
- Confirm icon tinting approach (white vs series color) in both views.
- Validate contrast on HDR/auto‑brightness displays.
- Verify chart performance at 60 samples history on low‑end GPUs.

---

### style.qss (starter theme)

```
/* Base */
* { color: #F5F7FA; }
QMainWindow, QWidget { background-color: #0E0F12; }
QStatusBar { background: #15171C; color: #AAB2C0; }

/* Panels */
QGroupBox, QDockWidget > QWidget { background: #15171C; border: 1px solid #222832; border-radius: 8px; }
QDockWidget::title { color: #AAB2C0; }

/* Table */
QHeaderView::section { background: #15171C; color: #AAB2C0; border: none; padding: 6px; }
QTableView::item:selected { background: rgba(255,122,24,0.18); }
QTableView::item:hover { background: rgba(255,255,255,0.06); }

/* Buttons */
QToolButton { background: transparent; border: none; }
QToolButton:hover { background: rgba(255,255,255,0.12); border-radius: 4px; }
```

