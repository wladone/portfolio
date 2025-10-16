(The file `d:\Power_APP_Cal_new\specs\001-real-time-system\spec.md` exists, but is empty)
# Real-time System Monitor Banner (Windows)

## Summary

Lightweight always-on-top "pill" banner that displays live system telemetry (CPU%, GPU%, RAM%, temperatures). Clicking the banner opens a compact "Performance Monitor" window with detailed tabs, per-process tables, quick export of logs, and basic process control.

## Goals

- Provide a near-instant overview of system health with minimal resource overhead.
- Allow quick inspection of top processes and exports for troubleshooting.
- Run without administrator privileges in a Basic Mode; enable extended GPU reporting if vendor SDK DLLs are present.
- Portable single-folder distribution with optional onefile packaging and code-signing readiness.

## Out of Scope / Non-Goals

- Any bundled advertising, driver updater, registry cleaner, or "RAM booster" features.
- Remote telemetry enabled by default (telemetry is opt-in only).

## Functional Requirements

1. Banner (Pill)
	- Draggable, rounded "pill" UI, dark theme by default, always-on-top option.
	- Displays: RAM% (used/total), CPU% (overall), GPU% (overall, if available), primary temperature (CPU package or GPU max) in real time.
	- Update cadence configurable; default update interval 0.5–1s for banner values.
	- Click expands to the compact "Performance Monitor" window.
	- Persist banner position across restarts.

2. Performance Monitor Window
	- Compact resizable window with four tabs: RAM, CPU, GPU, Disk.
	- Each tab shows a sortable/searchable table of top processes by RAM/CPU (columns: Process Name, PID, RAM (MB and %), CPU (%)).
	- Per-process actions: "End Task" with confirmation dialog (no force-kill without additional confirmation).
	- Quick-export controls for CSV and JSON (visible in each tab), and option to enable rolling 24h logging.
	- Table updates at ~1s cadence; UI remains responsive when scanning processes.

3. Logging
	- Exports: CSV and JSON formats for the current view (top N processes + totals + timestamps).
	- Optional rolling 24h log implemented as daily-file slices or circular buffer; configurable retention.
	- Log writing must be performant and not block the UI thread (I/O offloaded to worker thread/process).

4. Alerts
	- Configurable thresholds for CPU, GPU, RAM%, and temperatures.
	- When threshold breached, show Windows toast notification (use WinRT/Notification APIs) and optional persistent alert in the UI.

5. Startup & Tray
	- Start minimized to system tray (user-configurable). Tray menu: Show/Hide, Open Monitor, Start with Windows toggle, Preferences, Exit.
	- Toggle show/hide for banner and monitor window from tray.

6. Permissions Modes
	- Basic Mode (no admin): CPU/RAM/Disk via PDH/WMI/GlobalMemoryStatusEx; process list and End Task using standard Win32 APIs.
	- Extended GPU Mode: dynamically load NVML (NVIDIA) or ADLX (AMD) DLLs if present; gracefully degrade if missing.

7. Safety & Trust
	- No advertising or bundling of unrelated software.
	- Telemetry is opt-in; any telemetry must be documented and user-consent controlled.

## Non-Functional Requirements

- Minimal overhead: <1% CPU when idle; memory footprint <80MB in normal operation.
- Offline-first operation; network access only for opt-in features or update checks when enabled.
- Fast startup; banner visible within a few hundred milliseconds of launch when possible.
- Robustness: must handle missing vendor DLLs, low-privilege execution, and process enumeration race conditions.

## Technical Approach

- Architecture
  - UI: Python + PySide6 for banner and windows (single-threaded Qt event loop). UI communicates with sensor engine via stdout JSON or named pipe for extensibility.
  - Sensor Engine: Native C++ executable that queries sensors and enumerates lightweight metrics, emitting JSON objects to stdout at configured intervals.
  - IPC: Primary: read JSON lines from sensor-engine stdout; fallback/optional: named pipe for richer command/control.
  - Process operations (End Task) are executed by the UI process using native Win32 APIs; where privilege is insufficient, show guidance.

- Sensors
  - CPU/RAM/Disk: PDH queries, WMI fallbacks, and GlobalMemoryStatusEx for memory.
  - NVIDIA GPU: dynamically load NVML.dll (if available) and query utilization/temperatures. If not present, show "GPU unavailable (NVML missing)" in UI.
  - AMD GPU: dynamically load ADLX libraries when available.
  - Temperature selection: prefer CPU package temp from platform sensors; if unavailable, use highest reported core temp or GPU temp depending on tab.

- Data Model & Format
  - Sensor engine emits a JSON line per sample: timestamp, cpu_percent, ram_percent, gpu_percent, temps (per-sensor), per_process_summary (top N), system_totals.
  - The UI parses JSON lines and updates banner and detailed views.

## IPC / CLI Contract (Sensor Engine → UI)

- Sample JSON line (newline-delimited):

  {
	 "timestamp": "2025-09-15T12:34:56.789Z",
	 "cpu_percent": 12.3,
	 "ram_percent": 45.1,
	 "gpu_percent": 5.2,
	 "temps": { "cpu_package": 55.0, "gpu": 49.0 },
	 "process_top": [ { "name":"chrome.exe", "pid":1234, "ram_mb":450, "cpu_percent":2.1 }, ... ]
  }

- The UI must tolerate missing fields and partially populated GPU data.

## Security & Privacy

- End Task: require confirmation and do not escalate privileges automatically. If admin rights are required, prompt the user with guidance instead of attempting elevation silently.
- Telemetry only sent if user explicitly opts-in; store consent in local config.
- Logs may contain process names — treat as local-only sensitive data; provide clear export warnings.

## Packaging & Distribution

- Deliverable: single-folder portable distribution (UI Python runtime + native sensor engine DLL/exe). Provide optional onefile packaging (PyInstaller) for convenience.
- Prepare build scripts that support code-signing of both the Python bundle and the native C++ binary.

## Acceptance Criteria

- Values for CPU, RAM, GPU must match Task Manager / HWiNFO ±2% for sampled metrics.
- Banner updates at 0.5–1s; detailed view updates at ~1s.
- Export to CSV and JSON produces correctly formatted files containing timestamps and the visible rows.
- App runs in Basic Mode without administrative privileges and shows GPU data only when vendor DLLs are present.

## Test Plan

1. Metrics Accuracy
	- Compare sampled CPU/RAM/GPU values against Task Manager and HWiNFO across idle and load for 5 minutes; assert ±2%.

2. Performance
	- Measure CPU and RAM usage of combined app while idle; must be <1% CPU and <80MB RAM.

3. UI Behavior
	- Drag banner to multiple positions, restart app, ensure position persisted.
	- Open detail window, search processes, sort columns, run End Task on a safe test process and verify confirmation and termination.

4. Logging & Export
	- Export CSV/JSON from each tab and validate schema and content.

5. Alerts
	- Configure low threshold to trigger toast; validate toast appears and does not require admin.

6. Packaging
	- Build portable folder; verify app runs on a clean Windows 10 VM without additional installs.

## Implementation Plan (High-level)

1. Sensor Engine (C++) - Core
	- Implement PDH/WMI/GlobalMemoryStatusEx sampling and JSON output.
	- Add dynamic NVML/ADLX loading modules.
	- Implement process top-N summary generation.

2. UI (Python/PySide6)
	- Implement banner UI, drag/persist logic, and click-to-open behavior.
	- Implement Performance Monitor with tabs, table, search, End Task integration, and export buttons.
	- Implement tray integration and preferences UI for thresholds and logging.

3. IPC, Packaging, Tests
	- Wire stdout JSON parsing; add named-pipe extension later.
	- Add build scripts and packaging pipeline; create test harness to validate metrics and performance.

## Notes / Open Questions

- Exact sampling methods and counters for temperature across CPU vendors need refinement during C++ implementation.
- Process 'End Task' behavior when run under limited privileges: should we surface an 'Elevate to end task' flow or simply show guidance? Default to guidance + retry suggestion.

---

Spec generated from feature description: "Real-time System Monitor Banner (Windows)".

