Prototype: Real-time System Monitor Banner

Files:
- `ui/banner.py`: PySide6 banner UI that reads `sensor.json` periodically.
- `sensor_stub.py`: Simple Python stub that writes randomized samples to `sensor.json`.
- `engine_stub.cpp`: Example C++ stdout-emitting stub (not built here).
- `requirements.txt`: Python dependencies for the prototype.

Run (PowerShell):

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt
start-process -NoNewWindow -FilePath python -ArgumentList 'prototype\\sensor_stub.py'
start-process -NoNewWindow -FilePath python -ArgumentList 'prototype\\ui\\banner.py'
```

Notes:
- The real implementation should replace the file-based stub with a native C++ sensor engine that writes newline-delimited JSON to stdout and have the UI spawn and read that process.

Desktop App and Packaging
- Desktop UI: `prototype/ui/desktop_app.py` with charts and top-process table.
- Quick run: `./run.ps1 -Desktop` (uses `.venv`, installs deps).
- Build EXE: `powershell -ExecutionPolicy Bypass -File prototype/build/build-ui.ps1` → `dist/SysMonDesktop.exe`.
- NSIS installer script: `prototype/build/installer.nsi` (requires NSIS to compile).

Engine Stub (C++)
- Build (MSVC dev prompt): `powershell -ExecutionPolicy Bypass -File prototype/build/build-engine.ps1`
- Run with TCP mode: `bin\engine_stub.exe -t 20123`
- Install background runner:
  - Prefer NSSM: `prototype/build/install-engine-service.ps1 -Action Install -EnginePath ..\..\bin\engine_stub.exe -Port 20123`
  - Fallback (no NSSM): installs a Logon Scheduled Task of the same name.
