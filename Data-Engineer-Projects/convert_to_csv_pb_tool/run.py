"""Bootstrap a virtual environment and run regatta-to-powerbi."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List

SETUP_MARKER = ".regatta_env_ready"


def _run(cmd: List[str]) -> None:
    subprocess.run(cmd, check=True)


def _ensure_venv(venv_path: Path) -> None:
    if not venv_path.exists():
        _run([sys.executable, "-m", "venv", str(venv_path)])


def _venv_python(venv_path: Path) -> Path:
    if os.name == "nt":
        return venv_path / "Scripts" / "python.exe"
    return venv_path / "bin" / "python"


def _is_setup_complete(venv_path: Path) -> bool:
    return (venv_path / SETUP_MARKER).exists()


def _mark_setup_complete(venv_path: Path) -> None:
    marker = venv_path / SETUP_MARKER
    marker.write_text("ready", encoding="utf-8")


def _resolve_path(base: Path, value: str, ensure_dir: bool = False) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = (base / path).resolve()
    if ensure_dir:
        path.mkdir(parents=True, exist_ok=True)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a local virtual environment, install regatta-to-powerbi, and run it."
    )
    parser.add_argument(
        "--in-dir",
        default="input",
        help="Directory that holds the Excel workbook when --xlsx-path is not supplied.",
    )
    parser.add_argument(
        "--out-dir",
        default="powerbi_dataset",
        help="Output directory for generated CSV files.",
    )
    parser.add_argument(
        "--sheet",
        default="Regatta Results",
        help="Worksheet name within the workbook.",
    )
    parser.add_argument(
        "--workbook-name",
        help="Workbook file name inside --in-dir when multiple Excel files exist.",
    )
    parser.add_argument(
        "--xlsx-path",
        help="Explicit path to an Excel workbook; overrides --in-dir and --workbook-name.",
    )
    parser.add_argument(
        "--report",
        default="REPORT.txt",
        help="Validation report filename passed to the CLI.",
    )
    parser.add_argument(
        "--skip-install",
        action="store_true",
        help="Skip pip install/upgrade steps (assumes environment already prepared).",
    )
    parser.add_argument(
        "--force-install",
        action="store_true",
        help="Force reinstallation even if the environment marker is present.",
    )

    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent
    venv_path = project_root / ".venv"
    _ensure_venv(venv_path)

    python_exe = _venv_python(venv_path)
    if not python_exe.exists():
        raise RuntimeError(
            f"Virtual environment Python not found at {python_exe}")

    setup_ready = _is_setup_complete(venv_path)
    should_install = not args.skip_install and (
        args.force_install or not setup_ready)
    if should_install:
        _run([str(python_exe), "-m", "pip", "install", "-U", "pip"])
        _run([str(python_exe), "-m", "pip", "install", "-e", str(project_root)])
        _mark_setup_complete(venv_path)

    in_dir_path = _resolve_path(project_root, args.in_dir, ensure_dir=True)
    out_dir_path = _resolve_path(project_root, args.out_dir, ensure_dir=True)

    cli_cmd: List[str] = [str(python_exe), "-m", "regatta_to_powerbi.cli"]

    if args.xlsx_path:
        workbook_path = Path(args.xlsx_path)
        if not workbook_path.is_absolute():
            workbook_path = (project_root / workbook_path).resolve()
        cli_cmd.extend(["--xlsx", str(workbook_path)])
    else:
        cli_cmd.extend(["--in-dir", str(in_dir_path)])
        if args.workbook_name:
            cli_cmd.extend(["--workbook-name", args.workbook_name])

    cli_cmd.extend(["--out-dir", str(out_dir_path)])
    cli_cmd.extend(["--sheet", args.sheet])

    report_arg = Path(args.report)
    if not report_arg.is_absolute():
        report_arg = (out_dir_path / report_arg).resolve()
    cli_cmd.extend(["--report", str(report_arg)])

    _run(cli_cmd)


if __name__ == "__main__":
    main()
