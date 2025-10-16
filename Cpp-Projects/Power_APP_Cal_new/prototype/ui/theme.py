from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional, Dict, Any


def _candidates(rel: str):
    rel_path = Path(rel)
    # 1) PyInstaller extraction dir
    meipass = getattr(sys, '_MEIPASS', None)
    if meipass:
        yield Path(meipass) / rel_path
    # 2) CWD
    yield Path.cwd() / rel_path
    # 3) Repo-relative to this file
    here = Path(__file__).resolve()
    yield here.parent.parent / rel_path  # prototype/ + rel
    yield here.parent / rel_path         # ui/ + rel


def resource_path(rel: str) -> Optional[Path]:
    for p in _candidates(rel):
        if p.exists():
            return p
    return None


def load_qss() -> Optional[str]:
    p = resource_path('prototype/ui/style.qss')
    if p and p.exists():
        try:
            return p.read_text(encoding='utf-8')
        except Exception:
            return p.read_text(errors='ignore')
    return None


def load_theme_json() -> Dict[str, Any]:
    # Prefer packaged theme.json, then repo, finally user's Downloads
    for rel in ('prototype/ui/theme.json', 'prototype/ui/theme.default.json'):
        p = resource_path(rel)
        if p and p.exists():
            try:
                return json.loads(p.read_text(encoding='utf-8'))
            except Exception:
                pass
    # Downloads fallback if user provided an external theme
    try:
        dl = Path.home() / 'Downloads' / 'theme.json'
        if dl.exists():
            return json.loads(dl.read_text(encoding='utf-8'))
    except Exception:
        pass
    # Defaults
    return {
        'base': {
            'bg': '#0E0F12',
            'surface': '#15171C',
            'border': '#222832',
            'text': '#F5F7FA',
            'text_muted': '#C9CED6',
        },
        'series': {
            'cpu': '#FF7A18',
            'ram': '#FFC24B',
            'gpu': '#38BDF8',
            'temp': '#FF3B30',
        }
    }

