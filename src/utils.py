"""Utility helpers for text normalization and identifier handling used across the dedup pipeline."""

from __future__ import annotations
import re, unicodedata, hashlib
from typing import Any, List

ACCENT_RE = re.compile(r"[\u0300-\u036f]")
NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")

def normalize_text(s: str | None) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = ACCENT_RE.sub("", s)
    s = NON_ALNUM_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def digits_only(s: str | None) -> str:
    return re.sub(r"\D", "", s or "")

def normalize_gtin(x: Any) -> str:
    s = digits_only(str(x)) if x is not None else ""
    return s if len(s) in {8, 12, 13, 14} else ""

def canonical_gtin(x: Any) -> str:
    s = normalize_gtin(x)
    return s.zfill(14) if s else ""

def tokenize(s: str) -> List[str]:
    return normalize_text(s).split()

def has_letters_and_digits(tok: str) -> bool:
    return bool(re.search(r"[a-z]", tok)) and bool(re.search(r"[0-9]", tok))

def model_tokens(name: str) -> List[str]:
    toks = tokenize(name)
    return [t for t in toks if has_letters_and_digits(t) and len(t) <= 20]

def stable_uid(key: str) -> str:
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]

