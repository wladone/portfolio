"""Text normalization utilities for entity resolution workflows."""

from __future__ import annotations

import re
from typing import Iterable, Sequence

from unidecode import unidecode

_PUNCT_CHARS = ".,;:-_/\\()[]{}'\"&"
_PUNCT_RE = re.compile("[" + re.escape(_PUNCT_CHARS) + "]")
_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_basic(text: str, *, lower: bool, strip_punct: bool) -> str:
    """Apply unidecode, optional lowercase, optional punctuation stripping, collapse spaces."""
    cleaned = unidecode(text)
    if lower:
        cleaned = cleaned.lower()
    if strip_punct:
        cleaned = _PUNCT_RE.sub(" ", cleaned)
    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip()
    return cleaned


def _prepare_suffix_metadata(
    suffixes: Sequence[str] | Iterable[str],
    *,
    lower: bool,
    strip_punct: bool,
) -> tuple[list[str], set[str]]:
    """Return normalized suffix strings and token set for quick filtering."""
    normalized_strings: list[str] = []
    tokens: set[str] = set()
    for suff in suffixes or ():
        if not isinstance(suff, str):
            continue
        normalized = _normalize_basic(suff, lower=lower, strip_punct=strip_punct)
        if not normalized:
            continue
        normalized_strings.append(normalized)
        tokens.update(normalized.split())
    return normalized_strings, tokens


def _strip_suffix_sequences(tokens: list[str], suffix_strings: Sequence[str]) -> list[str]:
    """Remove trailing token sequences that represent legal suffixes."""
    if not tokens or not suffix_strings:
        return tokens

    remaining = list(tokens)
    while remaining:
        removed = False
        for suffix in suffix_strings:
            if not suffix:
                continue
            suffix_token_list = suffix.split()
            suffix_joined = suffix.replace(" ", "")
            length = len(suffix_token_list)

            if length and len(remaining) >= length:
                tail = remaining[-length:]
                if tail == suffix_token_list:
                    del remaining[-length:]
                    removed = True
                    break

            max_len = len(remaining)
            for span in range(1, max_len + 1):
                tail = remaining[-span:]
                if "".join(tail) == suffix_joined:
                    del remaining[-span:]
                    removed = True
                    break
            if removed:
                break
        if not removed:
            break
    return remaining


def normalize_name(
    s: str,
    lower: bool = True,
    strip_punct: bool = True,
    remove_legal_suffixes: bool = True,
    legal_suffixes: Iterable[str] = (),
) -> str:
    """Normalize organisation names with configurable transformations."""
    if not isinstance(s, str) or not s.strip():
        return ""

    base = _normalize_basic(s, lower=lower, strip_punct=strip_punct)
    if not base or not remove_legal_suffixes:
        return base

    suffix_strings, suffix_tokens = _prepare_suffix_metadata(
        legal_suffixes,
        lower=lower,
        strip_punct=strip_punct,
    )
    if not suffix_tokens and not suffix_strings:
        return base

    tokens = base.split()
    tokens = _strip_suffix_sequences(tokens, suffix_strings)
    if suffix_tokens:
        tokens = [token for token in tokens if token not in suffix_tokens]
    return " ".join(tokens)
