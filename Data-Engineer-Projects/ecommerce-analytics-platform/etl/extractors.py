"""File extractors for batch ETL jobs."""

from __future__ import annotations

import csv
import gzip
import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]
import structlog

logger = structlog.get_logger(__name__)

JSON_SUFFIXES = (".json", ".jsonl", ".ndjson")
CSV_SUFFIXES = (".csv", ".tsv")


def _resolve_paths(source: str) -> list[Path]:
    paths = sorted(Path().glob(source))
    if not paths:
        raise FileNotFoundError(f"No files matched pattern '{source}'")
    return paths


def _open_text(path: Path) -> Iterable[str]:
    if path.suffix == ".gz":
        with gzip.open(path, mode="rt", encoding="utf-8") as fh:
            yield from fh
    else:
        with path.open("r", encoding="utf-8") as fh:
            yield from fh


def _iter_json_records(
    path: Path, chunk_size: int
) -> Iterator[tuple[Path, list[dict[str, Any]]]]:
    buffer: list[dict[str, Any]] = []
    content = "".join(_open_text(path)).strip()
    if not content:
        logger.warning("empty_json_file", path=str(path))
        return
    try:
        if content.startswith("["):
            records = json.loads(content)
            iterator = iter(records if isinstance(records, list) else [records])
        else:
            iterator = (
                json.loads(line) for line in content.splitlines() if line.strip()
            )
    except json.JSONDecodeError as exc:
        logger.error("json_decode_error", path=str(path), error=str(exc))
        return

    for record in iterator:
        buffer.append(record)
        if len(buffer) >= chunk_size:
            yield path, buffer
            buffer = []
    if buffer:
        yield path, buffer


def _iter_csv_records(
    path: Path, chunk_size: int
) -> Iterator[tuple[Path, list[dict[str, Any]]]]:
    try:
        reader = pd.read_csv(
            path, chunksize=chunk_size, dtype=str, keep_default_na=False
        )
    except ValueError:
        with path.open("r", encoding="utf-8", newline="") as fh:
            csv_reader = csv.DictReader(fh)
            buffer: list[dict[str, Any]] = []
            for row in csv_reader:
                buffer.append(row)
                if len(buffer) >= chunk_size:
                    yield path, buffer
                    buffer = []
            if buffer:
                yield path, buffer
        return

    for chunk in reader:
        records: list[dict[str, Any]] = chunk.to_dict(orient="records")
        yield path, records


def read_json_stream(
    source: str, chunk_size: int = 5000
) -> Iterator[tuple[Path, list[dict[str, Any]]]]:
    """Yield batches of JSON objects from files matching the glob pattern."""
    for path in _resolve_paths(source):
        yield from _iter_json_records(path, chunk_size)


def read_csv_stream(
    source: str, chunk_size: int = 5000
) -> Iterator[tuple[Path, list[dict[str, Any]]]]:
    """Yield batches of CSV rows as dictionaries."""
    for path in _resolve_paths(source):
        yield from _iter_csv_records(path, chunk_size)


def _detect_format(path: Path) -> str:
    name = path.name.lower()
    for suffix in (".gz", ".gzip", ".bz2"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    if any(name.endswith(ext) for ext in JSON_SUFFIXES):
        return "json"
    if any(name.endswith(ext) for ext in CSV_SUFFIXES):
        return "csv"
    raise ValueError(f"Unsupported file extension for incremental load: {path}")


def read_mixed_stream(
    source: str, chunk_size: int = 5000
) -> Iterator[tuple[Path, list[dict[str, Any]]]]:
    """Yield batches from JSON or CSV files detected by extension."""
    for path in _resolve_paths(source):
        fmt = _detect_format(path)
        if fmt == "json":
            yield from _iter_json_records(path, chunk_size)
        else:
            yield from _iter_csv_records(path, chunk_size)
