"""Configuration loading and validation for Veridion POC (ER + QC)."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

import yaml


@dataclass(slots=True)
class Thresholds:
    """Entity resolution thresholds and bonuses."""

    accept: float
    maybe: float
    country_bonus: float


@dataclass(slots=True)
class InputCfg:
    """Input dataset configuration."""

    path: Path
    id_column: str | None
    name_column: str
    country_column: str


@dataclass(slots=True)
class CandCfg:
    """Candidate attribute patterns."""

    max_candidates: int
    name_pattern: str
    country_pattern: str
    address_pattern: str
    domain_pattern: str
    id_pattern: str


@dataclass(slots=True)
class NormCfg:
    """Normalization behaviour toggles."""

    lower: bool
    strip_punct: bool
    remove_legal_suffixes: bool
    legal_suffixes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class FeaturesCfg:
    """Optional downstream features."""

    enrichment: bool
    esg: bool
    procurement_segments: bool


@dataclass(slots=True)
class AppConfig:
    """Top-level application configuration container."""

    input: InputCfg
    candidates: CandCfg
    thresholds: Thresholds
    normalization: NormCfg
    features: FeaturesCfg

    def candidate_columns(self, n: int) -> dict[str, str]:
        """Return the concrete candidate column names for index ``n``."""
        if not (1 <= n <= self.candidates.max_candidates):
            raise ValueError(
                f"Candidate index n must be within [1..{self.candidates.max_candidates}] (got {n})."
            )
        c = self.candidates
        return {
            "name": c.name_pattern.format(n=n),
            "country": c.country_pattern.format(n=n),
            "address": c.address_pattern.format(n=n),
            "domain": c.domain_pattern.format(n=n),
            "id": c.id_pattern.format(n=n),
        }


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _ensure_mapping(data: Dict[str, Any], key: str) -> Dict[str, Any]:
    section = data.get(key, {})
    if section is None:
        section = {}
    _require(isinstance(section, dict), f"{key} section must be a mapping.")
    return section


def _ensure_int(value: Any, field_name: str) -> int:
    _require(isinstance(value, int) and not isinstance(value, bool), f"{field_name} must be an integer.")
    return int(value)


def _ensure_float(value: Any, field_name: str) -> float:
    valid = isinstance(value, (int, float)) and not isinstance(value, bool)
    _require(valid, f"{field_name} must be a number.")
    return float(value)


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ValueError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    _require(isinstance(data, dict), "Top-level YAML must be a mapping.")
    return data


def load_config(path: str) -> AppConfig:
    """Load and validate the YAML configuration file."""

    cfg_path = Path(path)
    raw = _load_yaml(cfg_path)

    raw_input = _ensure_mapping(raw, "input")
    raw_cand = _ensure_mapping(raw, "candidates")
    raw_thr = _ensure_mapping(raw, "thresholds")
    raw_norm = _ensure_mapping(raw, "normalization")
    raw_feat = _ensure_mapping(raw, "features")

    # --- Input section ---
    path_value = raw_input.get("path")
    _require(isinstance(path_value, str) and path_value.strip(), "input.path must be a non-empty string.")
    input_path = Path(path_value.strip())

    id_column = raw_input.get("id_column")
    if id_column is not None:
        _require(isinstance(id_column, str) and id_column.strip(), "input.id_column must be a non-empty string or null.")
        id_column = id_column.strip()

    name_column = raw_input.get("name_column", "input_name")
    _require(isinstance(name_column, str) and name_column.strip(), "input.name_column must be a non-empty string.")
    name_column = name_column.strip()

    country_column = raw_input.get("country_column", "input_country")
    _require(isinstance(country_column, str) and country_column.strip(), "input.country_column must be a non-empty string.")
    country_column = country_column.strip()

    input_cfg = InputCfg(
        path=input_path,
        id_column=id_column,
        name_column=name_column,
        country_column=country_column,
    )

    # --- Candidates section ---
    max_candidates = _ensure_int(raw_cand.get("max_candidates", 5), "candidates.max_candidates")
    _require(1 <= max_candidates <= 10, "candidates.max_candidates must be in [1..10].")

    patterns = {
        "name_pattern": raw_cand.get("name_pattern", "cand{n}_name"),
        "country_pattern": raw_cand.get("country_pattern", "cand{n}_country"),
        "address_pattern": raw_cand.get("address_pattern", "cand{n}_address"),
        "domain_pattern": raw_cand.get("domain_pattern", "cand{n}_domain"),
        "id_pattern": raw_cand.get("id_pattern", "cand{n}_id"),
    }

    for label, pattern in patterns.items():
        _require(isinstance(pattern, str) and pattern.strip(), f"candidates.{label} must be a non-empty string.")
        _require("{n}" in pattern, f"candidates.{label} must contain '{{n}}'.")
        patterns[label] = pattern.strip()

    cand_cfg = CandCfg(
        max_candidates=max_candidates,
        name_pattern=patterns["name_pattern"],
        country_pattern=patterns["country_pattern"],
        address_pattern=patterns["address_pattern"],
        domain_pattern=patterns["domain_pattern"],
        id_pattern=patterns["id_pattern"],
    )

    # --- Thresholds section ---
    thr_accept = _ensure_float(raw_thr.get("accept", 0.70), "thresholds.accept")
    thr_maybe = _ensure_float(raw_thr.get("maybe", 0.50), "thresholds.maybe")
    thr_bonus = _ensure_float(raw_thr.get("country_bonus", 0.05), "thresholds.country_bonus")

    _require(0.0 <= thr_maybe < thr_accept <= 1.0, "thresholds must satisfy 0.0 <= maybe < accept <= 1.0.")
    _require(0.0 <= thr_bonus <= 0.25, "thresholds.country_bonus must be in [0.0, 0.25].")

    thresholds_cfg = Thresholds(
        accept=thr_accept,
        maybe=thr_maybe,
        country_bonus=thr_bonus,
    )

    # --- Normalization section ---
    norm_lower = raw_norm.get("lower", True)
    norm_strip = raw_norm.get("strip_punct", True)
    norm_remove_suffixes = raw_norm.get("remove_legal_suffixes", True)

    for label, value in (
        ("lower", norm_lower),
        ("strip_punct", norm_strip),
        ("remove_legal_suffixes", norm_remove_suffixes),
    ):
        _require(isinstance(value, bool), f"normalization.{label} must be boolean.")

    legal_suffixes_raw = raw_norm.get("legal_suffixes", [])
    if legal_suffixes_raw is None:
        legal_suffixes_raw = []
    _require(isinstance(legal_suffixes_raw, list), "normalization.legal_suffixes must be list[str].")
    _require(all(isinstance(item, str) for item in legal_suffixes_raw), "normalization.legal_suffixes must be list[str].")

    norm_cfg = NormCfg(
        lower=norm_lower,
        strip_punct=norm_strip,
        remove_legal_suffixes=norm_remove_suffixes,
        legal_suffixes=list(legal_suffixes_raw),
    )

    # --- Features section ---
    feat_enrichment = raw_feat.get("enrichment", False)
    feat_esg = raw_feat.get("esg", False)
    feat_procurement = raw_feat.get("procurement_segments", False)

    for label, value in (
        ("enrichment", feat_enrichment),
        ("esg", feat_esg),
        ("procurement_segments", feat_procurement),
    ):
        _require(isinstance(value, bool), f"features.{label} must be boolean.")

    features_cfg = FeaturesCfg(
        enrichment=feat_enrichment,
        esg=feat_esg,
        procurement_segments=feat_procurement,
    )

    # --- Final validation ---
    _require(input_cfg.path.exists(), f"Input CSV not found at: {input_cfg.path}")
    _require(input_cfg.path.is_file(), f"input.path must point to a file: {input_cfg.path}")

    return AppConfig(
        input=input_cfg,
        candidates=cand_cfg,
        thresholds=thresholds_cfg,
        normalization=norm_cfg,
        features=features_cfg,
    )
