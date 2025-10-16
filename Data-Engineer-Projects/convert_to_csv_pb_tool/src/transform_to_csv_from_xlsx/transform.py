from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Tuple

import math
from datetime import datetime, time
import numpy as np
import pandas as pd

REQUIRED_COLUMNS = {
    "Regatta Number",
    "Race Number",
    "Division",
    "Sail Number",
    "Boat",
    "Class (boat type)",
    "Yacht Club",
    "Handicap/Rating",
    "Rank",
    "Points",
    "Elapsed Time",
    "Corrected Time",
}

TEXT_COLUMNS = [
    "Sail Number",
    "Division",
    "Boat",
    "Class (boat type)",
    "Yacht Club",
]

NUMERIC_COLUMNS = [
    "Rank",
    "Points",
    "Handicap/Rating",
    "Race Number",
    "Regatta Number",
]

INTEGER_COLUMNS = {"Rank", "Race Number", "Regatta Number"}

TIME_COLUMNS = {
    "Elapsed Time": ("Elapsed_sec", "Elapsed_HHMMSS"),
    "Corrected Time": ("Corrected_sec", "Corrected_HHMMSS"),
}

FACT_COLUMNS = [
    "FactKey",
    "RaceKey",
    "Regatta Number",
    "Race Number",
    "Division",
    "Sail Number",
    "Boat",
    "Class (boat type)",
    "Yacht Club",
    "Handicap/Rating",
    "Rank",
    "Points",
    "Elapsed_HHMMSS",
    "Elapsed_sec",
    "Corrected_HHMMSS",
    "Corrected_sec",
    "IsCorrectedMissing",
    "IsTieWithinDivision",
]

DIM_BOAT_COLUMNS = [
    "Sail Number",
    "Boat",
    "Class (boat type)",
    "Yacht Club",
    "Handicap/Rating",
]

DIM_RACE_COLUMNS = [
    "RaceKey",
    "Regatta Number",
    "Race Number",
]

DIM_DIVISION_COLUMNS = ["Division"]


def to_seconds(val: Any) -> float | np.nan:
    """Convert supported time representations into seconds."""
    if val is None:
        return np.nan

    if isinstance(val, (pd.Timedelta, np.timedelta64)):
        try:
            return float(pd.to_timedelta(val).total_seconds())
        except Exception:
            return np.nan

    if isinstance(val, (pd.Timestamp, np.datetime64, datetime)):
        ts = pd.to_datetime(val)
        return float(ts.hour * 3600 + ts.minute * 60 + ts.second)

    if isinstance(val, time):
        return float(val.hour * 3600 + val.minute * 60 + val.second)

    if isinstance(val, (int, float, np.number)) and not isinstance(val, bool):
        if pd.isna(val):
            return np.nan
        fval = float(val)
        if 0.0 <= fval < 1.0:
            return float(round(fval * 24 * 3600))
        if fval.is_integer() and fval >= 0:
            return fval

    if isinstance(val, str):
        stripped = val.strip()
        if not stripped:
            return np.nan
        if ":" in stripped:
            parts = stripped.split(":")
            try:
                parts = [int(float(part)) for part in parts]
            except Exception:
                return np.nan
            if len(parts) == 2:
                minutes, seconds = parts
                return float(minutes * 60 + seconds)
            if len(parts) == 3:
                hours, minutes, seconds = parts
                return float(hours * 3600 + minutes * 60 + seconds)
            return np.nan
        try:
            fval = float(stripped)
        except ValueError:
            return np.nan
        if 0.0 <= fval < 1.0:
            return float(round(fval * 24 * 3600))
        if fval.is_integer() and fval >= 0:
            return fval
        return np.nan

    return np.nan


def to_hms(seconds: float | int | None) -> str | None:
    """Render seconds as H:MM:SS (H may be zero)."""
    if seconds is None:
        return None
    try:
        numeric = float(seconds)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    total_seconds = int(round(numeric))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}"


def _normalize_text(value: Any) -> Any:
    if value is None:
        return np.nan
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return np.nan
        return stripped.title()
    if isinstance(value, (float, np.floating)):
        if pd.isna(value):
            return np.nan
        if float(value).is_integer():
            return str(int(value))
        return str(value)
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if pd.isna(value):
        return np.nan
    text_value = str(value).strip()
    if not text_value:
        return np.nan
    return text_value.title()


def _first_non_null(series: pd.Series) -> Any:
    for value in series:
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                return value
        elif pd.notna(value):
            return value
    return np.nan


def _compose_race_key(regatta: Any, race: Any) -> str | None:
    if pd.isna(regatta) or pd.isna(race):
        return None
    try:
        regatta_int = int(regatta)
        race_int = int(race)
    except (TypeError, ValueError):
        return None
    return f"{regatta_int}-{race_int}"


def _compose_fact_key(race_key: str | None, sail_number: Any) -> str | None:
    if race_key is None or pd.isna(race_key):
        return None
    if isinstance(sail_number, str):
        trimmed = sail_number.strip()
        return f"{race_key}-{trimmed}" if trimmed else None
    if sail_number is None or pd.isna(sail_number):
        return None
    return f"{race_key}-{sail_number}"


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich the raw regatta results dataframe."""
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")

    working = df.copy()

    for column in TEXT_COLUMNS:
        if column in working.columns:
            working[column] = working[column].apply(_normalize_text)

    for column in NUMERIC_COLUMNS:
        if column in working.columns:
            numeric = pd.to_numeric(working[column], errors="coerce")
            if column in INTEGER_COLUMNS:
                working[column] = numeric.round().astype("Int64")
            else:
                working[column] = numeric

    for source, targets in TIME_COLUMNS.items():
        seconds_col, hms_col = targets
        seconds = working[source].apply(to_seconds)
        working[seconds_col] = seconds
        working[hms_col] = seconds.apply(to_hms)

    working["RaceKey"] = working.apply(
        lambda row: _compose_race_key(row.get("Regatta Number"), row.get("Race Number")),
        axis=1,
    )
    working["FactKey"] = working.apply(
        lambda row: _compose_fact_key(row.get("RaceKey"), row.get("Sail Number")),
        axis=1,
    )

    working["IsCorrectedMissing"] = working["Corrected_sec"].isna()
    group_cols = ["Regatta Number", "Race Number", "Division", "Rank"]
    group_sizes = working.groupby(group_cols, dropna=False)["FactKey"].transform("size")
    working["IsTieWithinDivision"] = (group_sizes > 1) & working["Rank"].notna()

    return working


def dedup_dim_boat(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse boat dimension rows to one record per Sail Number."""
    if df.empty:
        return df.copy()

    grouped = (
        df.groupby("Sail Number", dropna=False)
        .agg(
            {
                "Boat": _first_non_null,
                "Class (boat type)": _first_non_null,
                "Yacht Club": _first_non_null,
                "Handicap/Rating": lambda s: pd.to_numeric(s, errors="coerce").median(),
            }
        )
        .reset_index()
    )

    grouped["Handicap/Rating"] = grouped["Handicap/Rating"].astype(float)
    grouped = grouped.sort_values(["Sail Number"], kind="stable").reset_index(drop=True)
    return grouped


def build_fact_dim_frames(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Split the cleaned dataframe into fact and dimension frames."""
    missing_fact = [column for column in FACT_COLUMNS if column not in df.columns]
    if missing_fact:
        raise ValueError(
            f"Clean dataframe missing columns needed for fact table: {', '.join(missing_fact)}"
        )

    fact = df[FACT_COLUMNS].copy()
    fact = fact.sort_values(["Regatta Number", "Race Number", "Sail Number"], kind="stable")
    fact = fact.reset_index(drop=True)

    dim_boat_raw = df[DIM_BOAT_COLUMNS].dropna(subset=["Sail Number"]).copy()
    dim_boat = dedup_dim_boat(dim_boat_raw)

    dim_race = (
        df[DIM_RACE_COLUMNS]
        .dropna(subset=["RaceKey"])
        .drop_duplicates()
        .sort_values(["Regatta Number", "Race Number"], kind="stable")
        .reset_index(drop=True)
    )

    dim_division = (
        df[DIM_DIVISION_COLUMNS]
        .dropna(subset=["Division"])
        .drop_duplicates()
        .sort_values(["Division"], kind="stable")
        .reset_index(drop=True)
    )

    return {
        "fact_results": fact,
        "dim_boat": dim_boat,
        "dim_race": dim_race,
        "dim_division": dim_division,
    }



def _normalize_key_series(series: pd.Series) -> pd.Series:
    values = series.astype(str).str.strip()
    return values.str.upper()


def parse_and_backfill_times(fact: pd.DataFrame, xlsx_path: str | None) -> pd.DataFrame:
    """Fill the time columns in fact_results, optionally backing from the workbook."""
    required = {
        "FactKey",
        "Regatta Number",
        "Race Number",
        "Sail Number",
        "Elapsed_sec",
        "Corrected_sec",
        "Elapsed_HHMMSS",
        "Corrected_HHMMSS",
        "IsCorrectedMissing",
    }
    missing = [column for column in required if column not in fact.columns]
    if missing:
        raise ValueError(f"Fact dataframe missing required columns: {', '.join(sorted(missing))}")

    result = fact.copy()
    original_order = list(result.columns)

    # capture existing completeness for reporting
    before_elapsed = float(result["Elapsed_sec"].notna().mean() * 100) if len(result) else 0.0
    before_corrected = float(result["Corrected_sec"].notna().mean() * 100) if len(result) else 0.0

    # Prepare helper join keys
    result["_RegattaKey"] = result["Regatta Number"].astype("Int64").astype(str).str.strip()
    result["_RaceKey"] = result["Race Number"].astype("Int64").astype(str).str.strip()
    result["_SailKey"] = _normalize_key_series(result["Sail Number"])

    has_raw_cols = all(col in result.columns for col in ("Elapsed Time", "Corrected Time"))

    if not has_raw_cols and xlsx_path:
        raw = pd.read_excel(xlsx_path, sheet_name="Regatta Results", engine="openpyxl")
        raw["_RegattaKey"] = pd.to_numeric(raw["Regatta Number"], errors="coerce").round().astype("Int64").astype(str).str.strip()
        raw["_RaceKey"] = pd.to_numeric(raw["Race Number"], errors="coerce").round().astype("Int64").astype(str).str.strip()
        raw["_SailKey"] = _normalize_key_series(raw["Sail Number"])

        merge_cols = ["_RegattaKey", "_RaceKey", "_SailKey"]
        times_cols = [col for col in ("Elapsed Time", "Corrected Time") if col in raw.columns]
        raw_subset = raw[merge_cols + times_cols]

        result = result.merge(raw_subset, on=merge_cols, how="left", suffixes=("", "_src"))
        for col in ("Elapsed Time", "Corrected Time"):
            if col not in fact.columns and f"{col}_src" in result.columns:
                result[col] = result[f"{col}_src"]

    # Parse seconds
    if "Elapsed Time" in result.columns:
        result["Elapsed_sec"] = result["Elapsed_sec"].apply(to_seconds)
        mask = result["Elapsed_sec"].isna()
        result.loc[mask, "Elapsed_sec"] = result.loc[mask, "Elapsed Time"].apply(to_seconds)
    else:
        result["Elapsed_sec"] = result["Elapsed_sec"].apply(to_seconds)

    if "Corrected Time" in result.columns:
        result["Corrected_sec"] = result["Corrected_sec"].apply(to_seconds)
        mask = result["Corrected_sec"].isna()
        result.loc[mask, "Corrected_sec"] = result.loc[mask, "Corrected Time"].apply(to_seconds)
    else:
        result["Corrected_sec"] = result["Corrected_sec"].apply(to_seconds)

    result["Elapsed_sec"] = pd.to_numeric(result["Elapsed_sec"], errors="coerce")
    result["Corrected_sec"] = pd.to_numeric(result["Corrected_sec"], errors="coerce")
    result["Elapsed_HHMMSS"] = result["Elapsed_sec"].apply(to_hms)
    result["Corrected_HHMMSS"] = result["Corrected_sec"].apply(to_hms)
    result["IsCorrectedMissing"] = result["Corrected_sec"].isna()

    after_elapsed = float(result["Elapsed_sec"].notna().mean() * 100) if len(result) else 0.0
    after_corrected = float(result["Corrected_sec"].notna().mean() * 100) if len(result) else 0.0

    # Drop helper columns before returning
    drop_cols = ["_RegattaKey", "_RaceKey", "_SailKey", "Elapsed Time_src", "Corrected Time_src"]
    result = result.drop(columns=[c for c in drop_cols if c in result.columns])

    # Some callers expect original column order
    for col in original_order:
        if col not in result.columns:
            result[col] = np.nan
    result = result[original_order]

    result.attrs["time_completeness"] = {
        "before_elapsed": before_elapsed,
        "before_corrected": before_corrected,
        "after_elapsed": after_elapsed,
        "after_corrected": after_corrected,
    }

    return result


def transform_workbook(path: Path | str, sheet: str = "Regatta Results") -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
    """Read, clean, and split a regatta workbook."""
    workbook_path = Path(path)
    if not workbook_path.exists():
        raise FileNotFoundError(f"Input workbook not found: {workbook_path}")
    raw = pd.read_excel(workbook_path, sheet_name=sheet, engine="openpyxl")
    cleaned = clean_dataframe(raw)
    frames = build_fact_dim_frames(cleaned)
    return frames, cleaned
