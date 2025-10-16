import numpy as np
import pandas as pd
import pytest
from datetime import datetime, time as datetime_time

from regatta_to_powerbi.transform import (
    build_fact_dim_frames,
    clean_dataframe,
    dedup_dim_boat,
    parse_and_backfill_times,
    to_hms,
    to_seconds,
)

EXPECTED_FACT_COLUMNS = [
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

EXPECTED_DIM_BOAT_COLUMNS = [
    "Sail Number",
    "Boat",
    "Class (boat type)",
    "Yacht Club",
    "Handicap/Rating",
]

EXPECTED_DIM_RACE_COLUMNS = [
    "RaceKey",
    "Regatta Number",
    "Race Number",
]

EXPECTED_DIM_DIVISION_COLUMNS = ["Division"]


def _sample_dataframe() -> pd.DataFrame:
    data = {
        "Regatta Number": [101, 101, 101, 102],
        "Race Number": [1, 1, "2", "1"],
        "Division": ["spinnaker ", "SpINnaKer", "Cruising", "club"],
        "Sail Number": [" usa 123 ", "USA 456", "  IRc 1", "gbR 77"],
        "Boat": [" sea breeze", "SECOND WIND", " tidal wave ", " horizon"],
        "Class (boat type)": ["J/24", "J/24", "beneteau", "x-yacht"],
        "Yacht Club": ["nyyc", "NYYC", " royal club ", "city sailors"],
        "Handicap/Rating": ["650", 640, "", 615],
        "Rank": ["1", "1", "2", ""],
        "Points": ["3", "3", "5", "7"],
        "Elapsed Time": ["5:30", "5:45", "62:15", "1:02:03"],
        "Corrected Time": ["5:00", "5:05", "", None],
    }
    return pd.DataFrame(data)


def test_to_seconds_and_to_hms_round_trip() -> None:
    seconds = to_seconds("1:02:03")
    assert seconds == pytest.approx(3723)
    assert to_hms(seconds) == "1:02:03"
    assert to_hms(to_seconds("05:30")) == "0:05:30"
    assert to_seconds(0.5) == pytest.approx(43200)
    assert to_seconds(datetime_time(hour=1, minute=1, second=1)) == pytest.approx(3661)
    dt_value = datetime(2023, 1, 1, 2, 3, 4)
    assert to_seconds(dt_value) == pytest.approx(7384)


def test_fact_and_dimension_frames() -> None:
    raw = _sample_dataframe()
    cleaned = clean_dataframe(raw)
    frames = build_fact_dim_frames(cleaned)

    fact = frames["fact_results"]
    assert list(fact.columns) == EXPECTED_FACT_COLUMNS
    assert fact["IsCorrectedMissing"].sum() == 2

    first_boat = fact.loc[fact["Sail Number"] == "Usa 123"].iloc[0]
    assert first_boat["RaceKey"] == "101-1"
    assert first_boat["FactKey"] == "101-1-Usa 123"
    assert bool(first_boat["IsTieWithinDivision"]) is True

    dim_boat = frames["dim_boat"]
    assert list(dim_boat.columns) == EXPECTED_DIM_BOAT_COLUMNS
    assert set(dim_boat["Sail Number"]) == {"Usa 123", "Usa 456", "Irc 1", "Gbr 77"}

    dim_race = frames["dim_race"]
    assert list(dim_race.columns) == EXPECTED_DIM_RACE_COLUMNS
    assert set(dim_race["RaceKey"]) == {"101-1", "101-2", "102-1"}

    dim_division = frames["dim_division"]
    assert list(dim_division.columns) == EXPECTED_DIM_DIVISION_COLUMNS
    assert set(dim_division["Division"]) == {"Spinnaker", "Cruising", "Club"}

    tie_combos = {
        tuple(row)
        for row in fact.loc[
            fact["IsTieWithinDivision"], ["Regatta Number", "Race Number", "Division", "Rank"]
        ].itertuples(index=False, name=None)
    }
    assert tie_combos == {(101, 1, "Spinnaker", 1)}

    assert not cleaned["Elapsed_HHMMSS"].isna().any()
    assert cleaned.loc[cleaned["Corrected_HHMMSS"].isna(), "IsCorrectedMissing"].all()


def test_dedup_dim_boat_collapses_duplicates() -> None:
    dim = pd.DataFrame(
        {
            "Sail Number": ["A", "A", "B"],
            "Boat": ["Alpha", "", "Bravo"],
            "Class (boat type)": ["Cat", "Cat", "Mono"],
            "Yacht Club": ["Club1", None, "Club2"],
            "Handicap/Rating": [600, 602, 700],
        }
    )

    deduped = dedup_dim_boat(dim)
    assert set(deduped["Sail Number"]) == {"A", "B"}
    row_a = deduped.loc[deduped["Sail Number"] == "A"].iloc[0]
    assert row_a["Boat"] == "Alpha"
    assert row_a["Yacht Club"] == "Club1"
    assert row_a["Handicap/Rating"] == pytest.approx(601)
    row_b = deduped.loc[deduped["Sail Number"] == "B"].iloc[0]
    assert row_b["Handicap/Rating"] == pytest.approx(700)


def test_parse_and_backfill_times_from_workbook(tmp_path) -> None:
    fact = pd.DataFrame(
        {
            "FactKey": ["F1", "F2"],
            "RaceKey": ["101-1", "101-2"],
            "Regatta Number": [101, 101],
            "Race Number": [1, 2],
            "Division": ["Spinnaker", "Spinnaker"],
            "Sail Number": ["Usa 123", "Usa 456"],
            "Boat": ["Sea Breeze", "Second Wind"],
            "Class (boat type)": ["J/24", "J/24"],
            "Yacht Club": ["Club", "Club"],
            "Handicap/Rating": [600.0, 605.0],
            "Rank": [1, 2],
            "Points": [3.0, 4.0],
            "Elapsed_HHMMSS": ["0:05:30", None],
            "Elapsed_sec": [330.0, np.nan],
            "Corrected_HHMMSS": ["0:05:00", None],
            "Corrected_sec": [300.0, np.nan],
            "IsCorrectedMissing": [False, True],
            "IsTieWithinDivision": [False, False],
        }
    )

    raw = pd.DataFrame(
        {
            "Regatta Number": [101],
            "Race Number": [2],
            "Sail Number": ["usa 456"],
            "Elapsed Time": ["1:02:03"],
            "Corrected Time": ["1:01:00"],
        }
    )
    xlsx_path = tmp_path / "regatta.xlsx"
    raw.to_excel(xlsx_path, sheet_name="Regatta Results", index=False)

    updated = parse_and_backfill_times(fact, str(xlsx_path))

    first = updated.loc[updated["FactKey"] == "F1"].iloc[0]
    second = updated.loc[updated["FactKey"] == "F2"].iloc[0]

    assert first["Elapsed_sec"] == pytest.approx(330)
    assert first["Corrected_sec"] == pytest.approx(300)

    assert second["Elapsed_sec"] == pytest.approx(3723)
    assert second["Corrected_sec"] == pytest.approx(3660)
    assert second["Elapsed_HHMMSS"] == "1:02:03"
    assert second["Corrected_HHMMSS"] == "1:01:00"
    assert not second["IsCorrectedMissing"]
