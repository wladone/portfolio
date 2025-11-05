from pathlib import Path
import sys

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.veridion_poc.config import (
    AppConfig,
    CandCfg,
    FeaturesCfg,
    InputCfg,
    NormCfg,
    Thresholds,
)
from src.veridion_poc.er import entity_resolution


def test_er_minimal_match_accept_or_maybe():
    df = pd.DataFrame(
        [
            {
                "input_name": "Kahoot! Denmark ApS",
                "input_country": "Denmark",
                "cand1_name": "Kahoot",
                "cand1_country": "Norway",
            }
        ]
    )

    cfg = AppConfig(
        input=InputCfg(
            path=Path("dummy.csv"),
            id_column=None,
            name_column="input_name",
            country_column="input_country",
        ),
        candidates=CandCfg(
            max_candidates=5,
            name_pattern="cand{n}_name",
            country_pattern="cand{n}_country",
            address_pattern="cand{n}_address",
            domain_pattern="cand{n}_domain",
            id_pattern="cand{n}_id",
        ),
        thresholds=Thresholds(accept=0.70, maybe=0.50, country_bonus=0.0),
        normalization=NormCfg(
            lower=True,
            strip_punct=True,
            remove_legal_suffixes=True,
            legal_suffixes=["aps", "a/s"],
        ),
        features=FeaturesCfg(
            enrichment=False,
            esg=False,
            procurement_segments=False,
        ),
    )

    out = entity_resolution(df, cfg)

    assert len(out) == 1
    status = out.loc[0, "status"]
    assert status in {"accept", "maybe"}
    score = float(out.loc[0, "score"])
    assert 0.0 <= score <= 1.0
