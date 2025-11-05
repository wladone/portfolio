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
from src.veridion_poc.qc import detect_duplicates


def test_detect_duplicates_two_rows_same_company():
    df = pd.DataFrame(
        [
            {"name": "ACME SRL", "country": "RO"},
            {"name": "Acme s.r.l.", "country": "RO"},
            {"name": "Other Co", "country": "RO"},
        ]
    )

    cfg = AppConfig(
        input=InputCfg(
            path=Path("dummy.csv"),
            id_column=None,
            name_column="name",
            country_column="country",
        ),
        candidates=CandCfg(
            max_candidates=1,
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
            legal_suffixes=["srl"],
        ),
        features=FeaturesCfg(
            enrichment=False,
            esg=False,
            procurement_segments=False,
        ),
    )

    duplicates = detect_duplicates(df, cfg)
    assert not duplicates.empty
    assert (duplicates["count"] == 2).any()
