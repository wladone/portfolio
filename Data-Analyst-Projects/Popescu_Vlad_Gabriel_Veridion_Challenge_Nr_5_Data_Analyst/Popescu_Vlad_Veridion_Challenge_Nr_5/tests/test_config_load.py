from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.veridion_poc.config import load_config


def test_load_config_with_existing_input_file():
    csv_path = Path("data/raw/presales_data_sample.csv")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.touch(exist_ok=True)

    cfg = load_config("config/config.yaml")

    assert cfg.thresholds.accept > cfg.thresholds.maybe
    assert cfg.candidates.max_candidates == 5
    assert cfg.candidate_columns(1)["name"] == "cand1_name"
