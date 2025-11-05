from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.veridion_poc.normalize import normalize_name


def test_normalize_ericsson_as():
    result = normalize_name(
        "L.M. Ericsson A/S",
        lower=True,
        strip_punct=True,
        remove_legal_suffixes=True,
        legal_suffixes=["a/s"],
    )
    assert result == "l m ericsson"
