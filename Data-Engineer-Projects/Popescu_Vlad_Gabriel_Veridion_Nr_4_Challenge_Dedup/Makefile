.PHONY: install run test clean

INPUT ?= data/raw/veridion_product_deduplication_challenge.parquet
OUT   ?= data/processed
TH    ?= 0.80

install:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

run:
	python scripts/run_dedup.py --input $(INPUT) --out-dir $(OUT) --threshold $(TH)

test:
	PYTHONPATH=src pytest -q

clean:
	rm -rf out __pycache__ .pytest_cache .venv
