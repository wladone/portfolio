
.PHONY: setup elasticity forecast zip

setup:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt || (.\.venv\Scripts\activate && pip install -r requirements.txt)

elasticity:
	python project/scripts/elasticity_pricing.py

forecast:
	python project/scripts/monthly_forecast.py

zip:
	python - <<'PY'
import shutil; shutil.make_archive('retail_sales_analytics_repo', 'zip', '.')
PY


release-zip:
	@VERSION?=$(shell date +%Y%m%d)
	zip -r retail_sales_analytics_$(VERSION).zip . -x "*.git*"
