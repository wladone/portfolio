# regatta-to-powerbi

`regatta-to-powerbi` converts race results maintained in Excel into tidy, Power BI ready CSV dimension and fact tables with the data quality signals analysts expect. The CLI can scan an input folder for a workbook, standardises text, coerces numeric values, parses elapsed and corrected times, builds surrogate keys, and writes a dataset you can load into Power BI with relationships and measures that just work.

An empty `input/` directory is provided for dropping Excel files, and `powerbi_dataset/` is the default output location (existing files are overwritten when you run the tool).

## Quick start

```
python -m venv .venv
# Windows
.venv\\Scripts\\activate
# macOS/Linux
source .venv/bin/activate
pip install -U pip
pip install -e .
regatta-to-powerbi --in-dir input --workbook-name "Technical Data Quality Analyst exercise[1][2][8][90].xlsx" --out-dir powerbi_dataset
```

### One-step bootstrap (creates venv + runs CLI)

```
python run_regatta_to_powerbi.py --in-dir input --workbook-name "Technical Data Quality Analyst exercise[1][2][8][90].xlsx" --out-dir powerbi_dataset
```

The first run creates/installs into `.venv`. Later runs detect the ready marker automatically; add `--force-install` if you want to refresh dependencies, or `--skip-install` to explicitly bypass the install step.

To point to a workbook outside the `input/` folder, add `--xlsx "path/to.xlsx"`. You can also change the validation report name with `--report custom-report.txt`.

## Power BI model

- `fact_results[Sail Number]` -> `dim_boat[Sail Number]` (many-to-one)
- `fact_results[RaceKey]` -> `dim_race[RaceKey]` (many-to-one)
- `fact_results[Division]` -> `dim_division[Division]` (many-to-one)

## Suggested DAX measures

```DAX
Boats = DISTINCTCOUNT(fact_results[Sail Number])
Races = DISTINCTCOUNT(fact_results[Race Number])
Regattas = DISTINCTCOUNT(fact_results[Regatta Number])
Winners = CALCULATE(COUNTROWS(fact_results), fact_results[Rank] = 1)
Avg Points = AVERAGE(fact_results[Points])
Median Points = MEDIAN(fact_results[Points])
Avg Corrected (sec) = AVERAGE(fact_results[Corrected_sec])
% Missing Corrected = DIVIDE(CALCULATE(COUNTROWS(fact_results), fact_results[IsCorrectedMissing] = TRUE()), COUNTROWS(fact_results))
```

## Project layout

```
regatta-to-powerbi/
  README.md
  LICENSE
  requirements.txt
  pyproject.toml
  .gitignore
  input/
  powerbi_dataset/
  run_regatta_to_powerbi.py
  src/regatta_to_powerbi/__init__.py
  src/regatta_to_powerbi/cli.py
  src/regatta_to_powerbi/transform.py
  tests/test_transform.py
```

## Development notes

- Python 3.10+
- Uses pandas, numpy, openpyxl, typer, rich, pytest
- Run `pytest` from an activated virtual environment to exercise the data shaping logic
