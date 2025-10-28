# Seed Data Generation

This directory contains deterministic generators for synthetic e-commerce datasets used to bootstrap the local analytics warehouse.

## Usage

1. Adjust `seed_config.yaml` to control counts and date ranges.
2. Run `make seed` (or individual targets) to generate files, seed `dw.dim_date`, and load data via ETL.
3. Generated files are placed in `infra/seed/data/` (gitignored).

## Files
- `seed_config.yaml`: deterministic configuration for RNG seed, counts, rates.
- `generate_seed.py`: emits synthetic products, customers, and orders based on configuration.
- `seed_dim_date.py`: populates `dw.dim_date` for configurable ranges.
- `data/`: output folder populated by the scripts.

## Determinism
Using the same configuration and RNG seed produces identical output files. Set different `rng_seed` values to create alternative datasets.
