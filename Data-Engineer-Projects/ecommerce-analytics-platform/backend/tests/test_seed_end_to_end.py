"""End-to-end validation of seed generation and ETL loading."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import text

from etl.db import session_scope
from etl.loaders import load_customers_to_stg, load_orders_to_stg, load_products_to_stg
from infra.seed.generate_seed import generate_dataset, load_config
from infra.seed.seed_dim_date import seed_dim_date

SMALL_CONFIG_TEMPLATE = """rng_seed: 303
start_date: 2024-02-01
end_date: 2024-02-10
n_customers: 120
n_products: 40
n_orders: 200
file_split_orders: 100
file_split_customers: 60
currency_pool: ["USD", "EUR"]
channel_weights:
  web: 0.6
  mobile: 0.3
  store: 0.1
missingness:
  email: 0.02
  phone: 0.05
outliers:
  high_quantity_rate: 0.02
  high_discount_rate: 0.03
price:
  base_mu: 3.2
  base_sigma: 0.5
"""


@pytest.mark.usefixtures("db_engine")
def test_seed_end_to_end(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    output_dir = tmp_path / "output"
    config_path.write_text(SMALL_CONFIG_TEMPLATE, encoding="utf-8")

    cfg = load_config(config_path)
    generate_dataset(cfg, output_dir)

    # Reset warehouse tables used in this scenario
    with session_scope() as session:
        session.execute(text("DELETE FROM dw.fact_sales"))
        session.execute(text("DELETE FROM stg.orders_raw"))
        session.execute(text("DELETE FROM stg.customers_raw"))
        session.execute(text("DELETE FROM stg.products_raw"))
        session.execute(text("DELETE FROM dw.dim_product"))
        session.execute(text("DELETE FROM dw.dim_customer"))
        session.execute(
            text("DELETE FROM dw.dim_date WHERE date BETWEEN :start AND :end"),
            {"start": date(2024, 1, 1), "end": date(2024, 12, 31)},
        )

    seed_dim_date(date(2024, 1, 1), date(2024, 12, 31), chunk_size=128)

    products_path = str(output_dir / "products.csv")
    customers_glob = str(output_dir / "customers_*.json")
    orders_glob = str(output_dir / "orders_*.json")

    load_products_to_stg(products_path, chunk_size=200, limit=None, dry_run=False)
    load_customers_to_stg(customers_glob, chunk_size=200, limit=None, dry_run=False)
    load_orders_to_stg(
        orders_glob, chunk_size=200, limit=None, dry_run=False, ensure_dim_date=True
    )

    with session_scope() as session:
        fact_count = session.execute(
            text("SELECT COUNT(*) FROM dw.fact_sales")
        ).scalar_one()
        missing_customers = session.execute(
            text(
                "SELECT COUNT(*) FROM dw.fact_sales fs LEFT JOIN dw.dim_customer dc ON fs.customer_id = dc.customer_id WHERE dc.customer_id IS NULL"
            )
        ).scalar_one()
        missing_products = session.execute(
            text(
                "SELECT COUNT(*) FROM dw.fact_sales fs LEFT JOIN dw.dim_product dp ON fs.product_id = dp.product_id WHERE dp.product_id IS NULL"
            )
        ).scalar_one()
        missing_channels = session.execute(
            text(
                "SELECT COUNT(*) FROM dw.fact_sales fs LEFT JOIN dw.dim_channel dc ON fs.channel_id = dc.channel_id WHERE dc.channel_id IS NULL"
            )
        ).scalar_one()

    assert fact_count > 0
    assert missing_customers == 0
    assert missing_products == 0
    assert missing_channels == 0

    # Clean up after test
    with session_scope() as session:
        session.execute(text("DELETE FROM dw.fact_sales"))
        session.execute(text("DELETE FROM stg.orders_raw"))
        session.execute(text("DELETE FROM stg.customers_raw"))
        session.execute(text("DELETE FROM stg.products_raw"))
        session.execute(text("DELETE FROM dw.dim_product"))
        session.execute(text("DELETE FROM dw.dim_customer"))
        session.execute(
            text("DELETE FROM dw.dim_date WHERE date BETWEEN :start AND :end"),
            {"start": date(2024, 1, 1), "end": date(2024, 12, 31)},
        )
