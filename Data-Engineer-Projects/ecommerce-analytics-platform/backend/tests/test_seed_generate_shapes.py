"""Validation for seed data generator shapes."""

from __future__ import annotations

import json
from pathlib import Path

from infra.seed.generate_seed import generate_dataset, load_config

SMALL_CONFIG_TEMPLATE = """rng_seed: 101
start_date: 2024-01-01
end_date: 2024-01-05
n_customers: 50
n_products: 20
n_orders: 100
file_split_orders: 50
file_split_customers: 50
currency_pool: ["USD", "EUR"]
channel_weights:
  web: 0.5
  mobile: 0.3
  store: 0.2
missingness:
  email: 0.05
  phone: 0.1
outliers:
  high_quantity_rate: 0.02
  high_discount_rate: 0.03
price:
  base_mu: 3.0
  base_sigma: 0.4
"""


def test_seed_generator_creates_expected_files(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(SMALL_CONFIG_TEMPLATE, encoding="utf-8")
    output_dir = tmp_path / "output"

    cfg = load_config(config_path)
    generate_dataset(cfg, output_dir)

    products_path = output_dir / "products.csv"
    assert products_path.exists()
    lines = products_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == cfg.n_products + 1  # header + rows

    customers_files = sorted(output_dir.glob("customers_*.json"))
    orders_files = sorted(output_dir.glob("orders_*.json"))
    assert len(customers_files) == 1
    assert len(orders_files) == 2

    # Validate JSON structure
    customers = json.loads(customers_files[0].read_text(encoding="utf-8"))
    assert len(customers) == cfg.n_customers
    required_customer_keys = {
        "customer_nk",
        "email",
        "first_name",
        "last_name",
        "phone",
        "country_code",
    }
    assert required_customer_keys <= customers[0].keys()

    orders_sample = json.loads(orders_files[0].read_text(encoding="utf-8"))
    assert orders_sample
    required_order_keys = {
        "order_id",
        "order_line_nbr",
        "customer_nk",
        "sku",
        "quantity",
        "unit_price",
        "discount_amount",
        "currency_code",
        "channel_code",
        "transaction_ts",
    }
    assert required_order_keys <= orders_sample[0].keys()
