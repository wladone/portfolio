"""Deterministic synthetic data generator for the analytics warehouse."""

from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import structlog
import yaml  # type: ignore
from numpy.typing import NDArray

logger = structlog.get_logger(__name__)


@dataclass
class SeedConfig:
    """Configuration for synthetic data generation."""

    rng_seed: int
    start_date: datetime
    end_date: datetime
    n_customers: int
    n_products: int
    n_orders: int
    file_split_orders: int
    file_split_customers: int
    currency_pool: list[str]
    channel_weights: dict[str, float]
    missing_email: float
    missing_phone: float
    high_quantity_rate: float
    high_discount_rate: float
    price_mu: float
    price_sigma: float

    @property
    def date_range_days(self) -> int:
        return (self.end_date - self.start_date).days + 1


def load_config(path: Path) -> SeedConfig:
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    start_date = datetime.fromisoformat(str(raw["start_date"]).strip()).replace(
        tzinfo=UTC
    )
    end_date = datetime.fromisoformat(str(raw["end_date"]).strip()).replace(tzinfo=UTC)
    if end_date < start_date:
        raise ValueError("end_date must be greater than or equal to start_date")

    weights = {str(k): float(v) for k, v in raw["channel_weights"].items()}
    total_weight = sum(weights.values())
    if total_weight <= 0:
        raise ValueError("channel_weights must contain positive values")
    normalized = {k: v / total_weight for k, v in weights.items()}

    return SeedConfig(
        rng_seed=int(raw["rng_seed"]),
        start_date=start_date,
        end_date=end_date,
        n_customers=int(raw["n_customers"]),
        n_products=int(raw["n_products"]),
        n_orders=int(raw["n_orders"]),
        file_split_orders=int(raw["file_split_orders"]),
        file_split_customers=int(raw["file_split_customers"]),
        currency_pool=[str(c) for c in raw["currency_pool"]],
        channel_weights=normalized,
        missing_email=float(raw["missingness"]["email"]),
        missing_phone=float(raw["missingness"]["phone"]),
        high_quantity_rate=float(raw["outliers"]["high_quantity_rate"]),
        high_discount_rate=float(raw["outliers"]["high_discount_rate"]),
        price_mu=float(raw["price"]["base_mu"]),
        price_sigma=float(raw["price"]["base_sigma"]),
    )


def init_rng(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)


def _isoformat_utc(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def generate_products(cfg: SeedConfig) -> list[dict[str, Any]]:
    brands = ["Orion", "Lumen", "Velox", "Nebula", "Apex", "Quantum", "Nova"]
    categories = [
        "Electronics",
        "Home",
        "Garden",
        "Fitness",
        "Fashion",
        "Books",
        "Toys",
    ]
    descriptors = ["Pro", "Plus", "Lite", "Max", "Go", "Edge", "Prime"]

    products: list[dict[str, Any]] = []
    for idx in range(cfg.n_products):
        sku = f"SKU-{idx:06X}"
        brand = random.choice(brands)
        category = random.choice(categories)
        descriptor = random.choice(descriptors)
        name = f"{brand} {descriptor}"
        price = max(1.0, float(np.random.lognormal(cfg.price_mu, cfg.price_sigma)))
        products.append(
            {
                "sku": sku,
                "name": name,
                "brand": brand,
                "category": category,
                "price_list": round(price, 2),
            }
        )
    return products


def generate_customers(cfg: SeedConfig) -> list[dict[str, Any]]:
    first_names = [
        "Alex",
        "Maria",
        "Andrei",
        "Ioana",
        "Matei",
        "Elena",
        "Luca",
        "Sofia",
    ]
    last_names = [
        "Popescu",
        "Ionescu",
        "Dumitrescu",
        "Stan",
        "Georgescu",
        "Radu",
        "Barbu",
    ]
    country_codes = ["RO", "DE", "FR", "IT", "ES", "GB", "US"]

    customers: list[dict[str, Any]] = []
    for idx in range(cfg.n_customers):
        nk = f"CUST-{idx:06X}"
        first = random.choice(first_names)
        last = random.choice(last_names)
        email: str | None = f"{first.lower()}.{last.lower()}{idx}@seed.local"
        if random.random() < cfg.missing_email:
            email = None
        phone: str | None = f"+40{random.randint(700000000, 799999999)}"
        if random.random() < cfg.missing_phone:
            phone = None
        customers.append(
            {
                "customer_nk": nk,
                "email": email,
                "first_name": first,
                "last_name": last,
                "phone": phone,
                "country_code": random.choice(country_codes),
            }
        )
    return customers


def _zipf_indices(n: int, size: int, exponent: float = 1.2) -> NDArray[np.int_]:
    ranks = np.arange(1, n + 1)
    weights = 1 / np.power(ranks, exponent)
    probabilities = weights / weights.sum()
    return np.random.choice(n, size=size, p=probabilities)


def _choose_channel(weights: dict[str, float]) -> str:
    channels = list(weights.keys())
    probs = list(weights.values())
    return random.choices(channels, weights=probs, k=1)[0]


def generate_orders(
    cfg: SeedConfig,
    customers: list[dict[str, Any]],
    products: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    customer_ids = [c["customer_nk"] for c in customers]
    customer_email = {c["customer_nk"]: c["email"] for c in customers}
    product_meta = {p["sku"]: p for p in products}
    product_skus = list(product_meta.keys())

    product_indices = _zipf_indices(len(products), cfg.n_orders)

    orders: list[dict[str, Any]] = []
    order_line_counts: dict[str, int] = {}
    channel_counter: Counter[str] = Counter()
    currency_counter: Counter[str] = Counter()

    current_order_id: str | None = None

    for idx in range(cfg.n_orders):
        customer_nk = random.choice(customer_ids)
        sku = product_skus[product_indices[idx]]
        product = product_meta[sku]

        base_date = cfg.start_date + timedelta(
            days=random.randint(0, cfg.date_range_days - 1)
        )
        seconds = random.randint(0, 86_399)
        txn_ts = base_date + timedelta(seconds=seconds)
        if txn_ts.weekday() >= 5 and random.random() < 0.2:
            txn_ts += timedelta(hours=random.randint(6, 18))

        new_order = current_order_id is None or random.random() < 0.7
        if new_order:
            current_order_id = f"ORD-{cfg.rng_seed % 10_000:04d}-{idx:06d}"

        assert current_order_id is not None

        channel_code = _choose_channel(cfg.channel_weights)
        currency = random.choice(cfg.currency_pool)

        quantity = int(max(1, np.random.poisson(1) + 1))
        if random.random() < cfg.high_quantity_rate:
            quantity = random.randint(6, 20)

        list_price = product["price_list"]
        unit_price = max(
            0.5, list_price + float(np.random.normal(0, 0.05 * list_price))
        )
        unit_price = round(unit_price, 2)

        max_discount = quantity * unit_price
        if random.random() < cfg.high_discount_rate:
            discount = random.uniform(0.2, 0.4) * max_discount
        else:
            discount = random.uniform(0.0, 0.15) * max_discount
        discount = round(min(max_discount, discount), 2)

        order_line_counts[current_order_id] = (
            order_line_counts.get(current_order_id, 0) + 1
        )
        order_line_nbr = order_line_counts[current_order_id]

        orders.append(
            {
                "order_id": current_order_id,
                "order_line_nbr": order_line_nbr,
                "customer_nk": customer_nk,
                "email": customer_email.get(customer_nk),
                "sku": sku,
                "product_name": product["name"],
                "brand": product["brand"],
                "category": product["category"],
                "quantity": quantity,
                "unit_price": float(unit_price),
                "discount_amount": float(discount),
                "currency_code": currency,
                "channel_code": channel_code,
                "transaction_ts": _isoformat_utc(txn_ts),
            }
        )

        channel_counter[channel_code] += 1
        currency_counter[currency] += 1

    logger.info(
        "seed_order_stats",
        channels=dict(channel_counter),
        currencies=dict(currency_counter),
        avg_quantity=float(np.mean([o["quantity"] for o in orders])) if orders else 0.0,
    )
    return orders


def write_products(path: Path, products: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = "sku,name,brand,category,price_list\n"
    with path.open("w", encoding="utf-8") as fh:
        fh.write(header)
        for product in products:
            fh.write(
                f"{product['sku']},{product['name']},{product['brand']},{product['category']},{product['price_list']:.2f}\n"
            )


def write_json_records(
    base: Path, prefix: str, records: Sequence[dict[str, Any]], chunk_size: int
) -> None:
    base.mkdir(parents=True, exist_ok=True)
    total = len(records)
    for start in range(0, total, chunk_size):
        chunk = records[start : start + chunk_size]
        index = start // chunk_size + 1
        file_path = base / f"{prefix}_{index:04d}.json"
        file_path.write_text(
            json.dumps(chunk, indent=2, ensure_ascii=False), encoding="utf-8"
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate deterministic synthetic seed data."
    )
    parser.add_argument(
        "--config", required=True, type=Path, help="Path to seed_config.yaml."
    )
    parser.add_argument(
        "--out", required=True, type=Path, help="Output directory for generated files."
    )
    return parser.parse_args(argv)


def generate_dataset(cfg: SeedConfig, out_dir: Path) -> None:
    init_rng(cfg.rng_seed)
    out_dir.mkdir(parents=True, exist_ok=True)

    products = generate_products(cfg)
    customers = generate_customers(cfg)
    orders = generate_orders(cfg, customers, products)

    write_products(out_dir / "products.csv", products)
    write_json_records(out_dir, "customers", customers, cfg.file_split_customers)
    write_json_records(out_dir, "orders", orders, cfg.file_split_orders)

    logger.info(
        "seed_generation_complete",
        products=len(products),
        customers=len(customers),
        orders=len(orders),
        output=str(out_dir),
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    cfg = load_config(args.config)
    generate_dataset(cfg, args.out)


if __name__ == "__main__":
    main()
