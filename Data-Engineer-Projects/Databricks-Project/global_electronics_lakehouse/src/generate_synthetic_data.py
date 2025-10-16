"""Synthetic data generator for the Global Electronics Lakehouse project."""
from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import random
import string
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


WAREHOUSES = ["FRA", "BUC", "MAD"]
CATEGORIES = {
    "Televisions": 0.12,
    "Smartphones": 0.18,
    "Laptops": 0.16,
    "Audio": 0.10,
    "Appliances": 0.14,
    "Accessories": 0.12,
    "Gaming": 0.08,
    "Networking": 0.10,
}
CHANNELS = ["online", "retail", "call_center", "mobile_app"]
PAYMENTS = ["card", "paypal", "bank_transfer", "cash"]
DEVICES = ["desktop", "mobile", "tablet"]
PROMO_CODES = ["SPRING10", "FREESHIP", "LOYALTY", "FLASH"]
COUNTRIES = ["Germany", "France", "Romania", "Spain", "Italy", "Netherlands"]


@dataclass
class Product:
    product_id: str
    product_name: str
    category: str
    base_price: float
    unit_cost: float
    supplier_id: str
    restock_threshold: int
    restock_lead_days: int


def random_string(prefix: str, length: int = 6) -> str:
    suffix = "".join(random.choices(
        string.ascii_uppercase + string.digits, k=length))
    return f"{prefix}{suffix}"


def get_pattern_multiplier(event_time: datetime, enable_seasonal: bool, enable_trending: bool, enable_cyclical: bool, enable_holidays: bool, start_date: datetime) -> float:
    mult = 1.0
    if enable_seasonal:
        month = event_time.month
        mult *= 1 + 0.3 * math.sin(2 * math.pi * (month - 1) / 12)
    if enable_trending:
        days_since_start = (event_time - start_date).days
        mult *= 1 + 0.005 * days_since_start
    if enable_cyclical:
        if event_time.weekday() >= 5:
            mult *= 1.5
    if enable_holidays:
        if (event_time.month == 12 and event_time.day in [24, 25, 31]) or (event_time.month == 1 and event_time.day == 1):
            mult *= 2.0
    return mult


def build_suppliers(count: int) -> list[dict]:
    suppliers: list[dict] = []
    for idx in range(1, count + 1):
        supplier_id = f"SUP{idx:03d}"
        categories = random.sample(
            list(CATEGORIES.keys()), k=random.randint(2, 4))
        start_date = datetime.utcnow() - timedelta(days=random.randint(180, 720))
        end_date = start_date + timedelta(days=random.randint(365, 1095))
        suppliers.append(
            {
                "supplier_id": supplier_id,
                "supplier_name": f"{random.choice(['Nova', 'Prime', 'Vertex', 'Axiom', 'Helios', 'Aurora'])} Electronics {idx}",
                "rating": round(random.uniform(3.2, 4.9), 2),
                "lead_time_days": random.randint(5, 20),
                "preferred": random.random() < 0.55,
                "contact_email": f"contact+{supplier_id.lower()}@globalelectro.example",
                "phone": f"+33{random.randint(100000000, 999999999)}",
                "country": random.choice(COUNTRIES),
                "category_focus": categories,
                "contract_start": start_date.strftime("%Y-%m-%d"),
                "contract_end": end_date.strftime("%Y-%m-%d"),
                "annual_capacity_units": random.randint(10000, 50000),
            }
        )
    return suppliers


def build_products(count: int, suppliers: list[dict]) -> list[Product]:
    weights = list(CATEGORIES.values())
    categories = list(CATEGORIES.keys())
    products: list[Product] = []
    for idx in range(1, count + 1):
        category = random.choices(categories, weights=weights, k=1)[0]
        base_price = round(random.uniform(30.0, 2500.0), 2)
        unit_cost = round(base_price * random.uniform(0.55, 0.82), 2)
        supplier = random.choice(suppliers)
        restock_threshold = random.randint(10, 60)
        restock_lead_days = random.randint(3, 14)
        products.append(
            Product(
                product_id=f"PRD{idx:05d}",
                product_name=f"{category.replace(' ', '')}-{idx:05d}",
                category=category,
                base_price=base_price,
                unit_cost=unit_cost,
                supplier_id=supplier["supplier_id"],
                restock_threshold=restock_threshold,
                restock_lead_days=restock_lead_days,
            )
        )
    return products


def write_suppliers(suppliers: list[dict], path: Path, logger) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for record in suppliers:
                handle.write(json.dumps(record))
                handle.write("\n")
        logger.info(f"Suppliers written: {len(suppliers)} -> {path}")
    except Exception as e:
        logger.error(f"Failed to write suppliers to {path}: {e}")
        raise


def write_inventory(products: list[Product], days: int, base_dir: Path, logger) -> None:
    try:
        today = datetime.utcnow().date()
        total_rows = 0
        for offset in range(days):
            current_date = today - timedelta(days=offset)
            day_dir = base_dir / current_date.strftime("%Y-%m-%d")
            day_dir.mkdir(parents=True, exist_ok=True)
            for warehouse in WAREHOUSES:
                file_path = day_dir / f"{warehouse}_inventory.csv"
                with file_path.open("w", encoding="utf-8", newline="") as handle:
                    writer = csv.writer(handle)
                    writer.writerow(
                        [
                            "inventory_date",
                            "warehouse_code",
                            "product_id",
                            "product_name",
                            "category",
                            "quantity_on_hand",
                            "unit_cost_euro",
                            "supplier_id",
                            "restock_threshold",
                            "restock_lead_days",
                            "is_active",
                            "last_updated_ts",
                            "last_sale_ts",
                            "lifetime_revenue_euro",
                        ]
                    )
                    for product in products:
                        quantity = max(0, int(random.gauss(120, 35)))
                        last_updated = datetime.combine(current_date, datetime.min.time(
                        )) + timedelta(hours=random.randint(0, 22), minutes=random.randint(0, 59))
                        last_sale = last_updated - \
                            timedelta(hours=random.randint(1, 48))
                        lifetime_revenue = round(
                            quantity * product.base_price * random.uniform(15, 65), 2)
                        writer.writerow(
                            [
                                current_date.isoformat(),
                                warehouse,
                                product.product_id,
                                product.product_name,
                                product.category,
                                quantity,
                                product.unit_cost,
                                product.supplier_id,
                                product.restock_threshold,
                                product.restock_lead_days,
                                True,
                                last_updated.isoformat(),
                                last_sale.isoformat(),
                                lifetime_revenue,
                            ]
                        )
                        total_rows += 1
        logger.info(f"Inventory rows written: {total_rows}")
    except Exception as e:
        logger.error(f"Failed to write inventory: {e}")
        raise


def build_sales_events(
    products: list[Product],
    sales_files: int,
    events_per_file: int,
    invalid_ratio: float,
    sales_days: int = 1,
    enable_seasonal: bool = False,
    enable_trending: bool = False,
    enable_cyclical: bool = False,
    enable_holidays: bool = False,
) -> list[list[dict]]:
    batches: list[list[dict]] = []
    all_events: list[dict] = []
    now = datetime.utcnow()
    start_date = now - timedelta(days=sales_days)
    for _ in range(sales_files * events_per_file):
        seconds_range = int((now - start_date).total_seconds())
        random_seconds = random.randint(0, seconds_range)
        event_time = start_date + timedelta(seconds=random_seconds)
        mult = get_pattern_multiplier(
            event_time, enable_seasonal, enable_trending, enable_cyclical, enable_holidays, start_date)
        if random.random() < invalid_ratio:
            product = random.choice(products)
            product_id = product.product_id + "_INVALID"
        else:
            product = random.choice(products)
            product_id = product.product_id
        order_id = random_string("ORD", 8)
        quantity = max(1, int(random.gauss(3, 1.2) * mult))
        discount = round(random.choice(
            [0.0, 0.05, 0.10, 0.15]) if random.random() < 0.35 else 0.0, 2)
        channel = random.choice(CHANNELS)
        payment = random.choice(PAYMENTS)
        warehouse = random.choice(WAREHOUSES)
        event = {
            "event_id": random_string("EVT", 10),
            "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "order_id": order_id,
            "product_id": product_id,
            "warehouse_code": warehouse,
            "channel": channel,
            "payment_type": payment,
            "customer_id": random_string("CUST", 6),
            "quantity": quantity,
            "unit_price_euro": round(product.base_price * random.uniform(0.9, 1.1), 2),
            "discount_rate": discount,
            "currency": "EUR",
            "sales_channel_region": random.choice(["EU-West", "EU-Central", "EU-South"]),
            "promotion_code": random.choice(PROMO_CODES) if discount > 0 else None,
            "is_priority_order": random.random() < 0.08,
            "device_type": random.choice(DEVICES),
        }
        all_events.append(event)
    for idx in range(sales_files):
        start = idx * events_per_file
        end = start + events_per_file
        batches.append(all_events[start:end])
    return batches


def write_sales(batches: list[list[dict]], base_dir: Path, logger) -> None:
    try:
        base_dir.mkdir(parents=True, exist_ok=True)
        for idx, events in enumerate(batches, start=1):
            file_path = base_dir / f"sales_{idx:03d}.json"
            with file_path.open("w", encoding="utf-8") as handle:
                for event in events:
                    handle.write(json.dumps(event))
                    handle.write("\n")
        logger.info(f"Sales files written: {len(batches)} -> {base_dir}")
    except Exception as e:
        logger.error(f"Failed to write sales: {e}")
        raise


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate synthetic data for the Global Electronics Lakehouse project.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--products", type=int, default=2500,
                        help="Number of products to generate")
    parser.add_argument("--days", type=int, default=7,
                        help="Number of days of inventory history")
    parser.add_argument("--suppliers", type=int, default=30,
                        help="Number of suppliers")
    parser.add_argument("--sales-files", type=int, default=10,
                        help="Number of sales JSON files")
    parser.add_argument("--events-per-file", type=int,
                        default=500, help="Events per sales file")
    parser.add_argument("--invalid-ratio", type=float, default=0.01,
                        help="Fraction of sales with invalid product_id")
    parser.add_argument("--sales-days", type=int, default=1,
                        help="Number of days for sales history")
    parser.add_argument("--enable-seasonal", action="store_true",
                        help="Enable seasonal variations in sales")
    parser.add_argument("--enable-trending", action="store_true",
                        help="Enable growth trends in sales")
    parser.add_argument("--enable-cyclical", action="store_true",
                        help="Enable cyclical patterns (weekday/weekend) in sales")
    parser.add_argument("--enable-holidays", action="store_true",
                        help="Enable holiday spikes in sales")
    parser.add_argument("--output", default=str(
        Path(__file__).resolve().parents[1] / "data"), help="Output data directory")
    parser.add_argument("--log-file", default=str(Path(__file__).resolve(
    ).parents[1] / "_logs" / "generate_data.log"), help="Log file path")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=args.log_file)
    logger = logging.getLogger(__name__)

    try:
        # Input validation
        if args.seed < 0:
            raise ValueError("Seed must be non-negative")
        if args.products <= 0 or args.days <= 0 or args.suppliers <= 0 or args.sales_files <= 0 or args.events_per_file <= 0:
            raise ValueError("All counts must be positive")
        if not (0 <= args.invalid_ratio <= 1):
            raise ValueError("Invalid ratio must be between 0 and 1")

        random.seed(args.seed)

        if args.enable_seasonal or args.enable_trending or args.enable_cyclical or args.enable_holidays:
            args.sales_days = max(args.sales_days, 30)

        output_dir = Path(args.output)
        inventory_dir = output_dir / "inventory"
        suppliers_dir = output_dir / "suppliers"
        sales_dir = output_dir / "sales_stream"

        suppliers_dir.mkdir(parents=True, exist_ok=True)

        suppliers = build_suppliers(max(20, min(args.suppliers, 50)))
        products = build_products(
            max(1000, min(args.products, 5000)), suppliers)

        write_suppliers(suppliers, suppliers_dir /
                        "suppliers_master.jsonl", logger)
        write_inventory(products, args.days, inventory_dir, logger)
        sales_batches = build_sales_events(
            products, args.sales_files, args.events_per_file, args.invalid_ratio, args.sales_days, args.enable_seasonal, args.enable_trending, args.enable_cyclical, args.enable_holidays)
        write_sales(sales_batches, sales_dir, logger)

        return 0
    except Exception as e:
        logger.error(f"Data generation failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
