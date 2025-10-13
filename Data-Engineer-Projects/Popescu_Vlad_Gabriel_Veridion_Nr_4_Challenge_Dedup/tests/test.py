"""Ad-hoc entry point for manually exercising the dedup pipeline on toy data or a file."""

from pipeline import deduplicate, assign_clusters
import os
import sys
import pandas as pd

# Import din src/
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(ROOT, "src"))


def run_toy_test():
    toy = pd.DataFrame([
        {"id": 1, "name": "Apple iPhone 13 Pro Max 128GB Graphite", "brand": "Apple",
            "model": "iPhone 13 Pro Max", "gtin": "0194252567834", "price": 1099},
        {"id": 2, "title": "APPLE iPhone 13 Pro Max - 128 GB, Grey", "brand": "apple",
            "model": "iPhone 13 Pro Max", "ean": "194252567834", "price": 1085},
        {"id": 3, "name": "Apple iPhone 13 Pro 128GB", "brand": "Apple",
            "model": "iPhone 13 Pro", "gtin": "0194252567835", "price": 999},
        {"id": 4, "title": "Samsung Galaxy S21 FE 5G (128GB)", "brand": "Samsung",
         "model": "S21 FE", "upc": "887276570123", "price": 599},
        {"id": 5, "name": "Galaxy S21 Fan Edition 5G 128 GB", "brand": "SAMSUNG",
            "model": "S21 FE", "upc": "887276570123", "price": 579},
    ])
    dedup = deduplicate(toy, threshold=0.80)
    mapping = assign_clusters(toy, dedup)

    print("=== DEDUP RESULT ===")
    print(dedup[['product_uid', 'name', 'title',
          'brand', 'model', 'cluster_size']])
    print("\n=== ROW → PRODUCT_UID ===")
    print(mapping.sort_values('row_index').reset_index(drop=True))

    assert dedup["cluster_size"].sum() == len(
        toy), "Cluster sizes should sum to original row count"
    assert (dedup["cluster_size"] > 1).any(
    ), "Should have at least one multi-row cluster"
    print("\n[OK] Toy test passed.")


def run_on_file(input_path: str):
    """Rulează pe un fișier local CSV/Parquet dacă îi dai calea ca argument."""
    ext = os.path.splitext(input_path)[1].lower()
    if ext in {".parquet", ".pq", ".snappy"}:
        df = pd.read_parquet(input_path)
    else:
        df = pd.read_csv(input_path)

    dedup = deduplicate(df, threshold=0.80)
    print("=== DEDUP SHAPE ===", dedup.shape)
    print(dedup.head(10))


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Quick test runner for product dedup")
    ap.add_argument("input", nargs="?", help="optional CSV/Parquet input file")
    args = ap.parse_args()

    if args.input:
        run_on_file(args.input)
    else:
        run_toy_test()

