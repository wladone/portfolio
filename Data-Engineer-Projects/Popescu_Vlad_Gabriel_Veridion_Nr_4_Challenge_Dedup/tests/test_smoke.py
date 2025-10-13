"""Smoke-test the deduplication pipeline on a tiny handcrafted dataset."""

from pipeline import deduplicate
import pandas as pd
import sys
import os

# Import din src/ când rulăm pytest din rădăcină
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))


def test_smoke_clusters():
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
    out = deduplicate(toy, threshold=0.80)
    assert out["cluster_size"].sum() == len(toy)
    assert (out["cluster_size"] > 1).any()

