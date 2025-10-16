#!/usr/bin/env python
import os, json
import requests
from src.common.config import cfg
from src.common.logger import get_logger

log = get_logger("extract.products")
API_URL = "https://fakestoreapi.com/products"


def main() -> None:
    os.makedirs(cfg.staging_dir, exist_ok=True)
    out_path = os.path.join(cfg.staging_dir, "produse.json")

    log.info(f"Requesting products from {API_URL}")
    r = requests.get(API_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or not data:
        raise ValueError("Unexpected API payload (expected non-empty list).")

    keep = []
    for item in data:
        if all(k in item for k in ("id","title","price","category")):
            keep.append({
                "id": item["id"],
                "title": str(item["title"]),
                "price": float(item["price"]),
                "category": str(item["category"])
            })
    if not keep:
        raise RuntimeError("No valid product records after validation.")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(keep, f, ensure_ascii=False, indent=2)

    log.info(f"Wrote {len(keep)} products → {out_path}")


if __name__ == "__main__":
    main()

