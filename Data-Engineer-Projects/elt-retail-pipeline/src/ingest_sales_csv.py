#!/usr/bin/env python
import os, shutil, sys
from src.common.config import cfg
from src.common.logger import get_logger

log = get_logger("ingest.sales")


def main():
    if len(sys.argv) < 2:
        print("Usage: ingest_sales_csv.py /path/to/vanzari.csv")
        raise SystemExit(1)
    src = sys.argv[1]
    if not os.path.exists(src):
        raise FileNotFoundError(src)
    os.makedirs(cfg.staging_dir, exist_ok=True)
    dst = os.path.join(cfg.staging_dir, "vanzari.csv")
    shutil.copy2(src, dst)
    log.info("Copied %s → %s", src, dst)


if __name__ == "__main__":
    main()

