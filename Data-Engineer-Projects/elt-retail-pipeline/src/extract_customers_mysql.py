#!/usr/bin/env python
import os, csv
import mysql.connector
from mysql.connector import Error
from src.common.config import cfg
from src.common.logger import get_logger

log = get_logger("extract.customers")
QUERY = "SELECT * FROM clienti"


def main() -> None:
    os.makedirs(cfg.staging_dir, exist_ok=True)
    out_path = os.path.join(cfg.staging_dir, "clienti.csv")
    log.info("Connecting to MySQL %s:%s/%s", cfg.mysql_host, cfg.mysql_port, cfg.mysql_db)

    try:
        conn = mysql.connector.connect(
            host=cfg.mysql_host,
            port=cfg.mysql_port,
            user=cfg.mysql_user,
            password=cfg.mysql_password,
            database=cfg.mysql_db
        )
        cur = conn.cursor(dictionary=True)
        cur.execute(QUERY)
        rows = cur.fetchall()
    except Error as e:
        log.error("MySQL error: %s", e)
        raise
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass

    if not rows:
        log.warning("No rows returned from clienti table.")
        rows = []

    fields = sorted(rows[0].keys()) if rows else ["ID_client","Nume","Email","Telefon","Oras"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    log.info("Wrote %d clienti → %s", len(rows), out_path)


if __name__ == "__main__":
    main()

