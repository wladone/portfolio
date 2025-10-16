import duckdb
import os
import pandas as pd
from pathlib import Path


def get_connection():
    """Get DuckDB connection"""
    # Read environment variable at runtime to ensure it's current
    db_path = os.getenv('DUCKDB_PATH', '/data/warehouse.duckdb')
    print(f"DEBUG: Connecting to database: {db_path}")
    return duckdb.connect(db_path)


def create_schema_duckdb():
    """Create schema in DuckDB"""
    conn = get_connection()
    with open('dags/sql/create_schema_duckdb.sql', 'r') as f:
        sql = f.read()
    conn.execute(sql)
    conn.close()
    print("Schema created in DuckDB")


def load_products_to_duckdb(date_str):
    """Load products CSV to DuckDB staging"""
    conn = get_connection()

    # Determine file path
    base_path = os.getenv('DATA_BASE_PATH', '.')
    gzip = os.getenv('GZIP_FILES', 'false').lower() == 'true'
    ext = '.csv.gz' if gzip else '.csv'
    file_path = Path(base_path) / 'raw' / 'products' / \
        date_str / f'products_{date_str}{ext}'

    if not file_path.exists():
        raise FileNotFoundError(f"Products file not found: {file_path}")

    # Load CSV
    df = pd.read_csv(file_path)
    conn.execute("INSERT INTO stg_products SELECT * FROM df")
    conn.close()
    print(f"Loaded {len(df)} products to staging")


def load_transactions_to_duckdb(date_str):
    """Load transactions JSON to DuckDB staging"""
    conn = get_connection()

    # Determine file path
    base_path = os.getenv('DATA_BASE_PATH', '.')
    gzip = os.getenv('GZIP_FILES', 'false').lower() == 'true'
    ext = '.json.gz' if gzip else '.json'
    file_path = Path(base_path) / 'raw' / 'transactions' / \
        date_str / f'transactions_{date_str}{ext}'

    if not file_path.exists():
        raise FileNotFoundError(f"Transactions file not found: {file_path}")

    # Load JSON Lines
    df = pd.read_json(file_path, lines=True)
    conn.execute("INSERT INTO stg_transactions SELECT * FROM df")
    conn.close()
    print(f"Loaded {len(df)} transactions to staging")
