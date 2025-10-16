import os
import duckdb

# Optional Snowflake import
try:
    import snowflake.connector
    from snowflake.connector import DictCursor
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False


def get_duckdb_connection():
    db_path = os.getenv('DUCKDB_PATH', '/data/warehouse.duckdb')
    return duckdb.connect(db_path)


def get_snowflake_connection():
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError(
            "Snowflake connector not available. Install snowflake-connector-python for Snowflake support.")
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'FASHION_RETAIL_WH'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'FASHION_RETAIL_DB'),
        schema=os.getenv('SNOWFLAKE_SCHEMA', 'RETAIL_SCHEMA')
    )


def validate_data_quality(backend='duckdb'):
    """Validate data quality for products_dim and sales_fact"""
    errors = []

    if backend == 'duckdb':
        conn = get_duckdb_connection()
        cursor = conn.cursor()
    elif backend == 'snowflake':
        conn = get_snowflake_connection()
        cursor = conn.cursor(DictCursor)

    try:
        # Check products_dim: no negative prices
        if backend == 'duckdb':
            result = cursor.execute(
                "SELECT COUNT(*) FROM products_dim WHERE price <= 0 OR cost <= 0").fetchone()
        else:
            cursor.execute(
                "SELECT COUNT(*) FROM products_dim WHERE price <= 0 OR cost <= 0")
            result = cursor.fetchone()
        if result[0] > 0:
            errors.append(
                f"Found {result[0]} products with non-positive prices or costs")

        # Check sales_fact: no negative amounts/quantities
        if backend == 'duckdb':
            result = cursor.execute(
                "SELECT COUNT(*) FROM sales_fact WHERE quantity <= 0 OR unit_price <= 0 OR total_amount <= 0").fetchone()
        else:
            cursor.execute(
                "SELECT COUNT(*) FROM sales_fact WHERE quantity <= 0 OR unit_price <= 0 OR total_amount <= 0")
            result = cursor.fetchone()
        if result[0] > 0:
            errors.append(
                f"Found {result[0]} sales with non-positive quantities, prices, or amounts")

        # Check for orphan sales
        if backend == 'duckdb':
            result = cursor.execute("""
                SELECT COUNT(*) FROM sales_fact sf
                LEFT JOIN products_dim pd ON sf.product_id = pd.product_id
                WHERE pd.product_id IS NULL
            """).fetchone()
        else:
            cursor.execute("""
                SELECT COUNT(*) FROM sales_fact sf
                LEFT JOIN products_dim pd ON sf.product_id = pd.product_id
                WHERE pd.product_id IS NULL
            """)
            result = cursor.fetchone()
        if result[0] > 0:
            errors.append(
                f"Found {result[0]} orphan sales (product_id not in products_dim)")

        # If errors, raise exception
        if errors:
            raise ValueError(
                "Data quality validation failed: " + "; ".join(errors))
        else:
            print("Data quality validation passed")

    finally:
        conn.close()


if __name__ == "__main__":
    backend = os.getenv('BACKEND', 'duckdb')
    validate_data_quality(backend)
