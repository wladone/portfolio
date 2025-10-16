#!/usr/bin/env python3
"""
Local Demo Runner for Fashion Retail ETL Pipeline
Runs the complete ETL pipeline without Docker using local files and DuckDB.
"""

from scripts.validate_data_quality import validate_data_quality
from scripts.load_to_duckdb import create_schema_duckdb, load_products_to_duckdb, load_transactions_to_duckdb
import os
import sys
import duckdb
from datetime import datetime

# Add current directory to path for imports
sys.path.append('.')


def setup_environment():
    """Setup environment variables for local demo"""
    # Use absolute paths relative to current working directory
    current_dir = os.getcwd()
    data_dir = os.path.join(current_dir, 'data')
    db_path = os.path.join(data_dir, 'warehouse.duckdb')

    os.environ['DUCKDB_PATH'] = db_path
    os.environ['DATA_BASE_PATH'] = data_dir
    os.environ['BACKEND'] = 'duckdb'

    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    print(f"Data directory: {data_dir}")
    print(f"Database path: {db_path}")
    print(f"DUCKDB_PATH env var: {os.environ.get('DUCKDB_PATH')}")


def run_local_pipeline(date_str=None):
    """Run the complete ETL pipeline locally"""
    if date_str is None:
        date_str = datetime.now().strftime('%Y%m%d')

    print("[RUNNING] Starting Local Fashion Retail ETL Pipeline")
    print(f"[DATE] Processing date: {date_str}")
    print("=" * 50)

    try:
        # 1. Create schema
        print("[STEP] Step 1: Creating database schema...")
        create_schema_duckdb()
        print("Schema created successfully")

        # 2. Load products
        print("[PRODUCTS] Step 2: Loading products...")
        load_products_to_duckdb(date_str)
        print("Products loaded successfully")

        # 3. Load transactions
        print("[TRANSACTIONS] Step 3: Loading transactions...")
        load_transactions_to_duckdb(date_str)
        print("Transactions loaded successfully")

        # 4. Transform data
        print("[TRANSFORM] Step 4: Transforming data...")
        conn = duckdb.connect('data/warehouse.duckdb')

        # Transform products
        print("   - Transforming products...")
        with open('dags/sql/transform_products.sql', 'r') as f:
            conn.execute(f.read())

        # Transform transactions
        print("   - Transforming transactions...")
        with open('dags/sql/transform_transactions.sql', 'r') as f:
            conn.execute(f.read())

        # Generate recommendations
        print("   - Generating product recommendations...")
        conn.execute("""
            CREATE TEMP TABLE temp_ranked AS
            SELECT
                region,
                product_id,
                total_quantity,
                DENSE_RANK() OVER (PARTITION BY region ORDER BY total_quantity DESC) as rank
            FROM (
                SELECT
                    region,
                    product_id,
                    SUM(quantity) as total_quantity
                FROM sales_fact
                GROUP BY region, product_id
            ) t;

            INSERT INTO product_recommendations (region, product_id, total_quantity, rank)
            SELECT region, product_id, total_quantity, rank
            FROM temp_ranked
            WHERE rank <= 10;

            DROP TABLE temp_ranked;
        """)

        conn.close()
        print("Data transformation completed")

        # 5. Validate data quality
        print("[VALIDATE] Step 5: Validating data quality...")
        validate_data_quality('duckdb')
        print("Data quality validation passed")

        # 6. Show summary
        print("\n" + "=" * 50)
        print("[STATS] Pipeline Summary:")
        conn = duckdb.connect('data/warehouse.duckdb')

        products_count = conn.execute(
            "SELECT COUNT(*) FROM products_dim").fetchone()[0]
        sales_count = conn.execute(
            "SELECT COUNT(*) FROM sales_fact").fetchone()[0]
        recommendations_count = conn.execute(
            "SELECT COUNT(*) FROM product_recommendations").fetchone()[0]

        print(f"   • Products: {products_count:,}")
        print(f"   • Sales transactions: {sales_count:,}")
        print(f"   • Product recommendations: {recommendations_count}")

        total_sales = conn.execute(
            "SELECT SUM(total_amount) FROM sales_fact").fetchone()[0]
        print(f"   • Total sales amount: ${total_sales:,.2f}")

        conn.close()

        print("\n[SUCCESS] Pipeline completed successfully!")
        print("[STATS] Data is ready for dashboard")
        print("\nTo view the dashboard, run:")
        print("cd showcase")
        print("streamlit run app.py --server.port 8501 --server.address 0.0.0.0")

    except Exception as e:
        print(f"[ERROR] Pipeline failed: {str(e)}")
        raise


def show_data_sample():
    """Show sample of the processed data"""
    if not os.path.exists('data/warehouse.duckdb'):
        print("[ERROR] Database not found. Run the pipeline first.")
        return

    print("\n" + "=" * 50)
    print("[STEP] Sample Data Preview:")
    conn = duckdb.connect('data/warehouse.duckdb')

    # Sample products
    print("\n[PRODUCTS]  Sample Products:")
    products = conn.execute("""
        SELECT product_id, product_name, category, brand, price
        FROM products_dim
        LIMIT 5
    """).fetchdf()
    print(products.to_string(index=False))

    # Sample sales
    print("\n[SALES] Sample Sales:")
    sales = conn.execute("""
        SELECT sale_id, product_id, region, quantity, total_amount
        FROM sales_fact
        LIMIT 5
    """).fetchdf()
    print(sales.to_string(index=False))

    # Top recommendations
    print("\n[TOP] Top Product Recommendations:")
    recs = conn.execute("""
        SELECT r.region, p.product_name, r.total_quantity, r.rank
        FROM product_recommendations r
        JOIN products_dim p ON r.product_id = p.product_id
        WHERE r.rank <= 3
        ORDER BY r.region, r.rank
        LIMIT 9
    """).fetchdf()
    print(recs.to_string(index=False))

    conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='Run Fashion Retail ETL Pipeline Locally')
    parser.add_argument(
        '--date', help='Date to process (YYYYMMDD)', default=None)
    parser.add_argument('--sample', action='store_true',
                        help='Show data sample after processing')

    args = parser.parse_args()

    setup_environment()
    run_local_pipeline(args.date)

    if args.sample:
        show_data_sample()
