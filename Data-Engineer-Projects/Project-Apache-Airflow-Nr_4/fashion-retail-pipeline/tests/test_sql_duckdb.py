import pytest
import duckdb
import os
import tempfile
import pandas as pd


@pytest.fixture
def temp_db():
    """Create a temporary DuckDB database for testing"""
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as f:
        db_path = f.name

    conn = duckdb.connect(db_path)
    yield conn
    conn.close()
    os.unlink(db_path)


def test_create_schema(temp_db):
    """Test schema creation SQL"""
    with open('dags/sql/create_schema_duckdb.sql', 'r') as f:
        sql = f.read()

    temp_db.execute(sql)

    # Check tables exist
    tables = temp_db.execute("SHOW TABLES").fetchall()
    table_names = [t[0] for t in tables]

    expected_tables = ['stg_products', 'stg_transactions',
                       'products_dim', 'sales_fact', 'product_recommendations']
    for table in expected_tables:
        assert table in table_names, f"Table {table} not created"


def test_transform_products(temp_db):
    """Test products transformation"""
    # Create schema
    with open('dags/sql/create_schema_duckdb.sql', 'r') as f:
        temp_db.execute(f.read())

    # Insert test data
    temp_db.execute("""
        INSERT INTO stg_products VALUES
        (1, 'Test Product', 'T-Shirts', 'Casual', 'Nike', 29.99, 15.00, 1001, 'US', '2023-01-01', '2023-01-01'),
        (2, 'Bad Product', 'Pants', 'Formal', 'Adidas', -10.00, 20.00, 1002, 'US', '2023-01-01', '2023-01-01')
    """)

    # Run transform
    with open('dags/sql/transform_products.sql', 'r') as f:
        sql = f.read()
    temp_db.execute(sql)

    # Check results
    result = temp_db.execute("SELECT COUNT(*) FROM products_dim").fetchone()[0]
    assert result == 1, "Should have 1 valid product after transform"

    product = temp_db.execute("SELECT * FROM products_dim").fetchone()
    assert product[4] == 29.99, "Price should be cleaned"


def test_transform_sales(temp_db):
    """Test sales transformation"""
    # Create schema
    with open('dags/sql/create_schema_duckdb.sql', 'r') as f:
        temp_db.execute(f.read())

    # Insert test data
    temp_db.execute("""
        INSERT INTO stg_products VALUES
        (1, 'Test Product', 'T-Shirts', 'Casual', 'Nike', 29.99, 15.00, 1001, 'US', '2023-01-01', '2023-01-01')
    """)

    temp_db.execute("""
        INSERT INTO stg_transactions VALUES
        ('S001', 1, '2023-01-01', 'US', 1, 2, 29.99, 59.98, 1001, 'Credit'),
        ('S002', 1, '2023-01-01', 'US', 1, -1, 29.99, -29.99, 1002, 'Cash')
    """)

    # Run transforms
    with open('dags/sql/transform_products.sql', 'r') as f:
        temp_db.execute(f.read())
    with open('dags/sql/transform_transactions.sql', 'r') as f:
        temp_db.execute(f.read())

    # Check results
    result = temp_db.execute("SELECT COUNT(*) FROM sales_fact").fetchone()[0]
    assert result == 1, "Should have 1 valid sale after transform"


def test_generate_recommendations(temp_db):
    """Test recommendations generation"""
    # Create schema and insert test data
    with open('dags/sql/create_schema_duckdb.sql', 'r') as f:
        temp_db.execute(f.read())

    # Insert test data
    temp_db.execute("""
        INSERT INTO products_dim VALUES
        (1, 'Test Product', 'T-Shirts', 'Casual', 'Nike', 29.99, 15.00, 1001, 'US', '2023-01-01', '2023-01-01')
    """)

    temp_db.execute("""
        INSERT INTO sales_fact VALUES
        ('S001', 1, '2023-10-01', 'US', 1, 10, 29.99, 299.90, 1001, 'Credit'),
        ('S002', 1, '2023-10-02', 'US', 1, 5, 29.99, 149.95, 1002, 'Cash')
    """)

    # Generate recommendations
    temp_db.execute("""
        INSERT INTO product_recommendations (region, product_id, total_quantity, rank)
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
            WHERE sale_date >= date('now', '-30 days')
            GROUP BY region, product_id
        ) t
        WHERE rank <= 10;
    """)

    # Check results
    result = temp_db.execute(
        "SELECT COUNT(*) FROM product_recommendations").fetchone()[0]
    assert result == 1, "Should have 1 recommendation"

    rec = temp_db.execute("SELECT * FROM product_recommendations").fetchone()
    assert rec[2] == 15, "Total quantity should be 15"
    assert rec[3] == 1, "Rank should be 1"
