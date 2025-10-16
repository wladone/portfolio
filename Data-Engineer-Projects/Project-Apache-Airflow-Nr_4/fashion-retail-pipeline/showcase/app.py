import streamlit as st
import duckdb
import pandas as pd
import os
from datetime import datetime, timedelta

# Page config
st.set_page_config(page_title="Fashion Retail Dashboard", layout="wide")

# Database connection


@st.cache_resource
def get_connection():
    # Try environment variable first, then check for local data directory
    db_path = os.getenv('DUCKDB_PATH')

    if not db_path:
        # Check if we're running locally (look for data directory relative to project root)
        current_dir = os.getcwd()
        # If we're in showcase directory, go up one level
        if current_dir.endswith('showcase'):
            project_root = os.path.dirname(current_dir)
        else:
            project_root = current_dir

        local_db_path = os.path.join(project_root, 'data', 'warehouse.duckdb')
        if os.path.exists(local_db_path):
            db_path = local_db_path
        else:
            # Fallback to Docker path
            db_path = '/data/warehouse.duckdb'

    print(f"Connecting to database: {db_path}")
    return duckdb.connect(db_path)


conn = get_connection()

# Title
st.title("Fashion Retail Analytics Dashboard")

# Overview KPIs
st.header("Overview KPIs (Last 30 Days)")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_sales = conn.execute("""
        SELECT SUM(total_amount) as total_sales
        FROM sales_fact
        WHERE sale_date >= CURRENT_DATE - INTERVAL 30 days
    """).fetchone()[0] or 0
    st.metric("Total Sales", f"${total_sales:,.2f}")

with col2:
    avg_order_value = conn.execute("""
        SELECT AVG(total_amount) as avg_order
        FROM sales_fact
        WHERE sale_date >= CURRENT_DATE - INTERVAL 30 days
    """).fetchone()[0] or 0
    st.metric("Avg Order Value", f"${avg_order_value:,.2f}")

with col3:
    units_sold = conn.execute("""
        SELECT SUM(quantity) as units_sold
        FROM sales_fact
        WHERE sale_date >= CURRENT_DATE - INTERVAL 30 days
    """).fetchone()[0] or 0
    st.metric("Units Sold", f"{units_sold:,}")

with col4:
    unique_customers = conn.execute("""
        SELECT COUNT(DISTINCT customer_id) as unique_customers
        FROM sales_fact
        WHERE sale_date >= CURRENT_DATE - INTERVAL 30 days
    """).fetchone()[0] or 0
    st.metric("Unique Customers", f"{unique_customers:,}")

# Product Recommendations
st.header("Top Product Recommendations by Region")

# Get all regions
regions = conn.execute(
    "SELECT DISTINCT region FROM product_recommendations ORDER BY region").fetchall()
regions = [r[0] for r in regions]

selected_region = st.selectbox("Select Region", regions)

if selected_region:
    recommendations = conn.execute("""
        SELECT
            pr.product_id,
            p.product_name,
            p.category,
            p.brand,
            pr.total_quantity,
            pr.rank
        FROM product_recommendations pr
        JOIN products_dim p ON pr.product_id = p.product_id
        WHERE pr.region = ?
        ORDER BY pr.rank
        LIMIT 10
    """, [selected_region]).fetchdf()

    st.dataframe(recommendations, use_container_width=True)

# Additional charts
st.header("Sales Trends")

# Sales by date
sales_trend = conn.execute("""
    SELECT
        sale_date,
        SUM(total_amount) as daily_sales,
        SUM(quantity) as daily_units
    FROM sales_fact
    WHERE sale_date >= CURRENT_DATE - INTERVAL 30 days
    GROUP BY sale_date
    ORDER BY sale_date
""").fetchdf()

if not sales_trend.empty:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Daily Sales")
        st.line_chart(sales_trend.set_index('sale_date')['daily_sales'])

    with col2:
        st.subheader("Daily Units Sold")
        st.line_chart(sales_trend.set_index('sale_date')['daily_units'])

# Top products overall
st.header("Top Products Overall")
top_products = conn.execute("""
    SELECT
        p.product_name,
        p.category,
        p.brand,
        SUM(sf.quantity) as total_quantity,
        SUM(sf.total_amount) as total_sales
    FROM sales_fact sf
    JOIN products_dim p ON sf.product_id = p.product_id
    WHERE sf.sale_date >= CURRENT_DATE - INTERVAL 30 days
    GROUP BY p.product_id, p.product_name, p.category, p.brand
    ORDER BY total_quantity DESC
    LIMIT 10
""").fetchdf()

st.dataframe(top_products, use_container_width=True)

# Footer
st.markdown("---")
st.markdown("Dashboard powered by DuckDB | Data updated daily via Airflow")
