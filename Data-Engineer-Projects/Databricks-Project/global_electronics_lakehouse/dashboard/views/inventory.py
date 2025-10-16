"""
Inventory management view
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from typing import Dict, Any

from ..core.data_service import DataService
from ..utils import format_currency, format_number
from ..config import config


def render_inventory(data_service: DataService) -> None:
    """Render the inventory management view"""
    inventory_summary = data_service.get_inventory_summary()

    # Hero section
    st.markdown("""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 2rem; border-radius: 12px; margin-bottom: 2rem;">
        <div>
            <h2 style="margin: 0 0 1rem 0; color: white;">Inventory exposure</h2>
            <p style="margin: 0; opacity: 0.9; line-height: 1.6;">
                Warehouse coverage and SKU value concentration
            </p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Key metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Units on hand",
            format_number(inventory_summary.get("total_units", 0)),
            help=f"Across {len(inventory_summary.get('by_warehouse', []))} warehouses"
        )

    with col2:
        st.metric(
            "Inventory value",
            format_currency(inventory_summary.get("total_value", 0)),
            help="Calculated from Lakehouse gold tables"
        )

    with col3:
        st.metric(
            "Reorder alerts",
            len(inventory_summary.get("reorder_alerts", [])),
            help="Delta expectations"
        )

    # Charts section
    st.markdown("### Warehouse & Category Analysis")

    col1, col2 = st.columns([2, 1])

    with col1:
        warehouse_data = inventory_summary.get("by_warehouse", [])
        if warehouse_data:
            warehouse_df = pd.DataFrame(warehouse_data).sort_values(
                "units", ascending=False)

            fig1 = px.bar(
                warehouse_df,
                x="warehouse",
                y="units",
                color_discrete_sequence=[config.theme.secondary_color]
            )
            fig1.update_layout(
                height=320,
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis_title="",
                yaxis_title="Units"
            )
            st.plotly_chart(fig1, use_container_width=True)

    with col2:
        category_data = inventory_summary.get("by_category", [])
        if category_data:
            category_df = pd.DataFrame(category_data).sort_values(
                "value", ascending=False)

            fig2 = px.bar(
                category_df.head(10),  # Top 10 categories
                x="value",
                y="category",
                orientation='h',
                color_discrete_sequence=[config.theme.success_color]
            )
            fig2.update_layout(
                height=320,
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis_title="Value (€)",
                yaxis_title=""
            )
            st.plotly_chart(fig2, use_container_width=True)

    # SKU Details table
    st.markdown("### SKU Details")
    st.caption("Ranked by inventory value")

    top_skus = inventory_summary.get("top_skus", [])
    if top_skus:
        table_data = []
        for item in top_skus[:50]:  # Limit to top 50 for performance
            value = item.total_value
            status = item.stock_status
            status_icon = {"healthy": "🟢", "monitor": "🟡",
                           "reorder": "🔴"}.get(status, "⚪")

            table_data.append({
                "SKU": item.product_id,
                "Product": item.product_name,
                "Warehouse": item.warehouse_code,
                "Category": item.category,
                "On Hand": format_number(item.quantity_on_hand),
                "Unit Cost": format_currency(item.unit_cost_euro),
                "Value": format_currency(value),
                "Status": f"{status_icon} {status.title()}"
            })

        df = pd.DataFrame(table_data)
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True
        )
