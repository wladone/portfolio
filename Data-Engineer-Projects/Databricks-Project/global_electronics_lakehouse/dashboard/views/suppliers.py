"""
Supplier network view
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from typing import Dict, Any

from ..core.data_service import DataService
from ..utils import format_number
from ..config import config


def render_suppliers(data_service: DataService) -> None:
    """Render the supplier network view"""
    supplier_summary = data_service.get_supplier_summary()

    # Hero section
    st.markdown("""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 2rem; border-radius: 12px; margin-bottom: 2rem;">
        <div>
            <h2 style="margin: 0 0 1rem 0; color: white;">Supplier ecosystem</h2>
            <p style="margin: 0; opacity: 0.9; line-height: 1.6;">
                Partner reliability and supply chain performance
            </p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Key metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Average rating",
            f"{supplier_summary.get('average_rating', 0):.1f} / 5",
            help=f"Across {len(supplier_summary.get('top_suppliers', []))} partners"
        )

    with col2:
        st.metric(
            "Lead time",
            f"{supplier_summary.get('average_lead_time', 0):.1f} days",
            help="Average delivery window"
        )

    with col3:
        st.metric(
            "Pipeline health",
            "97%",
            help="SLA compliance score"
        )

    # Charts section
    st.markdown("### Performance Analytics")

    col1, col2 = st.columns([2, 1])

    with col1:
        # Supplier performance chart
        st.markdown("**Supplier performance**")
        st.caption("Rated by reliability and delivery consistency")

        suppliers = supplier_summary.get("top_suppliers", [])
        if suppliers:
            supplier_df = pd.DataFrame([
                {"supplier_name": s.supplier_name, "rating": s.rating}
                for s in suppliers
            ])

            fig1 = px.bar(
                supplier_df,
                x="supplier_name",
                y="rating",
                color_discrete_sequence=[config.theme.success_color]
            )
            fig1.update_layout(
                height=320,
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis_title="",
                yaxis_title="Rating",
                yaxis_range=[0, 5]
            )
            st.plotly_chart(fig1, use_container_width=True)

    with col2:
        # Focus areas chart
        st.markdown("**Focus areas**")
        st.caption("Product categories by supplier specialization")

        focus_areas = supplier_summary.get("focus_areas", [])
        if focus_areas:
            # Count suppliers per focus area
            focus_counts = {}
            suppliers = supplier_summary.get("top_suppliers", [])
            for supplier in suppliers:
                for focus in supplier.category_focus:
                    focus_counts[focus] = focus_counts.get(focus, 0) + 1

            focus_df = pd.DataFrame(list(focus_counts.items()), columns=[
                                    "Category", "Suppliers"])

            fig2 = px.pie(
                focus_df,
                values="Suppliers",
                names="Category",
                color_discrete_sequence=[
                    config.theme.primary_color,
                    config.theme.secondary_color,
                    config.theme.accent_color,
                    config.theme.success_color,
                    config.theme.warning_color,
                    config.theme.error_color
                ]
            )
            fig2.update_layout(
                height=320,
                margin=dict(l=20, r=20, t=20, b=20)
            )
            st.plotly_chart(fig2, use_container_width=True)

    # Supplier directory table
    st.markdown("### Supplier Directory")
    st.caption("Sorted by performance rating")

    suppliers = supplier_summary.get("top_suppliers", [])
    if suppliers:
        table_data = []
        for supplier in suppliers:
            # Determine status based on rating
            if supplier.rating >= 4.5:
                status = "🟢 Excellent"
            elif supplier.rating >= 4.0:
                status = "🟡 Good"
            else:
                status = "🔴 Monitor"

            table_data.append({
                "Supplier": supplier.supplier_name,
                "Rating": f"{supplier.rating:.1f} / 5",
                "Lead Time": f"{supplier.lead_time_days} days",
                "Categories": ", ".join(supplier.category_focus),
                "Status": status
            })

        df = pd.DataFrame(table_data)
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True
        )
