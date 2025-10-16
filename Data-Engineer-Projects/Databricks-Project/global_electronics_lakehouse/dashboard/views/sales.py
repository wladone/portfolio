"""
Sales analytics view
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from typing import Dict, Any

from ..core.data_service import DataService
from ..core.models import TimeframeType, CategoryPerformance
from ..utils import format_currency, format_number, format_percent, calculate_percentage_change
from ..config import config


def render_sales(data_service: DataService, timeframe: TimeframeType) -> None:
    """Render the sales analytics view"""
    sales_trend = data_service.get_sales_trend(timeframe)
    category_performance = data_service.get_category_performance()

    # Calculate metrics
    total_revenue = sum(point.revenue for point in sales_trend)
    total_orders = sum(point.orders for point in sales_trend)
    average_order_value = total_revenue / total_orders if total_orders > 0 else 0

    # Calculate deltas
    revenue_delta = 0.0
    order_delta = 0.0
    if len(sales_trend) >= 2:
        last_point = sales_trend[-1]
        prev_point = sales_trend[-2]
        revenue_delta = calculate_percentage_change(
            last_point.revenue, prev_point.revenue)
        order_delta = calculate_percentage_change(
            last_point.orders, prev_point.orders)

    # Find fastest growing category
    best_category = max(
        category_performance, key=lambda x: x.growth) if category_performance else None

    # Hero section
    st.markdown("""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 2rem; border-radius: 12px; margin-bottom: 2rem;">
        <div>
            <h2 style="margin: 0 0 1rem 0; color: white;">Commercial momentum</h2>
            <p style="margin: 0; opacity: 0.9; line-height: 1.6;">
                Revenue and order cadence streamed into the Lakehouse
            </p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Key metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Period revenue",
            format_currency(total_revenue),
            f"{format_percent(revenue_delta)} vs prior period"
        )

    with col2:
        st.metric(
            "Orders processed",
            format_number(total_orders),
            f"AOV {format_currency(average_order_value)}"
        )

    with col3:
        if best_category:
            st.metric(
                "Fastest growing",
                best_category.category,
                f"Growth {format_percent(best_category.growth * 100)}"
            )

    # Charts section
    st.markdown("### Revenue Analytics")

    col1, col2 = st.columns([2, 1])

    with col1:
        # Revenue pattern chart
        st.markdown("**Streaming revenue pattern**")
        st.caption("Orders correlated with revenue output")

        fig1 = go.Figure()

        # Revenue line
        fig1.add_trace(go.Scatter(
            x=[point.date for point in sales_trend],
            y=[point.revenue for point in sales_trend],
            mode='lines',
            name='Revenue',
            line=dict(color=config.theme.primary_color, width=3)
        ))

        # Orders line
        fig1.add_trace(go.Scatter(
            x=[point.date for point in sales_trend],
            y=[point.orders for point in sales_trend],
            mode='lines',
            name='Orders',
            line=dict(color=config.theme.accent_color, width=2),
            yaxis='y2'
        ))

        fig1.update_layout(
            height=330,
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(orientation="h", yanchor="bottom",
                        y=1.02, xanchor="right", x=1),
            yaxis=dict(
                title="Revenue (€k)",
                tickformat=",.0f",
                tickprefix="€"
            ),
            yaxis2=dict(
                title="Orders",
                overlaying="y",
                side="right"
            )
        )

        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        # Category share chart
        st.markdown("**Category share**")
        st.caption("Revenue allocation per business unit")

        if category_performance:
            category_df = pd.DataFrame([
                {"category": cp.category, "revenue": cp.revenue}
                for cp in category_performance
            ])

            fig2 = px.bar(
                category_df,
                x="category",
                y="revenue",
                color_discrete_sequence=[config.theme.secondary_color]
            )
            fig2.update_layout(
                height=330,
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis_title="",
                yaxis_title="Revenue (€)",
                yaxis_tickformat=",.0f",
                yaxis_tickprefix="€"
            )
            st.plotly_chart(fig2, use_container_width=True)

    # Top performing days table
    st.markdown("### Top Performing Days")
    st.caption("Sorted by revenue capture")

    # Get top days by revenue
    top_days = sorted(sales_trend, key=lambda x: x.revenue, reverse=True)[:7]

    table_data = []
    for day in top_days:
        aov = day.revenue / day.orders if day.orders > 0 else 0
        table_data.append({
            "Date": day.date,
            "Revenue": format_currency(day.revenue),
            "Orders": format_number(day.orders),
            "Average Order Value": format_currency(aov)
        })

    df = pd.DataFrame(table_data)
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True
    )
