"""
Overview dashboard view with metrics, charts, and health panels
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from typing import Dict, Any

from ..core.data_service import DataService
from ..core.models import TimeframeType
from ..utils import format_currency, format_number, format_percent, calculate_percentage_change
from ..config import config


def render_overview(data_service: DataService, timeframe: TimeframeType) -> None:
    """
    Render the overview dashboard

    Args:
        data_service: Data service instance
        timeframe: Selected timeframe
    """
    # Get sales trend data
    sales_trend = data_service.get_sales_trend(timeframe)

    # Calculate dashboard metrics
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

    # Get inventory and supplier summaries
    inventory_summary = data_service.get_inventory_summary()
    supplier_summary = data_service.get_supplier_summary()

    # Render hero card
    _render_hero_card(sales_trend, revenue_delta)

    # Render metric cards
    _render_metric_cards(
        total_revenue, revenue_delta,
        total_orders, order_delta,
        inventory_summary, supplier_summary
    )

    # Render charts
    _render_charts(sales_trend, inventory_summary)

    # Render health panels
    _render_health_panels(inventory_summary, supplier_summary, data_service)


def _render_hero_card(sales_trend: list, revenue_delta: float) -> None:
    """Render the hero card with key metrics"""
    latest_revenue = sales_trend[-1].revenue if sales_trend else 0

    st.markdown(f"""
    <div style="background: linear-gradient(135deg, {config.theme.primary_color} 0%, #764ba2 100%); color: white; padding: 2rem; border-radius: 12px; margin-bottom: 2rem;">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div>
                <div style="background: rgba(255,255,255,0.2); padding: 0.5rem 1rem; border-radius: 20px; display: inline-block; margin-bottom: 1rem; font-size: 0.9rem;">
                    🏢 Lakehouse control plane
                </div>
                <h2 style="margin: 0 0 1rem 0; color: white;">Orchestrate analytics with Databricks discipline</h2>
                <p style="margin: 0; opacity: 0.9; line-height: 1.6;">
                    Monitor revenue velocity, inventory coverage, and supplier health in one Databricks-native command center.
                    Built to support the Global Electronics Lakehouse deployment across EU regions.
                </p>
            </div>
            <div style="text-align: right;">
                <div style="font-size: 0.9rem; opacity: 0.8;">Revenue velocity</div>
                <div style="font-size: 2rem; font-weight: bold;">{format_currency(latest_revenue)}</div>
                <div style="font-size: 0.9rem; opacity: 0.8;">{format_percent(revenue_delta)} vs previous extraction</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def _render_metric_cards(total_revenue: float, revenue_delta: float,
                         total_orders: int, order_delta: float,
                         inventory_summary: Dict[str, Any],
                         supplier_summary: Dict[str, Any]) -> None:
    """Render the metric cards grid"""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total revenue",
            format_currency(total_revenue),
            format_percent(revenue_delta),
            help="Databricks DLT aggregate"
        )

    with col2:
        st.metric(
            "Orders processed",
            format_number(total_orders),
            format_percent(order_delta),
            help="Streaming bronze → gold"
        )

    # Calculate coverage score
    total_units = inventory_summary.get("total_units", 0)
    reorder_units = sum(alert.get("quantity_on_hand", 0)
                        for alert in inventory_summary.get("reorder_alerts", []))
    coverage_score = 100 - (reorder_units / total_units *
                            100) if total_units > 0 else 0

    with col3:
        st.metric(
            "Inventory value",
            format_currency(inventory_summary.get("total_value", 0)),
            f"{coverage_score:.0f}%",
            help="Healthy coverage index"
        )

    with col4:
        avg_rating = supplier_summary.get("average_rating", 0)
        avg_lead_time = supplier_summary.get("average_lead_time", 0)
        # Mock supplier delta calculation
        supplier_delta = (5 - avg_lead_time / 2) * 2

        st.metric(
            "Supplier reliability",
            f"{avg_rating:.1f} / 5",
            f"{supplier_delta:.1f}%",
            help="On-time delivery trend"
        )


def _render_charts(sales_trend: list, inventory_summary: Dict[str, Any]) -> None:
    """Render the analytics charts"""
    st.markdown("### Analytics Dashboard")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown("**Revenue vs streaming orders**")
        st.caption("Real-time metrics synchronized via Delta Live Tables")

        # Create combined chart
        fig = go.Figure()

        # Revenue area chart
        fig.add_trace(go.Scatter(
            x=[point.date for point in sales_trend],
            y=[point.revenue for point in sales_trend],
            mode='lines',
            name='Revenue',
            line=dict(color=config.theme.primary_color, width=3),
            fill='tozeroy',
            fillcolor=f'rgba(255, 109, 82, 0.1)'
        ))

        # Orders line chart
        fig.add_trace(go.Scatter(
            x=[point.date for point in sales_trend],
            y=[point.orders for point in sales_trend],
            mode='lines',
            name='Orders',
            line=dict(color=config.theme.secondary_color, width=2),
            yaxis='y2'
        ))

        fig.update_layout(
            height=320,
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

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Warehouse coverage chart
        st.markdown("**Warehouse coverage**")
        st.caption("Units on hand by regional facility")

        warehouse_data = inventory_summary.get("by_warehouse", [])
        if warehouse_data:
            warehouse_df = pd.DataFrame(warehouse_data)

            fig2 = px.bar(
                warehouse_df,
                x='warehouse',
                y='units',
                color_discrete_sequence=['#2563eb']  # More visible blue
            )
            fig2.update_layout(
                height=280,
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis_title="",
                yaxis_title="Units"
            )
            st.plotly_chart(fig2, use_container_width=True)

    # Category allocation chart
    st.markdown("**Category allocation**")
    st.caption("Share of inventory units across categories")

    category_data = inventory_summary.get("by_category", [])
    if category_data:
        category_df = pd.DataFrame(category_data)

        fig3 = px.pie(
            category_df,
            values='units',
            names='category',
            color_discrete_sequence=[
                config.theme.primary_color,
                config.theme.secondary_color,
                config.theme.accent_color,
                config.theme.success_color,
                config.theme.warning_color,
                config.theme.error_color
            ]
        )
        fig3.update_layout(
            height=300,
            margin=dict(l=20, r=20, t=20, b=20)
        )
        st.plotly_chart(fig3, use_container_width=True)


def _render_health_panels(inventory_summary: Dict[str, Any],
                          supplier_summary: Dict[str, Any],
                          data_service: DataService) -> None:
    """Render the system health panels"""
    st.markdown("### System Health")

    col1, col2, col3 = st.columns(3)

    with col1:
        # Pipeline health panel
        pipeline_runs = data_service.get_pipeline_runs()
        healthy_count = sum(
            1 for run in pipeline_runs if run.status == "Healthy")

        with st.container():
            st.markdown("**Pipeline health**")
            st.caption(
                f"{healthy_count}/{len(pipeline_runs)} pipelines healthy")

            status_color = "🟢" if healthy_count == len(pipeline_runs) else "🟡"
            st.markdown(
                f"{status_color} **{'Healthy' if healthy_count == len(pipeline_runs) else 'Warning'}**")

            for run in pipeline_runs:
                status_icon = "🟢" if run.status == "Healthy" else "🟡" if run.status == "Warning" else "🔴"
                st.markdown(f"{status_icon} {run.name}")
                st.caption(f"Last run {run.last_run} • SLA {run.sla}")

    with col2:
        # Reorder alerts panel
        reorder_alerts = inventory_summary.get("reorder_alerts", [])

        with st.container():
            st.markdown("**Reorder alerts**")
            st.caption("Triggered from Delta expectations")

            if reorder_alerts:
                alert = reorder_alerts[0]  # Show top alert
                st.error(f"⚠️ {alert.product_name}")
                st.caption(f"{alert.warehouse_code} • {alert.category}")
                st.metric("Units remaining", alert.quantity_on_hand)

                # Show additional alerts as hints
                if len(reorder_alerts) > 1:
                    st.caption("Additional alerts:")
                    for additional_alert in reorder_alerts[1:4]:
                        st.caption(f"• {additional_alert.product_name}")
            else:
                st.success("✅ No active alerts • coverage is stable")

    with col3:
        # Supplier reliability panel
        avg_lead_time = supplier_summary.get("average_lead_time", 0)
        top_suppliers = supplier_summary.get("top_suppliers", [])

        with st.container():
            st.markdown("**Supplier reliability**")
            st.caption(f"Average lead time {avg_lead_time:.1f} days")

            if top_suppliers:
                top_supplier = top_suppliers[0]
                st.success(f"⭐ {top_supplier.supplier_name}")
                st.caption(
                    f"Rating {top_supplier.rating:.1f} • {top_supplier.lead_time_days} day lead")
                st.markdown("**Preferred**")

                # Focus areas
                focus_areas = supplier_summary.get("focus_areas", [])
                if focus_areas:
                    st.caption("Focus areas:")
                    for area in focus_areas[:5]:  # Limit display
                        st.caption(f"• {area}")
