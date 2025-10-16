"""
Global Electronics Lakehouse Dashboard
Production-ready Streamlit application with modular architecture
"""

from dashboard.views import (
    render_overview,
    render_inventory,
    render_sales,
    render_suppliers,
    render_logs,
    render_alerts,
    render_performance
)
from dashboard.utils import with_error_handling, with_loading_indicator
from dashboard.core.models import TimeframeType, ThemeType, TabType
from dashboard.core.data_service import DataService, DataServiceError
from dashboard.config import config
import streamlit as st
from typing import Optional

# Import our modular components
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


# Configure the page
st.set_page_config(
    page_title=config.app.title,
    page_icon=config.app.icon,
    layout=config.app.layout,
    initial_sidebar_state=config.app.initial_sidebar_state
)

# Initialize services
data_service = DataService()


def initialize_session_state():
    """Initialize Streamlit session state"""
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = "overview"
    if 'timeframe' not in st.session_state:
        st.session_state.timeframe = "30d"
    if 'theme' not in st.session_state:
        st.session_state.theme = "light"


def render_sidebar():
    """Render the application sidebar with navigation"""
    with st.sidebar:
        # Header with branding
        st.markdown(f"""
        <div style="display: flex; align-items: center; margin-bottom: 1rem;">
            <div style="font-size: 1.5rem; margin-right: 0.5rem;">{config.app.icon}</div>
            <div>
                <div style="font-weight: bold; font-size: 1.1rem;">Lakehouse Control</div>
                <div style="font-size: 0.8rem; color: #666;">Databricks workspace</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Navigation items
        nav_items = [
            {"id": "overview", "label": "Lakehouse Overview",
             "description": "Unified telemetry for revenue, inventory, and pipelines", "icon": "✨"},
            {"id": "inventory", "label": "Inventory Management",
             "description": "Live visibility into warehouse coverage and SKU velocity", "icon": "📦"},
            {"id": "sales", "label": "Sales Analytics",
             "description": "Streaming orders, revenue trends, and commercial insights", "icon": "📊"},
            {"id": "suppliers", "label": "Supplier Network",
             "description": "Reliability, lead times, and focus areas for partners", "icon": "🤝"},
            {"id": "logs", "label": "System Logs",
             "description": "Application logs, errors, and system events", "icon": "📋"},
            {"id": "alerts", "label": "Alerts & Notifications",
             "description": "Real-time alerts and system notifications", "icon": "🚨"},
            {"id": "performance", "label": "Performance Metrics",
             "description": "Query performance, system resources, and pipeline metrics", "icon": "📈"},
        ]

        # Navigation selector
        nav_options = [f"{item['icon']} {item['label']}" for item in nav_items]
        nav_ids = [item['id'] for item in nav_items]

        # Map current active tab to option
        current_option = next((opt for opt, id_val in zip(
            nav_options, nav_ids) if id_val == st.session_state.active_tab), nav_options[0])

        selected_option = st.selectbox(
            "Navigation",
            options=nav_options,
            index=nav_options.index(current_option),
            help="Select dashboard section",
            label_visibility="collapsed",
            key="nav_selector"
        )

        # Update active tab based on selection
        if selected_option != current_option:
            selected_index = nav_options.index(selected_option)
            st.session_state.active_tab = nav_ids[selected_index]
            st.rerun()

        st.divider()

        # Controls section
        st.markdown("### Controls")

        # Timeframe selector
        timeframe_options = {"7d": "7 days",
                             "30d": "30 days", "90d": "90 days"}
        selected_timeframe = st.selectbox(
            "Timeframe",
            options=list(timeframe_options.keys()),
            format_func=lambda x: timeframe_options[x],
            key="timeframe_selector"
        )
        st.session_state.timeframe = selected_timeframe

        # Theme toggle
        theme_options = {"light": "☀️ Light", "dark": "🌙 Dark"}
        selected_theme = st.selectbox(
            "Theme",
            options=list(theme_options.keys()),
            format_func=lambda x: theme_options[x],
            key="theme_selector"
        )
        st.session_state.theme = selected_theme


def render_header():
    """Render the main header"""
    current_nav = {
        "overview": {"label": "Lakehouse Overview", "description": "Unified telemetry for revenue, inventory, and pipelines"},
        "inventory": {"label": "Inventory Management", "description": "Live visibility into warehouse coverage and SKU velocity"},
        "sales": {"label": "Sales Analytics", "description": "Streaming orders, revenue trends, and commercial insights"},
        "suppliers": {"label": "Supplier Network", "description": "Reliability, lead times, and focus areas for partners"},
        "logs": {"label": "System Logs", "description": "Application logs, errors, and system events"},
        "alerts": {"label": "Alerts & Notifications", "description": "Real-time alerts and system notifications"},
        "performance": {"label": "Performance Metrics", "description": "Query performance, system resources, and pipeline metrics"},
    }.get(st.session_state.active_tab, {"label": "Dashboard", "description": "Analytics dashboard"})

    st.markdown(f"""
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 2rem;">
        <div>
            <h1 style="margin: 0;">{config.app.title}</h1>
            <p style="margin: 0; color: #666;">{current_nav['description']}</p>
        </div>
        <div style="background: #f0f0f0; padding: 0.5rem 1rem; border-radius: 20px; font-size: 0.9rem;">
            🇪🇺 EU Databricks region
        </div>
    </div>
    """, unsafe_allow_html=True)


@with_error_handling
@with_loading_indicator("Loading dashboard...")
def render_main_content():
    """Render the main content area based on active tab"""
    try:
        if st.session_state.active_tab == "overview":
            render_overview(data_service, st.session_state.timeframe)
        elif st.session_state.active_tab == "inventory":
            render_inventory(data_service)
        elif st.session_state.active_tab == "sales":
            render_sales(data_service, st.session_state.timeframe)
        elif st.session_state.active_tab == "suppliers":
            render_suppliers(data_service)
        elif st.session_state.active_tab == "logs":
            render_logs(data_service)
        elif st.session_state.active_tab == "alerts":
            render_alerts(data_service)
        elif st.session_state.active_tab == "performance":
            render_performance(data_service)
        else:
            st.error("Unknown tab selected")
    except DataServiceError as e:
        st.error(f"Data service error: {e}")
        st.info(
            "Please try refreshing the page or contact support if the issue persists.")
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        st.info(
            "Please try refreshing the page or contact support if the issue persists.")


def main():
    """Main application entry point"""
    try:
        # Initialize session state
        initialize_session_state()

        # Render sidebar
        render_sidebar()

        # Render header
        render_header()

        # Render main content
        render_main_content()

    except Exception as e:
        st.error("🚨 Critical application error occurred")
        st.error(str(e))
        st.info("Please refresh the page or contact support if the issue persists.")

        # Log the error for debugging
        import logging
        logging.error(f"Critical application error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
