"""
Alerts monitoring view for displaying system alerts and notifications
"""

import streamlit as st
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
from collections import defaultdict

from ..core.data_service import DataService
from ..config import config


def render_alerts(data_service: DataService) -> None:
    """
    Render the alerts monitoring dashboard

    Args:
        data_service: Data service instance
    """
    st.markdown("### System Alerts & Notifications")

    # Get alert data
    alerts = _get_recent_alerts(data_service)
    alert_stats = _analyze_alert_stats(alerts)

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        alert_levels = ["ALL"] + list(alert_stats["by_level"].keys())
        selected_level = st.selectbox(
            "Alert Level",
            options=alert_levels,
            index=0,
            help="Filter alerts by severity level"
        )

    with col2:
        time_filters = ["Last Hour", "Last 24 Hours", "Last 7 Days", "All"]
        selected_time = st.selectbox(
            "Time Range",
            options=time_filters,
            index=1,
            help="Filter alerts by time period"
        )

    with col3:
        status_filters = ["All", "Active", "Resolved"]
        selected_status = st.selectbox(
            "Status",
            options=status_filters,
            index=0,
            help="Filter by alert status"
        )

    # Apply filters
    filtered_alerts = _filter_alerts(
        alerts, selected_level, selected_time, selected_status)

    # Display stats
    _render_alert_stats(alert_stats)

    # Display alerts
    _render_alert_entries(filtered_alerts)

    # Alert management
    _render_alert_management(filtered_alerts)


def _get_recent_alerts(data_service: DataService) -> List[Dict[str, Any]]:
    """Get recent alerts from data service"""
    return data_service.get_system_alerts()


def _analyze_alert_stats(alerts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze alert statistics"""
    stats = {
        "total": len(alerts),
        "by_level": defaultdict(int),
        "by_status": defaultdict(int),
        "active_count": 0,
        "recent_count": 0
    }

    now = datetime.now()
    last_hour = now - timedelta(hours=1)

    for alert in alerts:
        stats["by_level"][alert["level"]] += 1
        stats["by_status"][alert["status"]] += 1

        if alert["status"] == "Active":
            stats["active_count"] += 1

        if alert["timestamp"] > last_hour:
            stats["recent_count"] += 1

    return dict(stats)


def _filter_alerts(alerts: List[Dict[str, Any]], level: str, time_range: str, status: str) -> List[Dict[str, Any]]:
    """Filter alerts based on criteria"""
    filtered = alerts

    # Filter by level
    if level != "ALL":
        filtered = [alert for alert in filtered if alert["level"] == level]

    # Filter by time
    now = datetime.now()
    if time_range == "Last Hour":
        cutoff = now - timedelta(hours=1)
    elif time_range == "Last 24 Hours":
        cutoff = now - timedelta(days=1)
    elif time_range == "Last 7 Days":
        cutoff = now - timedelta(days=7)
    else:
        cutoff = None

    if cutoff:
        filtered = [alert for alert in filtered if alert["timestamp"] > cutoff]

    # Filter by status
    if status != "All":
        filtered = [alert for alert in filtered if alert["status"] == status]

    return filtered


def _render_alert_stats(stats: Dict[str, Any]) -> None:
    """Render alert statistics cards"""
    st.markdown("#### Alert Statistics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Alerts",
            f"{stats['total']:,}",
            help="Total alerts in the system"
        )

    with col2:
        active_count = stats['active_count']
        st.metric(
            "Active Alerts",
            f"{active_count:,}",
            delta=f"{stats['recent_count']} in last hour",
            help="Currently active alerts requiring attention"
        )

    with col3:
        error_count = stats['by_level'].get('ERROR', 0)
        st.metric(
            "Critical Alerts",
            f"{error_count:,}",
            help="Error-level alerts"
        )

    with col4:
        warning_count = stats['by_level'].get('WARNING', 0)
        st.metric(
            "Warnings",
            f"{warning_count:,}",
            help="Warning-level alerts"
        )


def _render_alert_entries(alerts: List[Dict[str, Any]]) -> None:
    """Render the alerts table"""
    st.markdown("#### Recent Alerts")

    if not alerts:
        st.info("No alerts found matching the current filters.")
        return

    # Display as a table
    alert_data = []
    for alert in alerts:
        status_icon = "🔴" if alert["status"] == "Active" else "✅"
        level_icon = {
            "ERROR": "🚨",
            "WARNING": "⚠️",
            "INFO": "ℹ️"
        }.get(alert["level"], "❓")

        alert_data.append({
            "Status": f"{status_icon} {alert['status']}",
            "Level": f"{level_icon} {alert['level']}",
            "Time": alert["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
            "Message": alert["message"][:80] + "..." if len(alert["message"]) > 80 else alert["message"],
            "Source": alert["source"]
        })

    st.dataframe(
        alert_data,
        use_container_width=True,
        column_config={
            "Status": st.column_config.TextColumn("Status", width="small"),
            "Level": st.column_config.TextColumn("Level", width="small"),
            "Time": st.column_config.DatetimeColumn("Time", width="medium"),
            "Message": st.column_config.TextColumn("Message", width="large"),
            "Source": st.column_config.TextColumn("Source", width="medium")
        }
    )

    # Show detailed view
    with st.expander("View Alert Details", expanded=False):
        for i, alert in enumerate(alerts):
            with st.container():
                status_color = "red" if alert["status"] == "Active" else "green"
                st.markdown(f"**{i+1}. <span style='color:{status_color}'>{alert['level']}</span> - {alert['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}**",
                            unsafe_allow_html=True)
                st.markdown(f"**Message:** {alert['message']}")
                st.markdown(f"**Source:** {alert['source']}")
                st.markdown(f"**Status:** {alert['status']}")

                if alert.get("details"):
                    st.markdown("**Details:**")
                    st.json(alert["details"])

                st.divider()


def _render_alert_management(alerts: List[Dict[str, Any]]) -> None:
    """Render alert management controls"""
    st.markdown("#### Alert Management")

    active_alerts = [a for a in alerts if a["status"] == "Active"]

    if not active_alerts:
        st.success("✅ No active alerts requiring attention.")
        return

    st.warning(
        f"There are {len(active_alerts)} active alerts that may need attention.")

    # Bulk actions
    col1, col2 = st.columns(2)

    with col1:
        if st.button("🔄 Refresh Alerts", use_container_width=True):
            st.rerun()

    with col2:
        if st.button("📧 Send Alert Summary", use_container_width=True):
            st.success("Alert summary sent to configured recipients.")

    # Individual alert actions
    st.markdown("**Active Alert Actions:**")
    for alert in active_alerts:
        with st.container():
            col1, col2, col3 = st.columns([3, 1, 1])

            with col1:
                st.markdown(f"**{alert['level']}:** {alert['message']}")

            with col2:
                if st.button(f"Acknowledge {alert['id']}", key=f"ack_{alert['id']}"):
                    st.success(f"Alert '{alert['id']}' acknowledged.")

            with col3:
                if st.button(f"Resolve {alert['id']}", key=f"resolve_{alert['id']}"):
                    st.success(f"Alert '{alert['id']}' marked as resolved.")
