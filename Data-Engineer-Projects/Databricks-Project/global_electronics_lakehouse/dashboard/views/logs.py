"""
Logs monitoring view for displaying system logs and error tracking
"""

import streamlit as st
import os
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any
from collections import defaultdict

from ..core.data_service import DataService
from ..config import config


def render_logs(data_service: DataService) -> None:
    """
    Render the logs monitoring dashboard

    Args:
        data_service: Data service instance
    """
    st.markdown("### System Logs & Error Tracking")

    # Get log data
    log_entries = _get_log_entries()
    log_stats = _analyze_log_stats(log_entries)

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        log_levels = ["ALL"] + list(log_stats["by_level"].keys())
        selected_level = st.selectbox(
            "Log Level",
            options=log_levels,
            index=0,
            help="Filter logs by severity level"
        )

    with col2:
        time_filters = ["All", "Last Hour", "Last 24 Hours", "Last 7 Days"]
        selected_time = st.selectbox(
            "Time Range",
            options=time_filters,
            index=1,
            help="Filter logs by time period"
        )

    with col3:
        max_entries = st.selectbox(
            "Max Entries",
            options=[50, 100, 200, 500],
            index=1,
            help="Maximum number of log entries to display"
        )

    # Apply filters
    filtered_logs = _filter_logs(
        log_entries, selected_level, selected_time, max_entries)

    # Display stats
    _render_log_stats(log_stats)

    # Display logs
    _render_log_entries(filtered_logs)


def _get_log_entries() -> List[Dict[str, Any]]:
    """Get log entries from log files"""
    log_entries = []

    logs_dir = os.path.join(os.path.dirname(
        os.path.dirname(os.path.dirname(__file__))), "_logs")

    if not os.path.exists(logs_dir):
        return log_entries

    # Parse log files
    for filename in os.listdir(logs_dir):
        if filename.endswith('.log'):
            filepath = os.path.join(logs_dir, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        if line.strip():
                            parsed = _parse_log_line(
                                line.strip(), filename, line_num)
                            if parsed:
                                log_entries.append(parsed)
            except Exception as e:
                st.warning(f"Error reading log file {filename}: {e}")

    # Sort by timestamp descending
    log_entries.sort(key=lambda x: x['timestamp'], reverse=True)

    return log_entries


def _parse_log_line(line: str, filename: str, line_num: int) -> Dict[str, Any]:
    """Parse a single log line"""
    # Match format: 2025-10-06 15:25:06,038 - ERROR - Data generation failed: Seed must be non-negative
    pattern = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - (\w+) - (.+)$'
    match = re.match(pattern, line)

    if match:
        timestamp_str, level, message = match.groups()
        try:
            timestamp = datetime.strptime(
                timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
            return {
                'timestamp': timestamp,
                'level': level,
                'message': message,
                'filename': filename,
                'line_num': line_num,
                'raw': line
            }
        except ValueError:
            pass

    return None


def _analyze_log_stats(log_entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze log statistics"""
    stats = {
        "total": len(log_entries),
        "by_level": defaultdict(int),
        "by_hour": defaultdict(int),
        "recent_errors": 0,
        "recent_warnings": 0
    }

    now = datetime.now()
    last_hour = now - timedelta(hours=1)

    for entry in log_entries:
        stats["by_level"][entry["level"]] += 1

        # Count recent entries
        if entry["timestamp"] > last_hour:
            if entry["level"] == "ERROR":
                stats["recent_errors"] += 1
            elif entry["level"] == "WARNING":
                stats["recent_warnings"] += 1

        # Group by hour
        hour_key = entry["timestamp"].strftime("%Y-%m-%d %H:00")
        stats["by_hour"][hour_key] += 1

    return dict(stats)


def _filter_logs(log_entries: List[Dict[str, Any]], level: str, time_range: str, max_entries: int) -> List[Dict[str, Any]]:
    """Filter log entries based on criteria"""
    filtered = log_entries

    # Filter by level
    if level != "ALL":
        filtered = [entry for entry in filtered if entry["level"] == level]

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
        filtered = [entry for entry in filtered if entry["timestamp"] > cutoff]

    # Limit entries
    return filtered[:max_entries]


def _render_log_stats(stats: Dict[str, Any]) -> None:
    """Render log statistics cards"""
    st.markdown("#### Log Statistics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Logs",
            f"{stats['total']:,}",
            help="Total log entries across all files"
        )

    with col2:
        error_count = stats['by_level'].get('ERROR', 0)
        st.metric(
            "Errors",
            f"{error_count:,}",
            delta=f"{stats['recent_errors']} in last hour",
            help="Error-level log entries"
        )

    with col3:
        warning_count = stats['by_level'].get('WARNING', 0)
        st.metric(
            "Warnings",
            f"{warning_count:,}",
            delta=f"{stats['recent_warnings']} in last hour",
            help="Warning-level log entries"
        )

    with col4:
        info_count = stats['by_level'].get('INFO', 0)
        st.metric(
            "Info Logs",
            f"{info_count:,}",
            help="Info-level log entries"
        )


def _render_log_entries(log_entries: List[Dict[str, Any]]) -> None:
    """Render the log entries table"""
    st.markdown("#### Log Entries")

    if not log_entries:
        st.info("No log entries found matching the current filters.")
        return

    # Display as a table
    log_data = []
    for entry in log_entries:
        log_data.append({
            "Time": entry["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
            "Level": entry["level"],
            "Message": entry["message"][:100] + "..." if len(entry["message"]) > 100 else entry["message"],
            "Source": f"{entry['filename']}:{entry['line_num']}"
        })

    st.dataframe(
        log_data,
        use_container_width=True,
        column_config={
            "Time": st.column_config.DatetimeColumn("Time", width="medium"),
            "Level": st.column_config.TextColumn("Level", width="small"),
            "Message": st.column_config.TextColumn("Message", width="large"),
            "Source": st.column_config.TextColumn("Source", width="medium")
        }
    )

    # Show full message on expansion
    with st.expander("View Full Log Details", expanded=False):
        for i, entry in enumerate(log_entries):
            with st.container():
                st.markdown(
                    f"**{i+1}. {entry['level']} - {entry['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}**")
                st.code(entry['raw'], language="text")
                st.divider()
