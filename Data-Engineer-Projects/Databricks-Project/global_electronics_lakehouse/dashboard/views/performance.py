"""
Performance monitoring view for displaying system performance metrics
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any
import time

from ..core.data_service import DataService
from ..config import config


def render_performance(data_service: DataService) -> None:
    """
    Render the performance monitoring dashboard

    Args:
        data_service: Data service instance
    """
    st.markdown("### System Performance Metrics")

    # Get performance data
    perf_data = _get_performance_data(data_service)

    # Performance overview
    _render_performance_overview(perf_data)

    # Detailed metrics
    col1, col2 = st.columns(2)

    with col1:
        _render_query_performance(perf_data)

    with col2:
        _render_pipeline_performance(perf_data)

    # System resources
    _render_system_resources(perf_data)

    # Performance trends
    _render_performance_trends(perf_data)


def _get_performance_data(data_service: DataService) -> Dict[str, Any]:
    """Get performance metrics data"""
    # Mock performance data - in real implementation, this would query system metrics
    now = datetime.now()

    # Query performance metrics
    query_metrics = []
    for i in range(24):  # Last 24 hours
        timestamp = now - timedelta(hours=23-i)
        query_metrics.append({
            "timestamp": timestamp,
            "avg_query_time": 2.5 + (i % 3) * 0.5,  # Mock varying performance
            "query_count": 150 + (i % 5) * 20,
            "slow_queries": 5 + (i % 2),
            "failed_queries": 0 if i > 20 else 1  # Occasional failures
        })

    # Pipeline performance
    pipeline_metrics = []
    pipelines = ["Bronze Layer Ingest", "Silver Enrichment",
                 "Gold Merchandising", "Streaming Sales CDC"]
    for pipeline in pipelines:
        # Mock performance based on pipeline type
        base_time = 15 if "Bronze" in pipeline else 30 if "Silver" in pipeline else 45 if "Gold" in pipeline else 5
        pipeline_metrics.append({
            "pipeline": pipeline,
            "avg_runtime": base_time + (hash(pipeline) % 10),
            "success_rate": 0.95 + (hash(pipeline) % 5) / 100,
            "last_runtime": base_time + (hash(pipeline) % 5),
            "status": "Healthy" if hash(pipeline) % 10 != 0 else "Warning"
        })

    # System resources
    system_metrics = {
        "cpu_usage": 65.5,
        "memory_usage": 72.3,
        "disk_usage": 45.8,
        "network_io": 120.5,  # MB/s
        "active_connections": 45,
        "cache_hit_rate": 89.2
    }

    # Real pipeline runs from data service
    pipeline_runs = data_service.get_pipeline_runs()

    return {
        "query_metrics": query_metrics,
        "pipeline_metrics": pipeline_metrics,
        "system_metrics": system_metrics,
        "pipeline_runs": pipeline_runs,
        "timestamp": now
    }


def _render_performance_overview(perf_data: Dict[str, Any]) -> None:
    """Render performance overview cards"""
    st.markdown("#### Performance Overview")

    col1, col2, col3, col4 = st.columns(4)

    # Calculate some metrics
    latest_query = perf_data["query_metrics"][-1] if perf_data["query_metrics"] else {}
    avg_query_time = latest_query.get("avg_query_time", 0)
    query_count = latest_query.get("query_count", 0)

    system = perf_data["system_metrics"]

    with col1:
        st.metric(
            "Avg Query Time",
            f"{avg_query_time:.1f}s",
            delta="-0.3s",
            help="Average query execution time"
        )

    with col2:
        st.metric(
            "Queries/Hour",
            f"{query_count:,}",
            delta="+12",
            help="Number of queries executed per hour"
        )

    with col3:
        st.metric(
            "CPU Usage",
            f"{system['cpu_usage']:.1f}%",
            delta="+2.1%",
            help="Current CPU utilization"
        )

    with col4:
        st.metric(
            "Memory Usage",
            f"{system['memory_usage']:.1f}%",
            delta="-1.5%",
            help="Current memory utilization"
        )


def _render_query_performance(perf_data: Dict[str, Any]) -> None:
    """Render query performance metrics"""
    st.markdown("#### Query Performance")

    query_metrics = perf_data["query_metrics"]
    if not query_metrics:
        st.info("No query performance data available.")
        return

    # Query time trend
    df = pd.DataFrame(query_metrics)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['avg_query_time'],
        mode='lines+markers',
        name='Avg Query Time',
        line=dict(color=config.theme.primary_color, width=2),
        marker=dict(size=4)
    ))

    fig.update_layout(
        title="Query Execution Time Trend",
        xaxis_title="Time",
        yaxis_title="Execution Time (seconds)",
        height=300,
        margin=dict(l=20, r=20, t=40, b=20)
    )

    st.plotly_chart(fig, use_container_width=True)

    # Query statistics
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Query Statistics (Last Hour)**")
        latest = query_metrics[-1]
        st.metric("Total Queries", f"{latest['query_count']:,}")
        st.metric("Slow Queries (>5s)", latest['slow_queries'])
        st.metric("Failed Queries", latest['failed_queries'])

    with col2:
        st.markdown("**Performance Thresholds**")
        st.progress(0.7, text="Query Time: 2.8s (Target: <3s)")
        st.progress(0.95, text="Success Rate: 95% (Target: >95%)")
        st.progress(0.85, text="Cache Hit Rate: 85% (Target: >80%)")


def _render_pipeline_performance(perf_data: Dict[str, Any]) -> None:
    """Render pipeline performance metrics"""
    st.markdown("#### Pipeline Performance")

    pipeline_metrics = perf_data["pipeline_metrics"]
    if not pipeline_metrics:
        st.info("No pipeline performance data available.")
        return

    # Pipeline runtime comparison
    df = pd.DataFrame(pipeline_metrics)

    fig = px.bar(
        df,
        x='pipeline',
        y='avg_runtime',
        color='status',
        color_discrete_map={'Healthy': 'green',
                            'Warning': 'orange', 'Error': 'red'},
        title="Pipeline Average Runtime"
    )

    fig.update_layout(
        xaxis_title="Pipeline",
        yaxis_title="Runtime (minutes)",
        height=300,
        margin=dict(l=20, r=20, t=40, b=20)
    )

    st.plotly_chart(fig, use_container_width=True)

    # Pipeline health status
    st.markdown("**Pipeline Health Status**")
    cols = st.columns(len(pipeline_metrics))

    for i, pipeline in enumerate(pipeline_metrics):
        with cols[i]:
            status_color = {
                "Healthy": "🟢",
                "Warning": "🟡",
                "Error": "🔴"
            }.get(pipeline["status"], "⚪")

            st.metric(
                pipeline["pipeline"].split()[0],  # Short name
                f"{status_color} {pipeline['status']}",
                f"{pipeline['last_runtime']}min"
            )


def _render_system_resources(perf_data: Dict[str, Any]) -> None:
    """Render system resource usage"""
    st.markdown("#### System Resources")

    system = perf_data["system_metrics"]

    # Resource usage bars
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Resource Utilization**")

        # CPU
        cpu_percent = system['cpu_usage'] / 100
        st.progress(cpu_percent, text=f"CPU: {system['cpu_usage']:.1f}%")

        # Memory
        mem_percent = system['memory_usage'] / 100
        st.progress(mem_percent, text=f"Memory: {system['memory_usage']:.1f}%")

        # Disk
        disk_percent = system['disk_usage'] / 100
        st.progress(disk_percent, text=f"Disk: {system['disk_usage']:.1f}%")

    with col2:
        st.markdown("**System Metrics**")

        # Network I/O
        st.metric("Network I/O", f"{system['network_io']:.1f} MB/s")

        # Active Connections
        st.metric("Active Connections", f"{system['active_connections']:,}")

        # Cache Hit Rate
        cache_percent = system['cache_hit_rate'] / 100
        st.progress(cache_percent,
                    text=f"Cache Hit Rate: {system['cache_hit_rate']:.1f}%")


def _render_performance_trends(perf_data: Dict[str, Any]) -> None:
    """Render performance trends over time"""
    st.markdown("#### Performance Trends")

    query_metrics = perf_data["query_metrics"]
    if not query_metrics:
        st.info("No performance trend data available.")
        return

    # Create trend data
    df = pd.DataFrame(query_metrics)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Multi-metric trend chart
    fig = go.Figure()

    # Query count
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['query_count'],
        mode='lines',
        name='Query Count',
        line=dict(color=config.theme.primary_color),
        yaxis='y1'
    ))

    # Query time
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['avg_query_time'],
        mode='lines',
        name='Avg Query Time',
        line=dict(color=config.theme.secondary_color),
        yaxis='y2'
    ))

    fig.update_layout(
        title="Query Performance Trends",
        xaxis=dict(title="Time"),
        yaxis=dict(
            title="Query Count",
            tickfont=dict(color=config.theme.primary_color)
        ),
        yaxis2=dict(
            title="Query Time (s)",
            tickfont=dict(color=config.theme.secondary_color),
            overlaying='y',
            side='right'
        ),
        height=350,
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom",
                    y=1.02, xanchor="right", x=1)
    )

    st.plotly_chart(fig, use_container_width=True)

    # Performance insights
    st.markdown("**Performance Insights**")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.info("✅ Query performance is within acceptable ranges")

    with col2:
        st.warning("⚠️ Peak hours show increased query times")

    with col3:
        st.success("🎯 Cache hit rate improved by 5% this week")
