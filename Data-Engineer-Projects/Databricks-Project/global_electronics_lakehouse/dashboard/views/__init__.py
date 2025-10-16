"""
Dashboard views - modular UI components
"""

from .overview import render_overview
from .inventory import render_inventory
from .sales import render_sales
from .suppliers import render_suppliers
from .logs import render_logs
from .alerts import render_alerts
from .performance import render_performance

__all__ = [
    'render_overview',
    'render_inventory',
    'render_sales',
    'render_suppliers',
    'render_logs',
    'render_alerts',
    'render_performance'
]
