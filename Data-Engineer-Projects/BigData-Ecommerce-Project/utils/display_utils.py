#!/usr/bin/env python3
"""
Shared display utilities for the E-commerce Pipeline Demo

This module provides common display functionality to avoid code duplication
between demo.py and results_viewer.py.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional


class DisplayColors:
    """Centralized color constants for terminal display"""

    # Basic colors
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'

    # Text formatting
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

    @classmethod
    def disable_colors(cls) -> None:
        """Disable colors for environments that don't support them"""
        for attr_name in dir(cls):
            if not attr_name.startswith('_'):
                attr_value = getattr(cls, attr_name)
                if isinstance(attr_value, str) and attr_value.startswith('\033'):
                    # Keep END reset code but clear others
                    if attr_name != 'END':
                        setattr(cls, attr_name, '')


class TerminalUtils:
    """Terminal display utilities"""

    @staticmethod
    def clear_screen() -> None:
        """Clear the terminal screen"""
        print("\033[2J\033[H", end="")

    @staticmethod
    def format_number(value: float, precision: int = 1) -> str:
        """Format a number with thousand separators and specified precision"""
        try:
            if abs(value) >= 1000:
                return f"{value:,.1f}"
            return f"{value:.{precision}f}"
        except (ValueError, TypeError):
            return str(value)

    @staticmethod
    def format_percentage(value: float, precision: int = 1) -> str:
        """Format a percentage value"""
        try:
            return f"{value * 100:.{precision}f}%"
        except (ValueError, TypeError):
            return "0.0%"


class DataGenerator:
    """Generates realistic sample data for demonstrations"""

    def __init__(self, seed: Optional[int] = None):
        """Initialize with optional random seed for reproducible data"""
        if seed is not None:
            import random
            random.seed(seed)

        self._event_counter = 0
        self._products = [f"product_{i:03d}" for i in range(1, 21)]
        self._stores = [f"store_{i:02d}" for i in range(1, 6)]
        self._warehouses = [f"warehouse_{i}" for i in range(1, 4)]
        self._users = [f"user_{i:06d}" for i in range(1, 1001)]

    def generate_sample_events(self, count: int = 3) -> List[Dict[str, Any]]:
        """Generate realistic sample events"""
        events = []

        for i in range(count):
            self._event_counter += 1

            # Generate different event types with realistic distribution
            event_type = self._get_weighted_event_type()

            if event_type == "click":
                event = self._generate_click_event()
            elif event_type == "transaction":
                event = self._generate_transaction_event()
            else:
                event = self._generate_stock_event()

            events.append(event)

        return events

    def _get_weighted_event_type(self) -> str:
        """Get event type based on realistic weights"""
        import random
        return random.choices(
            ["click", "transaction", "stock"],
            # 60% clicks, 25% transactions, 15% stock
            weights=[0.6, 0.25, 0.15]
        )[0]

    def _generate_click_event(self) -> Dict[str, Any]:
        """Generate a realistic click event"""
        import random
        return {
            "event_type": "click",
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(1, 10))).isoformat(),
            "user_id": random.choice(self._users),
            "session_id": f"session_{random.randint(1, 1000)}",
            "product_id": random.choice(self._products),
            "page_type": random.choice(["product", "category", "search"]),
            "user_agent": "DemoBrowser/1.0"
        }

    def _generate_transaction_event(self) -> Dict[str, Any]:
        """Generate a realistic transaction event"""
        import random
        product_id = random.choice(self._products)
        store_id = random.choice(self._stores)
        quantity = random.randint(1, 5)
        price = round(random.uniform(10.0, 500.0), 2)

        return {
            "event_type": "transaction",
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(1, 5))).isoformat(),
            "transaction_id": f"txn_{random.randint(100000, 999999)}",
            "product_id": product_id,
            "store_id": store_id,
            "quantity": quantity,
            "unit_price": price,
            "total_amount": round(quantity * price, 2),
            "currency": "USD",
            "payment_method": random.choice(["credit_card", "paypal", "apple_pay"])
        }

    def _generate_stock_event(self) -> Dict[str, Any]:
        """Generate a realistic stock event"""
        import random
        product_id = random.choice(self._products)
        warehouse_id = random.choice(self._warehouses)
        quantity_change = random.randint(-10, 20)

        return {
            "event_type": "stock",
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(1, 3))).isoformat(),
            "product_id": product_id,
            "warehouse_id": warehouse_id,
            "quantity_change": quantity_change,
            "reason": random.choice(["restock", "sale", "return", "adjustment", "damaged"])
        }

    def generate_sample_metrics(self) -> Dict[str, Any]:
        """Generate realistic sample metrics"""
        import random

        # Base values with some randomization
        base_events = 15420 + random.randint(-100, 100)
        base_views = 8930 + random.randint(-50, 50)
        base_sales = 2340 + random.randint(-20, 20)
        base_stock = 4150 + random.randint(-30, 30)

        return {
            "total_events_processed": base_events,
            "events_per_second": round(random.uniform(8.0, 9.0), 1),
            "total_views": base_views,
            "total_sales": base_sales,
            "total_stock_updates": base_stock,
            # 1.5-2.5% error rate
            "error_rate": round(random.uniform(0.015, 0.025), 3),
            "uptime": "99.8%"
        }


class MetricsCalculator:
    """Safe metrics calculations with error handling"""

    @staticmethod
    def calculate_events_per_second(events_processed: int, start_time: datetime) -> float:
        """Safely calculate events per second"""
        try:
            if events_processed <= 0 or start_time is None:
                return 0.0

            elapsed_seconds = (datetime.now() - start_time).total_seconds()
            if elapsed_seconds <= 0:
                return 0.0

            return events_processed / elapsed_seconds
        except (AttributeError, TypeError, ZeroDivisionError):
            return 0.0

    @staticmethod
    def calculate_success_rate(total_events: int, invalid_events: int) -> float:
        """Safely calculate success rate percentage"""
        try:
            if total_events <= 0:
                return 100.0

            return ((total_events - invalid_events) / total_events) * 100
        except (TypeError, ZeroDivisionError):
            return 100.0

    @staticmethod
    def validate_metrics_data(metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and sanitize metrics data"""
        if not isinstance(metrics, dict):
            return {}

        validated = {}
        expected_fields = {
            "total_events_processed": 0,
            "events_per_second": 0.0,
            "total_views": 0,
            "total_sales": 0,
            "total_stock_updates": 0,
            "error_rate": 0.0,
            "uptime": "0%"
        }

        for field, default_value in expected_fields.items():
            value = metrics.get(field, default_value)
            # Type coercion with validation
            try:
                if isinstance(default_value, int):
                    validated[field] = max(0, int(value))
                elif isinstance(default_value, float):
                    validated[field] = max(0.0, float(value))
                else:
                    validated[field] = str(value)
            except (ValueError, TypeError):
                validated[field] = default_value

        return validated


class DisplayConfig:
    """Configuration for display formatting"""

    def __init__(self):
        self.show_colors = self._detect_color_support()
        self.refresh_interval = 10  # seconds
        self.max_events_display = 5
        self.number_format_precision = 1

    def _detect_color_support(self) -> bool:
        """Detect if terminal supports colors"""
        # Check environment variables
        if os.environ.get('NO_COLOR') or os.environ.get('TERM') == 'dumb':
            return False

        # Check if colors are disabled
        if not self._colors_enabled():
            return False

        return True

    def _colors_enabled(self) -> bool:
        """Check if colors should be enabled"""
        try:
            # Test if we can write color codes
            if not sys.stdout.isatty():
                return False
            return True
        except (AttributeError, OSError):
            return False


# Global instances for reuse
_data_generator = DataGenerator()
_display_config = DisplayConfig()


def get_data_generator() -> DataGenerator:
    """Get the global data generator instance"""
    return _data_generator


def get_display_config() -> DisplayConfig:
    """Get the global display configuration"""
    return _display_config


def reset_data_generator(seed: Optional[int] = None) -> None:
    """Reset the global data generator with optional seed"""
    global _data_generator
    _data_generator = DataGenerator(seed)
