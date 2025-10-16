"""
Data models for the dashboard with validation and type safety
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Literal
from datetime import datetime
import re


class ValidationError(Exception):
    """Custom exception for data validation errors"""
    pass


@dataclass(frozen=True)
class InventoryItem:
    """Represents an inventory item with validation"""
    product_id: str
    product_name: str
    warehouse_code: str
    quantity_on_hand: int
    unit_cost_euro: float
    category: str

    def __post_init__(self):
        """Validate data after initialization"""
        if not self.product_id or not isinstance(self.product_id, str):
            raise ValidationError("product_id must be a non-empty string")

        if not self.product_name or not isinstance(self.product_name, str):
            raise ValidationError("product_name must be a non-empty string")

        if not re.match(r'^[A-Z]{3}$', self.warehouse_code):
            raise ValidationError("warehouse_code must be 3 uppercase letters")

        if self.quantity_on_hand < 0:
            raise ValidationError("quantity_on_hand cannot be negative")

        if self.unit_cost_euro < 0:
            raise ValidationError("unit_cost_euro cannot be negative")

        if not self.category or not isinstance(self.category, str):
            raise ValidationError("category must be a non-empty string")

    @property
    def total_value(self) -> float:
        """Calculate total inventory value"""
        return self.quantity_on_hand * self.unit_cost_euro

    @property
    def is_low_stock(self) -> bool:
        """Check if item is in low stock"""
        return self.quantity_on_hand <= 35

    @property
    def is_reorder_needed(self) -> bool:
        """Check if item needs reordering"""
        return self.quantity_on_hand <= 40

    @property
    def stock_status(self) -> Literal["healthy", "monitor", "reorder"]:
        """Get stock status based on quantity"""
        if self.quantity_on_hand >= 70:
            return "healthy"
        elif self.quantity_on_hand >= 35:
            return "monitor"
        else:
            return "reorder"


@dataclass(frozen=True)
class SalesData:
    """Represents sales data with validation"""
    date: str
    revenue: float
    orders: int
    category: str

    def __post_init__(self):
        """Validate data after initialization"""
        # Validate date format (YYYY-MM-DD)
        try:
            datetime.strptime(self.date, '%Y-%m-%d')
        except ValueError:
            raise ValidationError("date must be in YYYY-MM-DD format")

        if self.revenue < 0:
            raise ValidationError("revenue cannot be negative")

        if self.orders < 0:
            raise ValidationError("orders cannot be negative")

        if not self.category or not isinstance(self.category, str):
            raise ValidationError("category must be a non-empty string")

    @property
    def average_order_value(self) -> float:
        """Calculate average order value"""
        return self.revenue / self.orders if self.orders > 0 else 0.0


@dataclass(frozen=True)
class Supplier:
    """Represents a supplier with validation"""
    supplier_id: str
    supplier_name: str
    rating: float
    lead_time_days: int
    category_focus: List[str]

    def __post_init__(self):
        """Validate data after initialization"""
        if not self.supplier_id or not isinstance(self.supplier_id, str):
            raise ValidationError("supplier_id must be a non-empty string")

        if not self.supplier_name or not isinstance(self.supplier_name, str):
            raise ValidationError("supplier_name must be a non-empty string")

        if not (0 <= self.rating <= 5):
            raise ValidationError("rating must be between 0 and 5")

        if self.lead_time_days < 0:
            raise ValidationError("lead_time_days cannot be negative")

        if not isinstance(self.category_focus, list) or not self.category_focus:
            raise ValidationError("category_focus must be a non-empty list")

        for category in self.category_focus:
            if not isinstance(category, str) or not category:
                raise ValidationError(
                    "all category_focus items must be non-empty strings")

    @property
    def performance_tier(self) -> Literal["excellent", "good", "monitor"]:
        """Get performance tier based on rating"""
        if self.rating >= 4.5:
            return "excellent"
        elif self.rating >= 4.0:
            return "good"
        else:
            return "monitor"


@dataclass(frozen=True)
class PipelineRun:
    """Represents a pipeline run status"""
    name: str
    status: Literal["Healthy", "Warning", "Investigate"]
    last_run: str
    sla: str

    def __post_init__(self):
        """Validate data after initialization"""
        if not self.name or not isinstance(self.name, str):
            raise ValidationError("name must be a non-empty string")

        if self.status not in ["Healthy", "Warning", "Investigate"]:
            raise ValidationError(
                "status must be Healthy, Warning, or Investigate")

        if not self.last_run or not isinstance(self.last_run, str):
            raise ValidationError("last_run must be a non-empty string")

        if not self.sla or not isinstance(self.sla, str):
            raise ValidationError("sla must be a non-empty string")


@dataclass
class CategoryPerformance:
    """Represents category performance metrics"""
    category: str
    share: float
    revenue: float
    growth: float
    margin: float

    def __post_init__(self):
        """Validate data after initialization"""
        if not self.category or not isinstance(self.category, str):
            raise ValidationError("category must be a non-empty string")

        if not (0 <= self.share <= 100):
            raise ValidationError("share must be between 0 and 100")

        if self.revenue < 0:
            raise ValidationError("revenue cannot be negative")

        # Growth and margin can be negative (losses)


@dataclass
class SalesTrendPoint:
    """Represents a point in sales trend data"""
    date: str
    revenue: float
    orders: int

    @property
    def average_order_value(self) -> float:
        """Calculate AOV for this point"""
        return self.revenue / self.orders if self.orders > 0 else 0.0


# Type aliases for better readability
InventoryList = List[InventoryItem]
SalesDataList = List[SalesData]
SupplierList = List[Supplier]
PipelineRunList = List[PipelineRun]
CategoryPerformanceList = List[CategoryPerformance]
SalesTrendData = List[SalesTrendPoint]

TimeframeType = Literal["7d", "30d", "90d"]
ThemeType = Literal["light", "dark"]
TabType = Literal["overview", "inventory", "sales", "suppliers"]
