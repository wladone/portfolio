"""
Data service layer with caching, validation, and error handling
"""

import logging
from typing import Dict, List, Optional, Any, Union
from functools import lru_cache
import time
import functools
from datetime import datetime, timedelta
import os
import smtplib
from email.mime.text import MIMEText
import requests

from databricks import sql

from .models import (
    InventoryItem, SalesData, Supplier, PipelineRun, CategoryPerformance,
    SalesTrendPoint, InventoryList, SalesDataList, SupplierList,
    PipelineRunList, CategoryPerformanceList, SalesTrendData,
    TimeframeType, ValidationError
)
from ..config import config

logger = logging.getLogger(__name__)


class DataServiceError(Exception):
    """Base exception for data service errors"""
    pass


class DataNotFoundError(DataServiceError):
    """Exception raised when requested data is not found"""
    pass


class DataValidationError(DataServiceError):
    """Exception raised when data validation fails"""
    pass


def retry_with_backoff(max_retries: int = 3, backoff_factor: float = 2.0, exceptions: tuple = (Exception,)):
    """Decorator to retry functions with exponential backoff."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(__name__)
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        wait_time = backoff_factor ** attempt
                        logger.warning(
                            f"Attempt {attempt + 1} failed for {func.__name__}: {e}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        logger.error(
                            f"All {max_retries + 1} attempts failed for {func.__name__}: {e}")
                        raise e
            raise last_exception
        return wrapper
    return decorator


def send_alert(message: str, level: str = "ERROR"):
    """Send alerts via logging, email, and webhook if configured."""
    logger = logging.getLogger(__name__)

    # Structured logging
    structured_msg = f"STRUCTURED_ALERT: {{\"level\": \"{level}\", \"message\": \"{message}\", \"timestamp\": \"{datetime.utcnow().isoformat()}\"}}"
    logger.info(structured_msg)

    # Email alert if configured
    email_config = getattr(config, 'alerting', {}).get('email', {})
    if email_config.get('enabled', False):
        try:
            msg = MIMEText(message)
            msg['Subject'] = f"Data Service Alert - {level}"
            msg['From'] = email_config.get('from', 'alerts@lakehouse.com')
            msg['To'] = email_config.get('to', '')

            server = smtplib.SMTP(email_config.get(
                'smtp_server', 'localhost'), email_config.get('smtp_port', 25))
            if email_config.get('use_tls', False):
                server.starttls()
            if email_config.get('username'):
                server.login(email_config['username'],
                             email_config.get('password', ''))
            server.sendmail(msg['From'], [msg['To']], msg.as_string())
            server.quit()
            logger.info("Email alert sent successfully")
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")

    # Webhook alert if configured
    webhook_config = getattr(config, 'alerting', {}).get('webhook', {})
    if webhook_config.get('enabled', False):
        try:
            payload = {
                "level": level,
                "message": message,
                "timestamp": datetime.utcnow().isoformat()
            }
            response = requests.post(
                webhook_config['url'], json=payload, timeout=5)
            response.raise_for_status()
            logger.info("Webhook alert sent successfully")
        except Exception as e:
            logger.error(f"Failed to send webhook alert: {e}")


class DataService:
    """Centralized data service with caching and error handling"""

    def __init__(self, cache_timeout: int = 300):
        self.cache_timeout = cache_timeout
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._connection = None
        self._setup_connection()

    def _setup_connection(self) -> None:
        """Setup connection - for local demo, we'll use mock data instead of Databricks"""
        try:
            # For local demo, we'll use mock data instead of connecting to Databricks
            # Check if we have environment variables for Databricks
            if (os.getenv('DATABRICKS_HOST') and os.getenv('DATABRICKS_TOKEN') and os.getenv('DATABRICKS_HTTP_PATH')):
                logger.info(
                    "Databricks credentials found, attempting connection...")
                self._connection = sql.connect(
                    server_hostname=config.databricks.host,
                    http_path=config.databricks.http_path,
                    access_token=config.databricks.token,
                )
                logger.info("Databricks connection established successfully")
            else:
                logger.info(
                    "No Databricks credentials found, using mock data for local demo")
                self._connection = None  # Will use mock data

        except Exception as e:
            logger.warning(
                f"Failed to connect to Databricks, falling back to mock data: {e}")
            self._connection = None  # Will use mock data

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache entry is still valid"""
        if cache_key not in self._cache:
            return False

        cache_entry = self._cache[cache_key]
        return time.time() - cache_entry['timestamp'] < self.cache_timeout

    def _get_cached(self, cache_key: str) -> Optional[Any]:
        """Get data from cache if valid"""
        if self._is_cache_valid(cache_key):
            logger.debug(f"Cache hit for {cache_key}")
            return self._cache[cache_key]['data']
        return None

    def _set_cached(self, cache_key: str, data: Any) -> None:
        """Store data in cache"""
        self._cache[cache_key] = {
            'data': data,
            'timestamp': time.time()
        }
        logger.debug(f"Cached data for {cache_key}")

    @retry_with_backoff(max_retries=3, backoff_factor=2.0)
    def get_inventory_data(self) -> InventoryList:
        """Get all inventory data from Databricks or mock data"""
        cache_key = "inventory_data"

        cached_data = self._get_cached(cache_key)
        if cached_data is not None:
            return cached_data

        # If no Databricks connection, return mock data
        if self._connection is None:
            logger.info("Using mock inventory data for local demo")
            mock_inventory = [
                InventoryItem(
                    product_id="PROD001",
                    product_name="Wireless Headphones",
                    warehouse_code="BUC",
                    quantity_on_hand=150,
                    unit_cost_euro=89.99,
                    category="Electronics"
                ),
                InventoryItem(
                    product_id="PROD002",
                    product_name="Smart Watch",
                    warehouse_code="FRA",
                    quantity_on_hand=75,
                    unit_cost_euro=299.99,
                    category="Electronics"
                ),
                InventoryItem(
                    product_id="PROD003",
                    product_name="Bluetooth Speaker",
                    warehouse_code="MAD",
                    quantity_on_hand=200,
                    unit_cost_euro=49.99,
                    category="Electronics"
                ),
            ]
            self._set_cached(cache_key, mock_inventory)
            return mock_inventory

        try:
            logger.info("Querying inventory data from Databricks")
            with self._connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT product_id, product_name, warehouse_code,
                           quantity_on_hand, unit_cost_euro, category
                    FROM {config.databricks.catalog}.{config.databricks.schema}.silver_inventory
                    WHERE inventory_date = (SELECT MAX(inventory_date) FROM {config.databricks.catalog}.{config.databricks.schema}.silver_inventory)
                """)
                rows = cursor.fetchall()

                inventory_items = []
                for row in rows:
                    try:
                        item = InventoryItem(
                            product_id=row[0],
                            product_name=row[1],
                            warehouse_code=row[2],
                            quantity_on_hand=int(row[3]),
                            unit_cost_euro=float(row[4]),
                            category=row[5]
                        )
                        inventory_items.append(item)
                    except ValidationError as e:
                        logger.warning(
                            f"Skipping invalid inventory item {row[0]}: {e}")

                logger.info(
                    f"Retrieved {len(inventory_items)} inventory items")
                self._set_cached(cache_key, inventory_items)
                return inventory_items

        except Exception as e:
            error_msg = f"Failed to query inventory data: {e}"
            logger.error(error_msg)
            send_alert(error_msg, "ERROR")
            raise DataServiceError(f"Failed to retrieve inventory data: {e}")

    @retry_with_backoff(max_retries=3, backoff_factor=2.0)
    def get_sales_data(self) -> SalesDataList:
        """Get all sales data from Databricks or mock data"""
        cache_key = "sales_data"

        cached_data = self._get_cached(cache_key)
        if cached_data is not None:
            return cached_data

        # If no Databricks connection, return mock data
        if self._connection is None:
            logger.info("Using mock sales data for local demo")
            from datetime import datetime, timedelta
            mock_sales = []
            base_date = datetime.now() - timedelta(days=30)
            for i in range(30):
                date = (base_date + timedelta(days=i)).strftime('%Y-%m-%d')
                mock_sales.append(SalesData(
                    date=date,
                    revenue=5000 + (i * 100),  # Increasing trend
                    orders=50 + (i * 2),
                    category="Electronics"
                ))
            self._set_cached(cache_key, mock_sales)
            return mock_sales

        try:
            logger.info("Querying sales data from Databricks")
            with self._connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT event_date,
                           SUM(sale_amount_euro) as revenue,
                           SUM(quantity) as orders,
                           'Electronics' as category
                    FROM {config.databricks.catalog}.{config.databricks.schema}.silver_sales_enriched
                    GROUP BY event_date
                    ORDER BY event_date DESC
                    LIMIT 30
                """)
                rows = cursor.fetchall()

                sales_data = []
                for row in rows:
                    try:
                        item = SalesData(
                            date=row[0],
                            revenue=float(row[1]),
                            orders=int(row[2]),
                            category=row[3]
                        )
                        sales_data.append(item)
                    except ValidationError as e:
                        logger.warning(
                            f"Skipping invalid sales data for {row[0]}: {e}")

                logger.info(f"Retrieved {len(sales_data)} sales data points")
                self._set_cached(cache_key, sales_data)
                return sales_data

        except Exception as e:
            error_msg = f"Failed to query sales data: {e}"
            logger.error(error_msg)
            send_alert(error_msg, "ERROR")
            raise DataServiceError(f"Failed to retrieve sales data: {e}")

    @retry_with_backoff(max_retries=3, backoff_factor=2.0)
    def get_suppliers(self) -> SupplierList:
        """Get all supplier data from Databricks or mock data"""
        cache_key = "suppliers"

        cached_data = self._get_cached(cache_key)
        if cached_data is not None:
            return cached_data

        # If no Databricks connection, return mock data
        if self._connection is None:
            logger.info("Using mock suppliers data for local demo")
            mock_suppliers = [
                Supplier(
                    supplier_id="SUP001",
                    supplier_name="TechCorp Electronics",
                    rating=4.5,
                    lead_time_days=7,
                    category_focus=["Electronics", "Audio"]
                ),
                Supplier(
                    supplier_id="SUP002",
                    supplier_name="Global Components Ltd",
                    rating=4.2,
                    lead_time_days=10,
                    category_focus=["Electronics", "Accessories"]
                ),
                Supplier(
                    supplier_id="SUP003",
                    supplier_name="Premium Audio Systems",
                    rating=4.8,
                    lead_time_days=5,
                    category_focus=["Audio", "Speakers"]
                ),
            ]
            self._set_cached(cache_key, mock_suppliers)
            return mock_suppliers

        try:
            logger.info("Querying suppliers data from Databricks")
            with self._connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT supplier_id, supplier_name, rating, lead_time_days, preferred, contact_email, category_focus
                    FROM {config.databricks.catalog}.{config.databricks.schema}.silver_suppliers
                """)
                rows = cursor.fetchall()

                suppliers = []
                for row in rows:
                    try:
                        item = Supplier(
                            supplier_id=row[0],
                            supplier_name=row[1],
                            rating=float(row[2]),
                            lead_time_days=int(row[3]),
                            category_focus=row[6] if row[6] else []
                        )
                        suppliers.append(item)
                    except ValidationError as e:
                        logger.warning(
                            f"Skipping invalid supplier {row[0]}: {e}")

                logger.info(f"Retrieved {len(suppliers)} suppliers")
                self._set_cached(cache_key, suppliers)
                return suppliers

        except Exception as e:
            error_msg = f"Failed to query suppliers data: {e}"
            logger.error(error_msg)
            send_alert(error_msg, "ERROR")
            raise DataServiceError(f"Failed to retrieve suppliers data: {e}")

    def get_pipeline_runs(self) -> PipelineRunList:
        """Get pipeline run data - using mock data for operational status"""
        # Pipeline status is operational data not stored in lakehouse tables
        # Keeping mock data for demonstration
        return [
            PipelineRun("Bronze Layer Ingest", "Healthy",
                        "5 min ago", "15 min"),
            PipelineRun("Silver Enrichment", "Healthy",
                        "12 min ago", "30 min"),
            PipelineRun("Gold Merchandising", "Warning",
                        "27 min ago", "30 min"),
            PipelineRun("Streaming Sales CDC", "Healthy", "live", "Real-time"),
        ]

    @retry_with_backoff(max_retries=3, backoff_factor=2.0)
    def get_category_performance(self) -> CategoryPerformanceList:
        """Get category performance data from Databricks or mock data"""
        cache_key = "category_performance"

        cached_data = self._get_cached(cache_key)
        if cached_data is not None:
            return cached_data

        # If no Databricks connection, return mock data
        if self._connection is None:
            logger.info("Using mock category performance data for local demo")
            mock_categories = [
                CategoryPerformance(
                    category="Electronics",
                    share=85.0,
                    revenue=150000.0,
                    growth=12.5,
                    margin=0.25
                ),
                CategoryPerformance(
                    category="Audio",
                    share=10.0,
                    revenue=18000.0,
                    growth=8.3,
                    margin=0.30
                ),
                CategoryPerformance(
                    category="Accessories",
                    share=5.0,
                    revenue=9000.0,
                    growth=15.2,
                    margin=0.35
                ),
            ]
            self._set_cached(cache_key, mock_categories)
            return mock_categories

        try:
            logger.info("Querying category performance from Databricks")
            with self._connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT category,
                           COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as share,
                           SUM(sale_amount_euro) as revenue,
                           0.0 as growth,  -- Placeholder, would need historical comparison
                           0.2 as margin   -- Placeholder, would need cost data
                    FROM {config.databricks.catalog}.{config.databricks.schema}.silver_sales_enriched
                    GROUP BY category
                    ORDER BY revenue DESC
                """)
                rows = cursor.fetchall()

                categories = []
                for row in rows:
                    try:
                        item = CategoryPerformance(
                            category=row[0],
                            share=float(row[1]),
                            revenue=float(row[2]),
                            growth=float(row[3]),
                            margin=float(row[4])
                        )
                        categories.append(item)
                    except ValidationError as e:
                        logger.warning(
                            f"Skipping invalid category {row[0]}: {e}")

                logger.info(
                    f"Retrieved {len(categories)} category performance records")
                self._set_cached(cache_key, categories)
                return categories

        except Exception as e:
            error_msg = f"Failed to query category performance: {e}"
            logger.error(error_msg)
            send_alert(error_msg, "ERROR")
            raise DataServiceError(
                f"Failed to retrieve category performance: {e}")

    @lru_cache(maxsize=32)
    def get_sales_trend(self, timeframe: TimeframeType) -> SalesTrendData:
        """
        Generate sales trend data for given timeframe with caching

        Args:
            timeframe: Time period ("7d", "30d", "90d")

        Returns:
            List of sales trend points
        """
        cache_key = f"sales_trend_{timeframe}"

        # Check cache first
        cached_data = self._get_cached(cache_key)
        if cached_data is not None:
            return cached_data

        try:
            # Map timeframe to days
            days_map = {"7d": 7, "30d": 30, "90d": 90}
            num_days = days_map[timeframe]

            # Generate trend data
            trend_data = self._generate_sales_trend(num_days)

            # Cache the result
            self._set_cached(cache_key, trend_data)

            return trend_data

        except Exception as e:
            logger.error(f"Error generating sales trend for {timeframe}: {e}")
            raise DataServiceError(f"Failed to generate sales trend: {e}")

    @retry_with_backoff(max_retries=3, backoff_factor=2.0)
    def _generate_sales_trend(self, num_days: int) -> SalesTrendData:
        """Generate sales trend data from recent sales history or mock data"""
        # If no Databricks connection, return mock data
        if self._connection is None:
            logger.info(f"Using mock sales trend data for {num_days} days")
            trend_points = []
            for i in range(num_days):
                date = (datetime.now() - timedelta(days=num_days-i-1)
                        ).strftime("%b %d")
                # Create a realistic trend with some variation
                base_revenue = 5000 + (i * 50)
                variation = (i % 7) * 200  # Weekly pattern
                revenue = base_revenue + variation
                orders = int(revenue / 100)  # Roughly 1 order per 100 euros
                trend_points.append(SalesTrendPoint(
                    date=date,
                    revenue=revenue,
                    orders=orders
                ))
            return trend_points

        try:
            logger.info(f"Generating sales trend for {num_days} days")
            with self._connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT event_date,
                           SUM(sale_amount_euro) as revenue,
                           SUM(quantity) as orders
                    FROM {config.databricks.catalog}.{config.databricks.schema}.silver_sales_enriched
                    WHERE event_date >= DATE_SUB(CURRENT_DATE(), {num_days})
                    GROUP BY event_date
                    ORDER BY event_date DESC
                """)
                rows = cursor.fetchall()

                trend_points = []
                for row in rows:
                    trend_points.append(SalesTrendPoint(
                        date=datetime.strptime(
                            row[0], '%Y-%m-%d').strftime("%b %d"),
                        revenue=float(row[1]),
                        orders=int(row[2])
                    ))

                # If we don't have enough data, fill with zeros
                while len(trend_points) < num_days:
                    trend_points.append(SalesTrendPoint(
                        date=(
                            datetime.now() - timedelta(days=len(trend_points))).strftime("%b %d"),
                        revenue=0,
                        orders=0
                    ))

                return trend_points[-num_days:]  # Return last num_days

        except Exception as e:
            error_msg = f"Failed to generate sales trend: {e}"
            logger.error(error_msg)
            send_alert(error_msg, "ERROR")
            # Fallback to empty trend
            return [SalesTrendPoint(date=(datetime.now() - timedelta(days=i)).strftime("%b %d"),
                                    revenue=0, orders=0) for i in range(num_days)]

    def get_inventory_summary(self) -> Dict[str, Any]:
        """Get comprehensive inventory summary with caching"""
        cache_key = "inventory_summary"

        cached_data = self._get_cached(cache_key)
        if cached_data is not None:
            return cached_data

        try:
            inventory = self.get_inventory_data()

            if not inventory:
                return {
                    "total_units": 0,
                    "total_value": 0.0,
                    "by_warehouse": [],
                    "by_category": [],
                    "top_skus": [],
                    "reorder_alerts": []
                }

            # Calculate totals
            total_units = sum(item.quantity_on_hand for item in inventory)
            total_value = sum(item.total_value for item in inventory)

            # Group by warehouse
            warehouse_data = {}
            for item in inventory:
                if item.warehouse_code not in warehouse_data:
                    warehouse_data[item.warehouse_code] = {
                        "units": 0, "value": 0.0}
                warehouse_data[item.warehouse_code]["units"] += item.quantity_on_hand
                warehouse_data[item.warehouse_code]["value"] += item.total_value

            by_warehouse = [
                {"warehouse": wh,
                    "units": data["units"], "value": data["value"]}
                for wh, data in warehouse_data.items()
            ]

            # Group by category
            category_data = {}
            for item in inventory:
                if item.category not in category_data:
                    category_data[item.category] = {"units": 0, "value": 0.0}
                category_data[item.category]["units"] += item.quantity_on_hand
                category_data[item.category]["value"] += item.total_value

            by_category = [
                {"category": cat,
                    "units": data["units"], "value": data["value"]}
                for cat, data in category_data.items()
            ]

            # Sort by value descending
            by_category.sort(key=lambda x: x["value"], reverse=True)

            # Top SKUs by value
            top_skus = sorted(
                inventory, key=lambda x: x.total_value, reverse=True)

            # Reorder alerts
            reorder_alerts = [
                item for item in inventory if item.is_reorder_needed]

            summary = {
                "total_units": total_units,
                "total_value": total_value,
                "by_warehouse": by_warehouse,
                "by_category": by_category,
                "top_skus": top_skus,
                "reorder_alerts": reorder_alerts
            }

            self._set_cached(cache_key, summary)
            return summary

        except Exception as e:
            logger.error(f"Error generating inventory summary: {e}")
            raise DataServiceError(
                f"Failed to generate inventory summary: {e}")

    def get_supplier_summary(self) -> Dict[str, Any]:
        """Get comprehensive supplier summary"""
        cache_key = "supplier_summary"

        cached_data = self._get_cached(cache_key)
        if cached_data is not None:
            return cached_data

        try:
            suppliers = self.get_suppliers()

            if not suppliers:
                return {
                    "average_rating": 0.0,
                    "average_lead_time": 0,
                    "focus_areas": [],
                    "top_suppliers": []
                }

            average_rating = sum(s.rating for s in suppliers) / len(suppliers)
            average_lead_time = sum(
                s.lead_time_days for s in suppliers) / len(suppliers)

            # Collect all focus areas
            focus_areas = set()
            for supplier in suppliers:
                focus_areas.update(supplier.category_focus)
            focus_areas = sorted(list(focus_areas))

            # Top suppliers by rating
            top_suppliers = sorted(
                suppliers, key=lambda x: x.rating, reverse=True)

            summary = {
                "average_rating": average_rating,
                "average_lead_time": average_lead_time,
                "focus_areas": focus_areas,
                "top_suppliers": top_suppliers
            }

            self._set_cached(cache_key, summary)
            return summary

        except Exception as e:
            logger.error(f"Error generating supplier summary: {e}")
            raise DataServiceError(f"Failed to generate supplier summary: {e}")

    def clear_cache(self) -> None:
        """Clear all cached data"""
        self._cache.clear()
        logger.info("Cache cleared")

    def get_system_alerts(self) -> List[Dict[str, Any]]:
        """Get recent system alerts - mock implementation for demonstration"""
        # In a real implementation, this would query an alerts table or monitoring system
        # For now, return mock alerts based on system state
        alerts = []

        # Check pipeline health
        pipeline_runs = self.get_pipeline_runs()
        for pipeline in pipeline_runs:
            if pipeline.status != "Healthy":
                alerts.append({
                    "id": f"pipeline_{pipeline.name.lower().replace(' ', '_')}",
                    "timestamp": datetime.now() - timedelta(minutes=30),
                    "level": "WARNING" if pipeline.status == "Warning" else "ERROR",
                    "message": f"Pipeline '{pipeline.name}' is in {pipeline.status} state",
                    "source": "Pipeline Monitor",
                    "status": "Active",
                    "details": {
                        "pipeline": pipeline.name,
                        "last_run": pipeline.last_run,
                        "status": pipeline.status,
                        "sla": pipeline.sla
                    }
                })

        # System resource alerts (mock)
        # In real implementation, this would check actual system metrics
        alerts.append({
            "id": "system_memory_high",
            "timestamp": datetime.now() - timedelta(hours=1),
            "level": "WARNING",
            "message": "High memory usage detected on worker node",
            "source": "System Monitor",
            "status": "Active",
            "details": {
                "metric": "memory_usage",
                "value": "85%",
                "threshold": "80%",
                "node": "worker-01"
            }
        })

        # Data quality alerts
        inventory_summary = self.get_inventory_summary()
        reorder_alerts = inventory_summary.get("reorder_alerts", [])
        if reorder_alerts:
            alerts.append({
                "id": "inventory_reorder_alert",
                "timestamp": datetime.now() - timedelta(hours=2),
                "level": "WARNING",
                "message": f"{len(reorder_alerts)} products require reorder",
                "source": "Inventory Monitor",
                "status": "Active",
                "details": {
                    "alert_count": len(reorder_alerts),
                    "products": [alert.product_name for alert in reorder_alerts[:3]]
                }
            })

        # Sort by timestamp descending
        alerts.sort(key=lambda x: x['timestamp'], reverse=True)

        return alerts

    def invalidate_cache(self, pattern: str = "*") -> None:
        """Invalidate cache entries matching pattern"""
        keys_to_remove = [
            k for k in self._cache.keys() if pattern in k or pattern == "*"]
        for key in keys_to_remove:
            del self._cache[key]
        logger.info(f"Invalidated {len(keys_to_remove)} cache entries")
