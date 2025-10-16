"""
Load testing for Global Electronics Lakehouse Dashboard
Tests Streamlit app performance under load
"""

import time
from locust import HttpUser, task, between
import json


class DashboardUser(HttpUser):
    """Simulates user interactions with the Streamlit dashboard"""

    wait_time = between(1, 3)  # Wait 1-3 seconds between tasks

    @task(3)
    def load_main_page(self):
        """Load the main dashboard page"""
        with self.client.get("/", catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(
                    f"Failed to load main page: {response.status_code}")

    @task(2)
    def switch_to_inventory_view(self):
        """Simulate switching to inventory view"""
        # Streamlit uses WebSocket for interactions, but we can test page loads
        # In a real scenario, this would involve more complex interaction simulation
        with self.client.get("/", catch_response=True) as response:
            if response.status_code == 200:
                # Check if page contains inventory-related content
                if "Inventory Management" in response.text:
                    response.success()
                else:
                    response.failure("Inventory view not loaded properly")
            else:
                response.failure(
                    f"Failed to load inventory view: {response.status_code}")

    @task(2)
    def switch_to_sales_view(self):
        """Simulate switching to sales analytics view"""
        with self.client.get("/", catch_response=True) as response:
            if response.status_code == 200:
                if "Sales Analytics" in response.text:
                    response.success()
                else:
                    response.failure("Sales view not loaded properly")
            else:
                response.failure(
                    f"Failed to load sales view: {response.status_code}")

    @task(1)
    def switch_to_suppliers_view(self):
        """Simulate switching to supplier network view"""
        with self.client.get("/", catch_response=True) as response:
            if response.status_code == 200:
                if "Supplier Network" in response.text:
                    response.success()
                else:
                    response.failure("Suppliers view not loaded properly")
            else:
                response.failure(
                    f"Failed to load suppliers view: {response.status_code}")

    @task(1)
    def test_data_refresh(self):
        """Test data refresh functionality"""
        # Simulate a data refresh by loading the page multiple times
        for _ in range(3):
            with self.client.get("/", catch_response=True) as response:
                if response.status_code != 200:
                    response.failure(
                        f"Data refresh failed: {response.status_code}")
                    break
            time.sleep(0.5)
        else:
            response.success()


class DataServiceLoadTest(HttpUser):
    """Load test for data service operations (if API endpoints exist)"""

    wait_time = between(0.5, 2)

    @task
    def test_data_queries(self):
        """Test data service query performance"""
        # This would test actual API endpoints if they existed
        # For now, we test the Streamlit app as a proxy
        with self.client.get("/", catch_response=True) as response:
            if response.status_code == 200:
                # Check response time for data loading
                if response.elapsed.total_seconds() < 5.0:  # 5 second threshold
                    response.success()
                else:
                    response.failure(
                        f"Response too slow: {response.elapsed.total_seconds()}s")
            else:
                response.failure(f"Data query failed: {response.status_code}")
