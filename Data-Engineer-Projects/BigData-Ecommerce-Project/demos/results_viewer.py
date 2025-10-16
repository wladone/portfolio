#!/usr/bin/env python3
"""
Simple Results Viewer for E-commerce Streaming Pipeline

This script provides a clean, easy-to-read view of pipeline results
and can display sample data, metrics, and processing status.

Works in tandem with demo.py - run the demo to see real-time data,
or use this viewer anytime for static results display.
"""

import os
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

# Import shared utilities
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    from ..utils.display_utils import (
        DisplayColors,
        TerminalUtils,
        get_data_generator,
        get_display_config,
        MetricsCalculator
    )
except ImportError:
    from utils.display_utils import (
        DisplayColors,
        TerminalUtils,
        get_data_generator,
        get_display_config,
        MetricsCalculator
    )


class ResultsViewer:
    """Simple results viewer for the streaming pipeline"""

    def __init__(self, data_generator: Optional[Any] = None):
        """Initialize with optional data generator"""
        self.data_generator = data_generator or get_data_generator()
        self.display_config = get_display_config()
        self.sample_events = self.data_generator.generate_sample_events(3)
        self.metrics = self.data_generator.generate_sample_metrics()

    def update_data(self, events: Optional[List[Dict[str, Any]]] = None,
                    metrics: Optional[Dict[str, Any]] = None) -> None:
        """Update the viewer with new data"""
        if events is not None:
            self.sample_events = events
        if metrics is not None:
            self.metrics = MetricsCalculator.validate_metrics_data(metrics)

    def show_header(self):
        """Display the results viewer header"""
        if self.display_config.show_colors:
            print(
                f"{DisplayColors.BOLD}{DisplayColors.BLUE}=== E-COMMERCE PIPELINE RESULTS VIEWER ==={DisplayColors.END}")
        else:
            print("=== E-COMMERCE PIPELINE RESULTS VIEWER ===")
        print(
            f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    def show_pipeline_status(self):
        """Display current pipeline status"""
        if self.display_config.show_colors:
            print(f"{DisplayColors.BOLD}Pipeline Status:{DisplayColors.END}")
            print(f"  {DisplayColors.GREEN}* Status: Running{DisplayColors.END}")
            print(f"  {DisplayColors.GREEN}* Mode: Demo Mode{DisplayColors.END}")
            print(
                f"  {DisplayColors.GREEN}* Events/sec: {TerminalUtils.format_number(self.metrics['events_per_second'])}{DisplayColors.END}")
            print(
                f"  {DisplayColors.GREEN}* Uptime: {self.metrics['uptime']}{DisplayColors.END}")
        else:
            print("Pipeline Status:")
            print(f"  * Status: Running")
            print(f"  * Mode: Demo Mode")
            print(
                f"  * Events/sec: {TerminalUtils.format_number(self.metrics['events_per_second'])}")
            print(f"  * Uptime: {self.metrics['uptime']}")
        print()

    def show_key_metrics(self):
        """Display key performance metrics"""
        if self.display_config.show_colors:
            print(f"{DisplayColors.BOLD}Key Metrics:{DisplayColors.END}")

            metrics = self.metrics
            print(
                f"  {DisplayColors.YELLOW}Total Events Processed:{DisplayColors.END} {TerminalUtils.format_number(float(metrics['total_events_processed']))}")
            print(
                f"  {DisplayColors.BLUE}Product Views:{DisplayColors.END} {TerminalUtils.format_number(float(metrics['total_views']))}")
            print(
                f"  {DisplayColors.GREEN}Sales Transactions:{DisplayColors.END} {TerminalUtils.format_number(float(metrics['total_sales']))}")
            print(
                f"  {DisplayColors.RED}Stock Updates:{DisplayColors.END} {TerminalUtils.format_number(float(metrics['total_stock_updates']))}")
            print(
                f"  {DisplayColors.YELLOW}Error Rate:{DisplayColors.END} {TerminalUtils.format_percentage(metrics['error_rate'])}")
        else:
            print("Key Metrics:")

            metrics = self.metrics
            print(
                f"  Total Events Processed: {TerminalUtils.format_number(float(metrics['total_events_processed']))}")
            print(
                f"  Product Views: {TerminalUtils.format_number(float(metrics['total_views']))}")
            print(
                f"  Sales Transactions: {TerminalUtils.format_number(float(metrics['total_sales']))}")
            print(
                f"  Stock Updates: {TerminalUtils.format_number(float(metrics['total_stock_updates']))}")
            print(
                f"  Error Rate: {TerminalUtils.format_percentage(metrics['error_rate'])}")
        print()

    def show_recent_events(self, limit: int = 5):
        """Display recent events processed by the pipeline"""
        if self.display_config.show_colors:
            print(
                f"{DisplayColors.BOLD}Recent Events (Last {limit}):{DisplayColors.END}")
        else:
            print(f"Recent Events (Last {limit}):")

        for i, event in enumerate(self.sample_events[-limit:], 1):
            event_type = event.get('event_type', 'unknown')

            if self.display_config.show_colors:
                color = {
                    'click': DisplayColors.BLUE,
                    'transaction': DisplayColors.GREEN,
                    'stock': DisplayColors.RED
                }.get(event_type, DisplayColors.YELLOW)

                print(f"  {i}. {color}{event_type.upper()}{DisplayColors.END}")
                print(f"     Product ID: {event.get('product_id', 'N/A')}")
                print(f"     Timestamp: {event.get('timestamp', 'N/A')}")
                if event_type == 'transaction':
                    print(f"     Amount: ${event.get('total_amount', 'N/A')}")
                elif event_type == 'stock':
                    qty = event.get('quantity_change', 0)
                    qty_color = DisplayColors.GREEN if qty > 0 else DisplayColors.RED
                    print(
                        f"     Change: {qty_color}{qty:+d}{DisplayColors.END}")
            else:
                print(f"  {i}. {event_type.upper()}")
                print(f"     Product ID: {event.get('product_id', 'N/A')}")
                print(f"     Timestamp: {event.get('timestamp', 'N/A')}")
                if event_type == 'transaction':
                    print(f"     Amount: ${event.get('total_amount', 'N/A')}")
                elif event_type == 'stock':
                    qty = event.get('quantity_change', 0)
                    print(f"     Change: {qty:+d}")
            print()

    def show_data_quality(self):
        """Display data quality metrics"""
        if self.display_config.show_colors:
            print(f"{DisplayColors.BOLD}Data Quality:{DisplayColors.END}")
            print(f"  {DisplayColors.GREEN}* Valid Events: 98%{DisplayColors.END}")
            print(
                f"  {DisplayColors.YELLOW}* Schema Errors: 1.5%{DisplayColors.END}")
            print(f"  {DisplayColors.RED}* Invalid Data: 0.5%{DisplayColors.END}")
        else:
            print("Data Quality:")
            print("  * Valid Events: 98%")
            print("  * Schema Errors: 1.5%")
            print("  * Invalid Data: 0.5%")
        print()

    def show_sample_outputs(self):
        """Show sample outputs that would be written to BigQuery/Bigtable"""
        if self.display_config.show_colors:
            print(f"{DisplayColors.BOLD}Sample Outputs:{DisplayColors.END}")

            # Sample BigQuery view aggregation
            print(f"  {DisplayColors.BLUE}BigQuery Views Table:{DisplayColors.END}")
            print(f"    product_005: 1,247 views (last 60s)")
            print(f"    product_003: 892 views (last 60s)")
            print(f"    product_007: 634 views (last 60s)")
            print()

            # Sample BigQuery sales aggregation
            print(
                f"  {DisplayColors.GREEN}BigQuery Sales Table:{DisplayColors.END}")
            print(f"    store_02: $2,847.50 (last 5min)")
            print(f"    store_01: $1,923.75 (last 5min)")
            print(f"    store_03: $1,456.20 (last 5min)")
            print()

            # Sample Bigtable view counters
            print(f"  {DisplayColors.YELLOW}Bigtable Counters:{DisplayColors.END}")
            print(f"    product_005: 15,234 total views")
            print(f"    product_003: 12,891 total views")
            print(f"    product_007: 8,567 total views")
        else:
            print("Sample Outputs:")

            # Sample BigQuery view aggregation
            print("  BigQuery Views Table:")
            print(f"    product_005: 1,247 views (last 60s)")
            print(f"    product_003: 892 views (last 60s)")
            print(f"    product_007: 634 views (last 60s)")
            print()

            # Sample BigQuery sales aggregation
            print("  BigQuery Sales Table:")
            print(f"    store_02: $2,847.50 (last 5min)")
            print(f"    store_01: $1,923.75 (last 5min)")
            print(f"    store_03: $1,456.20 (last 5min)")
            print()

            # Sample Bigtable view counters
            print("  Bigtable Counters:")
            print(f"    product_005: 15,234 total views")
            print(f"    product_003: 12,891 total views")
            print(f"    product_007: 8,567 total views")
        print()

    def show_summary(self):
        """Display a summary of all results"""
        if self.display_config.show_colors:
            print(
                f"{DisplayColors.BOLD}{DisplayColors.BLUE}=== SUMMARY ==={DisplayColors.END}")
        else:
            print("=== SUMMARY ===")
        print("Pipeline is processing e-commerce events successfully!")
        print(
            f"• {TerminalUtils.format_number(float(self.metrics['total_events_processed']))} total events processed")
        print(
            f"• {TerminalUtils.format_number(self.metrics['events_per_second'])} events per second average")
        print(
            f"• {TerminalUtils.format_percentage(self.metrics['error_rate'])} error rate")
        print("• All outputs (BigQuery, Bigtable) receiving data")
        print()

    def run_interactive_mode(self):
        """Run in interactive mode with auto-refresh"""
        try:
            while True:
                TerminalUtils.clear_screen()
                self.show_header()
                self.show_pipeline_status()
                self.show_key_metrics()
                self.show_recent_events()
                self.show_data_quality()
                self.show_sample_outputs()
                self.show_summary()

                if self.display_config.show_colors:
                    print(
                        f"{DisplayColors.YELLOW}Press Ctrl+C to exit, waiting {self.display_config.refresh_interval} seconds...{DisplayColors.END}")
                else:
                    print(
                        f"Press Ctrl+C to exit, waiting {self.display_config.refresh_interval} seconds...")
                time.sleep(self.display_config.refresh_interval)

        except KeyboardInterrupt:
            if self.display_config.show_colors:
                print(
                    f"\n{DisplayColors.YELLOW}Results viewer stopped.{DisplayColors.END}")
            else:
                print("\nResults viewer stopped.")


def main():
    """Main function"""
    try:
        viewer = ResultsViewer()

        if len(os.sys.argv) > 1 and os.sys.argv[1] == "--interactive":
            viewer.run_interactive_mode()
        else:
            # Single shot view
            viewer.show_header()
            viewer.show_pipeline_status()
            viewer.show_key_metrics()
            viewer.show_recent_events()
            viewer.show_data_quality()
            viewer.show_sample_outputs()
            viewer.show_summary()

            if viewer.display_config.show_colors:
                print(
                    f"{DisplayColors.GREEN}To run in interactive mode: python results_viewer.py --interactive{DisplayColors.END}")
            else:
                print(
                    "To run in interactive mode: python results_viewer.py --interactive")
    except Exception as e:
        print(f"Error running results viewer: {e}")
        return 1

    return 0


if __name__ == "__main__":
    main()
