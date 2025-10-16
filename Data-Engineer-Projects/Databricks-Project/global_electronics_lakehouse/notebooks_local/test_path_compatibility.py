"""Test script to simulate cross-platform path resolution for the Global Electronics Lakehouse."""

from pathlib import Path
import os
import logging
import sys


def test_path_resolution():
    """Test pathlib path resolution across different OS path separators."""
    try:
        # Simulate base directory
        # global_electronics_lakehouse
        base_dir = Path(__file__).resolve().parents[1]

        # Validate base directory exists
        if not base_dir.exists():
            raise FileNotFoundError(
                f"Base directory does not exist: {base_dir}")

        print(f"Current OS: {os.name}")
        print(f"Path separator: {os.sep}")
        print(f"Base directory: {base_dir}")
        print()

        # Test inventory path with wildcards
        inventory_path = base_dir / "data" / "inventory" / "*" / "*_inventory.csv"
        print("Inventory path:")
        print(f"  Path object: {inventory_path}")
        print(f"  str(): {str(inventory_path)}")
        print(f"  as_posix(): {inventory_path.as_posix()}")
        print()

        # Test suppliers path
        suppliers_path = base_dir / "data" / "suppliers" / "suppliers_master.jsonl"
        print("Suppliers path:")
        print(f"  Path object: {suppliers_path}")
        print(f"  str(): {str(suppliers_path)}")
        print(f"  as_posix(): {suppliers_path.as_posix()}")
        print()

        # Test sales path with wildcards
        sales_path = base_dir / "data" / "sales_stream" / "sales_*.json"
        print("Sales path:")
        print(f"  Path object: {sales_path}")
        print(f"  str(): {str(sales_path)}")
        print(f"  as_posix(): {sales_path.as_posix()}")
        print()

        # Test Delta paths
        delta_inventory = base_dir / "_delta" / "inventory"
        delta_sales = base_dir / "_delta" / "sales"
        print("Delta paths:")
        print(f"  Inventory: {delta_inventory.as_posix()}")
        print(f"  Sales: {delta_sales.as_posix()}")
        print()

        # Demonstrate that as_posix() always uses forward slashes
        print("Cross-platform compatibility:")
        print("  as_posix() ensures forward slashes, compatible with Spark and other tools")
        print("  regardless of the underlying OS path separator.")

    except Exception as e:
        logging.error(f"Error during path resolution test: {e}")
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    sys.exit(test_path_resolution())
