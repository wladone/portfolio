#!/usr/bin/env python3
"""
E-commerce Data Downloader

Python implementation of the EcomIngestor, providing API data ingestion
with retry logic, rate limiting, and NDJSON output.

Compatible CLI arguments with the Scala version.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RateLimiter:
    """Simple token bucket rate limiter"""

    def __init__(self, max_rps: int):
        self.max_rps = max_rps
        self.tokens = max_rps
        self.last_refill = time.time()
        self.token_rate = max_rps  # tokens per second

    def acquire(self):
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.max_rps, self.tokens +
                          elapsed * self.token_rate)
        self.last_refill = now

        if self.tokens >= 1:
            self.tokens -= 1
            return True
        else:
            sleep_time = (1 - self.tokens) / self.token_rate
            time.sleep(sleep_time)
            self.tokens = 0
            self.last_refill = time.time()
            return True


class ApiClient:
    """HTTP client with retry and rate limiting"""

    def __init__(self, base_url: str, api_key: Optional[str] = None,
                 max_rps: int = 2, timeout: int = 30, max_retries: int = 3):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.rate_limiter = RateLimiter(max_rps)
        self.timeout = timeout
        self.max_retries = max_retries

        # Setup session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def get(self, endpoint: str) -> Dict[str, Any]:
        """Make GET request with rate limiting and retries"""
        self.rate_limiter.acquire()

        url = f"{self.base_url}{endpoint}" if endpoint.startswith(
            '/') else endpoint
        headers = {}
        if self.api_key:
            headers['Authorization'] = f'Bearer {self.api_key}'

        print(f"Requesting: {url}")
        response = self.session.get(url, headers=headers, timeout=self.timeout)
        response.raise_for_status()

        return response.json()


class DataSource:
    """Abstract data source"""

    def __init__(self, base_url: str):
        self.base_url = base_url

    def get_endpoint(self, limit: int, skip: int) -> str:
        raise NotImplementedError

    def has_more_data(self, response: Dict[str, Any]) -> bool:
        raise NotImplementedError


class DummyJsonSource(DataSource):
    def get_endpoint(self, limit: int, skip: int) -> str:
        return f"/products?limit={limit}&skip={skip}"

    def has_more_data(self, response: Dict[str, Any]) -> bool:
        products = response.get('products', [])
        total = response.get('total', 0)
        skip = response.get('skip', 0)
        limit = response.get('limit', len(products))
        return skip + limit < total


class FakeStoreSource(DataSource):
    def get_endpoint(self, limit: int, skip: int) -> str:
        return f"/products?limit={limit}&offset={skip}"

    def has_more_data(self, response: Dict[str, Any]) -> bool:
        products = response.get('products', [])
        return len(products) == response.get('limit', len(products))


def get_data_source(source_name: str) -> DataSource:
    sources = {
        'dummyjson': DummyJsonSource("https://dummyjson.com"),
        'fakestore': FakeStoreSource("https://fakestoreapi.com")
    }

    if source_name not in sources:
        raise ValueError(f"Unknown source: {source_name}")

    return sources[source_name]


def main():
    parser = argparse.ArgumentParser(
        description='Download e-commerce data from public APIs')
    parser.add_argument('--source', choices=['dummyjson', 'fakestore'],
                        default='dummyjson', help='Data source to use')
    parser.add_argument('--out', default='data/ecommerce/raw',
                        help='Output directory')
    parser.add_argument('--page-size', type=int, default=100,
                        help='Number of items per page')
    parser.add_argument('--max-pages', type=int, default=50,
                        help='Maximum number of pages to fetch')
    parser.add_argument('--rps', type=int, default=2,
                        help='Maximum requests per second')
    parser.add_argument(
        '--run-date', help='Run date (YYYY-MM-DD), defaults to today UTC')

    args = parser.parse_args()

    # Determine run date
    if args.run_date:
        run_date = datetime.fromisoformat(args.run_date).date()
    else:
        run_date = datetime.now(timezone.utc).date()

    run_date_str = run_date.isoformat()

    # Setup data source and client
    data_source = get_data_source(args.source)
    client = ApiClient(
        base_url=data_source.base_url,
        max_rps=args.rps
    )

    # Create output directory
    output_dir = Path(args.out) / \
        f"source={args.source}" / f"run_date={run_date_str}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "part-00000.ndjson"

    total_rows = 0
    page = 0

    print(f"Starting download from {args.source}")
    print(f"Output: {output_file}")
    print(
        f"Page size: {args.page_size}, Max pages: {args.max_pages}, RPS: {args.rps}")

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            while page < args.max_pages:
                skip = page * args.page_size
                endpoint = data_source.get_endpoint(args.page_size, skip)

                print(f"Fetching page {page + 1}, skip={skip}")

                response = client.get(endpoint)
                products = response.get('products', [])

                # Write each product as JSON line
                for product in products:
                    json.dump(product, f, ensure_ascii=False)
                    f.write('\n')
                    total_rows += 1

                print(f"Page {page + 1}: {len(products)} products")

                if not data_source.has_more_data(response):
                    print("No more data available")
                    break

                page += 1

        # Calculate file size
        file_size = output_file.stat().st_size
        print(f"\nDownload completed successfully!")
        print(f"Total rows: {total_rows}")
        print(f"File size: {file_size:,} bytes")
        print(f"Output file: {output_file}")

    except Exception as e:
        print(f"Download failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
