#!/usr/bin/env python3
"""
Log Generator for Apache Combined Log Format

Generates realistic log entries for testing the log analytics pipeline.
Supports various HTTP methods, status codes, endpoints, and realistic user agents.
"""

import argparse
import random
import time
from datetime import datetime, timedelta, timezone
import os
import sys

# Common HTTP methods
HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH", "OPTIONS"]

# Common endpoints
ENDPOINTS = [
    "/", "/index.html", "/api/users", "/api/users/{id}", "/api/posts",
    "/api/posts/{id}", "/api/search", "/health", "/metrics", "/static/css/main.css",
    "/static/js/app.js", "/favicon.ico", "/robots.txt", "/sitemap.xml"
]

# Common status codes with realistic distribution
STATUS_CODES = [200, 201, 204, 301, 302,
                400, 401, 403, 404, 405, 500, 502, 503]

# Realistic user agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36",
    "curl/7.68.0",
    "python-requests/2.25.1",
    "Go-http-client/1.1",
    "HealthCheck/1.0",
    "Prometheus/2.26.0",
    "ELB-HealthChecker/2.0"
]

# Common referers
REFERERS = [
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://example.com/",
    "https://example.com/api",
    "-",
    "http://localhost:3000/",
    "https://developer.mozilla.org/",
    "https://stackoverflow.com/"
]


class LogGenerator:
    def __init__(self, seed=None):
        if seed:
            random.seed(seed)
        self.start_time = datetime.now(timezone.utc)

    def generate_ip(self):
        """Generate a realistic IP address"""
        return f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}"

    def generate_timestamp(self, offset_seconds=0):
        """Generate timestamp string in Apache log format"""
        timestamp = self.start_time + timedelta(seconds=offset_seconds)
        return timestamp.strftime("[%d/%b/%Y:%H:%M:%S %z]")

    def generate_request_line(self):
        """Generate HTTP request line"""
        method = random.choice(HTTP_METHODS)
        endpoint = random.choice(ENDPOINTS)

        # Add query parameters for some GET requests
        if method == "GET" and random.random() < 0.3:
            query_params = ["q=test", "page=1",
                            "limit=10", "sort=date", "filter=active"]
            param = random.choice(query_params)
            if "?" in endpoint:
                endpoint += f"&{param}"
            else:
                endpoint += f"?{param}"

        return f'"{method} {endpoint} HTTP/1.1"'

    def generate_log_entry(self, offset_seconds=0):
        """Generate a single log entry"""
        ip = self.generate_ip()
        timestamp = self.generate_timestamp(offset_seconds)
        request_line = self.generate_request_line()
        status_code = random.choice(STATUS_CODES)

        # Generate response size based on status code
        if status_code in [200, 201]:
            response_size = random.randint(100, 10000)
        elif status_code == 204:
            response_size = 0
        elif status_code in [301, 302]:
            response_size = random.randint(200, 500)
        elif status_code >= 400:
            response_size = random.randint(50, 1000)
        else:
            response_size = random.randint(100, 5000)

        referer = random.choice(REFERERS)
        user_agent = random.choice(USER_AGENTS)

        return f'{ip} - - {timestamp} {request_line} {status_code} {response_size} "{referer}" "{user_agent}"'

    def generate_burst(self, num_lines=100, burst_duration=60):
        """Generate a burst of log entries over a time period"""
        entries = []
        for i in range(num_lines):
            # Distribute entries over the burst duration
            offset = random.randint(0, burst_duration)
            entries.append(self.generate_log_entry(offset))
        return sorted(entries)

    def generate_realistic_traffic(self, num_lines=1000, duration_minutes=10):
        """Generate realistic traffic pattern"""
        entries = []

        for i in range(num_lines):
            # Create realistic time distribution (more traffic during "business hours")
            hour = random.choices(range(24), weights=[0.1, 0.05, 0.02, 0.01, 0.02, 0.05, 0.1, 0.2, 0.3,
                                  0.4, 0.35, 0.3, 0.25, 0.3, 0.35, 0.4, 0.35, 0.3, 0.25, 0.2, 0.15, 0.1, 0.08, 0.05][::-1])
            offset_seconds = i * (duration_minutes *
                                  60) // num_lines + random.randint(0, 60)

            entries.append(self.generate_log_entry(offset_seconds))

        return sorted(entries)


def main():
    parser = argparse.ArgumentParser(
        description="Generate Apache Combined Log Format entries")
    parser.add_argument("--output", "-o", default="input/access.log",
                        help="Output file path (default: input/access.log)")
    parser.add_argument("--lines", "-n", type=int, default=100,
                        help="Number of log lines to generate (default: 100)")
    parser.add_argument("--mode", "-m", choices=["steady", "burst", "realistic"],
                        default="steady", help="Generation mode (default: steady)")
    parser.add_argument("--duration", "-d", type=int, default=60,
                        help="Duration in seconds for burst mode (default: 60)")
    parser.add_argument("--duration-minutes", type=int, default=10,
                        help="Duration in minutes for realistic mode (default: 10)")
    parser.add_argument("--seed", "-s", type=int,
                        help="Random seed for reproducible output")
    parser.add_argument("--append", "-a", action="store_true",
                        help="Append to existing file instead of overwriting")

    args = parser.parse_args()

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    # Initialize generator
    generator = LogGenerator(seed=args.seed)

    # Generate entries based on mode
    if args.mode == "burst":
        entries = generator.generate_burst(args.lines, args.duration)
    elif args.mode == "realistic":
        entries = generator.generate_realistic_traffic(
            args.lines, args.duration_minutes)
    else:  # steady
        entries = [generator.generate_log_entry(i) for i in range(args.lines)]

    # Write to file
    mode = "a" if args.append else "w"
    with open(args.output, mode) as f:
        for entry in entries:
            f.write(entry + "\n")

    print(f"Generated {len(entries)} log entries in {args.mode} mode")
    print(f"Output written to: {args.output}")

    # Print sample entries
    print("\nSample entries:")
    for i, entry in enumerate(entries[:3]):
        print(f"  {i+1}: {entry}")
    if len(entries) > 3:
        print(f"  ... and {len(entries) - 3} more entries")


if __name__ == "__main__":
    main()
