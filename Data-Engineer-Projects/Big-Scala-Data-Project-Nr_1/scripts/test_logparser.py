#!/usr/bin/env python3
"""
Simple LogParser test using Python - no Spark or Scala compilation required!
Tests your LogParser logic by reading the input file and analyzing the data.
"""

import re
import sys
from datetime import datetime
from collections import defaultdict


def test_log_parser():
    """Test log parsing logic similar to your Scala LogParser"""

    print("🧪 LogParser Test (Python Version)")
    print("=" * 40)

    # Check if input file exists
    input_file = "input/access.log"
    try:
        with open(input_file, "r") as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"❌ Input file not found: {input_file}")
        print("💡 Generate test data first:")
        print("   python scripts/generate_logs.py --output input/access.log --lines 1000")
        return

    total_lines = len(lines)
    print(f"📖 Found {total_lines} log lines in {input_file}")

    # Test parsing of first few lines
    sample_size = min(100, total_lines)
    print(f"\n📋 Testing first {sample_size} log lines:")

    # Simple regex pattern (similar to your Scala version)
    log_pattern = r'^(\S+) ([^\s]+) ([^\s]+) \[([^\]]+)\] "([^"]+)" (\d+) (\d+|[0-9-]+) "([^"]*)" "([^"]*)"'

    valid_entries = 0
    status_counts = defaultdict(int)
    method_counts = defaultdict(int)
    endpoint_counts = defaultdict(int)

    for i, line in enumerate(lines[:sample_size]):  # Test first sample_size lines
        line = line.strip()
        if not line:
            continue

        match = re.match(log_pattern, line)
        if match:
            valid_entries += 1
            parts = match.groups()

            client_ip = parts[0]
            timestamp_str = parts[3]
            request_line = parts[4]
            status_code = int(parts[5])
            response_size = parts[6]
            referer = parts[7]
            user_agent = parts[8]

            # Extract method and endpoint (similar to your Scala logic)
            tokens = request_line.split()
            method = tokens[0] if tokens else "UNKNOWN"
            endpoint = tokens[1].split('?')[0] if len(tokens) > 1 else "/"

            # Status classification (similar to your Scala logic)
            status_class = f"{status_code // 100}xx"

            print(f"{i+1:2d}. ✅ {method} {endpoint} -> {status_code} ({status_class})")

            status_counts[status_class] += 1
            method_counts[method] += 1
            endpoint_counts[endpoint] += 1
        else:
            print(f"{i+1:2d}. ❌ Failed to parse: {line[:80]}...")

    print("\n📊 Analysis Results:")
    print(f"   Valid entries: {valid_entries}/{sample_size}")
    parse_rate = (valid_entries / sample_size * 100) if sample_size > 0 else 0.0
    print(f"   Parse success rate: {parse_rate:.1f}%")

    print("\n📈 Status Code Distribution:")
    for status_class in sorted(status_counts.keys()):
        count = status_counts[status_class]
        percentage = count / valid_entries * 100 if valid_entries > 0 else 0.0
        print(f"   {status_class}: {count} ({percentage:.1f}%)")

    print("\n🔗 HTTP Methods:")
    for method in sorted(method_counts.keys()):
        count = method_counts[method]
        print(f"   {method}: {count}")

    print("\n🎯 Top Endpoints:")
    for endpoint, count in sorted(endpoint_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"   {endpoint}: {count}")

    # Error analysis
    error_count = sum(status_counts.get(cls, 0) for cls in ['4xx', '5xx'])
    server_error_count = status_counts.get('5xx', 0)

    print("\n🚨 Error Analysis:")
    print(f"   Total errors (4xx+5xx): {error_count}")
    print(f"   Server errors (5xx): {server_error_count}")
    if valid_entries > 0:
        print(f"   Error rate: {error_count / valid_entries * 100:.1f}%")
    else:
        print("   Error rate: N/A")

    print("\n🎉 LogParser test complete!")

    # Performance test
    start_time = time.time()

    valid_count = 0
    for line in lines:
        if re.match(log_pattern, line.strip()):
            valid_count += 1

    end_time = time.time()
    duration = end_time - start_time
    throughput = len(lines) / duration if duration > 0 else float("inf")

    print("\n⚡ Performance Test:")
    print(f"   Processed {len(lines)} lines in {duration:.3f} seconds")
    if throughput == float("inf"):
        print(f"   Throughput: ∞ lines/second")
    else:
        print(f"   Throughput: {throughput:.0f} lines/second")
    valid_pct = (valid_count / len(lines) * 100) if len(lines) > 0 else 0.0
    print(f"   Valid entries: {valid_count}/{len(lines)} ({valid_pct:.1f}%)")


if __name__ == "__main__":
    test_log_parser()