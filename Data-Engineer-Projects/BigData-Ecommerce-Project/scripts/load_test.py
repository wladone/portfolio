#!/usr/bin/env python3
"""
Load Testing Script for Streaming Pipeline

This script performs load testing on the streaming pipeline to:
- Measure performance under high load
- Identify bottlenecks
- Validate scalability
- Test resilience and error handling

Usage:
    python scripts/load_test.py --duration 300 --rate 1000 --project your-project
"""

import argparse
import asyncio
import json
import os
import random
import statistics
import subprocess
import sys
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

from google.cloud import pubsub_v1


@dataclass
class LoadTestConfig:
    """Configuration for load testing."""
    duration_seconds: int = 300
    event_rate: int = 100  # events per second
    project_id: str = ""
    topic_prefix: str = "load-test"
    batch_size: int = 100
    ramp_up_seconds: int = 30
    enable_monitoring: bool = True
    output_file: str = "load_test_results.json"


@dataclass
class LoadTestMetrics:
    """Metrics collected during load testing."""
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None

    # Counters
    events_sent: int = 0
    events_acknowledged: int = 0
    publish_errors: int = 0

    # Timing (in milliseconds)
    publish_latencies: List[float] = field(default_factory=list)
    end_to_end_latencies: List[float] = field(default_factory=list)

    # Throughput tracking
    throughput_history: deque = field(
        default_factory=lambda: deque(maxlen=100))

    def record_publish_latency(self, latency_ms: float):
        """Record publish latency."""
        self.publish_latencies.append(latency_ms)

    def record_end_to_end_latency(self, latency_ms: float):
        """Record end-to-end latency."""
        self.end_to_end_latencies.append(latency_ms)

    def update_throughput(self, events_per_second: float):
        """Update throughput history."""
        self.throughput_history.append(events_per_second)

    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        if not self.publish_latencies:
            return {"error": "No data collected"}

        return {
            "duration_seconds": (self.end_time or datetime.now()).timestamp() - self.start_time.timestamp(),
            "events_sent": self.events_sent,
            "events_acknowledged": self.events_acknowledged,
            "publish_errors": self.publish_errors,
            "success_rate": self.events_acknowledged / max(self.events_sent, 1),
            "publish_latency": {
                "min": min(self.publish_latencies),
                "max": max(self.publish_latencies),
                "avg": statistics.mean(self.publish_latencies),
                "p50": statistics.median(self.publish_latencies),
                "p95": statistics.quantiles(self.publish_latencies, n=20)[18] if len(self.publish_latencies) >= 20 else None,
                "p99": statistics.quantiles(self.publish_latencies, n=100)[98] if len(self.publish_latencies) >= 100 else None,
            },
            "throughput": {
                "avg": statistics.mean(self.throughput_history) if self.throughput_history else 0,
                "max": max(self.throughput_history) if self.throughput_history else 0,
                "min": min(self.throughput_history) if self.throughput_history else 0,
            }
        }


class EventGenerator:
    """Generates test events for load testing."""

    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.test_run_id = str(uuid.uuid4())

        # Generate test data
        self.product_ids = [f"load-test-product-{i:03d}" for i in range(1000)]
        self.user_ids = [f"load-test-user-{i:06d}" for i in range(10000)]
        self.store_ids = [f"load-test-store-{i:02d}" for i in range(100)]
        self.warehouse_ids = [f"load-test-warehouse-{i}" for i in range(50)]

    def generate_click_event(self) -> Dict[str, Any]:
        """Generate a click event."""
        return {
            "test_run_id": self.test_run_id,
            "event_type": "click",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": random.choice(self.user_ids),
            "session_id": f"load-test-session-{random.randint(1, 10000)}",
            "product_id": random.choice(self.product_ids),
            "page_type": random.choice(["product", "category", "search"]),
            "user_agent": "LoadTest/1.0"
        }

    def generate_transaction_event(self) -> Dict[str, Any]:
        """Generate a transaction event."""
        product_id = random.choice(self.product_ids)
        store_id = random.choice(self.store_ids)
        quantity = random.randint(1, 10)
        price = round(random.uniform(5.0, 1000.0), 2)

        return {
            "test_run_id": self.test_run_id,
            "event_type": "transaction",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "transaction_id": f"load-test-txn-{random.randint(1000000, 9999999)}",
            "product_id": product_id,
            "store_id": store_id,
            "quantity": quantity,
            "unit_price": price,
            "total_amount": round(quantity * price, 2),
            "currency": "USD",
            "payment_method": random.choice(["credit_card", "paypal", "apple_pay"])
        }

    def generate_stock_event(self) -> Dict[str, Any]:
        """Generate a stock event."""
        product_id = random.choice(self.product_ids)
        warehouse_id = random.choice(self.warehouse_ids)
        quantity_change = random.randint(-50, 100)

        return {
            "test_run_id": self.test_run_id,
            "event_type": "stock",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "product_id": product_id,
            "warehouse_id": warehouse_id,
            "quantity_change": quantity_change,
            "reason": random.choice(["restock", "sale", "return", "adjustment", "damaged"])
        }

    def generate_batch(self, batch_size: int) -> List[Dict[str, Any]]:
        """Generate a batch of events."""
        batch = []
        for _ in range(batch_size):
            # Weight events: 60% clicks, 25% transactions, 15% stock
            event_type = random.choices(
                ["click", "transaction", "stock"],
                weights=[0.6, 0.25, 0.15]
            )[0]

            if event_type == "click":
                event = self.generate_click_event()
            elif event_type == "transaction":
                event = self.generate_transaction_event()
            else:
                event = self.generate_stock_event()

            batch.append(event)

        return batch


class LoadTester:
    """Main load testing class."""

    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.generator = EventGenerator(config)
        self.metrics = LoadTestMetrics()

        # Pub/Sub setup
        self.publisher = pubsub_v1.PublisherClient()
        self.topics = {}

    def setup_topics(self):
        """Set up Pub/Sub topics for testing."""
        print(f"🔧 Setting up load test topics...")

        topic_names = [
            f"{self.config.topic_prefix}-clicks",
            f"{self.config.topic_prefix}-transactions",
            f"{self.config.topic_prefix}-stock"
        ]

        for topic_name in topic_names:
            topic_path = self.publisher.topic_path(
                self.config.project_id, topic_name)

            try:
                # Check if topic exists
                self.publisher.get_topic(topic_path)
                print(f"  ✅ Using existing topic: {topic_name}")
            except Exception:
                # Create topic if it doesn't exist
                try:
                    self.publisher.create_topic(topic_path)
                    print(f"  ✅ Created topic: {topic_name}")
                except Exception as e:
                    print(f"  ❌ Failed to create topic {topic_name}: {e}")
                    raise

            self.topics[topic_name] = topic_path

    async def publish_events_async(self, duration_seconds: int):
        """Publish events asynchronously for the specified duration."""
        print(f"🚀 Starting load test for {duration_seconds} seconds...")
        print(f"📊 Target rate: {self.config.event_rate} events/second")

        start_time = time.time()
        end_time = start_time + duration_seconds

        # Calculate publishing interval
        interval = 1.0 / self.config.event_rate

        next_publish_time = start_time

        while time.time() < end_time:
            current_time = time.time()

            if current_time >= next_publish_time:
                # Generate and publish batch
                await self._publish_batch()

                # Schedule next batch
                next_publish_time += interval

                # Update throughput metrics
                actual_rate = self.config.batch_size / \
                    (time.time() - start_time)
                self.metrics.update_throughput(actual_rate)

            else:
                # Small sleep to prevent busy waiting
                await asyncio.sleep(0.001)

        self.metrics.end_time = datetime.now()
        print("🏁 Load test completed!")

    async def _publish_batch(self):
        """Publish a batch of events."""
        try:
            # Generate batch
            events = self.generator.generate_batch(self.config.batch_size)
            self.metrics.events_sent += len(events)

            # Group events by type
            events_by_type = defaultdict(list)
            for event in events:
                event_type = event["event_type"]
                topic_suffix = f"{event_type}s"
                topic_name = f"{self.config.topic_prefix}-{topic_suffix}"
                events_by_type[topic_name].append(event)

            # Publish to appropriate topics
            publish_start = time.time()

            for topic_name, topic_events in events_by_type.items():
                if topic_name in self.topics:
                    topic_path = self.topics[topic_name]

                    # Convert to JSON and encode
                    messages = [
                        json.dumps(event).encode('utf-8')
                        for event in topic_events
                    ]

                    # Publish batch
                    try:
                        futures = [
                            self.publisher.publish(topic_path, message)
                            for message in messages
                        ]

                        # Wait for acknowledgments
                        for future in futures:
                            future.result(timeout=30)  # 30 second timeout

                        self.metrics.events_acknowledged += len(messages)

                    except Exception as e:
                        print(f"  ❌ Publish error: {e}")
                        self.metrics.publish_errors += len(messages)

            # Record publish latency
            publish_latency = (time.time() - publish_start) * 1000
            self.metrics.record_publish_latency(publish_latency)

        except Exception as e:
            print(f"❌ Error in publish batch: {e}")
            self.metrics.publish_errors += self.config.batch_size

    def run_load_test(self):
        """Run the complete load test."""
        print("🧪 Streaming Pipeline Load Test")
        print("=" * 50)
        print(f"Project: {self.config.project_id}")
        print(f"Duration: {self.config.duration_seconds}s")
        print(f"Target Rate: {self.config.event_rate} events/sec")
        print(f"Batch Size: {self.config.batch_size}")
        print()

        try:
            # Setup
            self.setup_topics()

            # Run load test
            asyncio.run(self.publish_events_async(
                self.config.duration_seconds))

            # Print results
            self.print_results()

            # Save results
            self.save_results()

        except KeyboardInterrupt:
            print("\n⚠️  Load test interrupted by user")
            self.metrics.end_time = datetime.now()
            self.print_results()
        except Exception as e:
            print(f"❌ Load test failed: {e}")
            return False

        return True

    def print_results(self):
        """Print load test results."""
        print("\n📊 LOAD TEST RESULTS")
        print("=" * 50)

        summary = self.metrics.get_summary()

        print(f"Duration: {summary['duration_seconds']:.1f} seconds")
        print(f"Events Sent: {summary['events_sent']}")
        print(f"Events Acknowledged: {summary['events_acknowledged']}")
        print(f"Publish Errors: {summary['publish_errors']}")
        print(f"Success Rate: {summary['success_rate']:.2%}")

        if 'publish_latency' in summary:
            latency = summary['publish_latency']
            print("\n📈 Publish Latency (ms):")
            print(f"  Min: {latency['min']:.2f}")
            print(f"  Max: {latency['max']:.2f}")
            print(f"  Avg: {latency['avg']:.2f}")
            print(f"  P50: {latency['p50']:.2f}")
            if latency['p95']:
                print(f"  P95: {latency['p95']:.2f}")
            if latency['p99']:
                print(f"  P99: {latency['p99']:.2f}")

        if 'throughput' in summary:
            throughput = summary['throughput']
            print("\n⚡ Throughput (events/sec):")
            print(f"  Min: {throughput['min']:.2f}")
            print(f"  Max: {throughput['max']:.2f}")
            print(f"  Avg: {throughput['avg']:.2f}")

    def save_results(self):
        """Save results to file."""
        try:
            summary = self.metrics.get_summary()
            summary['config'] = {
                'duration_seconds': self.config.duration_seconds,
                'event_rate': self.config.event_rate,
                'batch_size': self.config.batch_size,
                'project_id': self.config.project_id
            }

            with open(self.config.output_file, 'w') as f:
                json.dump(summary, f, indent=2)

            print(f"\n💾 Results saved to: {self.config.output_file}")

        except Exception as e:
            print(f"⚠️  Failed to save results: {e}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Load test streaming pipeline")
    parser.add_argument("--duration", type=int, default=300,
                        help="Test duration in seconds")
    parser.add_argument("--rate", type=int, default=100,
                        help="Target event rate per second")
    parser.add_argument("--project", required=True,
                        help="GCP project ID")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Batch size for publishing")
    parser.add_argument("--output", default="load_test_results.json",
                        help="Output file for results")

    args = parser.parse_args()

    # Create configuration
    config = LoadTestConfig(
        duration_seconds=args.duration,
        event_rate=args.rate,
        project_id=args.project,
        batch_size=args.batch_size,
        output_file=args.output
    )

    # Validate environment
    if not config.project_id:
        print("❌ Project ID is required")
        sys.exit(1)

    # Run load test
    tester = LoadTester(config)
    success = tester.run_load_test()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
