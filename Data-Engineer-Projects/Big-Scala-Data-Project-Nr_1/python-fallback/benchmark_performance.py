#!/usr/bin/env python3
"""
Performance benchmarking script for log analytics pipeline.

Tests the system with various data volumes and measures:
- Processing throughput (records/second)
- Memory usage patterns
- Error rates and recovery
- Scalability characteristics
"""

import argparse
import time
import psutil
import subprocess
import os
import signal
import sys
from datetime import datetime
from pathlib import Path


class PerformanceBenchmark:
    def __init__(self, spark_master="local[*]", memory_limit="2g"):
        self.spark_master = spark_master
        self.memory_limit = memory_limit
        self.process = None
        self.results = []

    def generate_test_data(self, num_lines, output_file="input/benchmark_access.log"):
        """Generate test data of specified size"""
        print(f"📊 Generating {num_lines:,} test log entries...")

        os.makedirs("input", exist_ok=True)

        # Use existing generator script if available
        if os.path.exists("scripts/generate_logs.py"):
            cmd = [
                "python3", "scripts/generate_logs.py",
                "--output", output_file,
                "--lines", str(num_lines),
                "--mode", "realistic",
                "--seed", "42"  # Reproducible results
            ]

            try:
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=300)
                if result.returncode == 0:
                    print(f"✅ Generated {num_lines:,} log entries")
                    return True
                else:
                    print(f"❌ Failed to generate data: {result.stderr}")
                    return False
            except subprocess.TimeoutExpired:
                print("❌ Data generation timed out")
                return False
        else:
            print("❌ Log generator script not found")
            return False

    def run_benchmark(self, test_name, num_lines, duration_minutes=2):
        """Run a single benchmark test"""
        print(f"\n🧪 Starting benchmark: {test_name}")
        print(f"   Records: {num_lines:,}")
        print(f"   Duration: {duration_minutes} minutes")

        # Generate test data
        if not self.generate_test_data(num_lines):
            return None

        # Start memory monitoring
        memory_monitor = MemoryMonitor()
        memory_monitor.start()

        # Start the Spark application
        start_time = time.time()

        try:
            cmd = [
                "java", "-Xmx" + self.memory_limit, "-Xms1g",
                "-cp", "target/scala-2.12/log-analytics-pipeline-1.0.0.jar",
                "com.example.logstream.FileStreamApp",
                "--input", "input/",
                "--output", "output/",
                "--checkpoint", "checkpoint/",
                "--log-level", "WARN"
            ]

            print(f"🚀 Starting application: {' '.join(cmd)}")

            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                preexec_fn=os.setsid
            )

            # Monitor for specified duration
            time.sleep(duration_minutes * 60)

            # Collect results
            execution_time = time.time() - start_time
            memory_stats = memory_monitor.stop()

            # Check if process is still running
            if self.process.poll() is None:
                print("⏹️  Stopping application...")
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
                time.sleep(5)

            # Collect output
            stdout, stderr = self.process.communicate()

            # Analyze results
            result = {
                'test_name': test_name,
                'num_records': num_lines,
                'duration_minutes': duration_minutes,
                'execution_time': execution_time,
                'memory_peak_mb': memory_stats['peak_mb'],
                'memory_avg_mb': memory_stats['avg_mb'],
                'exit_code': self.process.returncode,
                'errors': stderr.count('ERROR'),
                'warnings': stderr.count('WARN'),
                'throughput_records_per_sec': num_lines / execution_time if execution_time > 0 else 0
            }

            self.results.append(result)
            self.print_result_summary(result)

            return result

        except KeyboardInterrupt:
            print("⏹️  Benchmark interrupted by user")
            if self.process:
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
            return None
        except Exception as e:
            print(f"❌ Benchmark failed: {e}")
            if self.process:
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
            return None
        finally:
            memory_monitor.stop()

    def print_result_summary(self, result):
        """Print a summary of benchmark results"""
        print("📊 Benchmark Results:")
        print(f"   Test: {result['test_name']}")
        print(f"   Records: {result['num_records']:,}")
        print(f"   Duration: {result['duration_minutes']:.1f} minutes")
        print(
            f"   Throughput: {result['throughput_records_per_sec']:.1f} records/sec")
        print(f"   Peak Memory: {result['memory_peak_mb']:.1f} MB")
        print(f"   Avg Memory: {result['memory_avg_mb']:.1f} MB")
        print(f"   Errors: {result['errors']}")
        print(f"   Warnings: {result['warnings']}")

        if result['exit_code'] == 0:
            print("   Status: ✅ SUCCESS")
        else:
            print(f"   Status: ❌ FAILED (Exit code: {result['exit_code']})")

    def run_scalability_tests(self):
        """Run a series of scalability tests"""
        test_scenarios = [
            ("Small", 1000, 1),
            ("Medium", 10000, 2),
            ("Large", 100000, 3),
            ("XL", 1000000, 5)
        ]

        print("🚀 Starting Scalability Tests")
        print("=" * 50)

        for test_name, num_lines, duration in test_scenarios:
            result = self.run_benchmark(test_name, num_lines, duration)
            if result:
                print("-" * 30)
                time.sleep(10)  # Cool down between tests

        self.print_final_summary()

    def print_final_summary(self):
        """Print final summary of all tests"""
        print("\n🏁 FINAL BENCHMARK SUMMARY")
        print("=" * 50)

        if not self.results:
            print("❌ No results to summarize")
            return

        total_records = sum(r['num_records'] for r in self.results)
        total_time = sum(r['execution_time'] for r in self.results)
        avg_throughput = total_records / total_time if total_time > 0 else 0

        print(f"📈 Overall Throughput: {avg_throughput:.1f} records/sec")
        print(f"📊 Total Records Processed: {total_records:,}")
        print(f"⏱️  Total Execution Time: {total_time:.1f} seconds")

        # Find best and worst performance
        best_result = max(
            self.results, key=lambda x: x['throughput_records_per_sec'])
        worst_result = min(
            self.results, key=lambda x: x['throughput_records_per_sec'])

        print(
            f"🏆 Best Performance: {best_result['test_name']} ({best_result['throughput_records_per_sec']:.1f} rec/sec)")
        print(
            f"🐌 Worst Performance: {worst_result['test_name']} ({worst_result['throughput_records_per_sec']:.1f} rec/sec)")

        # Memory analysis
        max_memory = max(r['memory_peak_mb'] for r in self.results)
        print(f"💾 Max Memory Usage: {max_memory:.1f} MB")

        # Success rate
        successful_tests = sum(1 for r in self.results if r['exit_code'] == 0)
        success_rate = (successful_tests / len(self.results)) * 100
        print(
            f"✅ Success Rate: {success_rate:.1f}% ({successful_tests}/{len(self.results)})")


class MemoryMonitor:
    def __init__(self, interval=1.0):
        self.interval = interval
        self.process = psutil.Process()
        self.memory_readings = []
        self.monitoring = False

    def start(self):
        """Start memory monitoring"""
        self.memory_readings = []
        self.monitoring = True

        def monitor():
            while self.monitoring:
                try:
                    memory_mb = self.process.memory_info().rss / 1024 / 1024
                    self.memory_readings.append(memory_mb)
                    time.sleep(self.interval)
                except:
                    break

        import threading
        self.monitor_thread = threading.Thread(target=monitor, daemon=True)
        self.monitor_thread.start()

    def stop(self):
        """Stop memory monitoring and return stats"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)

        if self.memory_readings:
            return {
                'peak_mb': max(self.memory_readings),
                'avg_mb': sum(self.memory_readings) / len(self.memory_readings),
                'min_mb': min(self.memory_readings),
                'samples': len(self.memory_readings)
            }
        else:
            return {'peak_mb': 0, 'avg_mb': 0, 'min_mb': 0, 'samples': 0}


def main():
    parser = argparse.ArgumentParser(
        description="Performance benchmark for log analytics pipeline")
    parser.add_argument(
        "--spark-master", default="local[*]", help="Spark master (default: local[*])")
    parser.add_argument("--memory", default="2g",
                        help="JVM memory limit (default: 2g)")
    parser.add_argument("--test", choices=["scalability", "stress", "single"],
                        default="scalability", help="Type of test to run")
    parser.add_argument("--lines", type=int, default=10000,
                        help="Number of lines for single test")
    parser.add_argument("--duration", type=int, default=2,
                        help="Duration in minutes for single test")

    args = parser.parse_args()

    # Check if JAR exists
    jar_path = "target/scala-2.12/log-analytics-pipeline-1.0.0.jar"
    if not os.path.exists(jar_path):
        print(f"❌ JAR file not found: {jar_path}")
        print("💡 Run 'sbt assembly' first to build the application")
        sys.exit(1)

    # Create and run benchmark
    benchmark = PerformanceBenchmark(args.spark_master, args.memory)

    if args.test == "scalability":
        benchmark.run_scalability_tests()
    elif args.test == "stress":
        print("🚨 Running stress test (this may take a while)...")
        benchmark.run_benchmark("Stress_Test", 1000000, 10)
    elif args.test == "single":
        benchmark.run_benchmark(
            f"Single_Test_{args.lines}", args.lines, args.duration)


if __name__ == "__main__":
    main()
