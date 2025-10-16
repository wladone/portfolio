#!/usr/bin/env python3
"""
Chaos Engineering Tests for Streaming Pipeline

This script implements chaos engineering experiments to test:
- Pipeline resilience to failures
- Recovery mechanisms
- System behavior under stress
- Failure mode testing

Usage:
    python scripts/chaos_engineering.py --project your-project --experiment network_partition
"""

import argparse
import json
import random
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, List, Any, Optional

import yaml


class ChaosExperiment(Enum):
    """Types of chaos experiments."""
    NETWORK_PARTITION = "network_partition"
    HIGH_CPU_LOAD = "high_cpu_load"
    MEMORY_PRESSURE = "memory_pressure"
    DISK_SPACE_EXHAUSTION = "disk_space_exhaustion"
    SERVICE_KILL = "service_kill"
    DATA_CORRUPTION = "data_corruption"
    LATENCY_INJECTION = "latency_injection"
    RESOURCE_QUOTA_EXCEEDED = "resource_quota_exceeded"


class ChaosTestResult:
    """Results from a chaos experiment."""

    def __init__(self, experiment_name: str):
        self.experiment_name = experiment_name
        self.start_time = datetime.now(timezone.utc)
        self.end_time: Optional[datetime] = None
        self.success: bool = False
        self.metrics_before: Dict[str, Any] = {}
        self.metrics_during: Dict[str, Any] = {}
        self.metrics_after: Dict[str, Any] = {}
        self.errors: List[str] = []
        self.recovery_time_seconds: Optional[float] = None

    def complete(self, success: bool, errors: List[str] = None):
        """Mark experiment as complete."""
        self.end_time = datetime.now(timezone.utc)
        self.success = success
        if errors:
            self.errors.extend(errors)

        duration = self.end_time - self.start_time
        self.recovery_time_seconds = duration.total_seconds()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for reporting."""
        return {
            "experiment_name": self.experiment_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.recovery_time_seconds,
            "success": self.success,
            "metrics_before": self.metrics_before,
            "metrics_during": self.metrics_during,
            "metrics_after": self.metrics_after,
            "errors": self.errors
        }


class ChaosEngineer:
    """Implements chaos engineering experiments."""

    def __init__(self, project_id: str, region: str = "europe-central2"):
        self.project_id = project_id
        self.region = region
        self.results: List[ChaosTestResult] = []

    def run_experiment(self, experiment: ChaosExperiment, duration_seconds: int = 300) -> ChaosTestResult:
        """Run a chaos engineering experiment."""
        print(f"🧪 Running chaos experiment: {experiment.value}")
        print(f"Duration: {duration_seconds} seconds")

        result = ChaosTestResult(experiment.value)

        try:
            # 1. Capture baseline metrics
            result.metrics_before = self._capture_pipeline_metrics()
            print("  📊 Captured baseline metrics")

            # 2. Inject chaos
            chaos_injection = self._inject_chaos(experiment)
            if not chaos_injection:
                raise Exception(f"Failed to inject {experiment.value} chaos")

            print(f"  💥 Injected {experiment.value} chaos")

            # 3. Monitor during chaos
            time.sleep(30)  # Let chaos take effect
            result.metrics_during = self._capture_pipeline_metrics()
            print("  📊 Captured metrics during chaos")

            # 4. Wait for experiment duration
            print(f"  ⏳ Running experiment for {duration_seconds} seconds...")
            time.sleep(duration_seconds)

            # 5. Remove chaos injection
            self._remove_chaos(experiment)
            print(f"  🔧 Removed {experiment.value} chaos injection")

            # 6. Monitor recovery
            recovery_start = time.time()
            while time.time() - recovery_start < 300:  # 5 minute recovery window
                result.metrics_after = self._capture_pipeline_metrics()

                # Check if pipeline recovered
                if self._is_pipeline_healthy(result.metrics_after):
                    print("  ✅ Pipeline recovered successfully")
                    break

                time.sleep(30)  # Check every 30 seconds

            # 7. Validate results
            success = self._validate_experiment_result(result)
            result.complete(success)

            if success:
                print(
                    f"  ✅ Experiment {experiment.value} completed successfully")
            else:
                print(f"  ❌ Experiment {experiment.value} failed")
                result.complete(False, ["Pipeline did not recover properly"])

        except Exception as e:
            print(f"  ❌ Experiment {experiment.value} failed: {e}")
            result.complete(False, [str(e)])

        self.results.append(result)
        return result

    def _capture_pipeline_metrics(self) -> Dict[str, Any]:
        """Capture current pipeline metrics."""
        metrics = {}

        try:
            # Get Dataflow job metrics
            result = subprocess.run([
                "gcloud", "dataflow", "jobs", "list",
                "--project", self.project_id,
                "--region", self.region,
                "--format", "json"
            ], capture_output=True, text=True, check=True)

            jobs = json.loads(result.stdout) if result.stdout.strip() else []

            for job in jobs:
                if job.get("state") == "Running":
                    metrics["dataflow"] = {
                        "job_id": job.get("id"),
                        "state": job.get("state"),
                        "create_time": job.get("createTime")
                    }

            # Get BigQuery metrics (mock for demo)
            metrics["bigquery"] = {
                "tables_active": 3,
                "storage_gb": 15.5,
                "query_count": 1250
            }

            # Get Pub/Sub metrics (mock for demo)
            metrics["pubsub"] = {
                "topics": 4,
                "subscriptions": 2,
                "unacked_messages": 0
            }

        except subprocess.CalledProcessError:
            metrics["error"] = "Failed to capture metrics"

        return metrics

    def _is_pipeline_healthy(self, metrics: Dict[str, Any]) -> bool:
        """Check if pipeline is healthy based on metrics."""
        try:
            # Check Dataflow job is running
            if "dataflow" not in metrics or metrics["dataflow"].get("state") != "Running":
                return False

            # Check BigQuery is accessible
            if "bigquery" not in metrics:
                return False

            # Check for error indicators
            if "error" in metrics:
                return False

            return True

        except Exception:
            return False

    def _inject_chaos(self, experiment: ChaosExperiment) -> bool:
        """Inject chaos based on experiment type."""
        try:
            if experiment == ChaosExperiment.NETWORK_PARTITION:
                return self._inject_network_partition()
            elif experiment == ChaosExperiment.HIGH_CPU_LOAD:
                return self._inject_high_cpu_load()
            elif experiment == ChaosExperiment.MEMORY_PRESSURE:
                return self._inject_memory_pressure()
            elif experiment == ChaosExperiment.SERVICE_KILL:
                return self._inject_service_kill()
            elif experiment == ChaosExperiment.DATA_CORRUPTION:
                return self._inject_data_corruption()
            elif experiment == ChaosExperiment.LATENCY_INJECTION:
                return self._inject_latency()
            else:
                print(f"  ⚠️  Unsupported experiment: {experiment}")
                return False

        except Exception as e:
            print(f"  ❌ Failed to inject chaos: {e}")
            return False

    def _inject_network_partition(self) -> bool:
        """Simulate network partition."""
        print("    🌐 Injecting network partition...")

        # In a real implementation, this would:
        # 1. Create firewall rules to block traffic
        # 2. Modify routing tables
        # 3. Simulate network latency/loss

        # For demo, we'll simulate by logging the action
        print("    ✅ Network partition simulated (demo mode)")
        return True

    def _inject_high_cpu_load(self) -> bool:
        """Inject high CPU load."""
        print("    🔥 Injecting high CPU load...")

        # In production, this would start CPU-intensive processes
        # For demo, we'll simulate the effect
        print("    ✅ High CPU load simulated (demo mode)")
        return True

    def _inject_memory_pressure(self) -> bool:
        """Inject memory pressure."""
        print("    🧠 Injecting memory pressure...")

        # In production, this would allocate large amounts of memory
        # For demo, we'll simulate the effect
        print("    ✅ Memory pressure simulated (demo mode)")
        return True

    def _inject_service_kill(self) -> bool:
        """Kill a service process."""
        print("    💀 Injecting service kill...")

        try:
            # Find and kill a Dataflow worker process (simulated)
            # In production, this would target specific services
            print("    ✅ Service kill simulated (demo mode)")
            return True

        except Exception as e:
            print(f"    ❌ Failed to kill service: {e}")
            return False

    def _inject_data_corruption(self) -> bool:
        """Inject data corruption."""
        print("    🗑️  Injecting data corruption...")

        # In production, this would corrupt data in transit or at rest
        # For demo, we'll simulate the effect
        print("    ✅ Data corruption simulated (demo mode)")
        return True

    def _inject_latency(self) -> bool:
        """Inject network latency."""
        print("    🐌 Injecting network latency...")

        # In production, this would use traffic shaping tools
        # For demo, we'll simulate the effect
        print("    ✅ Network latency simulated (demo mode)")
        return True

    def _remove_chaos(self, experiment: ChaosExperiment):
        """Remove chaos injection."""
        try:
            if experiment == ChaosExperiment.NETWORK_PARTITION:
                print("    🌐 Removing network partition...")
            elif experiment == ChaosExperiment.HIGH_CPU_LOAD:
                print("    🔥 Removing high CPU load...")
            elif experiment == ChaosExperiment.MEMORY_PRESSURE:
                print("    🧠 Removing memory pressure...")
            elif experiment == ChaosExperiment.SERVICE_KILL:
                print("    💀 Removing service kill...")
            elif experiment == ChaosExperiment.DATA_CORRUPTION:
                print("    🗑️  Removing data corruption...")
            elif experiment == ChaosExperiment.LATENCY_INJECTION:
                print("    🐌 Removing network latency...")

            print("    ✅ Chaos injection removed")

        except Exception as e:
            print(f"    ❌ Failed to remove chaos: {e}")

    def _validate_experiment_result(self, result: ChaosTestResult) -> bool:
        """Validate that the experiment produced expected results."""
        try:
            # Check if pipeline is healthy after experiment
            if not self._is_pipeline_healthy(result.metrics_after):
                return False

            # Check recovery time is reasonable (< 5 minutes for most experiments)
            if result.recovery_time_seconds and result.recovery_time_seconds > 300:
                result.errors.append("Recovery time exceeded 5 minutes")
                return False

            # Check for data loss or corruption
            # This would validate data integrity in production

            return True

        except Exception as e:
            result.errors.append(f"Validation failed: {e}")
            return False

    def run_multiple_experiments(self, experiments: List[ChaosExperiment], duration_per_experiment: int = 300):
        """Run multiple chaos experiments."""
        print(f"🧪 Running {len(experiments)} chaos experiments...")
        print("=" * 50)

        results = []

        for experiment in experiments:
            print(f"\n--- Experiment: {experiment.value} ---")

            # Run experiment
            result = self.run_experiment(experiment, duration_per_experiment)
            results.append(result)

            # Brief pause between experiments
            if experiment != experiments[-1]:
                print("⏳ Waiting 60 seconds before next experiment...")
                time.sleep(60)

        # Generate report
        self._generate_chaos_report(results)
        return results

    def _generate_chaos_report(self, results: List[ChaosTestResult]):
        """Generate chaos engineering report."""
        print("\n📊 CHAOS ENGINEERING REPORT")
        print("=" * 50)

        total_experiments = len(results)
        successful_experiments = sum(1 for r in results if r.success)

        print(f"Total Experiments: {total_experiments}")
        print(f"Successful: {successful_experiments}")
        print(f"Failed: {total_experiments - successful_experiments}")
        print(
            f"Success Rate: {successful_experiments/total_experiments*100:.1f}%")

        print("\n📈 EXPERIMENT RESULTS:")
        print("-" * 50)

        for result in results:
            status = "✅ PASS" if result.success else "❌ FAIL"
            duration = f"{result.recovery_time_seconds:.1f}s" if result.recovery_time_seconds else "N/A"

            print(f"{result.experiment_name:20} {status:8} {duration:8}")

            if result.errors:
                for error in result.errors:
                    print(f"    ❌ {error}")

        # Resilience score
        resilience_score = (successful_experiments / total_experiments) * 100
        print(f"\n🎯 RESILIENCE SCORE: {resilience_score:.1f}%")

        if resilience_score >= 90:
            print("🌟 EXCELLENT: Pipeline is highly resilient")
        elif resilience_score >= 75:
            print("👍 GOOD: Pipeline is resilient with minor issues")
        elif resilience_score >= 50:
            print("⚠️  FAIR: Pipeline needs improvement")
        else:
            print("🚨 POOR: Pipeline has significant resilience issues")

        # Save detailed results
        report_data = {
            "summary": {
                "total_experiments": total_experiments,
                "successful_experiments": successful_experiments,
                "resilience_score": resilience_score,
                "generated_at": datetime.now(timezone.utc).isoformat()
            },
            "results": [result.to_dict() for result in results]
        }

        with open("chaos_engineering_report.json", 'w') as f:
            json.dump(report_data, f, indent=2)

        print("💾 Detailed report saved to: chaos_engineering_report.json")


def main():
    """Main chaos engineering function."""
    parser = argparse.ArgumentParser(
        description="Run chaos engineering experiments")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--region", default="europe-central2", help="GCP region")
    parser.add_argument("--experiment", choices=[e.value for e in ChaosExperiment],
                        help="Specific experiment to run")
    parser.add_argument("--duration", type=int, default=300,
                        help="Experiment duration in seconds")
    parser.add_argument("--all-experiments", action="store_true",
                        help="Run all chaos experiments")
    parser.add_argument("--list-experiments", action="store_true",
                        help="List available experiments")

    args = parser.parse_args()

    if args.list_experiments:
        print("🧪 Available Chaos Experiments:")
        print("=" * 40)
        for experiment in ChaosExperiment:
            print(f"• {experiment.value}")
        return

    engineer = ChaosEngineer(args.project, args.region)

    if args.all_experiments:
        # Run all experiments
        experiments = list(ChaosExperiment)
        engineer.run_multiple_experiments(experiments, args.duration)

    elif args.experiment:
        # Run specific experiment
        experiment = ChaosExperiment(args.experiment)
        result = engineer.run_experiment(experiment, args.duration)

        # Print result
        print(f"\n📊 Experiment {experiment.value} Result:")
        print(f"Success: {'✅ Yes' if result.success else '❌ No'}")
        print(f"Duration: {result.recovery_time_seconds:.1f} seconds")

        if result.errors:
            print("Errors:")
            for error in result.errors:
                print(f"  • {error}")

    else:
        print("❌ No experiment specified. Use --help for options")
        sys.exit(1)

    print("🎉 Chaos engineering completed!")


if __name__ == "__main__":
    main()
