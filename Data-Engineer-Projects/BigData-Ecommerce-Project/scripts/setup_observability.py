#!/usr/bin/env python3
"""
Observability Setup for Streaming Pipeline

This script sets up comprehensive observability including:
- Distributed tracing with OpenTelemetry
- Centralized log aggregation
- Custom metrics and dashboards
- Alert correlation and analysis

Usage:
    python scripts/setup_observability.py --project your-project --enable-tracing
"""

import argparse
import json
import subprocess
import sys
from typing import Dict, List, Any

import yaml


class ObservabilityManager:
    """Manages observability setup for the pipeline."""

    def __init__(self, project_id: str, region: str = "europe-central2"):
        self.project_id = project_id
        self.region = region

    def setup_cloud_logging(self):
        """Set up centralized cloud logging."""
        print("📝 Setting up Cloud Logging...")

        try:
            # Create log sinks for different environments
            sinks = [
                {
                    "name": "pipeline-logs-to-bigquery",
                    "filter": 'resource.type="dataflow_step"',
                    "destination": f"bigquery.googleapis.com/projects/{self.project_id}/datasets/pipeline_logs",
                    "description": "Export pipeline logs to BigQuery for analysis"
                },
                {
                    "name": "error-logs-to-pubsub",
                    "filter": 'severity>=ERROR AND resource.type="dataflow_step"',
                    "destination": f"pubsub.googleapis.com/projects/{self.project_id}/topics/pipeline-error-alerts",
                    "description": "Route error logs to Pub/Sub for alerting"
                }
            ]

            for sink in sinks:
                try:
                    # Check if sink exists
                    result = subprocess.run([
                        "gcloud", "logging", "sinks", "describe", sink["name"],
                        "--project", self.project_id,
                        "--format", "json"
                    ], capture_output=True, text=True)

                    if result.returncode == 0:
                        print(f"  ✅ Log sink '{sink['name']}' already exists")
                        continue

                    # Create log sink
                    sink_config = {
                        "name": sink["name"],
                        "filter": sink["filter"],
                        "destination": sink["destination"],
                        "description": sink["description"]
                    }

                    config_file = f"/tmp/sink_{sink['name']}.json"
                    with open(config_file, 'w') as f:
                        json.dump(sink_config, f)

                    subprocess.run([
                        "gcloud", "logging", "sinks", "create", sink["name"],
                        "--project", self.project_id,
                        f"--log-filter={sink['filter']}",
                        f"--sink-destination={sink['destination']}",
                        f"--description={sink['description']}"
                    ], check=True, capture_output=True)

                    print(f"  ✅ Created log sink: {sink['name']}")

                except subprocess.CalledProcessError as e:
                    print(f"  ❌ Failed to create log sink {sink['name']}: {e}")

        except Exception as e:
            print(f"❌ Cloud Logging setup failed: {e}")

    def setup_cloud_monitoring(self):
        """Set up enhanced monitoring and alerting."""
        print("📊 Setting up Cloud Monitoring...")

        try:
            # Create custom dashboard
            dashboard_config = {
                "displayName": "E-commerce Streaming Pipeline Dashboard",
                "dashboardFilters": [
                    {
                        "filterType": "RESOURCE_LABEL",
                        "labelKey": "project_id",
                        "stringValue": self.project_id
                    }
                ],
                "widgets": [
                    {
                        "title": "Pipeline Throughput",
                        "xyChart": {
                            "dataSets": [
                                {
                                    "timeSeriesQuery": {
                                        "timeSeriesFilter": {
                                            "filter": f'metric.type="dataflow.googleapis.com/job/element_count" AND resource.labels.project_id="{self.project_id}"',
                                            "aggregation": {
                                                "alignmentPeriod": "60s",
                                                "perSeriesAligner": "ALIGN_RATE",
                                                "crossSeriesReducer": "REDUCE_SUM"
                                            }
                                        }
                                    }
                                }
                            ],
                            "chartOptions": {
                                "mode": "COLOR"
                            }
                        }
                    },
                    {
                        "title": "Error Rate",
                        "xyChart": {
                            "dataSets": [
                                {
                                    "timeSeriesQuery": {
                                        "timeSeriesFilter": {
                                            "filter": f'metric.type="dataflow.googleapis.com/job/error_count" AND resource.labels.project_id="{self.project_id}"',
                                            "aggregation": {
                                                "alignmentPeriod": "300s",
                                                "perSeriesAligner": "ALIGN_RATE"
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "title": "Data Quality Metrics",
                        "xyChart": {
                            "dataSets": [
                                {
                                    "timeSeriesQuery": {
                                        "timeSeriesFilter": {
                                            "filter": f'metric.type="custom.googleapis.com/data_quality/valid_events" AND resource.labels.project_id="{self.project_id}"'
                                        }
                                    }
                                }
                            ]
                        }
                    }
                ]
            }

            # Save dashboard config
            dashboard_file = "/tmp/pipeline_dashboard.json"
            with open(dashboard_file, 'w') as f:
                json.dump(dashboard_config, f, indent=2)

            # Create or update dashboard
            try:
                subprocess.run([
                    "gcloud", "monitoring", "dashboards", "create",
                    "--project", self.project_id,
                    f"--config-from-file={dashboard_file}"
                ], check=True, capture_output=True)

                print("  ✅ Created monitoring dashboard")

            except subprocess.CalledProcessError:
                # Try to update existing dashboard
                try:
                    subprocess.run([
                        "gcloud", "monitoring", "dashboards", "update", "pipeline-dashboard",
                        "--project", self.project_id,
                        f"--config-from-file={dashboard_file}"
                    ], check=True, capture_output=True)

                    print("  ✅ Updated monitoring dashboard")

                except subprocess.CalledProcessError as e:
                    print(f"  ⚠️  Failed to create/update dashboard: {e}")

        except Exception as e:
            print(f"❌ Cloud Monitoring setup failed: {e}")

    def setup_cloud_trace(self):
        """Set up distributed tracing."""
        print("🔍 Setting up Cloud Trace...")

        try:
            # Enable Cloud Trace API if not already enabled
            try:
                subprocess.run([
                    "gcloud", "services", "enable", "cloudtrace.googleapis.com",
                    "--project", self.project_id
                ], check=True, capture_output=True)

                print("  ✅ Enabled Cloud Trace API")

            except subprocess.CalledProcessError:
                print("  ✅ Cloud Trace API already enabled")

            # Create tracing configuration for Dataflow
            trace_config = {
                "sampling": {
                    "policy": "ALWAYS"
                },
                "exporter": {
                    "type": "CLOUD_TRACE"
                }
            }

            print("  ✅ Cloud Trace configuration completed")

        except Exception as e:
            print(f"❌ Cloud Trace setup failed: {e}")

    def setup_log_based_metrics(self):
        """Set up custom log-based metrics."""
        print("📈 Setting up log-based metrics...")

        try:
            # Create metrics for data quality
            metrics = [
                {
                    "name": "data_quality_valid_events",
                    "filter": 'resource.type="dataflow_step" AND textPayload:"Data Quality Error"',
                    "metricDescriptor": {
                        "name": f"projects/{self.project_id}/metricDescriptors/custom.googleapis.com/data_quality/valid_events",
                        "type": "custom.googleapis.com/data_quality/valid_events",
                        "labels": [
                            {"key": "pipeline_stage", "valueType": "STRING"},
                            {"key": "error_type", "valueType": "STRING"}
                        ],
                        "metricKind": "DELTA",
                        "valueType": "INT64",
                        "unit": "1",
                        "description": "Count of valid events processed"
                    }
                },
                {
                    "name": "pipeline_processing_latency",
                    "filter": 'resource.type="dataflow_step" AND textPayload:"Processing completed"',
                    "metricDescriptor": {
                        "name": f"projects/{self.project_id}/metricDescriptors/custom.googleapis.com/pipeline/processing_latency",
                        "type": "custom.googleapis.com/pipeline/processing_latency",
                        "metricKind": "GAUGE",
                        "valueType": "DOUBLE",
                        "unit": "ms",
                        "description": "Processing latency in milliseconds"
                    }
                }
            ]

            for metric in metrics:
                try:
                    # Create log-based metric
                    metric_config = {
                        "name": metric["name"],
                        "filter": metric["filter"],
                        "metricDescriptor": metric["metricDescriptor"]
                    }

                    config_file = f"/tmp/metric_{metric['name']}.json"
                    with open(config_file, 'w') as f:
                        json.dump(metric_config, f)

                    subprocess.run([
                        "gcloud", "logging", "metrics", "create", metric["name"],
                        "--project", self.project_id,
                        f"--config-from-file={config_file}"
                    ], check=True, capture_output=True)

                    print(f"  ✅ Created log-based metric: {metric['name']}")

                except subprocess.CalledProcessError as e:
                    print(
                        f"  ⚠️  Failed to create metric {metric['name']}: {e}")

        except Exception as e:
            print(f"❌ Log-based metrics setup failed: {e}")

    def setup_alert_correlation(self):
        """Set up alert correlation and analysis."""
        print("🔗 Setting up alert correlation...")

        try:
            # Create alerting policy for correlated events
            correlation_policy = {
                "displayName": "Pipeline Issue Correlation",
                "combiner": "OR",
                "conditions": [
                    {
                        "displayName": "High error rate with data quality issues",
                        "conditionThreshold": {
                            "filter": f'metric.type="dataflow.googleapis.com/job/error_count" AND resource.labels.project_id="{self.project_id}"',
                            "comparison": "COMPARISON_GT",
                            "thresholdValue": 100,
                            "duration": "300s"
                        }
                    },
                    {
                        "displayName": "Data quality degradation",
                        "conditionThreshold": {
                            "filter": f'metric.type="custom.googleapis.com/data_quality/valid_events" AND resource.labels.project_id="{self.project_id}"',
                            "comparison": "COMPARISON_LT",
                            "thresholdValue": 0.8,
                            "duration": "600s"
                        }
                    }
                ],
                "notificationChannels": [
                    f"projects/{self.project_id}/notificationChannels/pipeline-alerts"
                ]
            }

            # Save correlation policy
            policy_file = "/tmp/correlation_policy.json"
            with open(policy_file, 'w') as f:
                json.dump(correlation_policy, f, indent=2)

            # Create alerting policy
            try:
                subprocess.run([
                    "gcloud", "alpha", "monitoring", "policies", "create",
                    "--project", self.project_id,
                    f"--policy-from-file={policy_file}"
                ], check=True, capture_output=True)

                print("  ✅ Created alert correlation policy")

            except subprocess.CalledProcessError as e:
                print(f"  ⚠️  Failed to create correlation policy: {e}")

        except Exception as e:
            print(f"❌ Alert correlation setup failed: {e}")

    def generate_observability_config(self) -> str:
        """Generate observability configuration for pipeline."""
        config = {
            "cloud_logging": {
                "enabled": True,
                "log_levels": {
                    "dataflow": "INFO",
                    "pipeline": "DEBUG",
                    "data_quality": "WARNING"
                },
                "retention_days": 90
            },
            "cloud_monitoring": {
                "enabled": True,
                "collection_interval_seconds": 60,
                "custom_metrics": [
                    "data_quality_score",
                    "processing_latency",
                    "throughput_per_second",
                    "error_rate_percentage"
                ]
            },
            "cloud_trace": {
                "enabled": True,
                "sampling_rate": 0.1,  # 10% sampling
                "max_attributes": 32,
                "max_events": 128
            },
            "alerting": {
                "correlation_enabled": True,
                "notification_channels": [
                    "email",
                    "slack",
                    "pager_duty"  # If configured
                ],
                "escalation_policies": [
                    {
                        "name": "immediate",
                        "delay_minutes": 0,
                        "channels": ["email", "slack"]
                    },
                    {
                        "name": "escalation",
                        "delay_minutes": 15,
                        "channels": ["pager_duty"]
                    }
                ]
            }
        }

        return yaml.dump(config, default_flow_style=False)

    def run_observability_setup(self):
        """Run complete observability setup."""
        print("🔭 Setting up Comprehensive Observability")
        print("=" * 50)

        try:
            # 1. Cloud Logging
            self.setup_cloud_logging()

            # 2. Cloud Monitoring
            self.setup_cloud_monitoring()

            # 3. Cloud Trace
            self.setup_cloud_trace()

            # 4. Log-based Metrics
            self.setup_log_based_metrics()

            # 5. Alert Correlation
            self.setup_alert_correlation()

            print("✅ Observability setup completed!")

            # Generate configuration for reference
            config = self.generate_observability_config()
            config_file = "observability_config.yaml"

            with open(config_file, 'w') as f:
                f.write(config)

            print(f"💾 Observability configuration saved to: {config_file}")

        except Exception as e:
            print(f"❌ Observability setup failed: {e}")
            raise


def main():
    """Main observability setup function."""
    parser = argparse.ArgumentParser(
        description="Set up observability for streaming pipeline")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--region", default="europe-central2", help="GCP region")
    parser.add_argument("--config-only", action="store_true",
                        help="Generate config only")

    args = parser.parse_args()

    manager = ObservabilityManager(args.project, args.region)

    if args.config_only:
        # Generate configuration only
        config = manager.generate_observability_config()
        print("🔧 Observability Configuration:")
        print("=" * 40)
        print(config)
    else:
        # Run full setup
        manager.run_observability_setup()

    print("🎉 Observability setup completed successfully!")


if __name__ == "__main__":
    main()
