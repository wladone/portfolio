#!/usr/bin/env python3
"""
Ecosystem Overview - Shows ALL Project Components

This script provides a comprehensive overview of all components
in the BigData E-commerce Pipeline project.
"""

import os
from pathlib import Path


class EcosystemOverview:
    """Shows the complete project ecosystem"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent

    def show_component_inventory(self):
        """Display all components in the project"""
        print("\n" + "="*70)
        print("COMPLETE BIGDATA E-COMMERCE PIPELINE ECOSYSTEM")
        print("="*70)

        components = {
            "[ROCKET] Core Pipeline": {
                "files": [
                    "beam/streaming_pipeline.py - Main Apache Beam pipeline",
                    "beam/data_quality.py - Data validation logic",
                    "config.py - Centralized configuration"
                ],
                "description": "Real-time stream processing with Apache Beam"
            },
            "[TOOLS] DevOps Scripts": {
                "files": [
                    "scripts/create_venv.sh - Python environment setup",
                    "scripts/validate_environment.py - Environment validation",
                    "scripts/blue_green_deploy.py - Zero-downtime deployment",
                    "scripts/chaos_engineering.py - Resilience testing"
                ],
                "description": "Complete deployment and operations automation"
            },
            "[MONEY] Cost Management": {
                "files": [
                    "scripts/cost_guardrails.sh - Automated cost controls",
                    "scripts/cost_monitoring.py - Cost tracking",
                    "scripts/set_bq_ttls.sh - BigQuery retention (3 days)",
                    "scripts/set_gcs_lifecycle.sh - Storage cleanup"
                ],
                "description": "Automated cost optimization and monitoring"
            },
            "[CHART] Monitoring & Alerting": {
                "files": [
                    "monitoring/exporter.py - Metrics export",
                    "monitoring/dashboard_ecommerce_pipeline.json - Main dashboard",
                    "monitoring/alert_pipeline_health.json - Health alerts",
                    "monitoring/alert_dead_letter_policy.json - Error alerts"
                ],
                "description": "Real-time monitoring and alerting system"
            },
            "[MUSIC] Orchestration": {
                "files": [
                    "composer/dags/streaming_direct_dag.py - Direct execution",
                    "composer/dags/streaming_flex_dag.py - Flex template",
                    "composer/dags/streaming_smoke_dag.py - Smoke testing"
                ],
                "description": "Airflow-based workflow orchestration"
            },
            "[TEST] Testing Framework": {
                "files": [
                    "tests/test_streaming_pipeline_unit.py - Unit tests",
                    "tests/test_streaming_pipeline_integration.py - Integration",
                    "tests/test_streaming_pipeline_e2e.py - End-to-end",
                    "tests/perf/test_pipeline_performance.py - Performance"
                ],
                "description": "Comprehensive multi-layer testing"
            },
            "[DATABASE] Database & Schema": {
                "files": [
                    "sql/create_tables_partitioned.sql - Production schemas",
                    "sql/create_tables.sql - Development schemas"
                ],
                "description": "Optimized BigQuery table schemas"
            },
            "[GEAR] Operations Tools": {
                "files": [
                    "scripts/cleanup_plan.sh - Safe cleanup planning",
                    "scripts/compliance_audit.py - Security compliance",
                    "scripts/secret_management.py - Secrets handling",
                    "tools/dlq_consumer.py - Dead letter queue inspection"
                ],
                "description": "Production operations and maintenance"
            }
        }

        total_files = 0
        for category, info in components.items():
            print(f"\n{category}")
            print("-" * 50)
            print(f"Description: {info['description']}")
            print("Files:")
            for file_desc in info['files']:
                total_files += 1
                print(f"  • {file_desc}")

        print(f"\n{'='*70}")
        print(f"[CHART] ECOSYSTEM SUMMARY")
        print(f"{'='*70}")
        print(f"Total Categories: {len(components)}")
        print(f"Total Components: {total_files}")
        print(f"Enterprise Readiness: PRODUCTION-GRADE")

        print(f"\n[ROCKET] KEY CAPABILITIES:")
        print(f"  [OK] Real-time stream processing")
        print(f"  [OK] Enterprise DevOps automation")
        print(f"  [OK] Comprehensive monitoring")
        print(f"  [OK] Multi-layer testing")
        print(f"  [OK] Cost optimization")
        print(f"  [OK] Security compliance")
        print(f"  [OK] Production operations")

    def show_usage_guide(self):
        """Show how to use all components"""
        print(f"\n{'='*70}")
        print("COMPLETE USAGE GUIDE")
        print(f"{'='*70}")

        print(f"\n1. [ROCKET] CORE PIPELINE:")
        print(f"   python run.py local    # Local development")
        print(f"   python run.py flex     # Google Cloud Dataflow")
        print(f"   python run.py direct   # Cloud Composer")

        print(f"\n2. [DEMO] DEMONSTRATIONS:")
        print(f"   python demos/demo.py                    # Real-time demo")
        print(f"   python demos/results_viewer.py          # Static results")
        print(f"   python demos/ecosystem_overview.py      # Complete ecosystem")

        print(f"\n3. [TEST] TESTING:")
        print(f"   pytest -q                              # All tests")
        print(f"   RUN_PERF_TESTS=1 pytest -m performance # Performance tests")

        print(f"\n4. [MONITOR] MONITORING:")
        print(f"   chmod +x scripts/deploy_monitoring.sh && ./scripts/deploy_monitoring.sh")
        print(f"   python tools/dlq_consumer.py")

        print(f"\n5. [COST] COST MANAGEMENT:")
        print(f"   chmod +x scripts/set_bq_ttls.sh && ./scripts/set_bq_ttls.sh")
        print(f"   python scripts/cost_monitoring.py")

        print(f"\n6. [DEPLOY] DEPLOYMENT:")
        print(f"   python scripts/blue_green_deploy.py")
        print(f"   python scripts/validate_environment.py")

    def run_overview(self):
        """Run the complete ecosystem overview"""
        try:
            self.show_component_inventory()
            self.show_usage_guide()

            print(f"\n{'='*70}")
            print("CONCLUSION")
            print(f"{'='*70}")
            print("This is a COMPLETE enterprise-grade BigData ecosystem!")
            print("- Real pipeline + DevOps + Monitoring + Testing + Operations")
            print("- Production-ready with comprehensive tooling")
            print("- Zero-cost demo, scalable to massive production workloads")

        except Exception as e:
            print(f"Error in ecosystem overview: {e}")


def main():
    """Main function"""
    print("BIGDATA E-COMMERCE PIPELINE - COMPLETE ECOSYSTEM OVERVIEW")
    print("Showing ALL components and how they work together...")

    overview = EcosystemOverview()
    overview.run_overview()


if __name__ == "__main__":
    main()
