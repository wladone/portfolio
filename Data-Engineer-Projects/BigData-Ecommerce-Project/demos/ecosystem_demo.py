#!/usr/bin/env python3
"""
Complete Ecosystem Demo - Showcases ALL Project Components

This demo demonstrates the entire BigData E-commerce Pipeline ecosystem,
showing how all components work together in a real-world scenario.
"""

import os
import sys
from pathlib import Path
from typing import List, Dict, Any

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from utils.display_utils import DisplayColors, TerminalUtils
except ImportError:
    # Fallback if utils not available
    class DisplayColors:
        GREEN = '\033[92m'
        BLUE = '\033[94m'
        YELLOW = '\033[93m'
        RED = '\033[91m'
        MAGENTA = '\033[95m'
        CYAN = '\033[96m'
        BOLD = '\033[1m'
        END = '\033[0m'

    class TerminalUtils:
        @staticmethod
        def clear_screen():
            print("\033[2J\033[H", end="")


class EcosystemDemo:
    """Comprehensive demo showcasing ALL project components"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.components = self._discover_components()

    def _discover_components(self) -> Dict[str, List[str]]:
        """Discover all components in the project"""
        components = {
            "Core Pipeline": [
                "beam/streaming_pipeline.py - Main Apache Beam pipeline",
                "beam/data_quality.py - Data validation logic",
                "config.py - Centralized configuration"
            ],
            "DevOps Scripts": [
                "scripts/create_venv.sh - Python environment setup",
                "scripts/validate_environment.py - Environment validation",
                "scripts/blue_green_deploy.py - Zero-downtime deployment",
                "scripts/chaos_engineering.py - Resilience testing"
            ],
            "Cost Management": [
                "scripts/cost_guardrails.sh - Automated cost controls",
                "scripts/cost_monitoring.py - Cost tracking and alerts",
                "scripts/set_bq_ttls.sh - BigQuery data retention",
                "scripts/set_gcs_lifecycle.sh - Storage lifecycle policies"
            ],
            "Monitoring & Alerting": [
                "monitoring/exporter.py - Metrics export",
                "monitoring/dashboard_ecommerce_pipeline.json - Main dashboard",
                "monitoring/alert_pipeline_health.json - Health alerts",
                "monitoring/alert_dead_letter_policy.json - Error alerts"
            ],
            "Orchestration": [
                "composer/dags/streaming_direct_dag.py - Direct execution DAG",
                "composer/dags/streaming_flex_dag.py - Flex template DAG",
                "composer/dags/streaming_smoke_dag.py - Smoke testing DAG"
            ],
            "Testing Framework": [
                "tests/test_streaming_pipeline_unit.py - Unit tests",
                "tests/test_streaming_pipeline_integration.py - Integration tests",
                "tests/test_streaming_pipeline_e2e.py - End-to-end tests",
                "tests/perf/test_pipeline_performance.py - Performance tests"
            ],
            "Database & Schema": [
                "sql/create_tables_partitioned.sql - Production table schemas",
                "sql/create_tables.sql - Development table schemas"
            ],
            "Operations Tools": [
                "scripts/cleanup_plan.sh - Safe cleanup planning",
                "scripts/compliance_audit.py - Security compliance",
                "scripts/secret_management.py - Secrets handling",
                "tools/dlq_consumer.py - Dead letter queue inspection"
            ],
            "Documentation": [
                "docs/api_documentation.yaml - API documentation",
                "docs/runbooks.md - Operations runbooks",
                "README.md - Project overview"
            ]
        }
        return components

    def show_ecosystem_overview(self):
        """Show the complete project ecosystem"""
        TerminalUtils.clear_screen()
        print(
            f"{DisplayColors.BOLD}{DisplayColors.GREEN}[ROCKET] COMPLETE ECOSYSTEM DEMO{DisplayColors.END}")
        print(f"{DisplayColors.CYAN}================================================================================{DisplayColors.END}")

        print(
            f"\n{DisplayColors.YELLOW}[BUILDING] PROJECT ARCHITECTURE:{DisplayColors.END}")
        print(
            f"   This demo showcases a complete enterprise-grade BigData pipeline ecosystem")
        print(f"   with DevOps, monitoring, orchestration, and operations capabilities.")

        print(
            f"\n{DisplayColors.BOLD}{DisplayColors.CYAN}[CHART] COMPLETE COMPONENT INVENTORY:{DisplayColors.END}")

        for category, items in self.components.items():
            print(f"\n{DisplayColors.GREEN}{category}:{DisplayColors.END}")
            for item in items:
                print(f"   • {item}")

        print(
            f"\n{DisplayColors.BOLD}{DisplayColors.YELLOW}[TARGET] DEMO WORKFLOW:{DisplayColors.END}")
        print(f"   1. [GEAR] Environment Setup & Validation")
        print(f"   2. [TEST] Testing Framework Execution")
        print(f"   3. [ROCKET] Pipeline Deployment Strategies")
        print(f"   4. [MONITOR] Monitoring & Alerting Setup")
        print(f"   5. [DOLLAR] Cost Management Implementation")
        print(f"   6. [TOOLS] Operations & Maintenance")
        print(f"   7. [CLEAN] Cleanup & Compliance")

        print(f"\n{DisplayColors.BOLD}{DisplayColors.CYAN}Press Enter to explore each component...{DisplayColors.END}")
        try:
            input()
        except EOFError:
            pass

    def demonstrate_environment_setup(self):
        """Demonstrate environment setup and validation"""
        TerminalUtils.clear_screen()
        print(
            f"{DisplayColors.BOLD}{DisplayColors.CYAN}[TOOLS] ENVIRONMENT SETUP & VALIDATION{DisplayColors.END}")

        print(
            f"\n{DisplayColors.YELLOW}Step 1: Virtual Environment Setup{DisplayColors.END}")
        print(f"   • scripts/create_venv.sh - Creates Python virtual environment")
        print(f"   • scripts/activate_venv.ps1 - Windows activation script")
        print(f"   • requirements.txt - Core dependencies")
        print(f"   • requirements-dev.txt - Development dependencies")

        if os.path.exists("scripts/create_venv.sh"):
            print(
                f"\n{DisplayColors.GREEN}[OK] Environment setup scripts available{DisplayColors.END}")
        else:
            print(
                f"\n{DisplayColors.RED}[FAIL] Environment setup scripts not found{DisplayColors.END}")

        print(
            f"\n{DisplayColors.YELLOW}Step 2: Environment Validation{DisplayColors.END}")
        print(f"   • scripts/validate_environment.py - Validates GCP resources")
        print(f"   • config.py - Configuration validation")
        print(f"   • .env.sample - Environment template")

        print(
            f"\n{DisplayColors.BOLD}{DisplayColors.CYAN}Commands to run:{DisplayColors.END}")
        print(f"   {DisplayColors.GREEN}Linux/macOS:{DisplayColors.END}")
        print(f"   chmod +x scripts/create_venv.sh && ./scripts/create_venv.sh")
        print(f"   source .venv/bin/activate")
        print(f"   python scripts/validate_environment.py")

        print(f"\n   {DisplayColors.GREEN}Windows:{DisplayColors.END}")
        print(f"   .\\scripts\\activate_venv.ps1")
        print(f"   .\\.venv\\Scripts\\Activate.ps1")
        print(f"   python scripts\\validate_environment.py")

        input("\nPress Enter to continue...")

    def demonstrate_testing_framework(self):
        """Demonstrate the testing framework"""
        TerminalUtils.clear_screen()
        print(
            f"{DisplayColors.BOLD}{DisplayColors.CYAN}[TEST] TESTING FRAMEWORK{DisplayColors.END}")

        print(f"\n{DisplayColors.YELLOW}Test Categories:{DisplayColors.END}")
        print(f"   • Unit Tests - Individual component testing")
        print(f"   • Integration Tests - Component interaction testing")
        print(f"   • End-to-End Tests - Complete workflow testing")
        print(f"   • Performance Tests - Load and throughput testing")

        print(f"\n{DisplayColors.GREEN}Available Test Files:{DisplayColors.END}")
        test_files = [
            "tests/test_config.py",
            "tests/test_streaming_pipeline_unit.py",
            "tests/test_streaming_pipeline_integration.py",
            "tests/test_streaming_pipeline_e2e.py",
            "tests/test_streaming_pipeline_perf.py"
        ]

        for test_file in test_files:
            if os.path.exists(test_file):
                print(f"   [OK] {test_file}")
            else:
                print(f"   [FAIL] {test_file}")

        print(
            f"\n{DisplayColors.BOLD}{DisplayColors.CYAN}Test Execution Commands:{DisplayColors.END}")
        print(f"   {DisplayColors.GREEN}Run all tests:{DisplayColors.END}")
        print(f"   pytest -q")

        print(f"\n   {DisplayColors.GREEN}Run with coverage:{DisplayColors.END}")
        print(f"   pytest --cov=beam tests/")

        print(
            f"\n   {DisplayColors.GREEN}Performance tests only:{DisplayColors.END}")
        print(f"   RUN_PERF_TESTS=1 pytest -m performance")

        print(f"\n   {DisplayColors.GREEN}Integration tests:{DisplayColors.END}")
        print(f"   pytest tests/test_streaming_pipeline_integration.py")

        input("\nPress Enter to continue...")

    def demonstrate_deployment_strategies(self):
        """Demonstrate deployment and orchestration"""
        TerminalUtils.clear_screen()
        print(
            f"{DisplayColors.BOLD}{DisplayColors.CYAN}🚀 DEPLOYMENT & ORCHESTRATION{DisplayColors.END}")

        print(f"\n{DisplayColors.YELLOW}Deployment Strategies:{DisplayColors.END}")
        print(f"   • Direct Runner - Local development and testing")
        print(f"   • Flex Templates - Scalable cloud deployment")
        print(f"   • Cloud Composer - Orchestrated workflow execution")

        print(f"\n{DisplayColors.GREEN}Orchestration DAGs:{DisplayColors.END}")
        dag_files = [
            "composer/dags/streaming_direct_dag.py",
            "composer/dags/streaming_flex_dag.py",
            "composer/dags/streaming_smoke_dag.py"
        ]

        for dag_file in dag_files:
            if os.path.exists(dag_file):
                print(f"   ✅ {dag_file}")
            else:
                print(f"   ❌ {dag_file}")

        print(f"\n{DisplayColors.GREEN}Deployment Scripts:{DisplayColors.END}")
        deploy_scripts = [
            "scripts/blue_green_deploy.py - Zero-downtime deployment",
            "scripts/deploy_monitoring.sh - Monitoring setup",
            "scripts/setup_observability.py - Observability configuration"
        ]

        for script in deploy_scripts:
            print(f"   • {script}")

        print(
            f"\n{DisplayColors.BOLD}{DisplayColors.CYAN}Deployment Commands:{DisplayColors.END}")
        print(f"   {DisplayColors.GREEN}Local development:{DisplayColors.END}")
        print(f"   python run.py local")

        print(
            f"\n   {DisplayColors.GREEN}Cloud Dataflow (Flex):{DisplayColors.END}")
        print(f"   python run.py flex")

        print(f"\n   {DisplayColors.GREEN}Cloud Composer:{DisplayColors.END}")
        print(f"   python run.py direct")

        input("\nPress Enter to continue...")

    def demonstrate_monitoring_setup(self):
        """Demonstrate monitoring and alerting"""
        TerminalUtils.clear_screen()
        print(
            f"{DisplayColors.BOLD}{DisplayColors.CYAN}📊 MONITORING & ALERTING{DisplayColors.END}")

        print(f"\n{DisplayColors.YELLOW}Monitoring Components:{DisplayColors.END}")
        print(f"   • Real-time metrics collection")
        print(f"   • Custom dashboards")
        print(f"   • Intelligent alerting")
        print(f"   • Performance monitoring")

        print(f"\n{DisplayColors.GREEN}Available Dashboards:{DisplayColors.END}")
        dashboards = [
            "monitoring/dashboard_ecommerce_pipeline.json",
            "monitoring/dashboard_comprehensive.json"
        ]

        for dashboard in dashboards:
            if os.path.exists(dashboard):
                print(f"   ✅ {dashboard}")
            else:
                print(f"   ❌ {dashboard}")

        print(f"\n{DisplayColors.GREEN}Alert Policies:{DisplayColors.END}")
        alerts = [
            "monitoring/alert_pipeline_health.json",
            "monitoring/alert_dead_letter_policy.json",
            "monitoring/alert_low_throughput_policy.json"
        ]

        for alert in alerts:
            if os.path.exists(alert):
                print(f"   ✅ {alert}")
            else:
                print(f"   ❌ {alert}")

        print(
            f"\n{DisplayColors.BOLD}{DisplayColors.CYAN}Monitoring Commands:{DisplayColors.END}")
        print(f"   {DisplayColors.GREEN}Deploy monitoring:{DisplayColors.END}")
        print(f"   chmod +x scripts/deploy_monitoring.sh && ./scripts/deploy_monitoring.sh")

        print(
            f"\n   {DisplayColors.GREEN}Setup monitoring channels:{DisplayColors.END}")
        print(f"   python scripts/setup_monitoring_channels.py")

        print(
            f"\n   {DisplayColors.GREEN}Check dead letters:{DisplayColors.END}")
        print(f"   python tools/dlq_consumer.py")

        input("\nPress Enter to continue...")

    def demonstrate_cost_management(self):
        """Demonstrate cost management tools"""
        TerminalUtils.clear_screen()
        print(
            f"{DisplayColors.BOLD}{DisplayColors.CYAN}💰 COST MANAGEMENT{DisplayColors.END}")

        print(f"\n{DisplayColors.YELLOW}Cost Control Features:{DisplayColors.END}")
        print(f"   • Automated cost monitoring")
        print(f"   • Data retention policies")
        print(f"   • Storage lifecycle management")
        print(f"   • Budget alerts and guardrails")

        print(f"\n{DisplayColors.GREEN}Cost Management Scripts:{DisplayColors.END}")
        cost_scripts = [
            "scripts/cost_guardrails.sh - Automated cost controls",
            "scripts/cost_monitoring.py - Cost tracking",
            "scripts/set_bq_ttls.sh - BigQuery retention (3 days)",
            "scripts/set_gcs_lifecycle.sh - Storage cleanup"
        ]

        for script in cost_scripts:
            print(f"   • {script}")

        print(
            f"\n{DisplayColors.BOLD}{DisplayColors.CYAN}Cost Management Commands:{DisplayColors.END}")
        print(f"   {DisplayColors.GREEN}Set BigQuery TTLs:{DisplayColors.END}")
        print(f"   chmod +x scripts/set_bq_ttls.sh && ./scripts/set_bq_ttls.sh")

        print(f"\n   {DisplayColors.GREEN}Set GCS lifecycle:{DisplayColors.END}")
        print(f"   chmod +x scripts/set_gcs_lifecycle.sh && ./scripts/set_gcs_lifecycle.sh")

        print(
            f"\n   {DisplayColors.GREEN}Run cost guardrails:{DisplayColors.END}")
        print(f"   chmod +x scripts/cost_guardrails.sh && ./scripts/cost_guardrails.sh")

        print(f"\n   {DisplayColors.GREEN}Cost monitoring:{DisplayColors.END}")
        print(f"   python scripts/cost_monitoring.py")

        input("\nPress Enter to continue...")

    def demonstrate_operations(self):
        """Demonstrate operational workflows"""
        TerminalUtils.clear_screen()
        print(
            f"{DisplayColors.BOLD}{DisplayColors.CYAN}🛠️ OPERATIONS & MAINTENANCE{DisplayColors.END}")

        print(f"\n{DisplayColors.YELLOW}Operational Tools:{DisplayColors.END}")
        print(f"   • Safe cleanup and resource management")
        print(f"   • Security compliance auditing")
        print(f"   • Secret management")
        print(f"   • Load testing and validation")

        print(f"\n{DisplayColors.GREEN}Operations Scripts:{DisplayColors.END}")
        ops_scripts = [
            "scripts/cleanup_plan.sh - Safe cleanup planning",
            "scripts/cleanup_apply.sh - Execute cleanup",
            "scripts/compliance_audit.py - Security compliance",
            "scripts/secret_management.py - Secrets handling",
            "scripts/load_test.py - Load testing",
            "scripts/publish_samples.sh - Sample data publishing"
        ]

        for script in ops_scripts:
            print(f"   • {script}")

        print(f"\n{DisplayColors.GREEN}Operations Tools:{DisplayColors.END}")
        tools = [
            "tools/dlq_consumer.py - Dead letter queue inspection"
        ]

        for tool in tools:
            if os.path.exists(tool):
                print(f"   ✅ {tool}")
            else:
                print(f"   ❌ {tool}")

        print(
            f"\n{DisplayColors.BOLD}{DisplayColors.CYAN}Operations Commands:{DisplayColors.END}")
        print(f"   {DisplayColors.GREEN}Safe cleanup (dry run):{DisplayColors.END}")
        print(f"   chmod +x scripts/cleanup_plan.sh && ./scripts/cleanup_plan.sh")

        print(f"\n   {DisplayColors.GREEN}Execute cleanup:{DisplayColors.END}")
        print(f"   CONFIRM=DELETE ./scripts/cleanup_apply.sh")

        print(f"\n   {DisplayColors.GREEN}Load testing:{DisplayColors.END}")
        print(f"   python scripts/load_test.py")

        print(f"\n   {DisplayColors.GREEN}DLQ inspection:{DisplayColors.END}")
        print(f"   python tools/dlq_consumer.py")

        input("\nPress Enter to continue...")

    def demonstrate_database_schemas(self):
        """Demonstrate database schema management"""
        TerminalUtils.clear_screen()
        print(
            f"{DisplayColors.BOLD}{DisplayColors.CYAN}🗄️ DATABASE SCHEMAS{DisplayColors.END}")

        print(f"\n{DisplayColors.YELLOW}Schema Management:{DisplayColors.END}")
        print(f"   • Partitioned tables for cost efficiency")
        print(f"   • Clustered tables for query performance")
        print(f"   • Automated table creation")
        print(f"   • Schema evolution support")

        print(f"\n{DisplayColors.GREEN}Available Schemas:{DisplayColors.END}")
        schemas = [
            "sql/create_tables_partitioned.sql - Production optimized",
            "sql/create_tables.sql - Development schema"
        ]

        for schema in schemas:
            if os.path.exists(schema):
                print(f"   ✅ {schema}")
            else:
                print(f"   ❌ {schema}")

        print(
            f"\n{DisplayColors.BOLD}{DisplayColors.CYAN}Schema Commands:{DisplayColors.END}")
        print(
            f"   {DisplayColors.GREEN}Create BigQuery dataset:{DisplayColors.END}")
        print(f"   bq --location=europe-central2 mk --dataset your-project:ecommerce")

        print(
            f"\n   {DisplayColors.GREEN}Create partitioned tables:{DisplayColors.END}")
        print(f"   bq query --use_legacy_sql=false < sql/create_tables_partitioned.sql")

        print(
            f"\n   {DisplayColors.GREEN}View schema details:{DisplayColors.END}")
        print(f"   bq show your-project:ecommerce.product_views_summary")

        input("\nPress Enter to continue...")

    def run_comprehensive_demo(self):
        """Run the complete ecosystem demonstration"""
        try:
            self.show_ecosystem_overview()
            self.demonstrate_environment_setup()
            self.demonstrate_testing_framework()
            self.demonstrate_deployment_strategies()
            self.demonstrate_monitoring_setup()
            self.demonstrate_cost_management()
            self.demonstrate_operations()
            self.demonstrate_database_schemas()

            self.show_final_ecosystem_summary()

        except KeyboardInterrupt:
            print(
                f"\n{DisplayColors.YELLOW}Ecosystem demo interrupted by user{DisplayColors.END}")
        except Exception as e:
            print(
                f"\n{DisplayColors.RED}Error in ecosystem demo: {e}{DisplayColors.END}")

    def show_final_ecosystem_summary(self):
        """Show comprehensive summary of the entire ecosystem"""
        TerminalUtils.clear_screen()
        print(f"{DisplayColors.BOLD}{DisplayColors.GREEN}🎉 COMPLETE ECOSYSTEM DEMO FINISHED!{DisplayColors.END}")
        print(f"{DisplayColors.CYAN}================================================================================{DisplayColors.END}")

        print(
            f"\n{DisplayColors.BOLD}{DisplayColors.BLUE}🏆 WHAT YOU'VE EXPLORED:{DisplayColors.END}")

        total_components = sum(len(items)
                               for items in self.components.values())
        print(f"   • {total_components} total components demonstrated")
        print(f"   • Complete enterprise-grade pipeline ecosystem")
        print(f"   • Production-ready DevOps and monitoring")
        print(f"   • Comprehensive testing and validation")
        print(f"   • Advanced cost management and compliance")

        print(
            f"\n{DisplayColors.BOLD}{DisplayColors.YELLOW}🚀 PRODUCTION READINESS:{DisplayColors.END}")
        print(f"   • [GEAR] Environment setup and validation")
        print(
            f"   • [TEST] Multi-layer testing (unit, integration, e2e, performance)")
        print(f"   • [ROCKET] Multiple deployment strategies")
        print(f"   • [MONITOR] Real-time monitoring and alerting")
        print(f"   • [DOLLAR] Automated cost controls")
        print(f"   • [TOOLS] Complete operational toolkit")
        print(f"   • [CLEAN] Maintenance and compliance automation")

        print(f"\n{DisplayColors.BOLD}{DisplayColors.MAGENTA}💎 ENTERPRISE FEATURES DEMONSTRATED:{DisplayColors.END}")
        print(f"   • Zero-downtime blue-green deployments")
        print(f"   • Chaos engineering for resilience testing")
        print(f"   • Automated cost guardrails and budget management")
        print(f"   • Security compliance auditing")
        print(f"   • Comprehensive monitoring with custom dashboards")
        print(f"   • Airflow orchestration with multiple execution strategies")
        print(f"   • Dead letter queue processing and error analysis")

        print(
            f"\n{DisplayColors.BOLD}{DisplayColors.CYAN}🎯 KEY TAKEAWAY:{DisplayColors.END}")
        print(f"   This is not just a pipeline - it's a complete")
        print(f"   enterprise-grade BigData ecosystem with everything")
        print(f"   needed for production deployment and operations!")

        print(
            f"\n{DisplayColors.GREEN}Ready to deploy to production with confidence! 🚀{DisplayColors.END}")


def main():
    """Main ecosystem demo function"""
    print(f"\n{DisplayColors.BOLD}{DisplayColors.CYAN}*** BIGDATA E-COMMERCE PIPELINE ECOSYSTEM ***{DisplayColors.END}")
    print(f"{DisplayColors.GREEN}Complete Enterprise-Grade BigData Ecosystem Demonstration{DisplayColors.END}")

    demo = EcosystemDemo()
    demo.run_comprehensive_demo()


if __name__ == "__main__":
    main()
