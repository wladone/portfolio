#!/usr/bin/env python3
"""
Comprehensive environment variable validation for the streaming pipeline.

This script validates all environment variables before pipeline execution
to prevent runtime failures from bad configuration.
"""

import os
import re
import sys
import json
from typing import Dict, List, Any, Optional


class EnvironmentValidator:
    """Validates environment variables for the streaming pipeline."""

    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []

        # Validation patterns
        self.patterns = {
            'project_id': re.compile(r'^[a-z][a-z0-9-]{5,29}$'),
            'region': re.compile(r'^[a-z]+-[a-z]+[0-9]$'),
            'bucket_name': re.compile(r'^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$'),
            'bigtable_instance': re.compile(r'^[a-z][-a-z0-9]{5,32}$'),
            'email': re.compile(r'^[^@]+@[^@]+\.[^@]+$'),
            'url': re.compile(r'^https?://.+')
        }

        # Required variables for different modes
        self.required_vars = {
            'basic': [
                'PROJECT_ID',
                'REGION',
                'DATAFLOW_BUCKET'
            ],
            'bigtable': [
                'BIGTABLE_INSTANCE',
                'BIGTABLE_TABLE'
            ],
            'composer': [
                'COMPOSER_ENV'
            ],
            'monitoring': [
                'EMAIL_ALERTS',
                'SLACK_WEBHOOK'
            ]
        }

    def validate_project_id(self, value: str) -> bool:
        """Validate GCP project ID format."""
        if not self.patterns['project_id'].match(value):
            self.errors.append(
                f"PROJECT_ID '{value}' must match pattern: ^[a-z][a-z0-9-]{5,29}$"
            )
            return False
        return True

    def validate_region(self, value: str) -> bool:
        """Validate GCP region format."""
        if not self.patterns['region'].match(value):
            self.errors.append(
                f"REGION '{value}' must match pattern: ^[a-z]+-[a-z]+[0-9]$"
            )
            return False
        return True

    def validate_bucket_name(self, value: str) -> bool:
        """Validate GCS bucket name."""
        if not self.patterns['bucket_name'].match(value):
            self.errors.append(
                f"DATAFLOW_BUCKET '{value}' must be a valid GCS bucket name"
            )
            return False
        return True

    def validate_bigtable_instance(self, value: str) -> bool:
        """Validate Bigtable instance name."""
        if not self.patterns['bigtable_instance'].match(value):
            self.errors.append(
                f"BIGTABLE_INSTANCE '{value}' must match pattern: ^[a-z][-a-z0-9]{5,32}$"
            )
            return False
        return True

    def validate_email(self, value: str) -> bool:
        """Validate email format."""
        if not self.patterns['email'].match(value):
            self.errors.append(
                f"Email '{value}' must be a valid email address"
            )
            return False
        return True

    def validate_gcp_resources_exist(self, project_id: str, region: str) -> bool:
        """Validate that GCP resources actually exist."""
        success = True

        # Check if project exists (basic check)
        if not self._run_gcloud_command([
            "projects", "describe", project_id, "--format", "value(projectId)"
        ]):
            self.errors.append(
                f"GCP project '{project_id}' does not exist or is not accessible")
            success = False

        # Check if bucket exists
        bucket = os.getenv('DATAFLOW_BUCKET', '')
        if bucket and not self._run_gcloud_command([
            "storage", "buckets", "describe", f"gs://{bucket}", "--format", "value(name)"
        ]):
            self.warnings.append(
                f"GCS bucket '{bucket}' does not exist or is not accessible")

        # Check if Bigtable instance exists
        bt_instance = os.getenv('BIGTABLE_INSTANCE', '')
        if bt_instance and not self._run_gcloud_command([
            "bigtable", "instances", "describe", bt_instance,
            "--project", project_id, "--format", "value(name)"
        ]):
            self.warnings.append(
                f"Bigtable instance '{bt_instance}' does not exist")

        return success

    def _run_gcloud_command(self, args: List[str]) -> str:
        """Run a gcloud command and return output."""
        try:
            import subprocess
            result = subprocess.run(
                ["gcloud"] + args,
                capture_output=True,
                text=True,
                timeout=10
            )
            return result.stdout.strip() if result.returncode == 0 else ""
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return ""

    def validate_all(self) -> bool:
        """Validate all environment variables."""
        print("🔍 Validating environment configuration...")
        print("=" * 50)

        # Get environment variables
        env = os.environ

        # Basic validation
        if 'PROJECT_ID' in env:
            self.validate_project_id(env['PROJECT_ID'])

        if 'REGION' in env:
            self.validate_region(env['REGION'])

        if 'DATAFLOW_BUCKET' in env:
            self.validate_bucket_name(env['DATAFLOW_BUCKET'])

        if 'BIGTABLE_INSTANCE' in env:
            self.validate_bigtable_instance(env['BIGTABLE_INSTANCE'])

        if 'EMAIL_ALERTS' in env:
            self.validate_email(env['EMAIL_ALERTS'])

        # Check for required variables
        mode = os.getenv('DEPLOYMENT_MODE', 'basic')
        required = self.required_vars.get(mode, self.required_vars['basic'])

        for var in required:
            if var not in env or not env[var].strip():
                self.errors.append(
                    f"Required variable '{var}' is missing or empty")

        # Validate GCP resources exist (if gcloud is available)
        if 'PROJECT_ID' in env and 'REGION' in env:
            self.validate_gcp_resources_exist(env['PROJECT_ID'], env['REGION'])

        # Print results
        if self.errors:
            print("❌ VALIDATION ERRORS:")
            for error in self.errors:
                print(f"  • {error}")

        if self.warnings:
            print("⚠️  VALIDATION WARNINGS:")
            for warning in self.warnings:
                print(f"  • {warning}")

        if not self.errors:
            print("✅ Environment validation passed!")
            if self.warnings:
                print(f"⚠️  {len(self.warnings)} warnings found (see above)")

        return len(self.errors) == 0


def generate_env_template():
    """Generate a properly configured .env template."""
    template = {
        "PROJECT_ID": "your-actual-project-id",
        "REGION": "europe-central2",
        "DATAFLOW_BUCKET": "your-project-dataflow-bucket",
        "BIGTABLE_INSTANCE": "your-bigtable-instance",
        "BIGTABLE_TABLE": "product_stats",
        "COMPOSER_ENV": "your-composer-environment",
        "TEMPLATE_PATH": "gs://your-project-dataflow-bucket/templates/streaming_pipeline_flex_template.json",
        "EMAIL_ALERTS": "alerts@yourcompany.com",
        "SLACK_WEBHOOK": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK",
        "DEPLOYMENT_MODE": "basic",
        "LOG_LEVEL": "INFO"
    }

    print("📝 Generated .env template:")
    print("# Copy this to .env and fill in actual values")
    for key, value in template.items():
        print(f"{key}={value}")

    return template


def main():
    """Main validation function."""
    validator = EnvironmentValidator()

    if len(sys.argv) > 1 and sys.argv[1] == "--template":
        generate_env_template()
        return

    success = validator.validate_all()

    if not success:
        print("\n💡 To generate a proper .env template, run:")
        print("   python scripts/validate_environment.py --template")
        sys.exit(1)

    print("\n🚀 Environment is ready for deployment!")
    sys.exit(0)


if __name__ == "__main__":
    main()
