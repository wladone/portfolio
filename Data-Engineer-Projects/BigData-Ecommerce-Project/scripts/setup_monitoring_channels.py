#!/usr/bin/env python3
"""
Setup script for Cloud Monitoring notification channels.

This script helps configure proper notification channels for pipeline alerts
instead of leaving them empty (which is a critical security issue).
"""

import subprocess
import sys
import json
from typing import List, Dict, Any


def run_gcloud_command(args: List[str]) -> str:
    """Run a gcloud command and return output."""
    try:
        result = subprocess.run(
            ["gcloud"] + args,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running gcloud command: {e}")
        print(f"STDERR: {e.stderr}")
        return ""


def create_email_channel(project_id: str, email: str) -> str:
    """Create an email notification channel."""
    channel_name = f"pipeline-alerts-{email.replace('@', '-at-').replace('.', '-')}"

    # Check if channel already exists
    existing = run_gcloud_command([
        "monitoring", "channels", "list",
        f"--project={project_id}",
        "--filter", f"displayName:{channel_name}",
        "--format", "value(name)"
    ])

    if existing:
        print(f"Email channel already exists: {existing}")
        return existing

    # Create new email channel
    result = run_gcloud_command([
        "monitoring", "channels", "create", "email",
        f"--project={project_id}",
        f"--display-name={channel_name}",
        f"--email-address={email}"
    ])

    if "Created" in result:
        # Extract channel ID from result
        channel_id = result.split()[-1]
        print(f"Created email channel: {channel_id}")
        return channel_id

    return ""


def create_slack_channel(project_id: str, webhook_url: str) -> str:
    """Create a Slack notification channel."""
    # Check if channel already exists
    existing = run_gcloud_command([
        "monitoring", "channels", "list",
        f"--project={project_id}",
        "--filter", "displayName:pipeline-alerts-slack",
        "--format", "value(name)"
    ])

    if existing:
        print(f"Slack channel already exists: {existing}")
        return existing

    # Create new Slack channel
    result = run_gcloud_command([
        "monitoring", "channels", "create", "webhook",
        f"--project={project_id}",
        "--display-name=pipeline-alerts-slack",
        "--webhook-url", webhook_url
    ])

    if "Created" in result:
        # Extract channel ID from result
        channel_id = result.split()[-1]
        print(f"Created Slack channel: {channel_id}")
        return channel_id

    return ""


def update_alert_policy(project_id: str, policy_file: str, channel_ids: List[str]):
    """Update alert policy with notification channels."""
    try:
        with open(policy_file, 'r') as f:
            policy = json.load(f)

        # Update notification channels
        policy["notificationChannels"] = [
            f"projects/{project_id}/notificationChannels/{cid}" for cid in channel_ids]

        # Write back to file
        with open(policy_file, 'w') as f:
            json.dump(policy, f, indent=2)

        print(
            f"Updated {policy_file} with {len(channel_ids)} notification channels")

    except Exception as e:
        print(f"Error updating alert policy: {e}")


def main():
    """Main setup function."""
    print("🔧 Cloud Monitoring Notification Channels Setup")
    print("=" * 50)

    # Get project ID
    project_id = input("Enter your GCP Project ID: ").strip()
    if not project_id:
        print("❌ Project ID is required")
        sys.exit(1)

    print(f"\n📧 Setting up email notifications...")
    email = input("Enter email address for alerts: ").strip()
    email_channel_id = ""

    if email:
        email_channel_id = create_email_channel(project_id, email)

    print(f"\n💬 Setting up Slack notifications...")
    print("Get webhook URL from: https://api.slack.com/apps/A0F7XDUAZ")
    webhook_url = input(
        "Enter Slack webhook URL (or press Enter to skip): ").strip()
    slack_channel_id = ""

    if webhook_url:
        slack_channel_id = create_slack_channel(project_id, webhook_url)

    # Collect channel IDs
    channel_ids = []
    if email_channel_id:
        channel_ids.append(email_channel_id)
    if slack_channel_id:
        channel_ids.append(slack_channel_id)

    if not channel_ids:
        print("❌ No notification channels configured!")
        print("💡 You need at least one notification channel for alerts to work")
        sys.exit(1)

    print(f"\n✅ Configured {len(channel_ids)} notification channels:")
    for cid in channel_ids:
        print(f"  - {cid}")

    # Update alert policies
    print(f"\n🔧 Updating alert policies...")
    update_alert_policy(
        project_id, "monitoring/alert_pipeline_health.json", channel_ids)

    print(f"\n🎉 Setup complete!")
    print(f"\nNext steps:")
    print(f"1. Deploy monitoring: make deploy-monitoring")
    print(f"2. Test alerts by stopping the pipeline")
    print(f"3. Verify notifications are received")


if __name__ == "__main__":
    main()
