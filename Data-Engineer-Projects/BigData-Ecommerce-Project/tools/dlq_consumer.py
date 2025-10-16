#!/usr/bin/env python
"""Lightweight Pub/Sub DLQ consumer for inspecting failed events."""
from __future__ import annotations

import argparse
import json
import logging
import os
from typing import Sequence

from google.cloud import pubsub_v1

import config

LOGGER = logging.getLogger(__name__)


def _project_default() -> str:
    return os.getenv("PROJECT_ID", "")


def consume_once(
    project_id: str,
    subscription: str,
    max_messages: int,
    acknowledge: bool,
) -> None:
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription)

    LOGGER.info(
        "Pulling up to %s messages from %s", max_messages, subscription_path
    )

    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": max_messages}
    )
    if not response.received_messages:
        LOGGER.info("No messages available")
        return

    ack_ids: list[str] = []
    for received in response.received_messages:
        message = received.message
        payload = message.data.decode("utf-8", errors="replace")
        LOGGER.info("Message %s published at %s", message.message_id, message.publish_time)
        print(json.dumps({
            "id": message.message_id,
            "publish_time": message.publish_time.isoformat(),
            "attributes": dict(message.attributes),
            "data": payload,
        }, indent=2))
        ack_ids.append(received.ack_id)

    if acknowledge:
        LOGGER.info("Acknowledging %s messages", len(ack_ids))
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )
    else:
        LOGGER.info("Ack skipped (run with --ack to delete messages)")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect Pub/Sub DLQ messages")
    parser.add_argument(
        "--project",
        default=_project_default(),
        required=False,
        help="GCP project ID (defaults to PROJECT_ID env)",
    )
    parser.add_argument(
        "--subscription",
        default=config.DLQ_SUBSCRIPTION,
        help="Dead-letter subscription name",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=10,
        help="Maximum number of messages to pull in one batch",
    )
    parser.add_argument(
        "--ack",
        action="store_true",
        help="Acknowledge messages after printing (default: read-only)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv)
    if not args.project:
        raise SystemExit("Set PROJECT_ID env var or pass --project")
    consume_once(args.project, args.subscription, args.max_messages, args.ack)


if __name__ == "__main__":
    main()
