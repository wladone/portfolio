"""Streaming configuration settings."""

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings


class StreamingSettings(BaseSettings):
    """Settings for streaming components."""

    STREAM_POLL_INTERVAL_MS: int = 1000
    STREAM_BATCH_SIZE: int = 256
    STREAM_MAX_RETRIES: int = 3
    STREAM_RETRY_DELAY_MS: int = 1000

    # Kafka settings - use STREAMING_KAFKA_BOOTSTRAP_SERVERS env var
    KAFKA_BROKERS: str = Field(
        default="localhost:9092",  # For local dev against exposed Docker ports
        env="STREAMING_KAFKA_BOOTSTRAP_SERVERS",
    )
    KAFKA_SECURITY_PROTOCOL: Literal[
        "PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL", "SSL"
    ] = "PLAINTEXT"
    KAFKA_SASL_MECHANISM: str | None = None
    KAFKA_SASL_USERNAME: str | None = None
    KAFKA_SASL_PASSWORD: str | None = None
    KAFKA_SSL_CAFILE: str | None = None
    KAFKA_AUTO_OFFSET_RESET: Literal["latest", "earliest"] = "latest"
    KAFKA_MAX_POLL_RECORDS: int = Field(default=512, gt=0)
    KAFKA_ENABLE_AUTO_COMMIT: bool = False
    KAFKA_MAX_IN_FLIGHT_COMMITS: int = Field(default=4, gt=0)
    KAFKA_ORDERS_TOPIC: str = "orders"
    KAFKA_CONSUMER_GROUP: str = "ecom-orders-dev"
