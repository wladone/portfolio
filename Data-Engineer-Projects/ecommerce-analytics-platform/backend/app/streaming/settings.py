from pydantic_settings import BaseSettings


class StreamingSettings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_orders: str = "orders"
    kafka_topic_customers: str = "customers"
    kafka_topic_products: str = "products"
    kafka_group_id: str = "streaming_group"
    batch_size: int = 100
    poll_timeout_ms: int = 1000

    class Config:
        env_prefix = "STREAMING_"
