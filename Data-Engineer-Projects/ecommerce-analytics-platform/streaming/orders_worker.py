"""Orders event streaming worker."""

import argparse
import asyncio
import sys
from datetime import UTC, datetime

import structlog
from sqlalchemy import text

from backend.app.core.db import AsyncSessionLocal
from backend.app.models.dwh import FactSales
from backend.app.utils.cache import RedisCache
from etl.dwh import ensure_dim_date_loaded
from etl.settings import ETLSettings

from .event_reader import Event
from .file_reader import FileTailReader
from .kafka_reader import KafkaReader
from .settings import StreamingSettings

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S"),
        structlog.dev.ConsoleRenderer(),
    ]
)
logger = structlog.get_logger()

settings = StreamingSettings()
etl_settings = ETLSettings()


async def handle_orders_batch(
    events: list[Event], session: AsyncSessionLocal, cache: RedisCache | None = None
) -> tuple[int, int]:
    """Process a batch of order events.

    Args:
        events: List of events to process
        session: Database session
        cache: Optional Redis cache for invalidation

    Returns:
        Tuple of (inserts, updates) counts
    """
    if not events:
        return 0, 0

    # Extract order facts
    facts = []
    for event in events:
        order = event.payload

        # Map order fields to fact columns
        fact = FactSales(
            order_id=order["order_id"],
            order_line_nbr=order["order_line_nbr"],
            order_ts=event.timestamp,
            customer_id=order["customer_nk"],
            customer_email=order["email"],
            product_id=order["sku"],
            quantity=order["quantity"],
            unit_price=order["unit_price"],
            discount_pct=order.get("discount_pct", 0),
            ingestion_ts=datetime.now(UTC),
        )
        facts.append(fact)

    # Upsert facts
    result = await session.execute(
        text(
            """
        INSERT INTO dw.fact_sales (
            order_id, order_line_nbr, order_ts,
            customer_id, customer_email,
            product_id, quantity, unit_price, discount_pct,
            ingestion_ts
        )
        VALUES (:order_id, :order_line_nbr, :order_ts,
                :customer_id, :customer_email,
                :product_id, :quantity, :unit_price, :discount_pct,
                :ingestion_ts)
        ON CONFLICT (order_id, order_line_nbr) DO
        UPDATE SET
            customer_email = EXCLUDED.customer_email,
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            discount_pct = EXCLUDED.discount_pct,
            ingestion_ts = EXCLUDED.ingestion_ts
        """
        ),
        [vars(fact) for fact in facts],
    )

    await session.commit()

    # Invalidate cache if configured
    if cache:
        try:
            await cache.delete_pattern("sales:*")
        except Exception as e:
            logger.warning("Failed to invalidate cache", error=str(e))

    return len(facts), 0


async def run_worker(reader: KafkaReader | FileTailReader, once: bool = False) -> None:
    """Run the streaming worker.

    Args:
        reader: Event reader implementation
        once: Run single batch only
    """
    logger.info(f"Starting {reader.__class__.__name__}")

    try:
        async with AsyncSessionLocal() as session:
            # Initialize cache connection
            cache = (
                RedisCache(etl_settings.REDIS_URL) if etl_settings.REDIS_URL else None
            )

            await reader.open()

            while True:
                try:
                    # Poll next batch
                    events = await reader.poll(settings.STREAM_BATCH_SIZE)
                    if not events:
                        if once:
                            break
                        await asyncio.sleep(1)
                        continue

                    # Process batch
                    last_offset = events[-1].offset
                    inserts, updates = await handle_orders_batch(events, session, cache)

                    # Commit offset
                    await reader.ack(last_offset)

                    logger.info(
                        "Processed batch",
                        inserts=inserts,
                        updates=updates,
                        source=reader.source,
                        partition=reader.partition,
                        last_offset=last_offset,
                    )

                    if once:
                        break

                except Exception as e:
                    logger.error(f"Batch processing failed: {e}")
                    if once:
                        raise
                    await asyncio.sleep(settings.STREAM_RETRY_DELAY_MS / 1000)

    finally:
        await reader.close()


async def ensure_dim_dates(session: AsyncSessionLocal) -> None:
    """Ensure dimension tables are loaded."""
    logger.info("Ensuring dimension tables...")

    # Load dim_date if empty
    count = await session.scalar(text("SELECT COUNT(*) FROM dw.dim_date"))
    if not count:
        await ensure_dim_date_loaded(session)


def main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Orders streaming worker")
    parser.add_argument(
        "--kafka",
        action="store_true",
        help="Use Kafka reader (vs file tail)",
    )
    parser.add_argument(
        "--topic",
        default=settings.KAFKA_ORDERS_TOPIC,
        help="Kafka topic to consume from",
    )
    parser.add_argument(
        "--group",
        default=settings.KAFKA_CONSUMER_GROUP,
        help="Kafka consumer group",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.STREAM_BATCH_SIZE,
        help="Maximum events per batch",
    )
    parser.add_argument(
        "--ensure-dim-date",
        action="store_true",
        help="Ensure dim_date is loaded",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process single batch and exit",
    )
    parser.add_argument(
        "--file",
        help="Input file to tail (if not using Kafka)",
    )

    args = parser.parse_args()

    # Update settings from args
    settings.STREAM_BATCH_SIZE = args.batch_size

    async def run() -> None:
        if args.ensure_dim_date:
            async with AsyncSessionLocal() as session:
                await ensure_dim_dates(session)

        if args.kafka:
            # Use Kafka reader
            reader = KafkaReader(
                args.topic, args.group, settings.KAFKA_BROKERS, settings
            )
        else:
            # Use file tail reader
            if not args.file:
                parser.error("--file required when not using Kafka")
            reader = FileTailReader(args.file)

        await run_worker(reader, args.once)

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
