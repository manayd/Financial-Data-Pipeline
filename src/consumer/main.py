"""Consumer entry point with factory pattern for multiple consumer types."""

import signal
import threading
from typing import Protocol

import structlog

from src.config import Settings

logger = structlog.get_logger()


class Consumable(Protocol):
    _running: bool

    def consume(self) -> None: ...


def _create_consumers(settings: Settings) -> list[Consumable]:
    """Instantiate consumers based on settings.consumer_type.

    Supported types:
      - "console"  -> ConsoleConsumer (prints to stdout)
      - "db_sink"  -> DatabaseSink (writes to PostgreSQL)
      - "s3_sink"  -> S3ParquetSink (writes Parquet to S3)
      - "all"      -> DatabaseSink + S3ParquetSink
    """
    consumers: list[Consumable] = []
    consumer_type = settings.consumer_type.lower()

    if consumer_type == "console":
        from src.consumer.console_consumer import ConsoleConsumer

        consumers.append(ConsoleConsumer(settings))

    elif consumer_type == "db_sink":
        from src.storage.db_sink import DatabaseSink

        consumers.append(DatabaseSink(settings))

    elif consumer_type == "s3_sink":
        from src.storage.s3_sink import S3ParquetSink

        consumers.append(S3ParquetSink(settings))

    elif consumer_type == "all":
        from src.storage.db_sink import DatabaseSink
        from src.storage.s3_sink import S3ParquetSink

        consumers.append(DatabaseSink(settings))
        consumers.append(S3ParquetSink(settings))

    else:
        raise ValueError(f"Unknown consumer type: {settings.consumer_type}")

    return consumers


def main() -> None:
    settings = Settings()
    consumers = _create_consumers(settings)

    def shutdown_all(signum: int, frame: object) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        for c in consumers:
            c._running = False

    signal.signal(signal.SIGTERM, shutdown_all)
    signal.signal(signal.SIGINT, shutdown_all)

    logger.info(
        "consumer_starting",
        consumer_type=settings.consumer_type,
        count=len(consumers),
    )

    if len(consumers) == 1:
        consumers[0].consume()
    else:
        threads: list[threading.Thread] = []
        for c in consumers:
            t = threading.Thread(target=c.consume, name=c.__class__.__name__, daemon=True)
            t.start()
            threads.append(t)
            logger.info("consumer_thread_started", consumer=c.__class__.__name__)

        try:
            for t in threads:
                t.join()
        except KeyboardInterrupt:
            logger.info("main_interrupted_shutting_down")
            shutdown_all(signal.SIGINT, None)
            for t in threads:
                t.join(timeout=15)


if __name__ == "__main__":
    main()
