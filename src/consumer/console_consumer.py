import signal

import structlog
from confluent_kafka import Consumer, KafkaError

from src.config import Settings
from src.models import KafkaDeserializer
from src.models.events import RawFinancialNewsEvent

logger = structlog.get_logger()


class ConsoleConsumer:
    """Consumes from raw-financial-news and prints events to stdout."""

    TOPIC = "raw-financial-news"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._running = True
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": "console-consumer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum: int, frame: object) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        self._running = False

    def consume(self) -> None:
        self._consumer.subscribe([self.TOPIC])
        logger.info("consumer_started", topic=self.TOPIC)

        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                error = msg.error()
                if error:
                    if error.code() == KafkaError._PARTITION_EOF:
                        logger.debug("partition_eof", partition=msg.partition())
                        continue
                    logger.error("consumer_error", error=error)
                    continue

                raw = msg.value()
                if raw is None:
                    continue

                try:
                    event = KafkaDeserializer.deserialize(raw, RawFinancialNewsEvent)
                    self._print_event(event)
                    self._consumer.commit(asynchronous=False)
                except Exception:
                    logger.exception("deserialization_failed", offset=msg.offset())
        finally:
            self._consumer.close()
            logger.info("consumer_stopped")

    def _print_event(self, event: RawFinancialNewsEvent) -> None:
        print(f"\n{'=' * 80}")
        print(f"  Event ID:  {event.event_id}")
        print(f"  Time:      {event.timestamp}")
        print(f"  Source:    {event.source.value}")
        print(f"  Tickers:   {', '.join(event.tickers)}")
        print(f"  Title:     {event.title}")
        print(f"  Content:   {event.content[:200]}...")
        print(f"{'=' * 80}")
