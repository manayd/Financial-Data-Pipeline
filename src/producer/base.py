from abc import ABC, abstractmethod
from typing import Any

import structlog
from confluent_kafka import KafkaError, KafkaException, Producer
from pydantic import BaseModel

from src.config import Settings
from src.models import KafkaSerializer

logger = structlog.get_logger()


class BaseProducer(ABC):
    """Abstract base class for all Kafka producers."""

    TOPIC = "raw-financial-news"
    DLQ_TOPIC = "raw-financial-news-dlq"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._running = True
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "retries": 5,
                "retry.backoff.ms": 500,
                "linger.ms": 10,
                "batch.num.messages": 100,
            }
        )

    def _delivery_callback(self, err: KafkaError | None, msg: Any) -> None:
        if err is not None:
            logger.error("message_delivery_failed", error=str(err))
        else:
            logger.debug("message_delivered", topic=msg.topic(), partition=msg.partition())

    def _publish(self, topic: str, key: str, event: BaseModel) -> None:
        try:
            value = KafkaSerializer.serialize(event)
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value,
                callback=self._delivery_callback,
            )
            self._producer.poll(0)
        except KafkaException as e:
            logger.error("publish_failed", error=str(e), topic=topic, key=key)
            self._publish_to_dlq(key, event)

    def _publish_to_dlq(self, key: str, event: BaseModel) -> None:
        try:
            value = KafkaSerializer.serialize(event)
            self._producer.produce(
                topic=self.DLQ_TOPIC,
                key=key.encode("utf-8"),
                value=value,
            )
            self._producer.poll(0)
            logger.warning("message_sent_to_dlq", key=key)
        except KafkaException as e:
            logger.error("dlq_publish_failed", error=str(e), key=key)

    def flush(self) -> None:
        remaining = self._producer.flush(timeout=10)
        if remaining > 0:
            logger.warning("flush_incomplete", remaining_messages=remaining)

    @abstractmethod
    def produce(self) -> None:
        """Main production loop. Implementations should check self._running."""
        ...

    def run(self) -> None:
        logger.info("producer_starting", producer_type=self.__class__.__name__)
        try:
            self.produce()
        except KeyboardInterrupt:
            logger.info("producer_interrupted")
        finally:
            self.flush()
            logger.info("producer_stopped")
