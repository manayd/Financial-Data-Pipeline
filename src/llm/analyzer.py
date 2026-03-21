"""LLM analyzer: consumes processed articles, calls LLM, publishes analysis results."""

import asyncio
import signal
from datetime import UTC, datetime

import structlog
from confluent_kafka import Consumer, KafkaError, Producer

from src.config import Settings
from src.llm.provider import create_provider
from src.llm.rate_limiter import RateLimiter
from src.models import KafkaDeserializer, KafkaSerializer
from src.models.events import (
    ExtractedEntity,
    LLMAnalysisResult,
    ProcessedFinancialNewsEvent,
    SentimentLabel,
)

logger = structlog.get_logger()

PROCESSED_TOPIC = "processed-financial-news"
LLM_ANALYSIS_TOPIC = "llm-analysis-results"


def build_analysis_result(
    raw: dict,
    event_id: object,
    model_id: str,
) -> LLMAnalysisResult:
    """Parse raw LLM JSON output into an LLMAnalysisResult."""
    return LLMAnalysisResult(
        article_event_id=event_id,  # type: ignore[arg-type]
        summary=raw["summary"],
        sentiment=SentimentLabel(raw["sentiment"]),
        sentiment_confidence=float(raw["sentiment_confidence"]),
        entities=[ExtractedEntity(**e) for e in raw.get("entities", [])],
        key_topics=raw.get("key_topics", []),
        analyzed_at=datetime.now(UTC),
        model_id=model_id,
    )


class LLMAnalyzer:
    """Consumes processed articles, calls an LLM for analysis, publishes results."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._running = True
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": "llm-analyzer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
            }
        )
        self._provider = create_provider(settings)
        self._rate_limiter = RateLimiter(settings.llm_requests_per_minute)

    def _shutdown(self, signum: int, frame: object) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        self._running = False

    async def _analyze_article(
        self, event: ProcessedFinancialNewsEvent
    ) -> LLMAnalysisResult | None:
        """Call LLM with rate limiting. Returns None on failure."""
        await self._rate_limiter.acquire()
        try:
            raw = await self._provider.analyze(
                event.title, event.content, self._settings.llm_content_max_chars
            )
            return build_analysis_result(raw, event.event_id, self._settings.llm_model_id)
        except Exception:
            logger.exception("llm_analysis_failed", event_id=str(event.event_id))
            return None

    async def consume(self) -> None:
        """Main consume loop: poll → analyze → publish."""
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        self._consumer.subscribe([PROCESSED_TOPIC])
        logger.info("llm_analyzer_started", topic=PROCESSED_TOPIC)

        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                error = msg.error()
                if error:
                    if error.code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("consumer_error", error=error)
                    continue

                raw_bytes = msg.value()
                if raw_bytes is None:
                    continue

                try:
                    event = KafkaDeserializer.deserialize(raw_bytes, ProcessedFinancialNewsEvent)
                    result = await self._analyze_article(event)

                    if result is not None:
                        payload = KafkaSerializer.serialize(result)
                        self._producer.produce(LLM_ANALYSIS_TOPIC, value=payload)
                        self._producer.flush()
                        logger.debug(
                            "analysis_published",
                            event_id=str(event.event_id),
                        )
                    else:
                        logger.warning(
                            "analysis_skipped",
                            event_id=str(event.event_id),
                        )

                    self._consumer.commit(asynchronous=False)
                except Exception:
                    logger.exception(
                        "analyze_message_failed",
                        offset=msg.offset(),
                    )
        finally:
            self._producer.flush()
            self._consumer.close()
            logger.info("llm_analyzer_stopped")

    def run(self) -> None:
        """Entry point that runs the async consume loop."""
        asyncio.run(self.consume())
