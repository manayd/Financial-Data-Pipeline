"""PostgreSQL sink consumer: writes processed events and aggregations to the database."""

import signal

import structlog
from confluent_kafka import Consumer, KafkaError
from sqlalchemy.dialects.postgresql import insert

from src.config import Settings
from src.models import KafkaDeserializer
from src.models.events import LLMAnalysisResult, ProcessedFinancialNewsEvent, TickerAggregation
from src.storage.database import get_sync_engine, get_sync_session_factory
from src.storage.models import Article, LLMAnalysisRow, TickerAggregationRow

logger = structlog.get_logger()

PROCESSED_TOPIC = "processed-financial-news"
AGGREGATION_TOPIC = "aggregation-results"
LLM_ANALYSIS_TOPIC = "llm-analysis-results"


class DatabaseSink:
    """Consumes processed events and aggregations, writes to PostgreSQL."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._running = True
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": "db-sink-consumer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        engine = get_sync_engine(settings)
        self._session_factory = get_sync_session_factory(engine)

    def _shutdown(self, signum: int, frame: object) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        self._running = False

    def _handle_processed_event(self, data: bytes) -> None:
        event = KafkaDeserializer.deserialize(data, ProcessedFinancialNewsEvent)
        stmt = insert(Article).values(
            event_id=event.event_id,
            timestamp=event.timestamp,
            source=event.source.value,
            source_url=event.source_url,
            title=event.title,
            content=event.content,
            tickers=event.tickers,
            companies=event.companies,
            content_hash=event.content_hash,
            raw_metadata=event.raw_metadata,
            processed_at=event.processed_at,
        )
        stmt = stmt.on_conflict_do_nothing(index_elements=["content_hash"])

        with self._session_factory() as session:
            session.execute(stmt)
            session.commit()

        logger.debug("article_written", event_id=str(event.event_id))

    def _handle_aggregation(self, data: bytes) -> None:
        agg = KafkaDeserializer.deserialize(data, TickerAggregation)

        row = TickerAggregationRow(
            ticker=agg.ticker,
            company_name=agg.company_name,
            window_start=agg.window_start,
            window_end=agg.window_end,
            article_count=agg.article_count,
            avg_sentiment_score=agg.avg_sentiment_score,
            dominant_sentiment=agg.dominant_sentiment.value if agg.dominant_sentiment else None,
        )

        with self._session_factory() as session:
            session.add(row)
            session.commit()

        logger.debug("aggregation_written", ticker=agg.ticker)

    def _handle_llm_analysis(self, data: bytes) -> None:
        result = KafkaDeserializer.deserialize(data, LLMAnalysisResult)

        row = LLMAnalysisRow(
            article_event_id=result.article_event_id,
            summary=result.summary,
            sentiment=result.sentiment.value,
            sentiment_confidence=result.sentiment_confidence,
            entities=[e.model_dump() for e in result.entities],
            key_topics=result.key_topics,
            analyzed_at=result.analyzed_at,
            model_id=result.model_id,
            analysis_version=result.analysis_version,
        )

        with self._session_factory() as session:
            session.add(row)
            session.commit()

        logger.debug("llm_analysis_written", article_event_id=str(result.article_event_id))

    def consume(self) -> None:
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        self._consumer.subscribe([PROCESSED_TOPIC, AGGREGATION_TOPIC, LLM_ANALYSIS_TOPIC])
        logger.info(
            "db_sink_started",
            topics=[PROCESSED_TOPIC, AGGREGATION_TOPIC, LLM_ANALYSIS_TOPIC],
        )

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

                raw = msg.value()
                if raw is None:
                    continue

                try:
                    topic = msg.topic()
                    if topic == PROCESSED_TOPIC:
                        self._handle_processed_event(raw)
                    elif topic == AGGREGATION_TOPIC:
                        self._handle_aggregation(raw)
                    elif topic == LLM_ANALYSIS_TOPIC:
                        self._handle_llm_analysis(raw)

                    self._consumer.commit(asynchronous=False)
                except Exception:
                    logger.exception("db_sink_write_failed", topic=msg.topic(), offset=msg.offset())
        finally:
            self._consumer.close()
            logger.info("db_sink_stopped")
