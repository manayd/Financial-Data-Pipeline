"""Faust agents for stream processing: dedup, enrichment, and aggregation."""

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime

import structlog

from src.models import KafkaDeserializer, KafkaSerializer
from src.models.events import ProcessedFinancialNewsEvent, RawFinancialNewsEvent, TickerAggregation
from src.processor.app import aggregation_topic, app, processed_topic, raw_topic, settings
from src.processor.enrichment import ticker_to_company

logger = structlog.get_logger()

MAX_SEEN_HASHES = 50_000
_seen_hashes: set[str] = set()


def compute_content_hash(content: str) -> str:
    """Compute SHA-256 hash of content for deduplication."""
    return hashlib.sha256(content.encode()).hexdigest()


def build_processed_event(
    raw_event: RawFinancialNewsEvent,
    content_hash: str,
) -> ProcessedFinancialNewsEvent:
    """Transform a raw event into a processed event with enrichment."""
    companies = [ticker_to_company(t) for t in raw_event.tickers]
    return ProcessedFinancialNewsEvent(
        **raw_event.model_dump(),
        companies=companies,
        content_hash=content_hash,
        processed_at=datetime.now(UTC),
    )


@app.agent(raw_topic)
async def process_events(stream):  # type: ignore[no-untyped-def]
    """Consume raw events: deduplicate, enrich, publish to processed topic."""
    async for raw_bytes in stream:
        try:
            raw_event = KafkaDeserializer.deserialize(raw_bytes, RawFinancialNewsEvent)
        except Exception:
            logger.warning("raw_event_deserialize_failed", exc_info=True)
            continue

        content_hash = compute_content_hash(raw_event.content)

        if content_hash in _seen_hashes:
            logger.debug("duplicate_skipped", event_id=str(raw_event.event_id))
            continue

        _seen_hashes.add(content_hash)
        if len(_seen_hashes) > MAX_SEEN_HASHES:
            _seen_hashes.clear()
            logger.info("seen_hashes_cleared")

        processed = build_processed_event(raw_event, content_hash)

        key = (processed.tickers[0] if processed.tickers else "UNKNOWN").encode()
        await processed_topic.send(key=key, value=KafkaSerializer.serialize(processed))

        logger.info(
            "event_processed",
            event_id=str(processed.event_id),
            tickers=processed.tickers,
            companies=processed.companies,
            title=processed.title[:60],
        )


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


@dataclass
class WindowState:
    """Tracks per-ticker article counts within an aggregation window."""

    article_count: int = 0
    window_start: datetime = field(default_factory=lambda: datetime.now(UTC))


_ticker_windows: dict[str, WindowState] = {}


@app.agent(processed_topic)
async def aggregate_events(stream):  # type: ignore[no-untyped-def]
    """Consume processed events and update per-ticker aggregation counters."""
    async for raw_bytes in stream:
        try:
            event = KafkaDeserializer.deserialize(raw_bytes, ProcessedFinancialNewsEvent)
        except Exception:
            logger.warning("processed_event_deserialize_failed", exc_info=True)
            continue

        for ticker in event.tickers:
            if ticker not in _ticker_windows:
                _ticker_windows[ticker] = WindowState()
            _ticker_windows[ticker].article_count += 1

        logger.debug(
            "aggregation_updated",
            event_id=str(event.event_id),
            tickers=event.tickers,
        )


@app.timer(interval=settings.processor_emit_interval)
async def emit_aggregations() -> None:
    """Periodically emit per-ticker aggregation snapshots and reset windows."""
    if not _ticker_windows:
        return

    now = datetime.now(UTC)
    emitted = 0

    for ticker, state in _ticker_windows.items():
        agg = TickerAggregation(
            ticker=ticker,
            company_name=ticker_to_company(ticker),
            window_start=state.window_start,
            window_end=now,
            article_count=state.article_count,
            avg_sentiment_score=None,
            dominant_sentiment=None,
        )
        await aggregation_topic.send(
            key=ticker.encode(),
            value=KafkaSerializer.serialize(agg),
        )
        emitted += 1

    _ticker_windows.clear()
    logger.info("aggregation_emitted", ticker_count=emitted)
