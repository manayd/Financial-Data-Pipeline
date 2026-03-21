import hashlib
from datetime import UTC, datetime
from uuid import uuid4

from src.models import KafkaDeserializer, KafkaSerializer
from src.models.events import (
    ProcessedFinancialNewsEvent,
    RawFinancialNewsEvent,
    SourceType,
    TickerAggregation,
)
from src.processor.agents import (
    WindowState,
    _seen_hashes,
    _ticker_windows,
    build_processed_event,
    compute_content_hash,
)
from src.processor.enrichment import ticker_to_company


def _make_raw_event(
    content: str = "Apple reported strong earnings this quarter.",
    tickers: list[str] | None = None,
    title: str = "Apple Q4 Earnings Report",
) -> RawFinancialNewsEvent:
    return RawFinancialNewsEvent(
        event_id=uuid4(),
        timestamp=datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC),
        source=SourceType.ALPHA_VANTAGE,
        source_url="https://example.com/article",
        title=title,
        content=content,
        tickers=tickers or ["AAPL"],
        raw_metadata={"test": True},
    )


class TestComputeContentHash:
    def test_returns_sha256_hex(self) -> None:
        content = "test content"
        expected = hashlib.sha256(content.encode()).hexdigest()
        assert compute_content_hash(content) == expected

    def test_different_content_different_hash(self) -> None:
        h1 = compute_content_hash("content A")
        h2 = compute_content_hash("content B")
        assert h1 != h2

    def test_same_content_same_hash(self) -> None:
        h1 = compute_content_hash("identical")
        h2 = compute_content_hash("identical")
        assert h1 == h2


class TestBuildProcessedEvent:
    def test_preserves_event_id(self) -> None:
        raw = _make_raw_event()
        processed = build_processed_event(raw, "abc123")
        assert processed.event_id == raw.event_id

    def test_preserves_raw_fields(self) -> None:
        raw = _make_raw_event()
        processed = build_processed_event(raw, "abc123")
        assert processed.title == raw.title
        assert processed.content == raw.content
        assert processed.tickers == raw.tickers
        assert processed.source == raw.source
        assert processed.source_url == raw.source_url
        assert processed.raw_metadata == raw.raw_metadata

    def test_adds_content_hash(self) -> None:
        raw = _make_raw_event()
        processed = build_processed_event(raw, "deadbeef")
        assert processed.content_hash == "deadbeef"

    def test_adds_processed_at(self) -> None:
        raw = _make_raw_event()
        before = datetime.now(UTC)
        processed = build_processed_event(raw, "abc123")
        after = datetime.now(UTC)
        assert before <= processed.processed_at <= after

    def test_enriches_tickers_to_companies(self) -> None:
        raw = _make_raw_event(tickers=["AAPL", "MSFT"])
        processed = build_processed_event(raw, "abc123")
        assert processed.companies == ["Apple Inc.", "Microsoft Corporation"]

    def test_unknown_ticker_uses_ticker_as_company(self) -> None:
        raw = _make_raw_event(tickers=["ZZZZZ"])
        processed = build_processed_event(raw, "abc123")
        assert processed.companies == ["ZZZZZ"]

    def test_serialization_roundtrip(self) -> None:
        raw = _make_raw_event()
        processed = build_processed_event(raw, compute_content_hash(raw.content))
        data = KafkaSerializer.serialize(processed)
        restored = KafkaDeserializer.deserialize(data, ProcessedFinancialNewsEvent)
        assert restored.event_id == processed.event_id
        assert restored.content_hash == processed.content_hash
        assert restored.companies == processed.companies


class TestDeduplication:
    def setup_method(self) -> None:
        _seen_hashes.clear()

    def test_new_hash_not_in_set(self) -> None:
        h = compute_content_hash("new content")
        assert h not in _seen_hashes

    def test_added_hash_is_in_set(self) -> None:
        h = compute_content_hash("seen content")
        _seen_hashes.add(h)
        assert h in _seen_hashes

    def test_clear_removes_all(self) -> None:
        _seen_hashes.add("hash1")
        _seen_hashes.add("hash2")
        _seen_hashes.clear()
        assert len(_seen_hashes) == 0


class TestWindowState:
    def test_default_values(self) -> None:
        state = WindowState()
        assert state.article_count == 0
        assert state.window_start.tzinfo == UTC

    def test_increment(self) -> None:
        state = WindowState()
        state.article_count += 1
        state.article_count += 1
        assert state.article_count == 2


class TestAggregation:
    def setup_method(self) -> None:
        _ticker_windows.clear()

    def test_counts_articles_per_ticker(self) -> None:
        _ticker_windows["AAPL"] = WindowState(article_count=0)
        _ticker_windows["AAPL"].article_count += 3
        assert _ticker_windows["AAPL"].article_count == 3

    def test_multiple_tickers_tracked(self) -> None:
        _ticker_windows["AAPL"] = WindowState(article_count=2)
        _ticker_windows["MSFT"] = WindowState(article_count=5)
        assert len(_ticker_windows) == 2
        assert _ticker_windows["AAPL"].article_count == 2
        assert _ticker_windows["MSFT"].article_count == 5

    def test_aggregation_builds_valid_event(self) -> None:
        now = datetime.now(UTC)
        state = WindowState(article_count=10, window_start=now)
        agg = TickerAggregation(
            ticker="AAPL",
            company_name=ticker_to_company("AAPL"),
            window_start=state.window_start,
            window_end=now,
            article_count=state.article_count,
            avg_sentiment_score=None,
            dominant_sentiment=None,
        )
        assert agg.ticker == "AAPL"
        assert agg.company_name == "Apple Inc."
        assert agg.article_count == 10

    def test_aggregation_serialization_roundtrip(self) -> None:
        now = datetime.now(UTC)
        agg = TickerAggregation(
            ticker="TSLA",
            company_name=ticker_to_company("TSLA"),
            window_start=now,
            window_end=now,
            article_count=7,
            avg_sentiment_score=None,
            dominant_sentiment=None,
        )
        data = KafkaSerializer.serialize(agg)
        restored = KafkaDeserializer.deserialize(data, TickerAggregation)
        assert restored.ticker == "TSLA"
        assert restored.article_count == 7

    def test_reset_clears_windows(self) -> None:
        _ticker_windows["AAPL"] = WindowState(article_count=5)
        _ticker_windows["MSFT"] = WindowState(article_count=3)
        _ticker_windows.clear()
        assert len(_ticker_windows) == 0
