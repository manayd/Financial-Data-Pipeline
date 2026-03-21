from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest
from pydantic import ValidationError

from src.config import Settings
from src.models.events import RawFinancialNewsEvent, SourceType
from src.producer.alpha_vantage_producer import (
    AlphaVantageProducer,
    AVFeedItem,
    AVNewsResponse,
)

SAMPLE_AV_RESPONSE = {
    "items": "2",
    "feed": [
        {
            "title": "Apple Reports Q4 Earnings",
            "url": "https://example.com/article1",
            "summary": "Apple Inc reported strong earnings this quarter.",
            "source": "Reuters",
            "time_published": "20231215T143000",
            "authors": ["John Doe"],
            "overall_sentiment_score": 0.85,
            "overall_sentiment_label": "Bullish",
            "ticker_sentiment": [
                {
                    "ticker": "AAPL",
                    "relevance_score": "0.95",
                    "ticker_sentiment_score": "0.80",
                    "ticker_sentiment_label": "Bullish",
                }
            ],
        },
        {
            "title": "Tech Stocks Rally",
            "url": "https://example.com/article2",
            "summary": "Technology stocks rallied today on positive earnings.",
            "source": "Bloomberg",
            "time_published": "20231215T150000",
            "authors": [],
            "overall_sentiment_score": 0.60,
            "overall_sentiment_label": "Somewhat-Bullish",
            "ticker_sentiment": [],
        },
    ],
}


class TestAVResponseValidation:
    def test_valid_response_parses(self) -> None:
        response = AVNewsResponse.model_validate(SAMPLE_AV_RESPONSE)
        assert len(response.feed) == 2
        assert response.feed[0].title == "Apple Reports Q4 Earnings"

    def test_empty_feed_parses(self) -> None:
        response = AVNewsResponse.model_validate({"feed": []})
        assert response.feed == []

    def test_missing_feed_uses_default(self) -> None:
        response = AVNewsResponse.model_validate({})
        assert response.feed == []

    def test_feed_item_extracts_ticker_sentiment(self) -> None:
        item = AVFeedItem.model_validate(SAMPLE_AV_RESPONSE["feed"][0])
        assert len(item.ticker_sentiment) == 1
        assert item.ticker_sentiment[0].ticker == "AAPL"

    def test_malformed_item_raises(self) -> None:
        with pytest.raises(ValidationError):
            AVFeedItem.model_validate({"title": "missing required fields"})


class TestAlphaVantageProducer:
    @patch("src.producer.base.Producer")
    def test_produces_valid_events(self, mock_kafka_cls: MagicMock) -> None:
        settings = Settings(
            kafka_bootstrap_servers="localhost:9092",
            alpha_vantage_api_key="test-key",
            watchlist_tickers="AAPL",
            alpha_vantage_poll_interval=0,
        )
        producer = AlphaVantageProducer(settings)

        published: list[RawFinancialNewsEvent] = []

        def capture(topic: str, key: str, event: RawFinancialNewsEvent) -> None:
            published.append(event)
            if len(published) >= 2:
                producer._running = False

        producer._publish = capture  # type: ignore[assignment]
        producer._fetch_news = MagicMock(  # type: ignore[assignment]
            return_value=AVNewsResponse.model_validate(SAMPLE_AV_RESPONSE)
        )

        producer.produce()

        assert len(published) == 2
        assert published[0].source == SourceType.ALPHA_VANTAGE
        assert published[0].title == "Apple Reports Q4 Earnings"
        assert published[0].tickers == ["AAPL"]
        assert published[0].source_url == "https://example.com/article1"
        assert published[0].raw_metadata is not None
        assert published[0].raw_metadata["overall_sentiment_score"] == 0.85

    @patch("src.producer.base.Producer")
    def test_deduplication_skips_seen_urls(self, mock_kafka_cls: MagicMock) -> None:
        settings = Settings(
            kafka_bootstrap_servers="localhost:9092",
            alpha_vantage_api_key="test-key",
            watchlist_tickers="AAPL",
            alpha_vantage_poll_interval=0,
        )
        producer = AlphaVantageProducer(settings)

        call_count = 0
        published: list[RawFinancialNewsEvent] = []

        def capture(topic: str, key: str, event: RawFinancialNewsEvent) -> None:
            published.append(event)

        def fetch_and_count(ticker: str) -> AVNewsResponse:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                producer._running = False
            return AVNewsResponse.model_validate(SAMPLE_AV_RESPONSE)

        producer._publish = capture  # type: ignore[assignment]
        producer._fetch_news = fetch_and_count  # type: ignore[assignment]

        producer.produce()

        # First call publishes 2 events, second call deduplicates both
        assert len(published) == 2

    @patch("src.producer.base.Producer")
    def test_api_error_handled_gracefully(self, mock_kafka_cls: MagicMock) -> None:
        settings = Settings(
            kafka_bootstrap_servers="localhost:9092",
            alpha_vantage_api_key="test-key",
            watchlist_tickers="AAPL",
            alpha_vantage_poll_interval=0,
        )
        producer = AlphaVantageProducer(settings)

        call_count = 0

        def fail_then_stop(ticker: str) -> AVNewsResponse:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                producer._running = False
            raise httpx.TransportError("connection failed")

        producer._fetch_news = fail_then_stop  # type: ignore[assignment]
        # Should not crash
        producer.produce()

    @patch("src.producer.base.Producer")
    def test_timestamp_parsing(self, mock_kafka_cls: MagicMock) -> None:
        settings = Settings(
            kafka_bootstrap_servers="localhost:9092",
            alpha_vantage_api_key="test-key",
        )
        producer = AlphaVantageProducer(settings)

        dt = producer._parse_timestamp("20231215T143000")
        assert dt == datetime(2023, 12, 15, 14, 30, 0, tzinfo=UTC)

    @patch("src.producer.base.Producer")
    def test_invalid_timestamp_falls_back(self, mock_kafka_cls: MagicMock) -> None:
        settings = Settings(
            kafka_bootstrap_servers="localhost:9092",
            alpha_vantage_api_key="test-key",
        )
        producer = AlphaVantageProducer(settings)

        dt = producer._parse_timestamp("invalid-date")
        assert dt.tzinfo == UTC  # Falls back to now(UTC)

    @patch("src.producer.base.Producer")
    def test_seen_urls_cleared_when_over_limit(self, mock_kafka_cls: MagicMock) -> None:
        settings = Settings(
            kafka_bootstrap_servers="localhost:9092",
            alpha_vantage_api_key="test-key",
            watchlist_tickers="AAPL",
            alpha_vantage_poll_interval=0,
        )
        producer = AlphaVantageProducer(settings)
        producer._seen_urls = {f"https://example.com/{i}" for i in range(10_001)}

        published: list[RawFinancialNewsEvent] = []

        def capture(topic: str, key: str, event: RawFinancialNewsEvent) -> None:
            published.append(event)
            producer._running = False

        producer._publish = capture  # type: ignore[assignment]
        producer._fetch_news = MagicMock(  # type: ignore[assignment]
            return_value=AVNewsResponse.model_validate(SAMPLE_AV_RESPONSE)
        )

        producer.produce()

        # After clearing, should publish again
        assert len(published) >= 1
        assert len(producer._seen_urls) <= 3  # cleared then re-added the new items
