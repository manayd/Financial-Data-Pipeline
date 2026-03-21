from unittest.mock import MagicMock, patch

import feedparser
import httpx

from src.config import Settings
from src.models.events import RawFinancialNewsEvent, SourceType
from src.producer.sec_edgar_producer import SECEdgarProducer

SAMPLE_ATOM_FEED = """\
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>SEC EDGAR 8-K Filings</title>
  <entry>
    <title>8-K - Apple Inc (0000320193) (Filer)</title>
    <link href="https://www.sec.gov/Archives/edgar/data/320193/filing1.htm"/>
    <id>urn:tag:sec.gov,2008:accession-number=0000320193-23-000100</id>
    <updated>2023-12-15T14:30:00-05:00</updated>
    <summary>Filed 8-K for Apple Inc reporting material events.</summary>
  </entry>
  <entry>
    <title>8-K - Unknown Corp (0000999999) (Filer)</title>
    <link href="https://www.sec.gov/Archives/edgar/data/999999/filing2.htm"/>
    <id>urn:tag:sec.gov,2008:accession-number=0000999999-23-000200</id>
    <updated>2023-12-15T15:00:00-05:00</updated>
    <summary>Filed 8-K for Unknown Corp.</summary>
  </entry>
</feed>
"""


def _make_producer(mock_kafka_cls: MagicMock) -> SECEdgarProducer:
    settings = Settings(
        kafka_bootstrap_servers="localhost:9092",
        producer_type="sec_edgar",
        watchlist_tickers="AAPL,MSFT",
        sec_edgar_poll_interval=0,
    )
    return SECEdgarProducer(settings)


class TestSECEdgarProducer:
    @patch("src.producer.base.Producer")
    def test_produces_events_from_feed(self, mock_kafka_cls: MagicMock) -> None:
        producer = _make_producer(mock_kafka_cls)
        published: list[RawFinancialNewsEvent] = []

        def capture(topic: str, key: str, event: RawFinancialNewsEvent) -> None:
            published.append(event)
            if len(published) >= 2:
                producer._running = False

        producer._publish = capture  # type: ignore[assignment]
        producer._fetch_feed = MagicMock(  # type: ignore[assignment]
            return_value=feedparser.parse(SAMPLE_ATOM_FEED)
        )

        producer.produce()

        assert len(published) == 2
        assert published[0].source == SourceType.SEC_EDGAR
        assert "Apple" in published[0].title

    @patch("src.producer.base.Producer")
    def test_deduplication_by_entry_id(self, mock_kafka_cls: MagicMock) -> None:
        producer = _make_producer(mock_kafka_cls)
        published: list[RawFinancialNewsEvent] = []
        call_count = 0

        def capture(topic: str, key: str, event: RawFinancialNewsEvent) -> None:
            published.append(event)

        def fetch_and_count() -> feedparser.FeedParserDict:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                producer._running = False
            return feedparser.parse(SAMPLE_ATOM_FEED)

        producer._publish = capture  # type: ignore[assignment]
        producer._fetch_feed = fetch_and_count  # type: ignore[assignment]

        producer.produce()

        # First call publishes 2, second deduplicates both
        assert len(published) == 2

    @patch("src.producer.base.Producer")
    def test_extracts_tickers_from_title(self, mock_kafka_cls: MagicMock) -> None:
        producer = _make_producer(mock_kafka_cls)

        # "Apple" isn't the ticker — need to check if "AAPL" is in title/summary
        tickers = producer._extract_tickers(
            "8-K - AAPL Inc (0000320193)", "Filed 8-K for AAPL Inc."
        )
        assert "AAPL" in tickers

    @patch("src.producer.base.Producer")
    def test_no_tickers_when_none_match(self, mock_kafka_cls: MagicMock) -> None:
        producer = _make_producer(mock_kafka_cls)

        tickers = producer._extract_tickers("8-K - Unknown Corp", "Filed 8-K for Unknown Corp.")
        assert tickers == []

    @patch("src.producer.base.Producer")
    def test_http_error_handled_gracefully(self, mock_kafka_cls: MagicMock) -> None:
        producer = _make_producer(mock_kafka_cls)
        call_count = 0

        def fail_then_stop() -> feedparser.FeedParserDict:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                producer._running = False
            raise httpx.TransportError("connection failed")

        producer._fetch_feed = fail_then_stop  # type: ignore[assignment]
        # Should not crash
        producer.produce()

    @patch("src.producer.base.Producer")
    def test_user_agent_header_set(self, mock_kafka_cls: MagicMock) -> None:
        producer = _make_producer(mock_kafka_cls)
        assert "User-Agent" in producer._client.headers
        assert "FinancialDataPipeline" in producer._client.headers["User-Agent"]

    @patch("src.producer.base.Producer")
    def test_event_has_filing_metadata(self, mock_kafka_cls: MagicMock) -> None:
        producer = _make_producer(mock_kafka_cls)
        published: list[RawFinancialNewsEvent] = []

        def capture(topic: str, key: str, event: RawFinancialNewsEvent) -> None:
            published.append(event)
            producer._running = False

        producer._publish = capture  # type: ignore[assignment]
        producer._fetch_feed = MagicMock(  # type: ignore[assignment]
            return_value=feedparser.parse(SAMPLE_ATOM_FEED)
        )

        producer.produce()

        assert published[0].raw_metadata is not None
        assert published[0].raw_metadata["filing_type"] == "8-K"
        assert published[0].source_url is not None
