"""Tests for the DatabaseSink consumer."""

from datetime import UTC
from unittest.mock import MagicMock, patch

from src.storage.db_sink import DatabaseSink


@patch("src.storage.db_sink.get_sync_session_factory")
@patch("src.storage.db_sink.get_sync_engine")
@patch("src.storage.db_sink.Consumer")
def test_db_sink_subscribes_to_three_topics(mock_consumer_cls, mock_engine, mock_sf):
    settings = MagicMock()
    settings.kafka_bootstrap_servers = "localhost:9092"
    sink = DatabaseSink(settings)

    consumer = mock_consumer_cls.return_value

    # Simulate immediate shutdown after subscribe
    consumer.poll.return_value = None
    sink._running = False

    sink.consume()

    consumer.subscribe.assert_called_once()
    topics = consumer.subscribe.call_args[0][0]
    assert "processed-financial-news" in topics
    assert "aggregation-results" in topics
    assert "llm-analysis-results" in topics


@patch("src.storage.db_sink.get_sync_session_factory")
@patch("src.storage.db_sink.get_sync_engine")
@patch("src.storage.db_sink.Consumer")
def test_db_sink_handle_processed_event(mock_consumer_cls, mock_engine, mock_sf):
    mock_session = MagicMock()
    mock_sf.return_value = MagicMock(return_value=mock_session)
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    settings = MagicMock()
    settings.kafka_bootstrap_servers = "localhost:9092"
    sink = DatabaseSink(settings)

    # Build a minimal processed event message
    msg = MagicMock()
    msg.error.return_value = None
    msg.topic.return_value = "processed-financial-news"

    # Use a properly serialized event
    from datetime import datetime
    from uuid import uuid4

    from src.models.events import ProcessedFinancialNewsEvent, SourceType

    event = ProcessedFinancialNewsEvent(
        event_id=uuid4(),
        timestamp=datetime.now(tz=UTC),
        source=SourceType.SYNTHETIC,
        title="Test",
        content="Test content",
        tickers=["AAPL"],
        companies=["Apple"],
        content_hash="abc123",
        processed_at=datetime.now(tz=UTC),
    )
    msg.value.return_value = event.model_dump_json().encode()

    consumer = mock_consumer_cls.return_value
    # Return one message then stop
    consumer.poll.side_effect = [msg, None]

    sink._running = True

    # Run one iteration manually instead of full consume loop
    sink._handle_processed_event(msg.value())

    mock_session.execute.assert_called_once()
    mock_session.commit.assert_called_once()


@patch("src.storage.db_sink.get_sync_session_factory")
@patch("src.storage.db_sink.get_sync_engine")
@patch("src.storage.db_sink.Consumer")
def test_db_sink_handle_aggregation(mock_consumer_cls, mock_engine, mock_sf):
    mock_session = MagicMock()
    mock_sf.return_value = MagicMock(return_value=mock_session)
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    settings = MagicMock()
    settings.kafka_bootstrap_servers = "localhost:9092"
    sink = DatabaseSink(settings)

    from datetime import datetime

    from src.models.events import TickerAggregation

    agg = TickerAggregation(
        ticker="AAPL",
        company_name="Apple Inc.",
        window_start=datetime.now(tz=UTC),
        window_end=datetime.now(tz=UTC),
        article_count=5,
    )
    sink._handle_aggregation(agg.model_dump_json().encode())

    mock_session.add.assert_called_once()
    mock_session.commit.assert_called_once()


@patch("src.storage.db_sink.get_sync_session_factory")
@patch("src.storage.db_sink.get_sync_engine")
@patch("src.storage.db_sink.Consumer")
def test_db_sink_closes_consumer_on_stop(mock_consumer_cls, mock_engine, mock_sf):
    settings = MagicMock()
    settings.kafka_bootstrap_servers = "localhost:9092"
    sink = DatabaseSink(settings)

    consumer = mock_consumer_cls.return_value
    consumer.poll.return_value = None
    sink._running = False

    sink.consume()

    consumer.close.assert_called_once()
