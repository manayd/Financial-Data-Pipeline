"""Tests for the S3ParquetSink consumer."""

import time
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

from src.models.events import ProcessedFinancialNewsEvent, SourceType
from src.storage.s3_sink import S3ParquetSink, events_to_parquet_bytes, partition_key


def _make_event(**overrides):
    defaults = {
        "event_id": uuid4(),
        "timestamp": datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
        "source": SourceType.SYNTHETIC,
        "title": "Test Article",
        "content": "Test content",
        "tickers": ["AAPL"],
        "companies": ["Apple Inc."],
        "content_hash": "abc123",
        "processed_at": datetime.now(tz=UTC),
    }
    defaults.update(overrides)
    return ProcessedFinancialNewsEvent(**defaults)


def test_events_to_parquet_bytes():
    events = [_make_event(), _make_event(tickers=["MSFT"])]
    result = events_to_parquet_bytes(events)
    assert isinstance(result, bytes)
    assert len(result) > 0


def test_partition_key_format():
    event = _make_event()
    date_str, ticker = partition_key(event, "AAPL")
    assert date_str == "2024-06-15"
    assert ticker == "AAPL"


@patch("src.storage.s3_sink.boto3")
@patch("src.storage.s3_sink.Consumer")
def test_s3_sink_should_flush_batch_size(mock_consumer_cls, mock_boto3):
    settings = MagicMock()
    settings.kafka_bootstrap_servers = "localhost:9092"
    settings.s3_endpoint_url = "http://localhost:9000"
    settings.aws_access_key_id = "test"
    settings.aws_secret_access_key = "test"
    settings.aws_region = "us-east-1"
    settings.s3_bucket = "test-bucket"
    settings.s3_sink_batch_size = 2
    settings.s3_sink_flush_interval = 3600

    sink = S3ParquetSink(settings)
    assert sink._should_flush() is False

    sink._batch = [_make_event(), _make_event()]
    assert sink._should_flush() is True


@patch("src.storage.s3_sink.boto3")
@patch("src.storage.s3_sink.Consumer")
def test_s3_sink_should_flush_time_interval(mock_consumer_cls, mock_boto3):
    settings = MagicMock()
    settings.kafka_bootstrap_servers = "localhost:9092"
    settings.s3_endpoint_url = "http://localhost:9000"
    settings.aws_access_key_id = "test"
    settings.aws_secret_access_key = "test"
    settings.aws_region = "us-east-1"
    settings.s3_bucket = "test-bucket"
    settings.s3_sink_batch_size = 1000
    settings.s3_sink_flush_interval = 0  # flush immediately on any data

    sink = S3ParquetSink(settings)
    sink._batch = [_make_event()]
    sink._last_flush = time.monotonic() - 1  # past the interval
    assert sink._should_flush() is True


@patch("src.storage.s3_sink.boto3")
@patch("src.storage.s3_sink.Consumer")
def test_s3_sink_flush_writes_to_s3(mock_consumer_cls, mock_boto3):
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3

    settings = MagicMock()
    settings.kafka_bootstrap_servers = "localhost:9092"
    settings.s3_endpoint_url = "http://localhost:9000"
    settings.aws_access_key_id = "test"
    settings.aws_secret_access_key = "test"
    settings.aws_region = "us-east-1"
    settings.s3_bucket = "test-bucket"
    settings.s3_sink_batch_size = 100
    settings.s3_sink_flush_interval = 60

    sink = S3ParquetSink(settings)
    sink._batch = [_make_event(tickers=["AAPL"]), _make_event(tickers=["MSFT"])]

    sink._flush()

    # Two tickers = two put_object calls
    assert mock_s3.put_object.call_count == 2
    assert sink._batch == []


@patch("src.storage.s3_sink.boto3")
@patch("src.storage.s3_sink.Consumer")
def test_s3_sink_flush_empty_batch_noop(mock_consumer_cls, mock_boto3):
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3

    settings = MagicMock()
    settings.kafka_bootstrap_servers = "localhost:9092"
    settings.s3_endpoint_url = "http://localhost:9000"
    settings.aws_access_key_id = "test"
    settings.aws_secret_access_key = "test"
    settings.aws_region = "us-east-1"
    settings.s3_bucket = "test-bucket"
    settings.s3_sink_batch_size = 100
    settings.s3_sink_flush_interval = 60

    sink = S3ParquetSink(settings)
    sink._flush()

    mock_s3.put_object.assert_not_called()
