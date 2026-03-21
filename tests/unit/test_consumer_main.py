"""Tests for the consumer factory pattern."""

from unittest.mock import MagicMock, patch

import pytest

from src.consumer.main import _create_consumers


@patch("src.consumer.main.Settings")
def test_create_console_consumer(mock_settings_cls):
    settings = MagicMock()
    settings.consumer_type = "console"
    settings.kafka_bootstrap_servers = "localhost:9092"

    with patch("src.consumer.console_consumer.Consumer"):
        consumers = _create_consumers(settings)

    assert len(consumers) == 1
    assert consumers[0].__class__.__name__ == "ConsoleConsumer"


@patch("src.storage.db_sink.get_sync_session_factory")
@patch("src.storage.db_sink.get_sync_engine")
@patch("src.storage.db_sink.Consumer")
def test_create_db_sink_consumer(mock_consumer, mock_engine, mock_sf):
    settings = MagicMock()
    settings.consumer_type = "db_sink"
    settings.kafka_bootstrap_servers = "localhost:9092"

    consumers = _create_consumers(settings)

    assert len(consumers) == 1
    assert consumers[0].__class__.__name__ == "DatabaseSink"


@patch("src.storage.s3_sink.boto3")
@patch("src.storage.s3_sink.Consumer")
def test_create_s3_sink_consumer(mock_consumer, mock_boto3):
    settings = MagicMock()
    settings.consumer_type = "s3_sink"
    settings.kafka_bootstrap_servers = "localhost:9092"
    settings.s3_endpoint_url = "http://localhost:9000"
    settings.aws_access_key_id = "test"
    settings.aws_secret_access_key = "test"
    settings.aws_region = "us-east-1"
    settings.s3_bucket = "test-bucket"

    consumers = _create_consumers(settings)

    assert len(consumers) == 1
    assert consumers[0].__class__.__name__ == "S3ParquetSink"


@patch("src.storage.s3_sink.boto3")
@patch("src.storage.s3_sink.Consumer")
@patch("src.storage.db_sink.get_sync_session_factory")
@patch("src.storage.db_sink.get_sync_engine")
@patch("src.storage.db_sink.Consumer")
def test_create_all_consumers(mock_db_consumer, mock_engine, mock_sf, mock_s3_consumer, mock_boto3):
    settings = MagicMock()
    settings.consumer_type = "all"
    settings.kafka_bootstrap_servers = "localhost:9092"
    settings.s3_endpoint_url = "http://localhost:9000"
    settings.aws_access_key_id = "test"
    settings.aws_secret_access_key = "test"
    settings.aws_region = "us-east-1"
    settings.s3_bucket = "test-bucket"

    consumers = _create_consumers(settings)

    assert len(consumers) == 2
    class_names = {c.__class__.__name__ for c in consumers}
    assert "DatabaseSink" in class_names
    assert "S3ParquetSink" in class_names


def test_create_unknown_consumer_raises():
    settings = MagicMock()
    settings.consumer_type = "invalid_type"

    with pytest.raises(ValueError, match="Unknown consumer type"):
        _create_consumers(settings)
