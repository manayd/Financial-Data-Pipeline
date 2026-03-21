from unittest.mock import MagicMock, patch

import pytest

from src.config import Settings
from src.producer.main import _create_producers


class TestCreateProducers:
    @patch("src.producer.base.Producer")
    def test_fake_type_returns_fake_producer(self, mock_kafka: MagicMock) -> None:
        settings = Settings(
            kafka_bootstrap_servers="localhost:9092",
            producer_type="fake",
        )
        producers = _create_producers(settings)
        assert len(producers) == 1
        assert producers[0].__class__.__name__ == "FakeProducer"

    @patch("src.producer.base.Producer")
    def test_alpha_vantage_type(self, mock_kafka: MagicMock) -> None:
        settings = Settings(
            kafka_bootstrap_servers="localhost:9092",
            producer_type="alpha_vantage",
            alpha_vantage_api_key="test-key",
        )
        producers = _create_producers(settings)
        assert len(producers) == 1
        assert producers[0].__class__.__name__ == "AlphaVantageProducer"

    @patch("src.producer.base.Producer")
    def test_sec_edgar_type(self, mock_kafka: MagicMock) -> None:
        settings = Settings(
            kafka_bootstrap_servers="localhost:9092",
            producer_type="sec_edgar",
        )
        producers = _create_producers(settings)
        assert len(producers) == 1
        assert producers[0].__class__.__name__ == "SECEdgarProducer"

    @patch("src.producer.base.Producer")
    def test_real_type_returns_two_producers(self, mock_kafka: MagicMock) -> None:
        settings = Settings(
            kafka_bootstrap_servers="localhost:9092",
            producer_type="real",
            alpha_vantage_api_key="test-key",
        )
        producers = _create_producers(settings)
        assert len(producers) == 2
        names = {p.__class__.__name__ for p in producers}
        assert names == {"AlphaVantageProducer", "SECEdgarProducer"}

    @patch("src.producer.base.Producer")
    def test_all_type_returns_three_producers(self, mock_kafka: MagicMock) -> None:
        settings = Settings(
            kafka_bootstrap_servers="localhost:9092",
            producer_type="all",
            alpha_vantage_api_key="test-key",
        )
        producers = _create_producers(settings)
        assert len(producers) == 3
        names = {p.__class__.__name__ for p in producers}
        assert names == {"FakeProducer", "AlphaVantageProducer", "SECEdgarProducer"}

    @patch("src.producer.base.Producer")
    def test_alpha_vantage_requires_api_key(self, mock_kafka: MagicMock) -> None:
        settings = Settings(
            kafka_bootstrap_servers="localhost:9092",
            producer_type="alpha_vantage",
            alpha_vantage_api_key="",
        )
        with pytest.raises(ValueError, match="ALPHA_VANTAGE_API_KEY"):
            _create_producers(settings)

    @patch("src.producer.base.Producer")
    def test_unknown_type_raises(self, mock_kafka: MagicMock) -> None:
        settings = Settings(
            kafka_bootstrap_servers="localhost:9092",
            producer_type="nonexistent",
        )
        with pytest.raises(ValueError, match="Unknown producer type"):
            _create_producers(settings)
