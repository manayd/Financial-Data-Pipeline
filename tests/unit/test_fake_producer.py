from unittest.mock import MagicMock, patch

from src.config import Settings
from src.models.events import RawFinancialNewsEvent, SourceType
from src.producer.fake_producer import FakeProducer, _generate_content, _generate_headline


class TestFakeProducerHelpers:
    def test_generate_headline_is_nonempty_string(self) -> None:
        headline = _generate_headline()
        assert isinstance(headline, str)
        assert len(headline) > 10

    def test_generate_content_includes_headline(self) -> None:
        headline = "Test Headline"
        content = _generate_content(headline)
        assert content.startswith("Test Headline.")
        assert len(content) > len(headline)

    def test_generate_headline_varies(self) -> None:
        headlines = {_generate_headline() for _ in range(20)}
        # With 15 templates and random params, we should get variety
        assert len(headlines) > 1


class TestFakeProducer:
    @patch("src.producer.base.Producer")
    def test_produces_valid_events(self, mock_kafka_producer_cls: MagicMock) -> None:
        mock_producer = MagicMock()
        mock_kafka_producer_cls.return_value = mock_producer

        settings = Settings(
            kafka_bootstrap_servers="localhost:9092",
            fake_producer_interval=0.0,
        )
        producer = FakeProducer(settings)

        # Stop after one iteration
        published_events: list[RawFinancialNewsEvent] = []

        def capture_and_stop(topic: str, key: str, event: RawFinancialNewsEvent) -> None:
            published_events.append(event)
            producer._running = False

        producer._publish = capture_and_stop  # type: ignore[assignment]
        producer.produce()

        assert len(published_events) == 1
        event = published_events[0]
        assert isinstance(event, RawFinancialNewsEvent)
        assert event.source == SourceType.SYNTHETIC
        assert len(event.tickers) >= 1
        assert len(event.title) > 0
        assert len(event.content) > 0
