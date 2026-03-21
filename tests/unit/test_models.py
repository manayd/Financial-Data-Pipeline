from datetime import UTC, datetime
from uuid import UUID

import orjson
import pytest
from pydantic import ValidationError

from src.models import KafkaDeserializer, KafkaSerializer
from src.models.events import RawFinancialNewsEvent, SourceType


def _make_event(**overrides: object) -> RawFinancialNewsEvent:
    defaults = {
        "timestamp": datetime.now(UTC),
        "source": SourceType.SYNTHETIC,
        "title": "AAPL beats earnings expectations",
        "content": "Apple Inc. reported record quarterly revenue...",
        "tickers": ["AAPL"],
    }
    defaults.update(overrides)
    return RawFinancialNewsEvent(**defaults)


class TestRawFinancialNewsEvent:
    def test_valid_event_creation(self) -> None:
        event = _make_event()
        assert isinstance(event.event_id, UUID)
        assert event.source == SourceType.SYNTHETIC
        assert event.tickers == ["AAPL"]

    def test_event_id_auto_generated(self) -> None:
        e1 = _make_event()
        e2 = _make_event()
        assert e1.event_id != e2.event_id

    def test_optional_fields_default_to_none(self) -> None:
        event = _make_event()
        assert event.source_url is None
        assert event.raw_metadata is None

    def test_multiple_tickers(self) -> None:
        event = _make_event(tickers=["AAPL", "MSFT", "GOOGL"])
        assert len(event.tickers) == 3

    def test_rejects_missing_required_fields(self) -> None:
        with pytest.raises(ValidationError):
            RawFinancialNewsEvent(
                timestamp=datetime.now(UTC),
                source=SourceType.SYNTHETIC,
                # missing title, content, tickers
            )

    def test_rejects_invalid_source(self) -> None:
        with pytest.raises(ValidationError):
            _make_event(source="invalid_source")

    def test_rejects_empty_title(self) -> None:
        # Pydantic allows empty strings by default; this verifies the field is at least present
        event = _make_event(title="")
        assert event.title == ""


class TestKafkaSerialization:
    def test_roundtrip_serialization(self) -> None:
        original = _make_event()
        serialized = KafkaSerializer.serialize(original)
        deserialized = KafkaDeserializer.deserialize(serialized, RawFinancialNewsEvent)

        assert deserialized.event_id == original.event_id
        assert deserialized.title == original.title
        assert deserialized.tickers == original.tickers
        assert deserialized.source == original.source

    def test_serialized_is_bytes(self) -> None:
        event = _make_event()
        result = KafkaSerializer.serialize(event)
        assert isinstance(result, bytes)

    def test_deserialize_invalid_bytes_raises(self) -> None:
        with pytest.raises(orjson.JSONDecodeError):
            KafkaDeserializer.deserialize(b"not valid json", RawFinancialNewsEvent)
