"""Tests for LLM analyzer logic: result building, parsing, and storage model."""

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from src.llm.analyzer import build_analysis_result
from src.models.events import SentimentLabel
from src.storage.models import LLMAnalysisRow


def _sample_llm_output() -> dict:
    return {
        "summary": "Apple reported strong Q4 earnings beating analyst expectations.",
        "sentiment": "bullish",
        "sentiment_confidence": 0.92,
        "entities": [
            {"name": "Apple Inc.", "entity_type": "company", "relevance_score": 0.95},
            {"name": "Tim Cook", "entity_type": "person", "relevance_score": 0.7},
        ],
        "key_topics": ["earnings", "revenue", "iPhone sales"],
    }


class TestBuildAnalysisResult:
    def test_basic_fields(self) -> None:
        event_id = uuid.uuid4()
        raw = _sample_llm_output()
        result = build_analysis_result(raw, event_id, "gpt-4o-mini")

        assert result.article_event_id == event_id
        assert result.summary == raw["summary"]
        assert result.sentiment == SentimentLabel.BULLISH
        assert result.sentiment_confidence == 0.92
        assert result.model_id == "gpt-4o-mini"
        assert result.analysis_version == "1.0"

    def test_entities_parsed(self) -> None:
        raw = _sample_llm_output()
        result = build_analysis_result(raw, uuid.uuid4(), "gpt-4o-mini")

        assert len(result.entities) == 2
        assert result.entities[0].name == "Apple Inc."
        assert result.entities[0].entity_type == "company"
        assert result.entities[1].name == "Tim Cook"

    def test_key_topics(self) -> None:
        raw = _sample_llm_output()
        result = build_analysis_result(raw, uuid.uuid4(), "gpt-4o-mini")
        assert result.key_topics == ["earnings", "revenue", "iPhone sales"]

    def test_analyzed_at_set(self) -> None:
        raw = _sample_llm_output()
        before = datetime.now(UTC)
        result = build_analysis_result(raw, uuid.uuid4(), "gpt-4o-mini")
        after = datetime.now(UTC)
        assert before <= result.analyzed_at <= after

    def test_missing_entities_defaults_to_empty(self) -> None:
        raw = _sample_llm_output()
        del raw["entities"]
        result = build_analysis_result(raw, uuid.uuid4(), "gpt-4o-mini")
        assert result.entities == []

    def test_missing_key_topics_defaults_to_empty(self) -> None:
        raw = _sample_llm_output()
        del raw["key_topics"]
        result = build_analysis_result(raw, uuid.uuid4(), "gpt-4o-mini")
        assert result.key_topics == []

    def test_all_sentiment_labels(self) -> None:
        for label in ["bullish", "bearish", "neutral", "mixed"]:
            raw = _sample_llm_output()
            raw["sentiment"] = label
            result = build_analysis_result(raw, uuid.uuid4(), "gpt-4o-mini")
            assert result.sentiment == SentimentLabel(label)

    def test_invalid_sentiment_raises(self) -> None:
        raw = _sample_llm_output()
        raw["sentiment"] = "super_bullish"
        with pytest.raises(ValueError):
            build_analysis_result(raw, uuid.uuid4(), "gpt-4o-mini")

    def test_confidence_boundaries(self) -> None:
        raw = _sample_llm_output()

        raw["sentiment_confidence"] = 0.0
        result = build_analysis_result(raw, uuid.uuid4(), "gpt-4o-mini")
        assert result.sentiment_confidence == 0.0

        raw["sentiment_confidence"] = 1.0
        result = build_analysis_result(raw, uuid.uuid4(), "gpt-4o-mini")
        assert result.sentiment_confidence == 1.0

    def test_confidence_out_of_range_raises(self) -> None:
        raw = _sample_llm_output()
        raw["sentiment_confidence"] = 1.5
        with pytest.raises(ValueError):
            build_analysis_result(raw, uuid.uuid4(), "gpt-4o-mini")

    def test_serialization_roundtrip(self) -> None:
        from src.models import KafkaDeserializer, KafkaSerializer
        from src.models.events import LLMAnalysisResult

        raw = _sample_llm_output()
        result = build_analysis_result(raw, uuid.uuid4(), "gpt-4o-mini")

        payload = KafkaSerializer.serialize(result)
        restored = KafkaDeserializer.deserialize(payload, LLMAnalysisResult)

        assert restored.article_event_id == result.article_event_id
        assert restored.summary == result.summary
        assert restored.sentiment == result.sentiment
        assert len(restored.entities) == len(result.entities)


class TestLLMAnalysisRowModel:
    def test_table_name(self) -> None:
        assert LLMAnalysisRow.__tablename__ == "llm_analyses"

    def test_primary_key(self) -> None:
        pk_cols = [c.name for c in LLMAnalysisRow.__table__.primary_key.columns]
        assert pk_cols == ["id"]

    def test_column_types(self) -> None:
        cols = {c.name: c for c in LLMAnalysisRow.__table__.columns}
        assert isinstance(cols["id"].type, Integer)
        assert isinstance(cols["summary"].type, Text)
        assert isinstance(cols["sentiment"].type, String)
        assert isinstance(cols["entities"].type, JSONB)
        assert isinstance(cols["key_topics"].type, ARRAY)
        assert isinstance(cols["model_id"].type, String)
        assert isinstance(cols["analyzed_at"].type, DateTime)

    def test_foreign_key_exists(self) -> None:
        cols = {c.name: c for c in LLMAnalysisRow.__table__.columns}
        fks = list(cols["article_event_id"].foreign_keys)
        assert len(fks) == 1
        assert "articles.event_id" in str(fks[0].target_fullname)

    def test_index_exists(self) -> None:
        index_names = [idx.name for idx in LLMAnalysisRow.__table__.indexes]
        assert "ix_llm_analyses_article" in index_names
