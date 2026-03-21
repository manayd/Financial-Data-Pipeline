"""Tests for SQLAlchemy ORM models (metadata introspection, no live DB needed)."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from src.storage.models import Article, TickerAggregationRow


class TestArticleModel:
    def test_table_name(self) -> None:
        assert Article.__tablename__ == "articles"

    def test_primary_key(self) -> None:
        pk_cols = [c.name for c in Article.__table__.primary_key.columns]
        assert pk_cols == ["id"]

    def test_column_types(self) -> None:
        cols = {c.name: c for c in Article.__table__.columns}
        assert isinstance(cols["id"].type, Integer)
        assert isinstance(cols["source"].type, String)
        assert isinstance(cols["title"].type, Text)
        assert isinstance(cols["content"].type, Text)
        assert isinstance(cols["tickers"].type, ARRAY)
        assert isinstance(cols["companies"].type, ARRAY)
        assert isinstance(cols["content_hash"].type, String)
        assert isinstance(cols["raw_metadata"].type, JSONB)
        assert isinstance(cols["timestamp"].type, DateTime)
        assert cols["timestamp"].type.timezone is True

    def test_unique_constraints(self) -> None:
        cols = {c.name: c for c in Article.__table__.columns}
        assert cols["event_id"].unique is True
        assert cols["content_hash"].unique is True

    def test_created_at_has_server_default(self) -> None:
        cols = {c.name: c for c in Article.__table__.columns}
        assert cols["created_at"].server_default is not None

    def test_has_timestamp_index(self) -> None:
        index_names = {idx.name for idx in Article.__table__.indexes}
        assert "ix_articles_timestamp" in index_names

    def test_has_tickers_gin_index(self) -> None:
        index_names = {idx.name for idx in Article.__table__.indexes}
        assert "ix_articles_tickers" in index_names


class TestTickerAggregationRowModel:
    def test_table_name(self) -> None:
        assert TickerAggregationRow.__tablename__ == "ticker_aggregations"

    def test_primary_key(self) -> None:
        pk_cols = [c.name for c in TickerAggregationRow.__table__.primary_key.columns]
        assert pk_cols == ["id"]

    def test_column_types(self) -> None:
        cols = {c.name: c for c in TickerAggregationRow.__table__.columns}
        assert isinstance(cols["ticker"].type, String)
        assert isinstance(cols["company_name"].type, String)
        assert isinstance(cols["article_count"].type, Integer)
        assert isinstance(cols["window_start"].type, DateTime)
        assert cols["window_start"].type.timezone is True

    def test_nullable_sentiment_fields(self) -> None:
        cols = {c.name: c for c in TickerAggregationRow.__table__.columns}
        assert cols["avg_sentiment_score"].nullable is True
        assert cols["dominant_sentiment"].nullable is True

    def test_has_composite_index(self) -> None:
        index_names = {idx.name for idx in TickerAggregationRow.__table__.indexes}
        assert "ix_ticker_agg_ticker_window" in index_names

    def test_created_at_has_server_default(self) -> None:
        cols = {c.name: c for c in TickerAggregationRow.__table__.columns}
        assert cols["created_at"].server_default is not None


class TestPydanticToORMMapping:
    def test_article_from_processed_event_fields(self) -> None:
        """Verify the field mapping from ProcessedFinancialNewsEvent to Article kwargs."""
        kwargs = {
            "event_id": uuid.uuid4(),
            "timestamp": datetime(2024, 1, 15, 10, 30, tzinfo=UTC),
            "source": "alpha_vantage",
            "source_url": "https://example.com/article",
            "title": "Test Article",
            "content": "Article content here.",
            "tickers": ["AAPL", "MSFT"],
            "companies": ["Apple Inc.", "Microsoft Corporation"],
            "content_hash": "abc123def456",
            "raw_metadata": {"key": "value"},
            "processed_at": datetime(2024, 1, 15, 10, 31, tzinfo=UTC),
        }
        # Verify all required Article columns are covered
        required = {
            c.name
            for c in Article.__table__.columns
            if not c.nullable and c.server_default is None and c.name != "id"
        }
        assert required.issubset(kwargs.keys())

    def test_aggregation_row_from_ticker_aggregation(self) -> None:
        """Verify the field mapping from TickerAggregation to TickerAggregationRow."""
        kwargs = {
            "ticker": "AAPL",
            "company_name": "Apple Inc.",
            "window_start": datetime(2024, 1, 15, 10, 0, tzinfo=UTC),
            "window_end": datetime(2024, 1, 15, 11, 0, tzinfo=UTC),
            "article_count": 15,
            "avg_sentiment_score": None,
            "dominant_sentiment": None,
        }
        required = {
            c.name
            for c in TickerAggregationRow.__table__.columns
            if not c.nullable and c.server_default is None and c.name != "id"
        }
        assert required.issubset(kwargs.keys())
