"""SQLAlchemy ORM models for the serving database."""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Article(Base):
    """Persisted processed financial news events."""

    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID] = mapped_column(unique=True, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    tickers: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)
    companies: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    raw_metadata: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        Index("ix_articles_timestamp", "timestamp"),
        Index("ix_articles_tickers", "tickers", postgresql_using="gin"),
    )


class TickerAggregationRow(Base):
    """Persisted per-ticker aggregation snapshots."""

    __tablename__ = "ticker_aggregations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False)
    company_name: Mapped[str] = mapped_column(String(200), nullable=False)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    article_count: Mapped[int] = mapped_column(Integer, nullable=False)
    avg_sentiment_score: Mapped[float | None] = mapped_column(nullable=True)
    dominant_sentiment: Mapped[str | None] = mapped_column(String(20), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (Index("ix_ticker_agg_ticker_window", "ticker", "window_end"),)


class LLMAnalysisRow(Base):
    """Persisted LLM analysis results for articles."""

    __tablename__ = "llm_analyses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    article_event_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("articles.event_id"), nullable=False
    )
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    sentiment: Mapped[str] = mapped_column(String(20), nullable=False)
    sentiment_confidence: Mapped[float] = mapped_column(nullable=False)
    entities: Mapped[dict | None] = mapped_column(JSONB, nullable=False)
    key_topics: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)
    analyzed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    model_id: Mapped[str] = mapped_column(String(100), nullable=False)
    analysis_version: Mapped[str] = mapped_column(String(20), nullable=False, default="1.0")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (Index("ix_llm_analyses_article", "article_event_id"),)
