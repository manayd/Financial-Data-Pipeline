from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class SourceType(StrEnum):
    ALPHA_VANTAGE = "alpha_vantage"
    SEC_EDGAR = "sec_edgar"
    SYNTHETIC = "synthetic"


class RawFinancialNewsEvent(BaseModel):
    """Schema for the raw-financial-news Kafka topic."""

    event_id: UUID = Field(default_factory=uuid4)
    timestamp: datetime
    source: SourceType
    source_url: str | None = None
    title: str
    content: str
    tickers: list[str]
    raw_metadata: dict | None = None


class ProcessedFinancialNewsEvent(RawFinancialNewsEvent):
    """Schema for the processed-financial-news Kafka topic."""

    companies: list[str]
    content_hash: str
    processed_at: datetime


class SentimentLabel(StrEnum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"
    MIXED = "mixed"


class ExtractedEntity(BaseModel):
    name: str
    entity_type: str
    relevance_score: float


class LLMAnalysisResult(BaseModel):
    """Schema for the llm-analysis-results Kafka topic."""

    article_event_id: UUID
    summary: str
    sentiment: SentimentLabel
    sentiment_confidence: float = Field(ge=0.0, le=1.0)
    entities: list[ExtractedEntity]
    key_topics: list[str]
    analyzed_at: datetime
    model_id: str
    analysis_version: str = "1.0"


class TickerAggregation(BaseModel):
    """Schema for the aggregation-results Kafka topic."""

    ticker: str
    company_name: str
    window_start: datetime
    window_end: datetime
    article_count: int
    avg_sentiment_score: float | None = None
    dominant_sentiment: SentimentLabel | None = None
