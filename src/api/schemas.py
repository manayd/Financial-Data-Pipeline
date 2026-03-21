"""Pydantic response models for the API."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class ArticleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    event_id: UUID
    timestamp: datetime
    source: str
    source_url: str | None
    title: str
    content: str
    tickers: list[str]
    companies: list[str]
    processed_at: datetime


class AggregationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ticker: str
    company_name: str
    window_start: datetime
    window_end: datetime
    article_count: int
    avg_sentiment_score: float | None
    dominant_sentiment: str | None


class PaginatedArticlesResponse(BaseModel):
    items: list[ArticleResponse]
    count: int


class EntityResponse(BaseModel):
    name: str
    entity_type: str
    relevance_score: float


class LLMAnalysisResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    article_event_id: UUID
    summary: str
    sentiment: str
    sentiment_confidence: float
    entities: list[EntityResponse]
    key_topics: list[str]
    analyzed_at: datetime
    model_id: str
    analysis_version: str
