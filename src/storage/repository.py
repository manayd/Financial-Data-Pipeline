"""Data access layer for querying the serving database."""

import uuid
from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.storage.models import Article, LLMAnalysisRow, TickerAggregationRow


async def get_articles(
    session: AsyncSession,
    ticker: str | None = None,
    limit: int = 10,
    offset: int = 0,
) -> Sequence[Article]:
    """List articles, optionally filtered by ticker."""
    stmt = select(Article).order_by(Article.timestamp.desc()).limit(limit).offset(offset)
    if ticker:
        stmt = stmt.where(Article.tickers.any(ticker))  # type: ignore[arg-type]
    result = await session.execute(stmt)
    return result.scalars().all()


async def get_article_by_event_id(
    session: AsyncSession,
    event_id: uuid.UUID,
) -> Article | None:
    """Get a single article by its event_id."""
    stmt = select(Article).where(Article.event_id == event_id)
    result = await session.execute(stmt)
    return result.scalars().first()


async def get_latest_for_ticker(
    session: AsyncSession,
    ticker: str,
    limit: int = 5,
) -> Sequence[Article]:
    """Get the latest articles for a specific ticker."""
    stmt = (
        select(Article)
        .where(Article.tickers.any(ticker))  # type: ignore[arg-type]
        .order_by(Article.timestamp.desc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    return result.scalars().all()


async def get_aggregations(
    session: AsyncSession,
    ticker: str | None = None,
    limit: int = 10,
) -> Sequence[TickerAggregationRow]:
    """List aggregation snapshots, optionally filtered by ticker."""
    stmt = (
        select(TickerAggregationRow).order_by(TickerAggregationRow.window_end.desc()).limit(limit)
    )
    if ticker:
        stmt = stmt.where(TickerAggregationRow.ticker == ticker)
    result = await session.execute(stmt)
    return result.scalars().all()


async def get_analysis_for_article(
    session: AsyncSession,
    event_id: uuid.UUID,
) -> LLMAnalysisRow | None:
    """Get the LLM analysis for a specific article by event_id."""
    stmt = select(LLMAnalysisRow).where(LLMAnalysisRow.article_event_id == event_id)
    result = await session.execute(stmt)
    return result.scalars().first()


async def get_analyses(
    session: AsyncSession,
    limit: int = 10,
) -> Sequence[LLMAnalysisRow]:
    """List recent LLM analyses."""
    stmt = select(LLMAnalysisRow).order_by(LLMAnalysisRow.analyzed_at.desc()).limit(limit)
    result = await session.execute(stmt)
    return result.scalars().all()
