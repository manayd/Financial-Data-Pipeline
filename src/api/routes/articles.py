"""Article endpoints."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies import get_session
from src.api.schemas import ArticleResponse, LLMAnalysisResponse, PaginatedArticlesResponse
from src.storage import repository

router = APIRouter()


@router.get("/articles", response_model=PaginatedArticlesResponse)
async def list_articles(
    ticker: str | None = None,
    limit: int = Query(default=10, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> PaginatedArticlesResponse:
    articles = await repository.get_articles(session, ticker=ticker, limit=limit, offset=offset)
    return PaginatedArticlesResponse(
        items=[ArticleResponse.model_validate(a) for a in articles],
        count=len(articles),
    )


@router.get("/articles/{event_id}", response_model=ArticleResponse)
async def get_article(
    event_id: UUID,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> ArticleResponse:
    article = await repository.get_article_by_event_id(session, event_id)
    if article is None:
        raise HTTPException(status_code=404, detail="Article not found")
    return ArticleResponse.model_validate(article)


@router.get("/tickers/{ticker}/latest", response_model=list[ArticleResponse])
async def latest_for_ticker(
    ticker: str,
    limit: int = Query(default=5, ge=1, le=50),
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> list[ArticleResponse]:
    articles = await repository.get_latest_for_ticker(session, ticker=ticker, limit=limit)
    return [ArticleResponse.model_validate(a) for a in articles]


@router.get("/articles/{event_id}/analysis", response_model=LLMAnalysisResponse)
async def get_article_analysis(
    event_id: UUID,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> LLMAnalysisResponse:
    analysis = await repository.get_analysis_for_article(session, event_id)
    if analysis is None:
        raise HTTPException(status_code=404, detail="Analysis not found")
    return LLMAnalysisResponse.model_validate(analysis)
