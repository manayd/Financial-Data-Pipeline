"""Aggregation endpoints."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies import get_session
from src.api.schemas import AggregationResponse
from src.storage import repository

router = APIRouter()


@router.get("/aggregations", response_model=list[AggregationResponse])
async def list_aggregations(
    ticker: str | None = None,
    limit: int = Query(default=10, ge=1, le=100),
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> list[AggregationResponse]:
    aggregations = await repository.get_aggregations(session, ticker=ticker, limit=limit)
    return [AggregationResponse.model_validate(a) for a in aggregations]
