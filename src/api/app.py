"""FastAPI application with lifespan for database connection management."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.api.routes import aggregations, articles, health
from src.config import Settings
from src.storage.database import get_async_engine


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = Settings()
    engine = get_async_engine(settings)
    app.state.async_engine = engine
    yield
    await engine.dispose()


app = FastAPI(title="Financial Data Pipeline API", version="1.0.0", lifespan=lifespan)

app.include_router(health.router, prefix="/api/v1", tags=["health"])
app.include_router(articles.router, prefix="/api/v1", tags=["articles"])
app.include_router(aggregations.router, prefix="/api/v1", tags=["aggregations"])
