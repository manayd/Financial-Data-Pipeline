"""Database engine and session factories."""

from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from src.config import Settings


def get_sync_engine(settings: Settings) -> Engine:
    """Create a synchronous engine for consumer sinks."""
    return create_engine(settings.database_url, pool_pre_ping=True)


def get_async_engine(settings: Settings) -> AsyncEngine:
    """Create an async engine for FastAPI. Converts postgresql:// to postgresql+asyncpg://."""
    url = settings.database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return _create_async_engine(url, pool_pre_ping=True)


def get_sync_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Create a sync session factory."""
    return sessionmaker(bind=engine, expire_on_commit=False)


def get_async_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create an async session factory."""
    return async_sessionmaker(bind=engine, expire_on_commit=False)
