"""FastAPI dependency injection."""

from collections.abc import AsyncGenerator

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


async def get_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """Yield an async database session from the app's engine."""
    factory = async_sessionmaker(
        bind=request.app.state.async_engine,
        expire_on_commit=False,
    )
    async with factory() as session:
        yield session
