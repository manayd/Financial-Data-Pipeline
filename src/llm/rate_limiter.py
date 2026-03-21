"""Simple async rate limiter for LLM API calls."""

import asyncio
import time


class RateLimiter:
    """Enforces a maximum requests-per-minute rate via sleep-based throttling."""

    def __init__(self, requests_per_minute: int) -> None:
        self._interval = 60.0 / max(requests_per_minute, 1)
        self._last_request = 0.0

    async def acquire(self) -> None:
        """Wait until enough time has passed since the last request."""
        now = time.monotonic()
        elapsed = now - self._last_request
        if elapsed < self._interval:
            await asyncio.sleep(self._interval - elapsed)
        self._last_request = time.monotonic()
