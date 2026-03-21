import re
import time
from datetime import UTC, datetime

import feedparser
import httpx
import structlog
from pydantic import ValidationError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config import Settings
from src.models.events import RawFinancialNewsEvent, SourceType
from src.producer.base import BaseProducer

logger = structlog.get_logger()

SEC_EDGAR_ATOM_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcompany&type=8-K&dateb=&owner=include&count=40&action=getcompany&output=atom"
)

MAX_SEEN_IDS = 5_000


class SECEdgarProducer(BaseProducer):
    """Polls SEC EDGAR Atom feed for 8-K filings and publishes events to Kafka."""

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self._seen_ids: set[str] = set()
        self._client = httpx.Client(
            timeout=30.0,
            headers={"User-Agent": settings.sec_edgar_user_agent},
        )
        self._watchlist = {t.upper() for t in settings.watchlist_tickers_list}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
        reraise=True,
    )
    def _fetch_feed(self) -> feedparser.FeedParserDict:
        response = self._client.get(SEC_EDGAR_ATOM_URL)
        response.raise_for_status()
        return feedparser.parse(response.text)

    def _extract_tickers(self, title: str, summary: str) -> list[str]:
        """Extract watchlist tickers mentioned in the entry title or summary."""
        text = f"{title} {summary}".upper()
        # Match whole-word ticker symbols
        found = []
        for ticker in self._watchlist:
            if re.search(rf"\b{re.escape(ticker)}\b", text):
                found.append(ticker)
        return found

    def _parse_entry_timestamp(self, entry: dict) -> datetime:
        for field in ("updated_parsed", "published_parsed"):
            parsed = entry.get(field)
            if parsed:
                try:
                    year, month, day, hour, minute, second = parsed[:6]
                    return datetime(year, month, day, hour, minute, second, tzinfo=UTC)
                except (TypeError, ValueError):
                    continue

        for field in ("updated", "published"):
            raw = entry.get(field, "")
            if raw:
                try:
                    return datetime.fromisoformat(raw.replace("Z", "+00:00"))
                except ValueError:
                    continue

        return datetime.now(UTC)

    def _to_event(self, entry: dict) -> RawFinancialNewsEvent:
        title = entry.get("title", "Untitled SEC Filing")
        summary = entry.get("summary", "")
        tickers = self._extract_tickers(title, summary)

        return RawFinancialNewsEvent(
            timestamp=self._parse_entry_timestamp(entry),
            source=SourceType.SEC_EDGAR,
            source_url=entry.get("link", ""),
            title=title,
            content=summary,
            tickers=tickers,
            raw_metadata={
                "filing_type": "8-K",
                "entry_id": entry.get("id", ""),
            },
        )

    def _interruptible_sleep(self, seconds: int) -> None:
        for _ in range(seconds):
            if not self._running:
                return
            time.sleep(1)

    def produce(self) -> None:
        logger.info(
            "sec_edgar_producer_started",
            poll_interval=self.settings.sec_edgar_poll_interval,
        )

        while self._running:
            try:
                feed = self._fetch_feed()
            except (httpx.HTTPStatusError, httpx.TransportError) as e:
                logger.warning("feed_fetch_failed", error=str(e))
                self._interruptible_sleep(self.settings.sec_edgar_poll_interval)
                continue

            for entry in feed.entries:
                if not self._running:
                    return

                entry_id = entry.get("id") or entry.get("link", "")
                if entry_id in self._seen_ids:
                    continue

                try:
                    event = self._to_event(entry)
                except (ValidationError, KeyError) as e:
                    logger.warning("entry_parse_failed", entry_id=entry_id, error=str(e))
                    continue

                key = event.tickers[0] if event.tickers else "SEC-8K"
                self._publish(self.TOPIC, key, event)
                self._seen_ids.add(entry_id)

                logger.info(
                    "event_published",
                    event_id=str(event.event_id),
                    source="sec_edgar",
                    tickers=event.tickers,
                    title=event.title[:60],
                )

            # Cap dedup set size
            if len(self._seen_ids) > MAX_SEEN_IDS:
                self._seen_ids.clear()
                logger.info("seen_ids_cleared")

            self._interruptible_sleep(self.settings.sec_edgar_poll_interval)
