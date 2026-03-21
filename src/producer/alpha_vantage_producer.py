import time
from datetime import UTC, datetime

import httpx
import structlog
from pydantic import BaseModel, Field, ValidationError
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

MAX_SEEN_URLS = 10_000


class AVTickerSentiment(BaseModel):
    ticker: str
    relevance_score: str
    ticker_sentiment_score: str
    ticker_sentiment_label: str


class AVFeedItem(BaseModel):
    title: str
    url: str
    summary: str
    source: str
    time_published: str
    authors: list[str] = Field(default_factory=list)
    overall_sentiment_score: float
    overall_sentiment_label: str
    ticker_sentiment: list[AVTickerSentiment] = Field(default_factory=list)


class AVNewsResponse(BaseModel):
    items: str | None = None
    feed: list[AVFeedItem] = Field(default_factory=list)


class AlphaVantageProducer(BaseProducer):
    """Polls Alpha Vantage NEWS_SENTIMENT API and publishes events to Kafka."""

    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self._seen_urls: set[str] = set()
        self._client = httpx.Client(timeout=30.0)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
        reraise=True,
    )
    def _fetch_news(self, ticker: str) -> AVNewsResponse:
        response = self._client.get(
            self.BASE_URL,
            params={
                "function": "NEWS_SENTIMENT",
                "tickers": ticker,
                "apikey": self.settings.alpha_vantage_api_key,
            },
        )
        response.raise_for_status()
        return AVNewsResponse.model_validate(response.json())

    def _parse_timestamp(self, time_published: str) -> datetime:
        try:
            return datetime.strptime(time_published, "%Y%m%dT%H%M%S").replace(tzinfo=UTC)
        except ValueError:
            logger.warning("timestamp_parse_failed", value=time_published)
            return datetime.now(UTC)

    def _to_event(self, item: AVFeedItem) -> RawFinancialNewsEvent:
        tickers = [ts.ticker for ts in item.ticker_sentiment] or []
        return RawFinancialNewsEvent(
            timestamp=self._parse_timestamp(item.time_published),
            source=SourceType.ALPHA_VANTAGE,
            source_url=item.url,
            title=item.title,
            content=item.summary,
            tickers=tickers,
            raw_metadata={
                "overall_sentiment_score": item.overall_sentiment_score,
                "overall_sentiment_label": item.overall_sentiment_label,
                "source_domain": item.source,
                "authors": item.authors,
            },
        )

    def _interruptible_sleep(self, seconds: int) -> None:
        for _ in range(seconds):
            if not self._running:
                return
            time.sleep(1)

    def produce(self) -> None:
        tickers = self.settings.watchlist_tickers_list
        logger.info(
            "alpha_vantage_producer_started",
            poll_interval=self.settings.alpha_vantage_poll_interval,
            tickers=tickers,
        )

        while self._running:
            for ticker in tickers:
                if not self._running:
                    return

                try:
                    response = self._fetch_news(ticker)
                except (httpx.HTTPStatusError, httpx.TransportError) as e:
                    logger.warning("fetch_failed", ticker=ticker, error=str(e))
                    continue
                except ValidationError as e:
                    logger.warning("response_validation_failed", ticker=ticker, error=str(e))
                    continue

                for item in response.feed:
                    if item.url in self._seen_urls:
                        continue

                    try:
                        event = self._to_event(item)
                    except ValidationError as e:
                        logger.warning("event_validation_failed", url=item.url, error=str(e))
                        continue

                    primary_ticker = event.tickers[0] if event.tickers else ticker
                    self._publish(self.TOPIC, primary_ticker, event)
                    self._seen_urls.add(item.url)

                    logger.info(
                        "event_published",
                        event_id=str(event.event_id),
                        source="alpha_vantage",
                        tickers=event.tickers,
                        title=event.title[:60],
                    )

            # Cap dedup set size
            if len(self._seen_urls) > MAX_SEEN_URLS:
                self._seen_urls.clear()
                logger.info("seen_urls_cleared")

            self._interruptible_sleep(self.settings.alpha_vantage_poll_interval)
