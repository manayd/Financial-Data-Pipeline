import random
import time
from datetime import UTC, datetime

import structlog
from faker import Faker

from src.models.events import RawFinancialNewsEvent, SourceType
from src.producer.base import BaseProducer

logger = structlog.get_logger()
fake = Faker()

TICKERS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "TSLA",
    "META",
    "NVDA",
    "JPM",
    "V",
    "JNJ",
    "WMT",
    "PG",
    "UNH",
    "HD",
    "BAC",
    "MA",
    "DIS",
    "ADBE",
    "CRM",
    "NFLX",
]

HEADLINES = [
    "{company} Reports Record Q{quarter} Earnings, Beating Analyst Expectations",
    "{company} Announces Major Acquisition of {target} for ${amount}B",
    "{company} Shares Surge After New Product Launch",
    "SEC Investigates {company} Over Accounting Irregularities",
    "{company} CEO Steps Down Amid Board Restructuring",
    "Analysts Upgrade {company} to 'Buy' After Strong Revenue Growth",
    "{company} Expands Operations into European Markets",
    "{company} Faces Lawsuit Over Patent Infringement by {target}",
    "{company} Announces ${amount}B Share Buyback Program",
    "{company} Partners with {target} on AI Initiative",
    "Breaking: {company} Stock Drops on Disappointing Guidance",
    "{company} Raises Dividend by {pct}%, Signals Confidence",
    "{company} Misses Revenue Targets, Warns of Slowdown",
    "{company} Wins Major Government Contract Worth ${amount}B",
    "Insider Selling: {company} Executive Unloads ${amount}M in Shares",
]


def _generate_headline() -> str:
    template = random.choice(HEADLINES)
    return template.format(
        company=random.choice(TICKERS),
        target=random.choice(TICKERS),
        quarter=random.randint(1, 4),
        amount=round(random.uniform(0.5, 50.0), 1),
        pct=random.randint(3, 25),
    )


def _generate_content(headline: str) -> str:
    paragraphs = [
        headline + ".",
        fake.paragraph(nb_sentences=4),
        fake.paragraph(nb_sentences=3),
        fake.paragraph(nb_sentences=5),
    ]
    return "\n\n".join(paragraphs)


class FakeProducer(BaseProducer):
    """Produces synthetic financial news events for local development."""

    def produce(self) -> None:
        logger.info("fake_producer_started", interval=self.settings.fake_producer_interval)

        while self._running:
            tickers = random.sample(TICKERS, k=random.randint(1, 3))
            headline = _generate_headline()
            content = _generate_content(headline)

            event = RawFinancialNewsEvent(
                timestamp=datetime.now(UTC),
                source=SourceType.SYNTHETIC,
                title=headline,
                content=content,
                tickers=tickers,
            )

            primary_ticker = tickers[0]
            self._publish(self.TOPIC, primary_ticker, event)

            logger.info(
                "event_published",
                event_id=str(event.event_id),
                tickers=tickers,
                title=headline[:60],
            )

            time.sleep(self.settings.fake_producer_interval)
