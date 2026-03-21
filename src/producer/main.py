import signal
import threading

import structlog

from src.config import Settings
from src.producer.base import BaseProducer

logger = structlog.get_logger()


def _create_producers(settings: Settings) -> list[BaseProducer]:
    """Instantiate producers based on settings.producer_type.

    Supported types:
      - "fake"           -> FakeProducer only
      - "alpha_vantage"  -> AlphaVantageProducer only
      - "sec_edgar"      -> SECEdgarProducer only
      - "real"           -> AlphaVantageProducer + SECEdgarProducer
      - "all"            -> all three producers
    """
    producers: list[BaseProducer] = []
    producer_type = settings.producer_type.lower()

    if producer_type in ("fake", "all"):
        from src.producer.fake_producer import FakeProducer

        producers.append(FakeProducer(settings))

    if producer_type in ("alpha_vantage", "real", "all"):
        from src.producer.alpha_vantage_producer import AlphaVantageProducer

        if not settings.alpha_vantage_api_key:
            raise ValueError("ALPHA_VANTAGE_API_KEY is required for alpha_vantage producer")
        producers.append(AlphaVantageProducer(settings))

    if producer_type in ("sec_edgar", "real", "all"):
        from src.producer.sec_edgar_producer import SECEdgarProducer

        producers.append(SECEdgarProducer(settings))

    if not producers:
        raise ValueError(f"Unknown producer type: {settings.producer_type}")

    return producers


def main() -> None:
    settings = Settings()
    producers = _create_producers(settings)

    def shutdown_all(signum: int, frame: object) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        for p in producers:
            p._running = False

    signal.signal(signal.SIGTERM, shutdown_all)
    signal.signal(signal.SIGINT, shutdown_all)

    if len(producers) == 1:
        producers[0].run()
    else:
        threads: list[threading.Thread] = []
        for p in producers:
            t = threading.Thread(target=p.run, name=p.__class__.__name__, daemon=True)
            t.start()
            threads.append(t)
            logger.info("producer_thread_started", producer=p.__class__.__name__)

        try:
            for t in threads:
                t.join()
        except KeyboardInterrupt:
            logger.info("main_interrupted_shutting_down")
            shutdown_all(signal.SIGINT, None)
            for t in threads:
                t.join(timeout=15)


if __name__ == "__main__":
    main()
