from src.processor.enrichment import TICKER_TO_COMPANY, ticker_to_company


class TestTickerToCompany:
    def test_known_ticker_returns_company(self) -> None:
        assert ticker_to_company("AAPL") == "Apple Inc."
        assert ticker_to_company("MSFT") == "Microsoft Corporation"

    def test_unknown_ticker_returns_ticker(self) -> None:
        assert ticker_to_company("ZZZZZ") == "ZZZZZ"

    def test_case_insensitive_lookup(self) -> None:
        assert ticker_to_company("aapl") == "Apple Inc."
        assert ticker_to_company("Msft") == "Microsoft Corporation"

    def test_default_watchlist_covered(self) -> None:
        watchlist = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
        for ticker in watchlist:
            result = ticker_to_company(ticker)
            assert result != ticker, f"{ticker} should map to a company name"
            assert result in TICKER_TO_COMPANY.values()
