"""Ticker symbol to company name enrichment."""

TICKER_TO_COMPANY: dict[str, str] = {
    # Technology
    "AAPL": "Apple Inc.",
    "MSFT": "Microsoft Corporation",
    "GOOGL": "Alphabet Inc.",
    "GOOG": "Alphabet Inc.",
    "AMZN": "Amazon.com Inc.",
    "META": "Meta Platforms Inc.",
    "NVDA": "NVIDIA Corporation",
    "TSLA": "Tesla Inc.",
    "TSM": "Taiwan Semiconductor Manufacturing",
    "AVGO": "Broadcom Inc.",
    "ORCL": "Oracle Corporation",
    "ADBE": "Adobe Inc.",
    "CRM": "Salesforce Inc.",
    "NFLX": "Netflix Inc.",
    "AMD": "Advanced Micro Devices Inc.",
    "INTC": "Intel Corporation",
    "CSCO": "Cisco Systems Inc.",
    "IBM": "International Business Machines",
    "QCOM": "Qualcomm Inc.",
    "UBER": "Uber Technologies Inc.",
    # Finance
    "JPM": "JPMorgan Chase & Co.",
    "V": "Visa Inc.",
    "MA": "Mastercard Inc.",
    "BAC": "Bank of America Corporation",
    "WFC": "Wells Fargo & Company",
    "GS": "Goldman Sachs Group Inc.",
    "MS": "Morgan Stanley",
    "C": "Citigroup Inc.",
    "AXP": "American Express Company",
    "BLK": "BlackRock Inc.",
    # Healthcare
    "UNH": "UnitedHealth Group Inc.",
    "JNJ": "Johnson & Johnson",
    "LLY": "Eli Lilly and Company",
    "PFE": "Pfizer Inc.",
    "ABBV": "AbbVie Inc.",
    "MRK": "Merck & Co. Inc.",
    "TMO": "Thermo Fisher Scientific Inc.",
    # Consumer
    "WMT": "Walmart Inc.",
    "PG": "Procter & Gamble Company",
    "KO": "Coca-Cola Company",
    "PEP": "PepsiCo Inc.",
    "COST": "Costco Wholesale Corporation",
    "HD": "Home Depot Inc.",
    "DIS": "Walt Disney Company",
    "MCD": "McDonald's Corporation",
    "NKE": "Nike Inc.",
    "SBUX": "Starbucks Corporation",
    # Energy & Industrial
    "XOM": "Exxon Mobil Corporation",
    "CVX": "Chevron Corporation",
    "BA": "Boeing Company",
    "CAT": "Caterpillar Inc.",
    "GE": "General Electric Company",
}


def ticker_to_company(ticker: str) -> str:
    """Resolve ticker symbol to company name. Returns ticker if unknown."""
    return TICKER_TO_COMPANY.get(ticker.upper(), ticker)
