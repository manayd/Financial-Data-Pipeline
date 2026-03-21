"""Entry point for the LLM analyzer service."""

from src.config import Settings
from src.llm.analyzer import LLMAnalyzer


def main() -> None:
    settings = Settings()
    analyzer = LLMAnalyzer(settings)
    analyzer.run()


if __name__ == "__main__":
    main()
