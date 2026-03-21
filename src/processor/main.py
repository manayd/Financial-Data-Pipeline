"""Entry point for the Faust stream processor.

Run with: python -m src.processor.main worker -l info
"""

from src.processor import agents  # noqa: F401
from src.processor.app import app


def main() -> None:
    app.main()


if __name__ == "__main__":
    main()
