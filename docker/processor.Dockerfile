FROM python:3.12-slim-bookworm

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY src/ src/

CMD ["faust", "-A", "src.processor", "worker", "-l", "info"]
