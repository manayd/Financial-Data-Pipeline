"""Create articles and ticker_aggregations tables.

Revision ID: 0001
Revises:
Create Date: 2026-03-19
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "articles",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.String(length=50), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("tickers", postgresql.ARRAY(sa.String()), nullable=False),
        sa.Column("companies", postgresql.ARRAY(sa.String()), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("raw_metadata", postgresql.JSONB(), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("event_id"),
        sa.UniqueConstraint("content_hash"),
    )
    op.create_index("ix_articles_timestamp", "articles", ["timestamp"])
    op.create_index("ix_articles_tickers", "articles", ["tickers"], postgresql_using="gin")

    op.create_table(
        "ticker_aggregations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("ticker", sa.String(length=10), nullable=False),
        sa.Column("company_name", sa.String(length=200), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("article_count", sa.Integer(), nullable=False),
        sa.Column("avg_sentiment_score", sa.Float(), nullable=True),
        sa.Column("dominant_sentiment", sa.String(length=20), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ticker_agg_ticker_window", "ticker_aggregations", ["ticker", "window_end"]
    )


def downgrade() -> None:
    op.drop_index("ix_ticker_agg_ticker_window", table_name="ticker_aggregations")
    op.drop_table("ticker_aggregations")
    op.drop_index("ix_articles_tickers", table_name="articles")
    op.drop_index("ix_articles_timestamp", table_name="articles")
    op.drop_table("articles")
