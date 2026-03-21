"""Add llm_analyses table.

Revision ID: 0002
Revises: 0001
Create Date: 2026-03-20
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "llm_analyses",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("article_event_id", sa.Uuid(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("sentiment", sa.String(length=20), nullable=False),
        sa.Column("sentiment_confidence", sa.Float(), nullable=False),
        sa.Column("entities", postgresql.JSONB(), nullable=False),
        sa.Column("key_topics", postgresql.ARRAY(sa.String()), nullable=False),
        sa.Column("analyzed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("model_id", sa.String(length=100), nullable=False),
        sa.Column("analysis_version", sa.String(length=20), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["article_event_id"], ["articles.event_id"]),
    )
    op.create_index("ix_llm_analyses_article", "llm_analyses", ["article_event_id"])


def downgrade() -> None:
    op.drop_index("ix_llm_analyses_article", table_name="llm_analyses")
    op.drop_table("llm_analyses")
