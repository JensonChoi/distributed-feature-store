"""Add the durable streaming event ledger."""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "stream_events",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("feature_view", sa.String(160), nullable=False),
        sa.Column("event_id", sa.String(256), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("source_topic", sa.String(256), nullable=True),
        sa.Column("source_partition", sa.Integer(), nullable=True),
        sa.Column("source_offset", sa.BigInteger(), nullable=True),
        sa.Column("job_id", sa.String(36), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("staged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("feature_view", "event_id"),
    )
    op.create_index("ix_stream_events_feature_view", "stream_events", ["feature_view"])
    op.create_index("ix_stream_events_state", "stream_events", ["state"])
    op.create_index("ix_stream_events_job_id", "stream_events", ["job_id"])


def downgrade() -> None:
    op.drop_table("stream_events")
