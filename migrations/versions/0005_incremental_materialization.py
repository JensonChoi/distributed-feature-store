"""Add per-feature-view incremental materialization state."""

import sqlalchemy as sa
from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "materialization_states",
        sa.Column("feature_view", sa.String(160), primary_key=True),
        sa.Column("watermark", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_freshness_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("active_job_id", sa.String(36), nullable=True),
        sa.Column("last_successful_job_id", sa.String(36), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_materialization_states_active_job_id",
        "materialization_states",
        ["active_job_id"],
    )


def downgrade() -> None:
    op.drop_table("materialization_states")
