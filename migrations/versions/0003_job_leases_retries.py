"""Add job leases, heartbeats, and bounded retry metadata."""

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("jobs") as batch:
        batch.add_column(
            sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0")
        )
        batch.add_column(
            sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3")
        )
        batch.add_column(sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("worker_id", sa.String(128), nullable=True))
        batch.add_column(sa.Column("lease_token", sa.String(36), nullable=True))
        batch.add_column(sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("failure_kind", sa.String(32), nullable=True))
        batch.create_index("ix_jobs_next_attempt_at", ["next_attempt_at"])
        batch.create_index("ix_jobs_worker_id", ["worker_id"])
        batch.create_index("ix_jobs_lease_expires_at", ["lease_expires_at"])


def downgrade() -> None:
    with op.batch_alter_table("jobs") as batch:
        batch.drop_index("ix_jobs_lease_expires_at")
        batch.drop_index("ix_jobs_worker_id")
        batch.drop_index("ix_jobs_next_attempt_at")
        batch.drop_column("failure_kind")
        batch.drop_column("last_heartbeat_at")
        batch.drop_column("lease_expires_at")
        batch.drop_column("lease_token")
        batch.drop_column("worker_id")
        batch.drop_column("next_attempt_at")
        batch.drop_column("max_attempts")
        batch.drop_column("attempt_count")
