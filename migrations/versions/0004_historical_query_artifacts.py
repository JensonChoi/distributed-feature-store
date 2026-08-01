"""Add historical-query artifact and result metadata."""

import sqlalchemy as sa
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("jobs") as batch:
        batch.add_column(sa.Column("artifact_uri", sa.Text(), nullable=True))
        batch.add_column(sa.Column("result_uri", sa.Text(), nullable=True))
        batch.add_column(sa.Column("result_metadata", sa.JSON(), nullable=True))
        batch.add_column(
            sa.Column("artifact_expires_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch.add_column(
            sa.Column("artifacts_cleaned_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch.create_index("ix_jobs_artifact_expires_at", ["artifact_expires_at"])


def downgrade() -> None:
    with op.batch_alter_table("jobs") as batch:
        batch.drop_index("ix_jobs_artifact_expires_at")
        batch.drop_column("artifacts_cleaned_at")
        batch.drop_column("artifact_expires_at")
        batch.drop_column("result_metadata")
        batch.drop_column("result_uri")
        batch.drop_column("artifact_uri")
