"""Add sparse mutable lifecycle metadata for registry targets."""

import sqlalchemy as sa
from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "registry_lifecycle",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "registry_record_id",
            sa.String(36),
            sa.ForeignKey("registry_records.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("feature_name", sa.String(128), nullable=True),
        sa.Column("owners", sa.JSON(), nullable=False),
        sa.Column("tags", sa.JSON(), nullable=False),
        sa.Column("documentation_links", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("deprecated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deprecation_message", sa.Text(), nullable=True),
        sa.Column("replacement", sa.JSON(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("registry_record_id", "feature_name"),
    )
    op.create_index(
        "ix_registry_lifecycle_registry_record_id",
        "registry_lifecycle",
        ["registry_record_id"],
    )
    op.create_index(
        "uq_registry_lifecycle_object",
        "registry_lifecycle",
        ["registry_record_id"],
        unique=True,
        sqlite_where=sa.text("feature_name IS NULL"),
        postgresql_where=sa.text("feature_name IS NULL"),
    )


def downgrade() -> None:
    op.drop_table("registry_lifecycle")
