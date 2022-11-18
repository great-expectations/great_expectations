"""Create "batch_metric_computations" table.

Revision ID: 9dccb6701dd6
Revises:
Create Date: 2022-11-16 15:27:13.202261

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9dccb6701dd6"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "batch_metric_computations",
        sa.Column(
            "id",
            sa.Integer(),
            nullable=False,
            primary_key=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "deleted_at",
            sa.DateTime(),
            nullable=True,
        ),
        sa.Column(
            "deleted",
            sa.Boolean(),
            nullable=False,
            default=False,
        ),
        sa.Column(
            "status",
            sa.Integer(),
            nullable=False,
            default=0,
        ),
        sa.Column(
            "data_context_uuid",
            sa.UnicodeText(),
            nullable=True,
        ),
        sa.Column(
            "datasource_name",
            sa.Unicode(128),
            nullable=False,
        ),
        sa.Column(
            "data_asset_name",
            sa.Unicode(128),
            nullable=False,
        ),
        sa.Column(
            "batch_name",
            sa.UnicodeText(),
            nullable=False,
        ),
        sa.Column(
            "batch_uuid",
            sa.UnicodeText(),
            nullable=False,
        ),
        sa.Column(
            "metric_name",
            sa.Unicode(128),
            nullable=False,
        ),
        sa.Column(
            "metric_configuration_uuid",
            sa.UnicodeText(),
            nullable=False,
        ),
        sa.Column(
            "value",
            sa.JSON(),
            nullable=True,
        ),
        sa.Column(
            "details",
            sa.JSON(),
            nullable=True,
            default={
                "exception_type": "",
                "exception_message": "",
            },
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("idx_batch_metric_computations_batch_uuid"),
        table_name="batch_metric_computations",
        columns=[
            "batch_uuid",
        ],
        unique=False,
    )
    op.create_index(
        op.f("idx_batch_metric_computations_metric_configuration_uuid"),
        table_name="batch_metric_computations",
        columns=[
            "metric_configuration_uuid",
        ],
        unique=False,
    )
    op.create_index(
        op.f("idx_batch_metric_computations_batch_uuid_metric_configuration_uuid"),
        table_name="batch_metric_computations",
        columns=[
            "batch_uuid",
            "metric_configuration_uuid",
        ],
        unique=False,
    )


def downgrade() -> None:
    op.drop_table("batch_metric_computations")
