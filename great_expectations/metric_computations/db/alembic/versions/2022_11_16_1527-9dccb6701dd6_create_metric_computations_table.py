"""Create "metric_computations" table.

Revision ID: 9dccb6701dd6
Revises:
Create Date: 2022-11-16 15:27:13.202261

"""
import logging

logger = logging.getLogger(__name__)

try:
    import sqlalchemy as sa
except ImportError:
    logger.debug("No SqlAlchemy module available.")
    sa = None

try:
    from alembic import op
except ImportError:
    logger.debug("No alembic module available.")
    op = None

# revision identifiers, used by Alembic.
revision = "9dccb6701dd6"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "metric_computations",
        sa.Column(
            "id",
            sa.Integer(),
            nullable=False,
            primary_key=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
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
            "archived_at",
            sa.DateTime(),
            nullable=True,
        ),
        sa.Column(
            "archived",
            sa.Boolean(),
            nullable=False,
            default=False,
        ),
        # TODO: <Alex>ALEX</Alex>
        # sa.Column(
        #     "status",
        #     sa.Integer(),
        #     nullable=False,
        #     default=0,
        # ),
        # TODO: <Alex>ALEX</Alex>
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
            "metric_domain_kwargs_uuid",
            sa.UnicodeText(),
            nullable=False,
        ),
        sa.Column(
            "metric_value_kwargs_uuid",
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
        op.f("idx_metric_computations_batch_uuid"),
        table_name="metric_computations",
        columns=[
            "batch_uuid",
        ],
        unique=False,
    )
    op.create_index(
        op.f("idx_metric_computations_metric_name"),
        table_name="metric_computations",
        columns=[
            "metric_name",
        ],
        unique=False,
    )
    op.create_index(
        op.f("idx_metric_computations_metric_domain_kwargs_uuid"),
        table_name="metric_computations",
        columns=[
            "metric_domain_kwargs_uuid",
        ],
        unique=False,
    )
    op.create_index(
        op.f("idx_metric_computations_metric_value_kwargs_uuid"),
        table_name="metric_computations",
        columns=[
            "metric_value_kwargs_uuid",
        ],
        unique=False,
    )
    op.create_index(
        op.f("idx_metric_computations_batch_uuid_metric_configuration_uuid"),
        table_name="metric_computations",
        columns=[
            "batch_uuid",
            "metric_name",
            "metric_domain_kwargs_uuid",
            "metric_value_kwargs_uuid",
        ],
        unique=False,
    )


def downgrade() -> None:
    op.drop_table("metric_computations")
