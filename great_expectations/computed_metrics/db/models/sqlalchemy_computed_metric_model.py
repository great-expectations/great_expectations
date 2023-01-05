import logging

from marshmallow import Schema

from great_expectations.computed_metrics.computed_metric import computedMetricSchema

logger = logging.getLogger(__name__)

try:
    import sqlalchemy as sa
except ImportError:
    logger.debug("No SqlAlchemy module available.")
    sa = None

from great_expectations.computed_metrics.db.models.base import (
    AccountMixin,
    ArchiveMixin,
)
from great_expectations.computed_metrics.db.models.base import (
    Base as SqlAlchemyModelBase,
)
from great_expectations.computed_metrics.db.models.base import (
    PrimaryKeyMixin,
    SoftDeleteMixin,
    TimestampsMixin,
)

# TODO: <Alex>ALEX</Alex>
# from sqlalchemy.dialects.postgresql import UUID, VARCHAR
# from sqlalchemy.orm import relationship
# TODO: <Alex>ALEX</Alex>


class ComputedMetric(
    SqlAlchemyModelBase,
    PrimaryKeyMixin,
    TimestampsMixin,
    SoftDeleteMixin,
    ArchiveMixin,
    AccountMixin,
):
    """
    SQLAlchemy model for each row in "computed_metrics" table.
    """

    datasource_name = sa.Column(
        sa.Unicode(128),
        nullable=False,
    )
    data_asset_name = sa.Column(
        sa.Unicode(128),
        nullable=False,
    )
    batch_name = sa.Column(
        sa.UnicodeText(),
        nullable=False,
    )
    batch_id = sa.Column(
        sa.UnicodeText(),
        nullable=False,
    )
    metric_name = sa.Column(
        sa.Unicode(128),
        nullable=False,
    )
    metric_domain_kwargs_id = sa.Column(
        sa.UnicodeText(),
        nullable=False,
    )
    metric_value_kwargs_id = sa.Column(
        sa.UnicodeText(),
        nullable=False,
    )
    value = sa.Column(
        sa.JSON(),
        nullable=True,
    )
    details = sa.Column(
        sa.JSON(),
        nullable=True,
        default={
            "exception_type": "",
            "exception_message": "",
        },
    )

    @classmethod
    def get_marshmallow_schema_instance(cls) -> Schema:
        # noinspection PyTypeChecker
        return computedMetricSchema

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "deleted_at": self.deleted_at,
            "deleted": self.deleted,
            "archived_at": self.archived_at,
            "archived": self.archived,
            "data_context_uuid": self.data_context_uuid,
            "datasource_name": self.datasource_name,
            "data_asset_name": self.data_asset_name,
            "batch_name": self.batch_name,
            "batch_id": self.batch_name,
            "metric_name": self.metric_name,
            "metric_domain_kwargs_id": self.metric_domain_kwargs_id,
            "metric_value_kwargs_id": self.metric_value_kwargs_id,
            "value": self.value,
            "details": self.details,
        }
