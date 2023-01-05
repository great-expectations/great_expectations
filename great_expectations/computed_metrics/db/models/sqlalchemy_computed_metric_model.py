# TODO: <Alex>ALEX</Alex>
# import uuid
# TODO: <Alex>ALEX</Alex>

import logging

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
    SQLAlchemy model for each row in "computed_metric" table.
    """

    id = sa.Column(sa.Integer(), nullable=False, primary_key=True)
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
    batch_uuid = sa.Column(
        sa.UnicodeText(),
        nullable=False,
    )
    metric_name = sa.Column(
        sa.Unicode(128),
        nullable=False,
    )
    metric_domain_kwargs_uuid = sa.Column(
        sa.UnicodeText(),
        nullable=False,
    )
    metric_value_kwargs_uuid = sa.Column(
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
