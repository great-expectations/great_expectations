from __future__ import annotations

import enum
import logging

logger = logging.getLogger(__name__)


class MetricDomainTypes(enum.Enum):
    """Enum type, whose members signify the data "Domain", on which a metric can be computed.

    A wide variety of "Domain" types can be defined with applicable metrics associated with their respective "Domain"
    types.  The "Domain" types currently in use (`TABLE`, `COLUMN`, `COLUMN_PAIR`, and `MULTICOLUMN`) are declared here.
    """  # noqa: E501

    TABLE = "table"
    COLUMN = "column"
    COLUMN_PAIR = "column_pair"
    MULTICOLUMN = "multicolumn"
