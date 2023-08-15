import enum
import logging

from great_expectations.core._docs_decorators import public_api

logger = logging.getLogger(__name__)


@public_api
class MetricDomainTypes(enum.Enum):
    """Enum type, whose members signify the data "Domain", on which a metric can be computed.

    A wide variety of "Domain" types can be defined with applicable metrics associated with their respective "Domain"
    types.  The "Domain" types currently in use (`BATCH`, `COLUMN`, `COLUMN_PAIR`, `MULTICOLUMN`, and `TABLE`) are
    declared here.

    The `TABLE` Domain type has been deprecated in favor of the `BATCH` Domain type.
    """

    BATCH = "batch"
    COLUMN = "column"
    COLUMN_PAIR = "column_pair"
    MULTICOLUMN = "multicolumn"
    TABLE = "table"
