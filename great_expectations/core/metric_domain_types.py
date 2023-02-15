import enum
import logging

from great_expectations.core._docs_decorators import public_api

logger = logging.getLogger(__name__)


@public_api
class MetricDomainTypes(enum.Enum):
    """Enum type, whose members signify the data "Domain", on which a metric can be computed.

    A wide variety of "Domain" types can be defined with applicable metrics associated with their respective "Domain"
    types.  The "Domain" types currently in use (`TABLE`, `COLUMN`, `COLUMN_PAIR`, and `MULTICOLUMN`) are declared here.
    """

    TABLE = "table"
    COLUMN = "column"
    COLUMN_PAIR = "column_pair"
    MULTICOLUMN = "multicolumn"
