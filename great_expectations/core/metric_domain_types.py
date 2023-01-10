import enum
import logging

logger = logging.getLogger(__name__)


class MetricDomainTypes(enum.Enum):
    TABLE = "table"
    COLUMN = "column"
    COLUMN_PAIR = "column_pair"
    MULTICOLUMN = "multicolumn"
