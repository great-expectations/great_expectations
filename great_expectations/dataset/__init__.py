import logging

from .dataset import Dataset
from .pandas_dataset import MetaPandasDataset, PandasDataset

LOGGER = logging.getLogger(__name__)

try:
    from .sqlalchemy_dataset import MetaSqlAlchemyDataset, SqlAlchemyDataset
except ImportError:
    LOGGER.debug(
        "Unable to load sqlalchemy dataset; install optional sqlalchemy dependency for support."
    )

try:
    from .sparkdf_dataset import MetaSparkDFDataset, SparkDFDataset
except ImportError:
    LOGGER.debug(
        "Unable to load spark dataset; install optional spark dependency for support."
    )
