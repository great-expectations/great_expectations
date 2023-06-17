import logging

from .dataset import Dataset
from .pandas_dataset import MetaPandasDataset, PandasDataset

logger = logging.getLogger(__name__)

try:
    from .sparkdf_dataset import MetaSparkDFDataset, SparkDFDataset
except ImportError:
    logger.debug(
        "Unable to load spark dataset; install optional spark dependency for support."
    )
