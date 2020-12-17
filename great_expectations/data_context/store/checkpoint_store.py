import logging

from great_expectations.data_context.store import ConfigurationStore
from great_expectations.data_context.types.base import CheckpointConfig

logger = logging.getLogger(__name__)


class CheckpointStore(ConfigurationStore):
    """
    A CheckpointStore manages Checkpoints for the DataContext.
    """

    _configuration_class = CheckpointConfig
