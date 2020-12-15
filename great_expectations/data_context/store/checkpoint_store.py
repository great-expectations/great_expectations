import logging

from great_expectations.data_context.store import ConfigurationStore
from great_expectations.data_context.types.base import CheckpointConfig

logger = logging.getLogger(__name__)


class CheckpointStore(ConfigurationStore):
    """
    A CheckpointStore manages Checkpoints for the DataContext.
    """

    def __init__(
        self,
        store_name: str,
        store_backend: dict = None,
        overwrite_existing: bool = False,
        runtime_environment: dict = None,
    ):
        super().__init__(
            configuration_class=CheckpointConfig,
            store_name=store_name,
            store_backend=store_backend,
            overwrite_existing=overwrite_existing,
            runtime_environment=runtime_environment,
        )
