import logging

from .batch_generator import BatchGenerator
from great_expectations.datasource.types import InMemoryBatchKwargs
from great_expectations.exceptions import BatchKwargsError

logger = logging.getLogger(__name__)


class InMemoryGenerator(BatchGenerator):
    """A basic generator that simply captures an existing object."""

    def __init__(self, name="default", datasource=None):
        super(InMemoryGenerator, self).__init__(name, datasource=datasource)

    def _get_iterator(self, generator_asset, **kwargs):
        return iter([])

    def get_available_data_asset_names(self):
        logger.warning(
            "InMemoryGenerator cannot identify data_asset_names, but can accept any object as a valid data_asset."
        )
        return []

    def get_available_partition_ids(self, generator_asset):
        logger.warning(
            "InMemoryGenerator cannot identify partition_ids, but can accept partition_id together with a valid GE "
            "object."
        )
        return []

    def build_batch_kwargs_from_partition_id(self, generator_asset, partition_id=None, batch_kwargs=None, **kwargs):
        kwargs.update(batch_kwargs)
        if "dataset" not in kwargs:
            raise BatchKwargsError(
                "InMemoryGenerator cannot build batch_kwargs without an explicit dataset, but it can provide"
                "a namespace for any data asset.",
                kwargs
            )

        batch_kwargs = InMemoryBatchKwargs(kwargs)
        if partition_id is not None:
            batch_kwargs["partition_id"] = partition_id
        return batch_kwargs
