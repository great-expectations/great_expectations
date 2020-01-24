import logging

from .batch_kwargs_generator import BatchKwargsGenerator
from great_expectations.datasource.types import BatchKwargs

logger = logging.getLogger(__name__)


class PassthroughGenerator(BatchKwargsGenerator):
    """PassthroughGenerator does not yield or describe data assets; it simply provides a namespace for
    manually-constructed BatchKwargs."""

    def __init__(self, name="default", datasource=None):
        super(PassthroughGenerator, self).__init__(name, datasource=datasource)

    def _get_iterator(self, generator_asset, **kwargs):
        return iter([kwargs])

    def get_available_data_asset_names(self):
        logger.debug(
            "PassthroughGenerator cannot identify data_asset_names, but can accept any object as a valid data_asset."
        )
        return {"names": []}

    def get_available_partition_ids(self, generator_asset):
        logger.warning(
            "PassthroughGenerator cannot identify partition_ids, but can accept partition_id together with a valid GE "
            "object."
        )
        return []

    def build_batch_kwargs_from_partition_id(self, generator_asset, partition_id=None, batch_kwargs=None, **kwargs):
        kwargs.update(batch_kwargs)
        batch_kwargs = BatchKwargs(kwargs)
        if partition_id is not None:
            batch_kwargs["partition_id"] = partition_id
        return batch_kwargs
