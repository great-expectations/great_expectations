import os
import glob
import re
import datetime
import logging
import warnings
from copy import deepcopy

from six import string_types

from great_expectations.datasource.generator.batch_kwargs_generator import BatchKwargsGenerator
from great_expectations.datasource.types import PathBatchKwargs
from great_expectations.exceptions import BatchKwargsError, InvalidBatchKwargsError

logger = logging.getLogger(__name__)


class ManualGenerator(BatchKwargsGenerator):
    """ManualGenerator returns manually-configured batch_kwargs for named data assets. It provides a convenient way to
    capture complete batch definitions without requiring the configuration of a more fully-featured generator.

    A fully configured ManualGenerator in yml might look like the following::

        my_datasource:
          class_name: PandasDatasource
          generators:
            my_generator:
              class_name: ManualGenerator
              assets:
                asset1:
                  - partition_id: 1
                    path: /data/file_1.csv
                    reader_options:
                      sep: ;
                  - partition_id: 2
                    path: /data/file_2.csv
                    reader_options:
                      header: 0
                logs:
                  path: data/log.csv
    """
    recognized_batch_parameters = {"name"}

    def __init__(self, name="default",
                 datasource=None,
                 assets=None):
        logger.debug("Constructing ManualGenerator {!r}".format(name))
        super(ManualGenerator, self).__init__(name, datasource=datasource)

        if assets is None:
            assets = {}

        self._assets = assets

    @property
    def assets(self):
        return self._assets

    def get_available_data_asset_names(self):
        return {"names": list(self.assets.keys())}

    def _get_generator_asset_config(self, generator_asset):
        if generator_asset is None:
            return

        elif generator_asset in self.assets:
            return self.assets[generator_asset]

        raise InvalidBatchKwargsError("No asset definition for requested asset %s" % generator_asset)

    def _get_iterator(self, generator_asset, **kwargs):
        asset_definition = deepcopy(self._get_generator_asset_config(generator_asset))
        if isinstance(asset_definition, list):
            for batch_definition in asset_definition:
                batch_definition.update(kwargs)
            return iter(asset_definition)
        else:
            asset_definition.update(kwargs)
            return iter([asset_definition])

    def get_available_partition_ids(self, generator_asset):
        partition_ids = []
        asset_definition = self._get_generator_asset_config(generator_asset=generator_asset)
        if isinstance(asset_definition, list):
            for batch_definition in asset_definition:
                try:
                    partition_ids.append(batch_definition['partition_id'])
                except KeyError:
                    pass
        elif isinstance(asset_definition, dict):
            try:
                partition_ids.append(asset_definition['partition_id'])
            except KeyError:
                pass
        return partition_ids

    def _build_batch_kwargs(self, batch_parameters):
        """Build batch kwargs from a partition id."""
        batch_kwargs = None
        if "partition_id" in batch_parameters:
            asset_definition = self._get_generator_asset_config(generator_asset=batch_parameters.get("name"))
            if isinstance(asset_definition, list):
                for batch_definition in asset_definition:
                    try:
                        if batch_definition['partition_id'] == batch_parameters.get("partition_id"):
                            batch_kwargs = deepcopy(batch_definition)
                    except KeyError:
                        pass
            elif isinstance(asset_definition, dict):
                try:
                    if asset_definition['partition_id'] == batch_parameters.get("partition_id"):
                        batch_kwargs = deepcopy(asset_definition)
                except KeyError:
                    pass
        else:
            batch_kwargs = next(self._get_iterator(batch_parameters.get("name")))

        if batch_kwargs is not None:
            return batch_kwargs
        else:
            raise BatchKwargsError("Unable to find batch_kwargs for given batch_parameters", batch_parameters)

