import datetime
import glob
import logging
import os
# TODO: <Alex>Do we need these two imports?</Alex>
import re
import warnings
from typing import List

from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
# TODO: <Alex>Do we need these two imports?</Alex>
from great_expectations.exceptions import BatchKwargsError
from great_expectations.execution_environment.types import PathBatchKwargs

logger = logging.getLogger(__name__)


class FilesDataConnector(DataConnector):
    def __init__(
        self,
        name,
        execution_environment,
        partitioners=None,
        default_partitioner=None,
        assets=None,
        batch_definition_defaults=None,
        **kwargs
    ):
        logger.debug("Constructing FilesDataConnector {!r}".format(name))
        super().__init__(
            name=name,
            execution_environment=execution_environment,
            partitioners=partitioners,
            default_partitioner=default_partitioner,
            assets=assets,
            batch_definition_defaults=batch_definition_defaults,
            **kwargs
        )

        # TODO: <Alex>Do we need error handling here?</Alex>
        self._base_directory = self.config_params["base_directory"]

    @property
    def base_directory(self):
        # If base directory is a relative path, interpret it as relative to the data context's
        # context root directory (parent directory of great_expectation dir)
        if (
            os.path.isabs(self._base_directory)
            or self._execution_environment.data_context is None
        ):
            return self._base_directory
        else:
            return os.path.join(
                self._execution_environment.data_context.root_directory,
                self._base_directory,
            )

    # TODO: <Alex>Also read files the glob way</Alex>
    def get_available_data_asset_names(self):
        # should it be a list of just the keys?
        # TODO: <Alex>This needs to check for assets to have any files and include only if that is true.</Alex>
        return self.assets.keys()

    def get_available_partitions(self, partition_name: str = None, data_asset_name: str = None) -> List[Partition]:
        partitioner: Partitioner
        if not (
            data_asset_name and self.assets and self.assets.get(data_asset_name)
            and self.assets[data_asset_name].get("partitioner")
        ):
            partitioner = self.get_partitioner(name=self.default_partitioner)
        else:
            partitioner_name: str = self.assets[data_asset_name]["partitioner"]
            partitioner = self.get_partitioner(name=partitioner_name)
        partitioner.paths = self._get_file_paths(data_asset_name=data_asset_name)
        return partitioner.get_available_partitions(partition_name=partition_name, data_asset_name=data_asset_name)

    # TODO: <Alex>Like subdir reader...</Alex>
    # TODO: <Alex>Also need to support the case if assets exist and have names, then directory to search for file_paths is base_directory/asset_name</Alex>
    # TODO: <Alex>Should we make this better by returning all leaf files and applying a regex instead of glob_directive to narrow the list of paths?</Alex>
    def _get_file_paths(self, data_asset_name: str = None) -> list:
        """
        Returns:
            paths (list)
        """
        # TODO: <Alex>Need a better default (all inclusive).  Is this good enough?</Alex>
        glob_directive: str
        if not (
            data_asset_name and self.assets
            and self.assets.get(data_asset_name)
            and self.assets[data_asset_name].get("config_params")
            and self.assets[data_asset_name]["config_params"]
        ):
            glob_directive = self.config_params.get("glob_directive", "*")
        else:
            glob_directive = self.assets[data_asset_name]["config_params"].get("glob_directive", "*")
        return glob.glob(os.path.join(self.base_directory, glob_directive))

