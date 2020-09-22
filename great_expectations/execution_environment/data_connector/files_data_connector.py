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
        name="default",
        execution_environment=None,
        # TODO: <Alex>How do we want to find assets?  Look at the old glob_reader...</Alex>
        assets=None,
        **kwargs
    ):
        logger.debug("Constructing FilesDataConnector {!r}".format(name))
        super().__init__(name, execution_environment=execution_environment, **kwargs)

        # TODO: <Alex>Do we need error handling here?</Alex>
        self._base_directory = self.config_params["base_directory"]
        self._assets = assets # <WILL> this is config (handle it differently?)

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

    @property
    def assets(self):
        return self._assets

    def get_available_data_asset_names(self):
        # should it be a list of just the keys?
        return self._assets.keys()

    def get_available_partitions(self, partition_name: str = None, data_asset_name: str = None) -> List[Partition]:
        partitioner: Partitioner = self.get_partitioner(name=self.partitioner_name)
        partitioner.paths = self._get_file_paths()
        return partitioner.get_available_partitions(partition_name=partition_name, data_asset_name=data_asset_name)

    # TODO: <Alex>Refactor: DataConnector can supply get_available_partition_names -- and remove from Partitioner</Alex>
    # def get_available_partition_names(self, data_asset_name: str = None) -> List[str]:

    # TODO: <Alex>Should we make this better by returning all leaf files and applying a regex instead of glob_directive to narrow the list of paths?</Alex>
    def _get_file_paths(self) -> list:
        """
        Returns:
            paths (list)
        """
        # TODO: <Alex>Need a better default (all inclusive).  Is this good enough?</Alex>
        glob_directive: str = self.config_params.get("glob_directive", "*")
        return glob.glob(os.path.join(self.base_directory, glob_directive))

