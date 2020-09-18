import datetime
import glob
import logging
import os
import re
import warnings

from great_expectations.exceptions import BatchKwargsError
from great_expectations.execution_environment.data_connector.data_connector import (
    DataConnector,
)
from great_expectations.execution_environment.types import PathBatchKwargs

logger = logging.getLogger(__name__)
class FilesDataConnector(DataConnector):
    def __init__(
        self,
        name="default",
        execution_environment=None,
        base_directory=None,
        assets=None,
        partitioners=None,
    ):
        logger.debug("Constructing FilesDataConnector {!r}".format(name))
        super().__init__(name, execution_environment=execution_environment)

        self._base_directory = base_directory
        self._assets = assets # <WILL> this is config (handle it differently?)
        self._partitioners = partitioners # <WILL> this is config (handle it differently?)

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

    @property
    def partitioners(self):
        return self._partitioners


    def get_available_data_asset_names(self):
        # this returns the full assets dict = {'testing_data': {'partitioner_name': 'my_partitioner'}, 'testing_data_v2': {'partitioner_name': 'my_partitioner'}}
        # should it be a list of just the keys?
        return self._assets.keys()

