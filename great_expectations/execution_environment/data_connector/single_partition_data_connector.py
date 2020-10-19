import logging
from typing import List
from pathlib import Path

from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
    PartitionRequest,
)

logger = logging.getLogger(__name__)


class SinglePartitionDataConnector(DataConnector):
    """SinglePartitionDataConnector is a base class for DataConnectors that require exactly one Partitioner be configured in the declaration.

    Instead, its data_references are stored in a data_reference_dictionary : {
        "pretend/path/A-100.csv" : pandas_df_A_100,
        "pretend/path/A-101.csv" : pandas_df_A_101,
        "pretend/directory/B-1.csv" : pandas_df_B_1,
        "pretend/directory/B-2.csv" : pandas_df_B_2,
        ...
    }
    """
    def __init__(
        self,
        name: str,
        partitioner: dict,
        data_assets: dict = None,
        base_directory: str = None,
    ):
        logger.debug(f'Constructing SinglePartitionDataConnector "{name}".')

        self.base_directory = base_directory
        super().__init__(
            name=name,
            partitioners={
                "ONE_AND_ONLY_PARTITIONER" : partitioner
            },
            assets=data_assets,
            default_partitioner="ONE_AND_ONLY_PARTITIONER"
        )

class SinglePartitionDictDataConnector(SinglePartitionDataConnector):
    def __init__(
        self,
        name: str,
        data_reference_dict: {},
        **kwargs,
    ):
        logger.debug(f'Constructing SinglePartitionDictDataConnector "{name}".')
        super().__init__(
            name,
            **kwargs
        )

        # This simulates the underlying filesystem
        self.data_reference_dict = data_reference_dict

    def _get_data_reference_list(self):
        data_reference_keys = list(self.data_reference_dict.keys())
        data_reference_keys.sort()
        return data_reference_keys

class SinglePartitionFileDataConnector(SinglePartitionDataConnector):
    def __init__(
        self,
        name: str,
        base_directory: str,
        glob_directive: str = "*",
        **kwargs,
    ):
        logger.debug(f'Constructing SinglePartitionFileDataConnector "{name}".')

        self.glob_directive = glob_directive

        super().__init__(
            name,
            base_directory=base_directory,
            **kwargs
        )

    def _get_data_reference_list(self):
        globbed_paths = Path(self.base_directory).glob(self.glob_directive)
        path_list = [
            str(posix_path) for posix_path in globbed_paths
        ]

        # Trim paths to exclude the base_directory
        base_directory_len = len(str(self.base_directory))
        path_list = [path[base_directory_len:] for path in path_list]

        return path_list

    def get_available_data_asset_names(self) -> List[str]:
        """Return the list of asset names known by this data connector.

        Returns:
            A list of available names
        """
        if self._data_references_cache == None:
            self.refresh_data_references_cache()

        available_data_asset_names = []

        for k,v in self._data_references_cache.items():
            print(k,v)
            if v != None:
                available_data_asset_names.append(v.data_asset_name)

        return list(set(available_data_asset_names))
