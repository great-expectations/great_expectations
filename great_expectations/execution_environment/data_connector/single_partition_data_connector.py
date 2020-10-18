import logging
from typing import List

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
