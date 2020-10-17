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
        partitioner: dict = {},
        assets: dict = None,
    ):
        logger.debug(f'Constructing SinglePartitionDataConnector "{name}".')

        self.partitioner = self._build_partitioner_from_config("ONE_AND_ONLY_PARTITIONER", partitioner)

        super().__init__(
            name=name,
            partitioners={},
            assets=assets,
        )

    #TODO Abe 20201015: This method is extremely janky. Needs better supporting methods, plus more thought and hardening.
    def _map_data_reference_to_batch_request_list(self, data_reference) -> List[BatchDefinition]:
        #TODO Abe 20201016: Instead of _find_partitions_for_path, this should be convert_data_reference_to_batch_request
        partition = self.partitioner._find_partitions_for_path(data_reference)
        if partition == None:
            return None

        return BatchRequest(
            execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME",
            data_connector_name=self.name,
            data_asset_name="FAKE_DATA_ASSET_NAME",
            partition_request=partition.definition,
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
