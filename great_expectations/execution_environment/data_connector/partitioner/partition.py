# -*- coding: utf-8 -*-

from typing import Union, Dict, Any

import logging

from great_expectations.execution_environment.data_connector.partitioner.partition_spec import PartitionSpec
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


# TODO: <Alex>What to do if partition_index is provided?..</Alex>
def get_partition_spec(
    partition_spec_config: Union[str, Dict[str, Union[str, Dict]], PartitionSpec] = None
) -> PartitionSpec:
    if isinstance(partition_spec_config, PartitionSpec):
        return partition_spec_config
    elif isinstance(partition_spec_config, str):
        partition_spec: PartitionSpec = PartitionSpec(name=partition_spec_config)
        return partition_spec
    elif isinstance(partition_spec_config, Dict):
        partition_specification_keys: set = set(partition_spec_config.keys())
        if not partition_specification_keys <= PartitionSpec.RECOGNIZED_PARTITION_SPECIFICATION_KEYS:
            raise ge_exceptions.PartitionerError(
                f'''Unrecognized partition_spec key(s):
"{str(partition_specification_keys - PartitionSpec.RECOGNIZED_PARTITION_SPECIFICATION_KEYS)}" detected.
                '''
            )
        partition_name: str = partition_spec_config.get("name")
        if partition_name and not isinstance(partition_name, str):
            raise ge_exceptions.PartitionerError(
                f'''The type of a partition name must be a string (Python "str").  The type given is
"{str(type(partition_name))}", which is illegal.
                '''
            )
        data_asset_name: str = partition_spec_config.get("data_asset_name")
        if data_asset_name and not isinstance(data_asset_name, str):
            raise ge_exceptions.PartitionerError(
                f'''The type of a data asset name must be a string (Python "str").  The type given is
"{str(type(data_asset_name))}", which is illegal.
                '''
            )
        partition_definition: dict = partition_spec_config.get("definition")
        if partition_definition:
            if not isinstance(partition_definition, dict):
                raise ge_exceptions.PartitionerError(
                    f'''The type of a partition definition must be a dictionary (Python "dict").  The type given is
"{str(type(partition_definition))}", which is illegal.
                    '''
                )
            if not all([isinstance(key, str) for key in partition_definition.keys()]):
                raise ge_exceptions.PartitionerError('All partition definition keys must strings (Python "str").')
        partition_spec: PartitionSpec = PartitionSpec(
            name=partition_name,
            data_asset_name=data_asset_name,
            definition=partition_definition
        )
        return partition_spec
    else:
        raise ge_exceptions.PartitionerError(
            f'Invalid partition_spec_config type "{str(type(partition_spec_config))}" detected.'
        )


class Partition(PartitionSpec):
    def __init__(self, name: str = None, data_asset_name: str = None, definition: dict = None, source: Any = None):
        super().__init__(
            name=name,
            data_asset_name=data_asset_name,
            definition=definition
        )
        self._source = source

    @property
    def partition_spec(self) -> PartitionSpec:
        return super()

    @property
    def source(self) -> Any:
        return self._source

    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "name": self.name,
            "data_asset_name": self.data_asset_name,
            "definition": self.definition,
            "source": self.source
        }
        return str(doc_fields_dict)
