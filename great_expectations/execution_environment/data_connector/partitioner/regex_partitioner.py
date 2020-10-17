import regex as re
from typing import List, Union, Any
from pathlib import Path

import logging

from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.core.id_dict import PartitionDefinition
import great_expectations.exceptions as ge_exceptions

from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
)

logger = logging.getLogger(__name__)


class RegexPartitioner(Partitioner):
    DEFAULT_GROUP_NAME_PATTERN: str = "group_"

    def __init__(
        self,
        name: str,
        sorters: list = None,
        allow_multipart_partitions: bool = False,
        runtime_keys: list = None,
        config_params: dict = None,
        **kwargs
    ):
        logger.debug(f'Constructing RegexPartitioner "{name}".')
        super().__init__(
            name=name,
            sorters=sorters,
            allow_multipart_partitions=allow_multipart_partitions,
            runtime_keys=runtime_keys,
            config_params=config_params,
            **kwargs
        )

        self._regex = self._process_regex_config()

    def _process_regex_config(self) -> dict:
        regex: Union[dict, None]
        if self.config_params:
            regex = self.config_params.get("regex")
            # check if dictionary
            if not isinstance(regex, dict):
                raise ge_exceptions.PartitionerError(
                    f'''RegexPartitioner "{self.name}" requires a regex pattern configured as a dictionary. 
                    It is currently of type "{type(regex)}. Please check your configuration.''')
            # check if correct key exists
            if not ("pattern" in regex.keys()):
                raise ge_exceptions.PartitionerError(
                    f'''RegexPartitioner "{self.name}" requires a regex pattern to be specified in its configuration.
                    ''')
            # check if group_names exists in regex config, if not add empty list
            if not ("group_names" in regex.keys() and isinstance(regex["group_names"], list)):
                regex["group_names"] = []
        else:
            # if no configuration exists at all, set defaults
            regex = {
                "pattern": r"(.*)",
                "group_names": [
                    "group_0",
                ]
            }
        return regex

    @property
    def regex(self) -> dict:
        return self._regex


    def _convert_batch_request_to_data_reference(
        self,
        data_asset_name: str = None,
        runtime_parameters: Union[dict, None] = None,
        batch_request: BatchRequest = None,
        **kwargs,
    ) -> Any:

    def _convert_data_reference_to_batch_request(
            self,
            data_asset_name: str = None,
            runtime_parameters: Union[dict, None] = None,
            data_reference: Any = None,
            **kwargs,
    ) -> BatchRequest:
        # <WILL> data_reference can be Any, since it can be a string that links to a path, or an actual data_frame
        raise NotImplementedError



    def _convert_batch_request_to_data_reference(
        self,
        batch_request: BatchRequest,
        path: str,
        data_asset_name: str = None,
        runtime_parameters: Union[dict, None] = None
    ) -> Union[Partition, None]:
        print("Hi will i got this far, isn't it wonderful?")
        print(batch_request)

        matches: Union[re.Match, None] = re.match(self.regex["pattern"], path)
        if matches is None:
            logger.warning(f'No match found for path: "{path}".')
            return None
        else:
            groups: tuple = matches.groups()
            group_names: list = [
                f"{RegexPartitioner.DEFAULT_GROUP_NAME_PATTERN}{idx}" for idx, group_value in enumerate(groups)
            ]
            self._validate_sorters_configuration(
                partition_keys=self.regex["group_names"],
                num_actual_partition_keys=len(groups)
            )
            for idx, group_name in enumerate(self.regex["group_names"]):
                group_names[idx] = group_name
            partition_definition: dict = {}
            for idx, group_value in enumerate(groups):
                group_name: str = group_names[idx]
                partition_definition[group_name] = group_value
            partition_definition: PartitionDefinition = PartitionDefinition(partition_definition)
            if runtime_parameters:
                partition_definition.update(runtime_parameters)
            partition_name: str = self.DEFAULT_DELIMITER.join(
                [str(value) for value in partition_definition.values()]
            )
        return Partition(
            name=partition_name,
            data_asset_name=data_asset_name,
            definition=partition_definition,
            data_reference=path
        )


    def _conver