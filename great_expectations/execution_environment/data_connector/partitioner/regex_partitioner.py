import regex as re
from typing import List, Union
from pathlib import Path

import logging

from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.core.id_dict import PartitionDefinition
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


class RegexPartitioner(Partitioner):
    DEFAULT_GROUP_NAME_PATTERN: str = "group_"

    def __init__(
        self,
        name: str,
        data_connector: DataConnector,
        sorters: list = None,
        allow_multipart_partitions: bool = False,
        runtime_keys: list = None,
        config_params: dict = None,
        **kwargs
    ):
        logger.debug(f'Constructing RegexPartitioner "{name}".')
        super().__init__(
            name=name,
            data_connector=data_connector,
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

    def _compute_partitions_for_data_asset(
        self,
        data_asset_name: str = None,
        *,
        runtime_parameters: Union[dict, None] = None,
        paths: list = None,
        auto_discover_assets: bool = False
    ) -> List[Partition]:
        if not paths or len(paths) == 0:
            return []
        partitions: List[Partition] = []
        partitioned_path: Partition
        if auto_discover_assets:
            for path in paths:
                partitioned_path = self._find_partitions_for_path(
                    path=path,
                    data_asset_name=Path(path).stem,
                    runtime_parameters=runtime_parameters
                )
                if partitioned_path is not None:
                    partitions.append(partitioned_path)
        else:
            for path in paths:
                partitioned_path = self._find_partitions_for_path(
                    path=path,
                    data_asset_name=data_asset_name,
                    runtime_parameters=runtime_parameters
                )
                if partitioned_path is not None:
                    partitions.append(partitioned_path)
        return partitions

    def _find_partitions_for_path(
        self,
        path: str,
        data_asset_name: str = None,
        runtime_parameters: Union[dict, None] = None
    ) -> Union[Partition, None]:
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
