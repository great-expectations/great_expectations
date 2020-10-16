import logging
from pathlib import Path
from typing import List, Union

import regex as re

import great_expectations.exceptions as ge_exceptions
from great_expectations.execution_environment.data_connector.data_connector import (
    DataConnector,
)
from great_expectations.execution_environment.data_connector.partitioner.partition import (
    Partition,
)
from great_expectations.execution_environment.data_connector.partitioner.partitioner import (
    Partitioner,
)

logger = logging.getLogger(__name__)


class RegexPartitioner(Partitioner):
    DEFAULT_GROUP_NAME_PATTERN: str = "group_"
    DEFAULT_DELIMITER: str = "-"

    def __init__(
        self,
        name: str,
        data_connector: DataConnector,
        sorters: list = None,
        allow_multipart_partitions: bool = False,
        config_params: dict = None,
        **kwargs,
    ):
        logger.debug(f'Constructing RegexPartitioner "{name}".')
        super().__init__(
            name=name,
            data_connector=data_connector,
            sorters=sorters,
            allow_multipart_partitions=allow_multipart_partitions,
            config_params=config_params,
            **kwargs,
        )

        self._regex = self._process_regex_config()

    def _process_regex_config(self) -> dict:
        regex: Union[dict, None]
        if self.config_params:
            regex = self.config_params.get("regex")
            # check if dictionary
            if not isinstance(regex, dict):
                raise ge_exceptions.PartitionerError(
                    f"""RegexPartitioner "{self.name}" requires a regex pattern configured as a dictionary.
                    It is currently of type "{type(regex)}. Please check your configuration."""
                )
            # check if correct key exists
            if not ("pattern" in regex.keys()):
                raise ge_exceptions.PartitionerError(
                    f"""RegexPartitioner "{self.name}" requires a regex pattern to be specified in its configuration.
                    """
                )
            # check if group_names exists in regex config, if not add empty list
            if not (
                "group_names" in regex.keys() and isinstance(regex["group_names"], list)
            ):
                regex["group_names"] = []
        else:
            # if no configuration exists at all, set defaults
            regex = {"pattern": r"(.*)", "group_names": ["group_0",]}
        return regex

    @property
    def regex(self) -> dict:
        return self._regex

    def _compute_partitions_for_data_asset(
        self,
        data_asset_name: str = None,
        *,
        paths: list = None,
        auto_discover_assets: bool = False,
    ) -> List[Partition]:
        if not paths or len(paths) == 0:
            return []
        partitions: List[Partition] = []
        partitioned_path: Partition
        if auto_discover_assets:
            for path in paths:
                partitioned_path = self._find_partitions_for_path(
                    path=path, data_asset_name=Path(path).stem
                )
                if partitioned_path is not None:
                    partitions.append(partitioned_path)
        else:
            for path in paths:
                partitioned_path = self._find_partitions_for_path(
                    path=path, data_asset_name=data_asset_name
                )
                if partitioned_path is not None:
                    partitions.append(partitioned_path)
        return partitions

    def _find_partitions_for_path(
        self, path: str, data_asset_name: str = None
    ) -> Union[Partition, None]:
        matches: Union[re.Match, None] = re.match(self.regex["pattern"], path)
        if matches is None:
            logger.warning(f'No match found for path: "{path}".')
            return None
        else:
            groups: tuple = matches.groups()
            group_names: list = [
                f"{RegexPartitioner.DEFAULT_GROUP_NAME_PATTERN}{idx}"
                for idx, group_value in enumerate(groups)
            ]
            for idx, group_name in enumerate(self.regex["group_names"]):
                group_names[idx] = group_name
            if self.sorters and len(self.sorters) > 0:
                if any(
                    [
                        sorter.name not in self.regex["group_names"]
                        for sorter in self.sorters
                    ]
                ):
                    raise ge_exceptions.PartitionerError(
                        f"""RegexPartitioner "{self.name}" specifies one or more sort keys that do not appear among
configured match group names.
                        """
                    )
                if len(group_names) < len(self.sorters):
                    raise ge_exceptions.PartitionerError(
                        f"""RegexPartitioner "{self.name}", configured with {len(group_names)}, matches {len(groups)}
group names, which is fewer than number of sorters specified is {len(self.sorters)}.
                        """
                    )
            partition_definition: dict = {}
            for idx, group_value in enumerate(groups):
                group_name: str = group_names[idx]
                partition_definition[group_name] = group_value
            partition_name: str = RegexPartitioner.DEFAULT_DELIMITER.join(
                partition_definition.values()
            )

        return Partition(
            name=partition_name,
            data_asset_name=data_asset_name,
            definition=partition_definition,
            data_reference=path,
        )
