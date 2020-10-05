import regex as re
from typing import List, Union
from pathlib import Path

import logging

from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


class RegexPartitioner(Partitioner):
    DEFAULT_DELIMITER: str = "-"

    def __init__(
        self,
        name: str,
        data_connector: DataConnector,
        sorters: list = None,
        allow_multipart_partitions: bool = False,
        config_params: dict = None,
        **kwargs
    ):
        logger.debug(f'Constructing RegexPartitioner "{name}".')
        super().__init__(
            name=name,
            data_connector=data_connector,
            sorters=sorters,
            allow_multipart_partitions=allow_multipart_partitions,
            config_params=config_params,
            **kwargs
        )

        self._regex = self._process_regex_config()

    def _process_regex_config(self) -> dict:
        regex: dict = self.config_params.get("regex")
        if regex and isinstance(regex, dict):
            assert "pattern" in regex.keys(), "Regex configuration requires pattern to be specified."
            if not ("group_names" in regex.keys() and isinstance(regex["group_names"], list)):
                regex["group_names"] = []
        else:
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
        paths: list = [],
        auto_discover_assets: bool = False
    ) -> List[Partition]:
        if not paths or len(paths) == 0:
            return []
        partitions: List[Partition] = []
        partitioned_path: Partition
        if auto_discover_assets:
            for path in paths:
                partitioned_path = self._find_partitions_for_path(path=path, data_asset_name=Path(path).stem)
                if partitioned_path is not None:
                    partitions.append(partitioned_path)
        else:
            for path in paths:
                partitioned_path = self._find_partitions_for_path(path=path, data_asset_name=data_asset_name)
                if partitioned_path is not None:
                    partitions.append(partitioned_path)
        return partitions

    def _find_partitions_for_path(self, path: str, data_asset_name: str = None) -> Union[Partition, None]:
        if self.regex is None:
            raise ge_exceptions.PartitionerError("Regex configuration is not specified.")

        matches: Union[re.Match, None] = re.match(self.regex["pattern"], path)
        if matches is None:
            logger.warning(f'No match found for path: "{path}".')
            return None
        else:
            groups: tuple = matches.groups()
            if len(groups) != len(self.regex["group_names"]):
                raise ge_exceptions.PartitionerError(
                    f'''RegexPartitioner "{self.name}" matched {len(groups)} groups in "{path}", but number of match
group names specified is {len(self.regex["group_names"])}.
                    '''
                )
            if self.sorters and len(self.sorters) > 0:
                if any([sorter.name not in self.regex["group_names"] for sorter in self.sorters]):
                    raise ge_exceptions.PartitionerError(
                        f'''RegexPartitioner "{self.name}" specifies one or more sort keys that do not appear among
configured match group names.
                        '''
                    )
                if len(self.regex["group_names"]) < len(self.sorters):
                    raise ge_exceptions.PartitionerError(
                        f'''RegexPartitioner "{self.name}" is configured with {len(self.regex["group_names"])} match
group names, which is fewer than number of sorters specified is {len(self.sorters)}.
                        '''
                    )
            partition_definition: dict = {}
            for idx, group_value in enumerate(groups):
                group_name: str = self.regex["group_names"][idx]
                partition_definition[group_name] = group_value
            partition_name: str = RegexPartitioner.DEFAULT_DELIMITER.join(partition_definition.values())

        return Partition(
            name=partition_name,
            definition=partition_definition,
            source=path,
            data_asset_name=data_asset_name
        )
