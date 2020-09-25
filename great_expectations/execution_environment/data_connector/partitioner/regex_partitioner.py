import logging
import regex as re
from typing import List, Union
from pathlib import Path
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition

logger = logging.getLogger(__name__)


class RegexPartitioner(Partitioner):
    DEFAULT_DELIMITER: str = "-"

    def __init__(
        self,
        data_connector: DataConnector,
        name: str,
        **kwargs
    ):
        logger.debug("Constructing RegexPartitioner {!r}".format(name))
        super().__init__(name=name, data_connector=data_connector, **kwargs)

        self._regex = self._process_regex_config()
        self._allow_multifile_partitions = self.config_params.get("allow_multifile_partitions")

        self._auto_discover_assets = True
        self._paths = None

    def _process_regex_config(self) -> dict:
        regex: dict
        regex = self.config_params.get("regex")
        if regex and isinstance(regex, dict):
            assert "pattern" in regex.keys(), "Regex configuration requires pattern to be specified."
            if not ("group_names" in regex.keys() and isinstance(regex["group_names"], list)):
                regex["group_names"] = []
        else:
            regex = {
                "pattern": r"^((19|20)\d\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01])_(.*))\.csv",
                "group_names": [
                    "group_0",
                    "group_1",
                    "group_2",
                    "group_3",
                    "group_4",
                ]
            }
        return regex

    @property
    def regex(self) -> dict:
        return self._regex

    @property
    def allow_multifile_partitions(self) -> bool:
        return self._allow_multifile_partitions

    @property
    def auto_discover_assets(self) -> bool:
        return self._auto_discover_assets

    @auto_discover_assets.setter
    def auto_discover_assets(self, auto_discover_assets: bool):
        self._auto_discover_assets = auto_discover_assets

    @property
    def paths(self) -> List[str]:
        return self._paths

    @paths.setter
    def paths(self, paths: List[str]):
        self._paths = paths

    def get_available_partitions(self, partition_name: str = None, data_asset_name: str = None) -> List[Partition]:
        cached_partitions: List[Partition] = self.data_connector.get_cached_partitions(
            data_asset_name=data_asset_name
        )
        if cached_partitions is None or len(cached_partitions) == 0:
            self._find_available_partitions(data_asset_name=data_asset_name)
            cached_partitions = self.data_connector.get_cached_partitions(
                data_asset_name=data_asset_name
            )
        if cached_partitions is None or len(cached_partitions) == 0:
            return []
        cached_partitions = self.get_sorted_partitions(partitions=cached_partitions)
        return self._apply_allow_multifile_partitions_flag(
            partitions=cached_partitions,
            partition_name=partition_name
        )

    def _find_available_partitions(self, data_asset_name: str = None):
        partitions: List[Partition] = []
        for path in self.paths:
            if self.auto_discover_assets:
                data_asset_name = Path(path).stem
            partitioned_path: Partition = self._find_partitions_for_path(path=path, data_asset_name=data_asset_name)
            if partitioned_path is not None:
                partitions.append(partitioned_path)
        self.data_connector.update_partitions_cache(partitions=partitions)

    def _find_partitions_for_path(self, path: str, data_asset_name: str = None) -> Union[Partition, None]:
        if self.regex is None:
            raise ValueError("Regex configuration is not specified.")

        matches: Union[re.Match, None] = re.match(self.regex["pattern"], path)
        if matches is None:
            logger.warning(f'No match found for path: "{path}".')
            return None
        else:
            groups: tuple = matches.groups()
            if len(groups) != len(self.regex["group_names"]):
                raise ValueError(
                    f'''RegexPartitioner "{self.name}" matched {len(groups)} groups in "{path}", but number of match
group names specified is {len(self.regex["group_names"])}.
                    '''
                )
            if len(self.sorters) > 0:
                if any([sorter.name not in self.regex["group_names"] for sorter in self.sorters]):
                    raise ValueError(
                        f'''RegexPartitioner "{self.name}" specifies one or more sort keys that do not appear among
configured match group names.
                        '''
                    )
                if len(self.regex["group_names"]) < len(self.sorters):
                    raise ValueError(
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

    def _apply_allow_multifile_partitions_flag(
        self,
        partitions: List[Partition],
        partition_name: str = None
    ) -> List[Partition]:
        if partition_name is None:
            for partition in partitions:
                # noinspection PyUnusedLocal
                res: List[Partition] = self._apply_allow_multifile_partitions_flag_to_single_partition(
                    partitions=partitions,
                    partition_name=partition.name
                )
            return partitions
        else:
            return self._apply_allow_multifile_partitions_flag_to_single_partition(
                partitions=partitions,
                partition_name=partition_name
            )

    def _apply_allow_multifile_partitions_flag_to_single_partition(
        self,
        partitions: List[Partition],
        partition_name: str,
    ) -> List[Partition]:
        partitions: List[Partition] = list(
            filter(
                lambda partition: partition.name == partition_name, partitions
            )
        )
        if not self.allow_multifile_partitions and len(partitions) > 1 and len(set(partitions)) == 1:
            raise ValueError(
                f'''RegexPartitioner "{self.name}" detected multiple partitions for partition name "{partition_name}" of
data asset "{partitions[0].data_asset_name}"; however, allow_multifile_partitions is set to False.
                '''
            )
        return partitions
