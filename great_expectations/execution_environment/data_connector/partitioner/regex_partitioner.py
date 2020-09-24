import logging
import regex as re
from typing import List, Iterator, Union
from pathlib import Path
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.sorter.sorter import Sorter

logger = logging.getLogger(__name__)


class RegexPartitioner(Partitioner):
    # TODO: <Alex>What makes sense to have here, or is this even needed?</Alex>
    recognized_batch_parameters = {
        "regex",
        "sorters",
    }

    # defaults
    DEFAULT_DELIMITER: str = "-"
    DEFAULT_GROUP_NAME: str = "group"

    def __init__(
        self,
        data_connector: DataConnector,
        name: str,
        **kwargs
    ):
        logger.debug("Constructing RegexPartitioner {!r}".format(name))
        super().__init__(name=name, data_connector=data_connector, **kwargs)

        self._regex = self.config_params.get("regex") \
            or r"^((19|20)\d\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01])_(.*))\.csv"
        self._allow_multifile_partitions = self.config_params.get("allow_multifile_partitions")

        self._auto_discover_assets = True
        self._paths = None

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

    @property
    def regex(self) -> str:
        return self._regex

    @property
    def allow_multifile_partitions(self) -> bool:
        return self._allow_multifile_partitions

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
        sorters: Iterator[Sorter] = reversed(self.sorters)
        for sorter in sorters:
            cached_partitions = sorter.get_sorted_partitions(partitions=cached_partitions)
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
            raise ValueError("Regex is not defined")

        matches: Union[re.Match, None] = re.match(self.regex, path)
        if matches is None:
            logger.warning(f'No match found for path: "{path}".')
            return None
        else:
            partition_definition: dict = {}
            groups: tuple = matches.groups()
            # TODO: <Alex>TODO: Allow number of sorters to be <= number of groups -- this will impact Configuration</Alex>
            if len(self.sorters) == 0:
                for idx, group in enumerate(groups):
                    part_name = f"{RegexPartitioner.DEFAULT_GROUP_NAME}_{idx}"
                    partition_definition[part_name] = group
            else:
                # TODO: <Alex>TODO: Allow number of sorters to be <= number of groups -- this will impact Configuration</Alex>
                part_names: list = [sorter.name for sorter in self.sorters]
                if len(part_names) != len(groups):
                    raise ValueError(
                        f'''RegexPartitioner "{self.name}" matched {len(groups)} groups in "{path}", but number of
sorters specified is {len(part_names)}.
                        '''
                    )
                for idx, group in enumerate(groups):
                    part_name: str = part_names[idx]
                    partition_definition[part_name] = group

            part_name_list: list = [part_value for part_name, part_value in partition_definition.items()]
            partition_name: str = RegexPartitioner.DEFAULT_DELIMITER.join(part_name_list)

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
        if not self.allow_multifile_partitions and len(partitions) > 1 \
                and len(set([partition.data_asset_name for partition in partitions])) == 1:
            raise ValueError(
                f'''RegexPartitioner "{self.name}" detected multiple partitions for partition name "{partition_name}" of
data asset "{partitions[0].data_asset_name}"; however, allow_multifile_partitions is set to False.
                '''
            )
        return partitions
