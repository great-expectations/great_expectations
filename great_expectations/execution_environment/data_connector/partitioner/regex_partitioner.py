import logging
import regex as re
from typing import List, Dict, Iterator, Union
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.sorter import Sorter

logger = logging.getLogger(__name__)


class RegexPartitioner(Partitioner):

    recognized_batch_parameters = {
        "regex",
        "sorters",
    }

    # defaults
    DEFAULT_DELIMITER = "-"
    DEFAULT_GROUP_NAME = "group"

    def __init__(
        self,
        data_connector: DataConnector,
        name: str,
        paths: List[str],
        regex: str = None,
        allow_multifile_partitions: bool = False,
        sorters: List[Sorter] = None
    ):
        logger.debug("Constructing RegexPartitioner {!r}".format(name))
        super().__init__(name=name, data_connector=data_connector)

        self._paths = paths
        self._regex = regex
        self._allow_multifile_partitions = allow_multifile_partitions
        if sorters is None:
            sorters = []
        self._sorters = sorters

    @property
    def paths(self) -> List[str]:
        return self._paths

    @paths.setter
    def paths(self, paths: List[str]):
        self._paths = paths

    @property
    def regex(self) -> str:
        return self._regex

    @regex.setter
    def regex(self, regex: str):
        self._regex = regex

    @property
    def allow_multifile_partitions(self) -> bool:
        return self._allow_multifile_partitions

    @allow_multifile_partitions.setter
    def allow_multifile_partitions(self, allow_multifile_partitions: bool):
        self._allow_multifile_partitions = allow_multifile_partitions

    @property
    def sorters(self) -> List[Sorter]:
        return self._sorters

    def get_available_partitions(self, partition_name: str = None, data_asset_name: str = None) -> List[Partition]:
        cached_partitions: Union[List[Partition], Dict[str, Partition]] = self.data_connector.get_cached_partitions(
            data_asset_name=data_asset_name
        )
        if cached_partitions is None:
            self.find_available_partitions(data_asset_name=data_asset_name)
            cached_partitions = self.data_connector.get_cached_partitions(
                data_asset_name=data_asset_name
            )
        if partition_name is None:
            return cached_partitions
        partitions: List[Partition] = list(
            filter(
                lambda partition: partition.name == partition_name, cached_partitions
            )
        )
        if not self.allow_multifile_partitions and len(partitions) > 1:
            logger.warning(
                f'''RegexPartitioner "{self.name}' detected multiple partitions detected for partition name
"{partition_name}"; however, allow_multifile_partitions is set to False.
                '''
            )
            raise ValueError(
                f'''RegexPartitioner "{self.name}' detected multiple partitions detected for partition name
"{partition_name}"; however, allow_multifile_partitions is set to False.
                '''
            )
        return partitions

    # TODO: <Alex>Implement "UpSert" using Partition name for guidance.  Make this part of DataConnector.  No Caching HERE!</Alex>
    def find_available_partitions(self, data_asset_name: str = None) -> List[Partition]:
        partitions: List[Partition] = []
        for path in self.paths:
            partitioned_path: Partition = self._find_partitions_for_path(path=path)
            if partitioned_path is not None:
                partitions.append(partitioned_path)

        sorters: Iterator[Sorter] = reversed(self.sorters)
        for sorter in sorters:
            partitions = sorter.get_sorted_partitions(partitions=partitions)
        self.data_connector.update_partitions_cache(partitions=partitions, data_asset_name=data_asset_name)
        return self.data_connector.get_cached_partitions(data_asset_name=data_asset_name)

    def _find_partitions_for_path(self, path: str) -> Partition:
        if self.regex is None:
            raise ValueError("Regex is not defined")

        matches: Union[re.Match, None] = re.match(self.regex, path)
        if matches is None:
            logger.warning(f'No match found for path: "{path}".')
            raise ValueError(f'No match found for path: "{path}".')
        else:
            partition_definition: dict = {}
            groups: tuple = matches.groups()
            if len(self.sorters) == 0:
                for idx, group in enumerate(groups):
                    part_name = f"{RegexPartitioner.DEFAULT_GROUP_NAME}_{idx}"
                    partition_definition[part_name] = group
            else:
                # TODO: <Alex>TODO</Alex>
                part_names: list = [sorter.name for sorter in self.sorters]
                if len(part_names) != len(groups):
                    logger.warning(
                        f'''RegexPartitioner "{self.name}" matched {len(groups)} groups in "{path}", but number of
sorters specified is {len(part_names)}.
                        '''
                    )
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

        return Partition(name=partition_name, definition=partition_definition, source=path)

