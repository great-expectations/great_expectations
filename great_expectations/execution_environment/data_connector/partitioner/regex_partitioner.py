import logging
import regex as re
from typing import List, Iterator, Union
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
        name: str,
        regex: str = None,
        sorters: List[Sorter] = None
    ):
        logger.debug("Constructing RegexPartitioner {!r}".format(name))
        super().__init__(name)

        self._regex = regex
        if sorters is None:
            sorters = []
        self._sorters = sorters
        # TODO: <Alex></Alex>
        # self._partitions = {}
        self._partitions: List[Partition] = []

    @property
    def regex(self) -> str:
        return self._regex

    @regex.setter
    def regex(self, regex: str):
        self._regex = regex

    @property
    def sorters(self) -> List[Sorter]:
        return self._sorters

    # TODO: <Alex>Implement "UpSert" using Partition name for guidance.  Make this part of DataConnector.  No Caching HERE!</Alex>
    def get_available_partitions(self, paths: List[str]) -> List[Partition]:
        if len(self._partitions) > 0:
            return self._partitions

        partitions: List[Partition] = []
        for path in paths:
            partitioned_path: Partition = self._get_partitions_for_path(path=path)
            if partitioned_path is not None:
                partitions.append(partitioned_path)

        sorters: Iterator[Sorter] = reversed(self.sorters)
        for sorter in sorters:
            partitions = sorter.get_sorted_partitions(partitions=partitions)
        self._partitions = partitions
        # TODO: <Alex>OK for now, but not clear that this is how calculate and return should be...  Need to revisit.</Alex>
        # return self._partitions (should this be another method?)
        return self._partitions

    def _get_partitions_for_path(self, path: str) -> Partition:
        if self.regex is None:
            raise ValueError("Regex is not defined")

        matches: Union[re.Match, None] = re.match(self.regex, path)
        if matches is None:
            logger.warning("No match found for path: %s" % path)
            raise ValueError("No match found for path: %s" % path)
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
                        'Number of Regex groups matched in "%s" is %d but number of sorters specified is %d.' %
                        (path, len(groups), len(part_names))
                    )
                    raise ValueError(
                        'Number of Regex groups matched in "%s" is %d but number of sorters specified is %d.' %
                        (path, len(groups), len(part_names))
                    )
                for idx, group in enumerate(groups):
                    part_name: str = part_names[idx]
                    partition_definition[part_name] = group

            part_name_list: list = [part_value for part_name, part_value in partition_definition.items()]
            partition_name: str = RegexPartitioner.DEFAULT_DELIMITER.join(part_name_list)

        return Partition(name=partition_name, definition=partition_definition)

    def get_available_partition_names(self, paths: list) -> List[str]:
        return [
            partition.name for partition in self.get_available_partitions(paths)
        ]

    def get_partition(self, partition_name: str) -> Partition:
        pass
