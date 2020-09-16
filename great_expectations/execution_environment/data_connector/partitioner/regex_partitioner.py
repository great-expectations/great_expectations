import logging
import regex as re
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition

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
        name,
        regex=None,
        sorters=None
    ):
        logger.debug("Constructing RegexPartitioner {!r}".format(name))
        super().__init__(name)

        self._regex = regex
        self._sorters = sorters
        self._partitions = {}

    @property
    def regex(self):
        return self._regex

    @regex.setter
    def regex(self, regex):
        self._regex = regex

    @property
    def sorters(self):
        return self._sorters

    def get_partition(self, partition_name):
        # this will return : Part object (Will and Alex part - aka single part)
        pass

    def get_available_partition_names(self, paths):
        return [
            partition.name for partition in self.get_available_partitions(paths)
        ]

    def get_available_partitions(self, paths):
        if len(self._partitions) > 0:
            return self._partitions

        partitions = []
        for path in paths:
            partitioned_path = self._get_partitions_for_path(path=path)
            if partitioned_path is not None:
                partitions.append(partitioned_path)

        if self.sorters is not None:
            sorters = reversed(self.sorters)
            for sorter in sorters:
                partitions = sorter.get_sorted_partitions(partitions=partitions)
        self._partitions = partitions
        # TODO: <Alex>OK for now, but not clear that this is how calculate and return should be...  Need to revisit.</Alex>
        # return self._partitions (should this be another method?)
        return self._partitions

    def _get_partitions_for_path(self, path):
        if self.regex is None:
            raise ValueError("Regex is not defined")

        # TODO: <Alex>Cleanup</Alex>
        ####################################
        matches = re.match(self.regex, path)
        ####################################
        if matches is None:
            logger.warning("No match found for path: %s" % path)
            raise ValueError("No match found for path: %s" % path)
        else:
            partition_definition = {}
            groups = matches.groups()
            if self.sorters is None:
                for idx, group in enumerate(groups):
                    part_name = f"{RegexPartitioner.DEFAULT_GROUP_NAME}_{idx}"
                    partition_definition[part_name] = group
            else:
                # TODO: <Alex>TODO</Alex>
                part_names = [sorter.name for sorter in self.sorters]
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
                    part_name = part_names[idx]
                    partition_definition[part_name] = group

            part_name_list = [part_value for part_name, part_value in partition_definition.items()]
            partition_name = RegexPartitioner.DEFAULT_DELIMITER.join(part_name_list)

        return Partition(name=partition_name, definition=partition_definition)
