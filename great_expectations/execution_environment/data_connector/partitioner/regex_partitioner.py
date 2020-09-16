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
        # set self:
        self._partitions = partitions
        # TODO: <Alex>Cleanup</Alex>
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
            # default case : there are no named ordered fields?
            # and add the name?
            partition_definition = {}
            if self.sorters is None:
                # then we want to use the defaults:
                # NOTE : matches begin with the full regex match at index=0 and then each matching group
                # and then each subsequent match in following indices.
                # this is why partition_definition_inner_dict is loaded with partition_params[i] as name
                # and matches[i+1] as value
                for i in range(len(matches)-1):
                    part_name = RegexPartitioner.DEFAULT_GROUP_NAME + "_" + str(i)
                    partition_definition[part_name] = matches[i+1]
            else:
                # TODO: <Alex>TODO</Alex>
                pass

            part_name_list = [part_value for part_name, part_value in partition_definition.items()]
            partition_name = RegexPartitioner.DEFAULT_DELIMITER.join(part_name_list)

        return Partition(name=partition_name, definition=partition_definition)
