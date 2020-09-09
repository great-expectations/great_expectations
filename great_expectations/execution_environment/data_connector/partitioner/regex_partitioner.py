import datetime
import glob
import logging
import os
import re

from great_expectations.execution_environment.data_connector.data_connector import \
    DataConnector

logger = logging.getLogger(__name__)


class RegexPartitioner(object):
    """
    A regex partitioner generates partitions based on the regex and names that are passed in a parameters
    <WILL> write a better description

    CURRENTLY THIS ISN'T CONNECTED TO ANYTHING
    """

    def __init__(self, asset_regex, asset_field_definitions, use_directory=True):

        self._asset_regex = asset_regex
        self.asset_field_definitions = asset_field_definitions
        self.partitions = []
        # see if we can
        self.partition_ids = "FOO"

    def get_available_partitions(self, data_asset_name=None):
        files_config = self._get_data_asset_config(data_asset_name=data_asset_name)
        batch_paths = self._get_data_asset_paths(data_asset_name=data_asset_name)
        partitions = [
            self._partitioner(path, files_config)
            for path in batch_paths
            if self._partitioner(path, files_config) is not None
        ]
        return partitions

    def get_available_partition_ids(self, data_asset_name=None):
        partitions = self.get_available_partitions(data_asset_name)
        partition_ids = []
        for partition in partitions:
            partition_ids.append(partition["partition_id"])
        return partition_ids

    def get_available_partition_definitions(self, data_asset_name=None):
        partitions = self.get_available_partitions(data_asset_name)
        # return partitions
        partition_definitions = []
        for partition in partitions:
            partition_definitions.append(partition["partition_definition"])
        return partition_definitions

    def _partitioner(self, path, files_config):
        partitions = {}

        # if not configured then we return the default.
        if "partition_regex" not in files_config:
            partitions = {
                "partition_definition": {},
                "partition_id": (
                    datetime.datetime.now(datetime.timezone.utc).strftime(
                        "%Y%m%dT%H%M%S.%fZ"
                    )
                    + "__unmatched"
                ),
            }
            return partitions
        else:
            # TODO: check if this is really dangerous, because it seems that way

            partition_regex = (
                self._base_directory + "/" + files_config["partition_regex"]
            )
            # matches = re.match(files_config["partition_regex"], path)
            matches = re.match(partition_regex, path)
            if matches is None:
                logger.warning("No match found for path: %s" % path)
                # return the empty partition
                partitions = {
                    "partition_definition": {},
                    "partition_id": (
                        datetime.datetime.now(datetime.timezone.utc).strftime(
                            "%Y%m%dT%H%M%S.%fZ"
                        )
                        + "__unmatched"
                    ),
                }
                return partitions
            else:
                # need to check that matches length is the same as partition_param
                if "partition_param" in files_config:
                    partition_params = files_config["partition_param"]

                    try:
                        _ = matches[
                            len(partition_params)
                        ]  # check validitiy of partition params and whether the regex was configured correctly

                    except:
                        logger.warning(
                            "The number of matches not match the delimiter. See if your partitions are defined correctly"
                        )
                        print("please check regex")  # TODO: Beef up this error message

                    # NOTE : matches begin with the full regex match at index=0 and then each matching group
                    # and then each subsequent match in following indices.
                    # this is why partition_definition_inner_dict is loaded with partition_params[i] as key
                    # and matches[i+1] as value
                    partition_definition_inner_dict = {}
                    for i in range(len(partition_params)):
                        partition_definition_inner_dict[partition_params[i]] = matches[
                            i + 1
                        ]
                    partitions["partition_definition"] = partition_definition_inner_dict

                if "partition_delimiter" in files_config:
                    delim = files_config["partition_delimiter"]
                else:
                    delim = "-"

                # process partition_definition into partition id
                partition_id = []
                for key in partitions["partition_definition"].keys():
                    partition_id.append(str(partitions["partition_definition"][key]))
                partition_id = delim.join(partition_id)
                partitions["partition_id"] = partition_id

        return partitions
