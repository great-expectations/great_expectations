import datetime
import glob
import logging
import os
import re
import warnings

from great_expectations.exceptions import BatchKwargsError
from great_expectations.execution_environment.data_connector.data_connector import (
    DataConnector,
)
from great_expectations.execution_environment.types import PathBatchKwargs

logger = logging.getLogger(__name__)

class FilesDataConnector(DataConnector):
    r"""FilesDataConnector processes files in a directory according to glob patterns to produce batches of data.

ExecutionEnvironment:
  my_execution_environment:
    execution_engine:
      module_name: great_expectations.execution_engine.pandas_execution_engine
      class_name: PandasExecutionEngine
    data_connectors:
      my_connector:
        class_name: FilesDataConnector
        module_name: great_expectations.execution_environment.data_connector.files_data_connector
        base_directory: mydir/
        assets:
          my_asset: ### <~ this is `data_asset_name`
            partitioner_name: my_partitioner
            ### additional asset-level params can live here ###
        partitioners:
          my_partitioner:
            class_name: RegexPartitioner
            module_name: great_expectations.execution_environment.data_connector.partitioner.regex_partitioner
            regex: r'(.*)_(.*)_(.*).csv'
            sorters:
              name:
                class_name: LexicographicalSorter
                module_name: great_expectations.execution_environment.data_connector.partitioner.sorter.lexicographical_sorter
                orderby: desc
              date:
                class_name: DateTimeSorter
                module_name: great_expectations.execution_environment.data_connector.partitioner.sorter.datetime_sorter
                timeformatstring: 'yyyymmdd' # <~~ kick it to the datetime module
                orderby: asc
              price:
                class_name: NumericalSorter
                module_name: great_expectations.execution_environment.data_connector.partitioner.sorter.numerical_sorter
                orderby: asc
    """
    recognized_batch_parameters = {
        "assets",
        "data_asset_name",
        "partitioner_name",
        "partitioners",
        "reader_method",
        "reader_options",
        "limit",
    }

    def __init__(
        self,
        name="default",
        execution_environment=None,
        base_directory="/data",
        data_asset_name=None,
        assets=None,
        reader_options=None,
        partitioner_name=None,
        partitioners=None,
        reader_method=None,
    ):
        logger.debug("Constructing FilesDataConnector {!r}".format(name))
        super().__init__(name, execution_environment=execution_environment)

        if reader_options is None:
            reader_options = {}

        self._base_directory = base_directory
        self._data_asset_name = data_asset_name
        self._assets = assets
        self._reader_options = reader_options
        self._partitioner_name = partitioner_name
        self._partitioners = partitioners
        self._reader_method = reader_method

    @property
    def assets(self):
        return self._assets

    @property
    def reader_options(self):
        return self._reader_options

    @property
    def partitioner_name(self):
        return self._partitioner_name

    @property
    def partitioners(self):
        return self._partitioners

    @property
    def reader_method(self):
        return self._reader_method

    @property
    def base_directory(self):
        # If base directory is a relative path, interpret it as relative to the data context's
        # context root directory (parent directory of great_expectation dir)
        if (
            os.path.isabs(self._base_directory)
            or self._execution_environment.data_context is None
        ):
            return self._base_directory
        else:
            return os.path.join(
                self._execution_environment.data_context.root_directory,
                self._base_directory,
            )

    def get_available_partitions(self, data_asset_name=None):
        return self._data_connector_config

        #
        #files_config = self._get_data_asset_config(data_asset_name=data_asset_name)
        #batch_paths = self._get_data_asset_paths(data_asset_name=data_asset_name)

        #partitions = [
        #    self._partitioner(path, files_config)
        #    for path in batch_paths
        #    if self._partitioner(path, files_config) is not None
        #]
        #return partitions
    def _get_data_asset_config(self, data_asset_name):



        try:
            return self.asset_param[data_asset_name]
        except KeyError:
            batch_kwargs = {
                "data_asset_name": data_asset_name,
            }
            raise BatchKwargsError(
                "Unknown asset_name %s" % data_asset_name, batch_kwargs
            )

    """
    def get_regex(self, data_asset_name=None):
        files_config = self._get_data_asset_config(data_asset_name=data_asset_name)
        batch_paths = self._get_data_asset_paths(data_asset_name=data_asset_name)

        partitions = [
            self._get_regex(path, files_config)
            for path in batch_paths
            if self._get_regex(path, files_config) is not None
        ]
        return partitions

    def _get_regex(self, path, files_config):
        partition_regex = self._base_directory + files_config["partition_regex"]
        return partition_regex

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
            # logger.warning("no partition_regex configuration found for path: %s" % path)
            # partitions = {
            #    "partition_definition": {},
            #    "partition_id": (
            #                datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ") + "__unmatched")}
            # return (partitions)
            return None
        else:
            # TODO: check if this is really dangerous, because it seems that way
            partition_regex = self._base_directory + files_config["partition_regex"]
            matches = re.match(partition_regex, path)
            if matches is None:
                logger.warning("No match found for path: %s" % path)
                return None
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
    
    def _get_data_asset_paths(self, data_asset_name):
        Returns a list of filepaths associated with the given data_asset_name
        Args:
            data_asset_name:

        Returns:
            paths (list)
        glob_config = self._get_data_asset_config(data_asset_name)
        globs = sorted(glob.glob(self.base_directory + "/**", recursive=True))
        files = [f for f in globs if os.path.isfile(f)]

        if "partition_regex" in glob_config.keys():
            pattern = re.compile(glob_config["partition_regex"])
            files = [file for file in files if pattern.match(file)]
        return files

    def _get_iterator(
        self, data_asset_name, reader_method=None, reader_options=None, limit=None
    ):
        glob_config = self._get_data_asset_config(data_asset_name)
        paths = glob.glob(os.path.join(self.base_directory, glob_config["glob"]))
        return self._build_batch_kwargs_path_iter(
            paths,
            glob_config,
            reader_method=reader_method,
            reader_options=reader_options,
            limit=limit,
        )

    def _build_batch_kwargs_path_iter(
        self,
        path_list,
        glob_config,
        reader_method=None,
        reader_options=None,
        limit=None,
    ):
        for path in path_list:
            yield self._build_batch_kwargs_from_path(
                path,
                glob_config,
                reader_method=reader_method,
                reader_options=reader_options,
                limit=limit,
            )

    def _build_batch_kwargs_from_path(
        self, path, glob_config, reader_method=None, reader_options=None, limit=None
    ):

        batch_kwargs = self._execution_environment.execution_engine.process_batch_parameters(
            reader_method=reader_method
            or glob_config.get("reader_method")
            or self.reader_method,
            reader_options=reader_options
            or glob_config.get("reader_options")
            or self.reader_options,
            limit=limit or glob_config.get("limit"),
        )

        batch_kwargs["path"] = path
        batch_kwargs[
            "execution_environment"
        ] = self._execution_environment.name  # TODO : check if this breaks anything
        return PathBatchKwargs(batch_kwargs)

    def _get_data_asset_config(self, data_asset_name):
        try:
            return self.asset_param[data_asset_name]
        except KeyError:
            batch_kwargs = {
                "data_asset_name": data_asset_name,
            }
            raise BatchKwargsError(
                "Unknown asset_name %s" % data_asset_name, batch_kwargs
            )


    """
