from pathlib import Path
import itertools
from typing import List, Union, Any
import os

import logging

from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.partition_query import PartitionQuery
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.core.batch import BatchRequest
from great_expectations.core.id_dict import (
    PartitionDefinitionSubset,
    BatchSpec
)
from great_expectations.core.batch import (
    BatchMarkers,
    BatchDefinition,
)
from great_expectations.execution_environment.types import PathBatchSpec
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


KNOWN_EXTENSIONS = [
    ".csv",
    ".tsv",
    ".parquet",
    ".xls",
    ".xlsx",
    ".json",
    ".csv.gz",
    ".tsv.gz",
    ".feather",
]


class FilesDataConnector(DataConnector):
    def __init__(
        self,
        name: str,
        base_directory: str,
        glob_directive: str,
        partitioners: dict = None,
        default_partitioner: str = None,
        assets: dict = None,
        known_extensions: list = None,
        reader_options: dict = None,
        reader_method: str = None,
        execution_engine: ExecutionEngine = None,
        data_context_root_directory: str = None,
        **kwargs
    ):
        logger.debug(f'Constructing FilesDataConnector "{name}".')
        super().__init__(
            name=name,
            partitioners=partitioners,
            default_partitioner=default_partitioner,
            assets=assets,
            execution_engine=execution_engine,
            data_context_root_directory=data_context_root_directory,
            **kwargs
        )

        if known_extensions is None:
            known_extensions = KNOWN_EXTENSIONS
        self._known_extensions = known_extensions

        if reader_options is None:
            reader_options = self._default_reader_options
        self._reader_options = reader_options

        self._reader_method = reader_method
        self._base_directory = os.path.join(base_directory, '') #Add trailing slash if it's not there already
        self._glob_directive = glob_directive

    @property
    def reader_options(self):
        return self._reader_options

    @property
    def reader_method(self):
        return self._reader_method

    @property
    def known_extensions(self):
        return self._known_extensions

    @property
    def base_directory(self):
        return self._normalize_directory_path(dir_path=self._base_directory)

    def _get_available_partitions(
        self,
        partitioner: Partitioner,
        data_asset_name: str = None,
        partition_query: Union[PartitionQuery, None] = None,
        in_memory_dataset: Any = None,
        runtime_parameters: Union[PartitionDefinitionSubset, None] = None,
        repartition: bool = None
    ) -> List[Partition]:
        # TODO: <Alex>TODO: Each specific data_connector should verify the given partitioner against the list of supported partitioners.</Alex>
        paths: List[str] = self._get_file_paths_for_data_asset(data_asset_name=data_asset_name)
        data_asset_config_exists: bool = data_asset_name and self.assets and self.assets.get(data_asset_name)
        auto_discover_assets: bool = not data_asset_config_exists
        return partitioner.find_or_create_partitions(
            data_asset_name=data_asset_name,
            partition_query=partition_query,
            runtime_parameters=runtime_parameters,
            # The next two (2) parameters are specific for the partitioners that work under the present data connector.
            paths=paths,
            auto_discover_assets=auto_discover_assets
        )

    def _normalize_directory_path(self, dir_path: str) -> str:
        # If directory is a relative path, interpret it as relative to the data context's
        # context root directory (parent directory of great_expectation dir)
        if Path(dir_path).is_absolute() or self._data_context_root_directory is None:
            return dir_path
        else:
            return Path(self._data_context_root_directory).joinpath(dir_path)

    def _get_file_paths_for_data_asset(self, data_asset_name: str = None) -> list:
        """
        Returns:
            paths (list)
        """
        base_directory: str
        glob_directive: str

        data_asset_directives: dict = self._get_data_asset_directives(data_asset_name=data_asset_name)
        base_directory = data_asset_directives["base_directory"]
        glob_directive = data_asset_directives["glob_directive"]

        if Path(base_directory).is_dir():
            path_list: list
            if glob_directive:
                path_list = [
                    str(posix_path) for posix_path in Path(base_directory).glob(glob_directive)
                ]
            else:
                path_list = [
                    str(posix_path) for posix_path in self._get_valid_file_paths(base_directory=base_directory)
                ]

            # Trim paths to exclude the base_directory
            base_directory_len = len(str(base_directory))
            path_list = [path[base_directory_len:] for path in path_list]

            return self._verify_file_paths(path_list=path_list)
        raise ge_exceptions.DataConnectorError(f'Expected a directory, but path "{base_directory}" is not a directory.')

    def _get_data_asset_directives(self, data_asset_name: str = None) -> dict:
        glob_directive: str
        base_directory: str
        if (
            data_asset_name
            and self.assets
            and self.assets.get(data_asset_name)
            and self.assets[data_asset_name].get("config_params")
            and self.assets[data_asset_name]["config_params"]
        ):
            base_directory = self._normalize_directory_path(
                dir_path=self.assets[data_asset_name]["config_params"].get("base_directory", self.base_directory)
            )
            glob_directive = self.assets[data_asset_name]["config_params"].get("glob_directive")
        else:
            base_directory = self.base_directory
            glob_directive = self._glob_directive
        return {"base_directory": base_directory, "glob_directive": glob_directive}

    @staticmethod
    def _verify_file_paths(path_list: list) -> list:
        if not all(
            [not Path(path).is_dir() for path in path_list]
        ):
            raise ge_exceptions.DataConnectorError(
                "All paths for a configured data asset must be files (a directory was detected)."
            )
        return path_list

    def _get_valid_file_paths(self, base_directory: str = None) -> list:
        if base_directory is None:
            base_directory = self.base_directory
        path_list: list = list(Path(base_directory).iterdir())
        for path in path_list:
            for extension in self.known_extensions:
                if path.endswith(extension) and not path.startswith("."):
                    path_list.append(path)
                elif Path(path).is_dir:
                    # Make sure there is at least one valid file inside the subdirectory.
                    subdir_path_list: list = self._get_valid_file_paths(base_directory=path)
                    if len(subdir_path_list) > 0:
                        path_list.append(subdir_path_list)
        return list(
            set(
                list(
                    itertools.chain.from_iterable(
                        [
                            element for element in path_list
                        ]
                    )
                )
            )
        )

    def _build_batch_spec_from_partition(
        self,
        partition: Partition,
        batch_request: BatchRequest,
        batch_spec: BatchSpec
    ) -> PathBatchSpec:
        """
        Args:
            partition:
            batch_request:
            batch_spec:
        Returns:
            batch_spec
        """
        if not batch_spec.get("path"):
            path: str = os.path.join(self._base_directory, partition.data_reference)
            batch_spec["path"] = path
        return PathBatchSpec(batch_spec)

    def _generate_batch_spec_parameters_from_batch_definition(
        self,
        batch_definition: BatchDefinition
    ) -> dict:
    
        #TODO Abe 20201018: This is an absolutely horrible way to get a path from a single partition_definition, but AFIACT it's the only method currently supported by our Partitioner
        available_partitions = self.get_available_partitions(
            data_asset_name=batch_definition.data_asset_name,
        )
        for partition in available_partitions:
            if partition.definition == batch_definition.partition_definition:
                path = os.path.join(self._base_directory, partition.data_reference)
                continue
        try:
            path
        except UnboundLocalError:
            raise ValueError(f"No partition in {available_partitions} matches the given partition definition {batch_definition.partition_definition} from batch definition {batch_definition}")

        return {
            "path" : path
        }
