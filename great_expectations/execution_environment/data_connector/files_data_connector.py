import os
from pathlib import Path
import itertools
from typing import List, Union, Any, Dict
import os

import logging

from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector.asset.asset import Asset
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.partition_request import PartitionRequest
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
from great_expectations.data_context.util import instantiate_class_from_config
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


# TODO: <Alex>Should we make this a "set" object?</Alex>
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
        execution_environment_name: str,
        base_directory: str,
        default_regex: dict,
        assets: dict,

        glob_directive: str = "*",
        # partitioners: dict = {},
        # default_partitioner_name: str = None,
        # known_extensions: list = None,
        # reader_options: dict = None,
        # reader_method: str = None,
        execution_engine: ExecutionEngine = None,
        data_context_root_directory: str = None,
    ):
        logger.debug(f'Constructing FilesDataConnector "{name}".')
        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            # partitioners=partitioners,
            # default_partitioner_name=default_partitioner_name,
            # assets=assets,
            execution_engine=execution_engine,
            # data_context_root_directory=data_context_root_directory
        )
        self._glob_directive = glob_directive

        # TODO: <Alex>This trailing slash appears to be unnecessary; in addition, the path must be normalized internally.</Alex>
        # self._base_directory = os.path.join(base_directory, '') #Add trailing slash if it's not there already
        self._base_directory = self._normalize_directory_path(dir_path=base_directory)

        self._glob_directive = glob_directive

        # TODO: Maybe make this a typed object?
        self._default_regex: dict = default_regex

        # if known_extensions is None:
        #     known_extensions = KNOWN_EXTENSIONS
        # self._known_extensions = known_extensions

        # if reader_options is None:
        #     reader_options = self._default_reader_options
        # self._reader_options = reader_options

        # self._reader_method = reader_method

        if assets is None:
            assets = {}
        _assets: Dict[str, Union[dict, Asset]] = assets
        self._assets = _assets
        self._build_assets_from_config(config=assets)


    @property
    def base_directory(self) -> str:
        return str(self._base_directory)

    @property
    def glob_directive(self) -> str:
        return self._glob_directive

    # @property
    # def reader_options(self) -> dict:
    #     return self._reader_options

    # @property
    # def reader_method(self) -> str:
    #     return self._reader_method

    # @property
    # def known_extensions(self) -> List[str]:
    #     return self._known_extensions

    def _build_assets_from_config(self, config: Dict[str, dict]):
        for name, asset_config in config.items():
            if asset_config is None:
                raise ValueError("Asset config should not be None.")
            for property in asset_config.keys():
                if asset_config[property] is None:
                    raise ValueError(
                        f'If Asset config defines the property "{property}", then its value must be specified.'
                    )
            new_asset: Asset = self._build_asset_from_config(
                name=name,
                config=asset_config,
            )
            self.assets[name] = new_asset

    def _build_asset_from_config(self, name: str, config: dict):
        """Build an Asset using the provided configuration and return the newly-built Asset."""
        runtime_environment: dict = {
            "name": name,
            "data_connector": self
        }
        asset: Asset = instantiate_class_from_config(
            config=config,
            runtime_environment=runtime_environment,
            config_defaults={
                "module_name": "great_expectations.execution_environment.data_connector.asset",
                "class_name": "Asset"
            },
        )
        if not asset:
            raise ge_exceptions.ClassInstantiationError(
                module_name="great_expectations.execution_environment.data_connector.asset",
                package_name=None,
                class_name=config["class_name"],
            )
        return asset

    def get_available_data_asset_names(self) -> List[str]:
        """Return the list of asset names known by this data connector.

        Returns:
            A list of available names
        """
        return list(self.assets.keys())

    def _validate_sorters_configuration(self, partition_keys: List[str], num_actual_partition_keys: int):
        if self.sorters and len(self.sorters) > 0:
            if any([sorter.name not in partition_keys for sorter in self.sorters]):
                raise ge_exceptions.PartitionerError(
                    f'''Partitioner "{self.name}" specifies one or more sort keys that do not appear among the
                configured partition keys.
                    '''
                )
            if len(partition_keys) < len(self.sorters):
                raise ge_exceptions.PartitionerError(
                    f'''Partitioner "{self.name}", configured with {len(partition_keys)} partition keys, matches
                    {num_actual_partition_keys} actual partition keys; this is fewer than number of sorters specified, which is
                    {len(self.sorters)}.
                    '''
                )

    def _validate_runtime_keys_configuration(self, runtime_keys: List[str]):
        if runtime_keys and len(runtime_keys) > 0:
            if not (self.runtime_keys and set(runtime_keys) <= set(self.runtime_keys)):
                raise ge_exceptions.PartitionerError(
                    f'''Partitioner "{self.name}" was invoked with one or more runtime keys that do not appear among the
configured runtime keys.
                    '''
                )

    def _normalize_directory_path(self, dir_path: str) -> str:
        # If directory is a relative path, interpret it as relative to the data context's
        # context root directory (parent directory of great_expectation dir)
        if Path(dir_path).is_absolute() or self._data_context_root_directory is None:
            return dir_path
        else:
            return Path(self._data_context_root_directory).joinpath(dir_path)

    # def _get_file_paths_for_data_asset(self, data_asset_name: str = None) -> list:
    #     """
    #     Returns:
    #         paths (list)
    #     """
    #     base_directory: str
    #     glob_directive: str

    #     data_asset_directives: dict = self._get_data_asset_directives(data_asset_name=data_asset_name)
    #     base_directory = data_asset_directives["base_directory"]
    #     glob_directive = data_asset_directives["glob_directive"]

    #     if Path(base_directory).is_dir():
    #         path_list: list
    #         if glob_directive:
    #             path_list = self._get_data_reference_list()
    #         else:
    #             path_list = [
    #                 str(posix_path) for posix_path in self._get_valid_file_paths(base_directory=base_directory)
    #             ]

    #         # Trim paths to exclude the base_directory
    #         base_directory_len = len(str(base_directory))
    #         path_list = [path[base_directory_len:] for path in path_list]

    #         return self._verify_file_paths(path_list=path_list)
    #     raise ge_exceptions.DataConnectorError(f'Expected a directory, but path "{base_directory}" is not a directory.')

    # def _get_data_asset_directives(self, data_asset_name: str = None) -> dict:
    #     base_directory: str
    #     glob_directive: str
    #     if (
    #         data_asset_name is not None
    #         and isinstance(self.assets.get(data_asset_name), Asset)
    #     ):
    #         asset = self.assets[data_asset_name]
    #         base_directory = asset.base_directory
    #         if not base_directory:
    #             base_directory = self.base_directory
    #         base_directory = self._normalize_directory_path(dir_path=base_directory)

    #         glob_directive: str = asset.glob_directive
    #         if not glob_directive:
    #             glob_directive = self.glob_directive
    #     else:
    #         base_directory = self.base_directory
    #         glob_directive = self.glob_directive
    #     return {"base_directory": base_directory, "glob_directive": glob_directive}

    # @staticmethod
    # def _verify_file_paths(path_list: list) -> list:
    #     if not all(
    #         [not Path(path).is_dir() for path in path_list]
    #     ):
    #         raise ge_exceptions.DataConnectorError(
    #             "All paths for a configured data asset must be files (a directory was detected)."
    #         )
    #     return path_list

    # #NOTE Abe 20201015: This looks like dead code.
    # def _get_valid_file_paths(self, base_directory: str = None) -> list:
    #     if base_directory is None:
    #         base_directory = self.base_directory
    #     path_list: list = list(Path(base_directory).iterdir())
    #     for path in path_list:
    #         for extension in self.known_extensions:
    #             if path.endswith(extension) and not path.startswith("."):
    #                 path_list.append(path)
    #             elif Path(path).is_dir:
    #                 # Make sure there is at least one valid file inside the subdirectory.
    #                 subdir_path_list: list = self._get_valid_file_paths(base_directory=path)
    #                 if len(subdir_path_list) > 0:
    #                     path_list.append(subdir_path_list)
    #     return list(
    #         set(
    #             list(
    #                 itertools.chain.from_iterable(
    #                     [
    #                         element for element in path_list
    #                     ]
    #                 )
    #             )
    #         )
    #     )
    
    def _get_data_reference_list(self):
        globbed_paths = Path(self.base_directory).glob(self._glob_directive)
        paths = [
            os.path.relpath(str(posix_path), self.base_directory) for posix_path in globbed_paths
        ]
        return paths

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

        # TODO Will - convert to use batch_request_to_data_reference()
        # TODO Abe 20201018: This is an absolutely horrible way to get a path from a single partition_definition, but AFIACT it's the only method currently supported by our Partitioner.
        # Update: Abe 20201026: We should be able re-implement this now, with a call to RegexPartitioner.convert_batch_definition_to_data_reference
        # TODO: <Alex>This call is deprecated -- it leeds to _get_available_partitions() above, which was commented out.</Alex>
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
            "path": path
        }

    def get_data_reference_list_count(self):
        if self._data_references_cache == None:
            raise ValueError(f"data references cache for {self.__class__.__name__} {self.name} has not yet been populated.")

        total_references = 0
        for data_asset_name in self._data_references_cache:
            total_references += len(self._data_references_cache[data_asset_name])
            
        return  total_references

    def refresh_data_references_cache(
        self,
    ):
        """
        """
        #Map data_references to batch_definitions
        self._data_references_cache = {}

        for data_asset_name in self.get_available_data_asset_names():
            self._data_references_cache[data_asset_name] = {}

            for data_reference in self._get_data_reference_list():
                mapped_batch_definition_list = self._map_data_reference_to_batch_definition_list(
                    data_reference,
                    data_asset_name,
                )
                self._data_references_cache[data_asset_name][data_reference] = mapped_batch_definition_list

    def get_batch_definition_list_from_batch_request(
        self,
        batch_request: BatchRequest,
    ) -> List[BatchDefinition]:
        if batch_request.data_connector_name != self.name:
            raise ValueError(f"data_connector_name {batch_request.data_connector_name} does not match name {self.name}.")

        if self._data_references_cache == None:
            self.refresh_data_references_cache()
        
        batches = []
        for data_asset_name, sub_cache in self._data_references_cache.items():
            for data_reference, batch_definition in sub_cache.items():
                if batch_definition == None:
                    # The data_reference is unmatched.
                    continue
                if self._batch_definition_matches_batch_request(batch_definition[0], batch_request):
                    batches += batch_definition

        return batches

    def _map_data_reference_to_batch_definition_list(
        self,
        data_reference: Any,
        data_asset_name: str,
    ) -> List[BatchDefinition]:
    
        # TODO: Need better logic here
        regex_config = self._default_regex

        batch_request: BatchRequest = self.convert_data_reference_to_batch_request(
            data_reference=data_reference,
            pattern=regex_config["pattern"],
            group_names=regex_config["group_names"],
        )
        if batch_request is None:
            return None

        return [
            BatchDefinition(
                execution_environment_name=self.execution_environment_name,
                data_connector_name=self.name,
                data_asset_name=data_asset_name,
                partition_definition=batch_request.partition_request,
            )
        ]