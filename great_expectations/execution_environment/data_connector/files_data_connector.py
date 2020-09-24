from pathlib import Path
import itertools
import logging
from typing import List

from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.types import PathBatchSpec
from great_expectations.exceptions import BatchSpecError

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
        name,
        execution_environment,
        partitioners=None,
        default_partitioner=None,
        assets=None,
        batch_definition_defaults=None,
        known_extensions=None,
        reader_options=None,
        reader_method=None,
        **kwargs
    ):
        # TODO: <Alex>Does "known_extensions" need to be in Configuration?</Alex>
        logger.debug("Constructing FilesDataConnector {!r}".format(name))
        super().__init__(
            name=name,
            execution_environment=execution_environment,
            partitioners=partitioners,
            default_partitioner=default_partitioner,
            assets=assets,
            batch_definition_defaults=batch_definition_defaults,
            **kwargs
        )

        if known_extensions is None:
            known_extensions = KNOWN_EXTENSIONS
        self._known_extensions = known_extensions

        if reader_options is None:
            reader_options = self._default_reader_options
        self._reader_options = reader_options

        self._reader_method = reader_method

        self._base_directory = self.config_params["base_directory"]

    @property
    def reader_options(self):
        return self._reader_options

    @property
    def known_extensions(self):
        return self._known_extensions

    @property
    def reader_method(self):
        return self._reader_method

    @property
    def base_directory(self):
        return self._normalize_directory_path(dir_path=self._base_directory)

    def get_available_data_asset_names(self) -> list:
        if self.assets:
            return list(self.assets.keys())
        return [Path(path).stem for path in self._get_file_paths_for_data_asset(data_asset_name=None)]

    def get_available_partitions(self, partition_name: str = None, data_asset_name: str = None) -> List[Partition]:
        partitioner_name: str
        partitioner: Partitioner
        data_asset_config_exists: bool = data_asset_name and self.assets and self.assets.get(data_asset_name)
        if data_asset_config_exists and self.assets[data_asset_name].get("partitioner"):
            partitioner_name = self.assets[data_asset_name]["partitioner"]
        else:
            partitioner_name = self.default_partitioner
        partitioner = self.get_partitioner(name=partitioner_name)
        if data_asset_config_exists:
            partitioner.auto_discover_assets = False
        else:
            partitioner.auto_discover_assets = True
        partitioner.paths = self._get_file_paths_for_data_asset(data_asset_name=data_asset_name)
        return partitioner.get_available_partitions(partition_name=partition_name, data_asset_name=data_asset_name)

    def _normalize_directory_path(self, dir_path: str) -> str:
        # If directory is a relative path, interpret it as relative to the data context's
        # context root directory (parent directory of great_expectation dir)
        if Path(dir_path).is_absolute() or self._execution_environment.data_context is None:
            return dir_path
        else:
            return Path(self._execution_environment.data_context.root_directory).joinpath(dir_path)

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
            return self._verify_file_paths(path_list=path_list)
        raise ValueError(f'Expected a directory, but path "{base_directory}" is not a directory.')

    def _get_data_asset_directives(self, data_asset_name: str = None) -> dict:
        glob_directive: str
        base_directory: str
        path_list: list
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
            glob_directive = self.config_params.get("glob_directive")
        return {"base_directory": base_directory, "glob_directive": glob_directive}

    @staticmethod
    def _verify_file_paths(path_list: list) -> list:
        if not all(
            [not Path(path).is_dir() for path in path_list]
        ):
            raise ValueError("All paths for a configured data asset must be files (a directory was detected).")
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

    def _build_batch_spec(self, batch_definition: dict, batch_spec: dict = None) -> dict:
        """
        Args:
            batch_definition:
            batch_spec:
        Returns:
            batch_spec
        """
        if batch_spec is None:
            batch_spec = {}

        try:
            data_asset_name: str = batch_definition.pop("data_asset_name")
        except KeyError:
            raise BatchSpecError(
                message="Unable to build BatchKwargs: no data_asset_name provided in batch_definition."
            )

        partition_name: str = batch_definition.get("partition_name")
        # TODO: <Alex>If partition_name is not specified in batch_definition, then assume "latest" (or "most recent" as defined by the first element in the sorted list of partitions).</Alex>
        partitions: List[Partition] = self.get_available_partitions(
            partition_name=partition_name, data_asset_name=data_asset_name
        )
        if len(partitions) == 0:
            raise BatchSpecError(message=f'Unable to build batch_spec for data asset "{data_asset_name}".')
        # TODO: <Alex>If the list has multiple elements, we are using the first one (TBD/TODO multifile config)</Alex>
        path: str = str(partitions[0].source)
        return self._build_batch_spec_from_path(path, batch_definition, batch_spec)

    def _build_batch_spec_from_path(self, path: str, batch_definition: dict, batch_spec: dict) -> PathBatchSpec:
        batch_spec["path"] = path
        batch_spec = self._execution_environment.execution_engine.process_batch_definition(
            batch_definition=batch_definition, batch_spec=batch_spec
        )
        return PathBatchSpec(batch_spec)
