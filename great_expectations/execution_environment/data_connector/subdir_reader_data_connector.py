import logging
import os
import warnings

from great_expectations.exceptions import BatchSpecError
from great_expectations.execution_environment.data_connector.data_connector import \
    DataConnector
from great_expectations.execution_environment.types import PathBatchSpec

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


class SubdirReaderDataConnector(DataConnector):
    """The SubdirReaderDataConnector inspects a filesystem and produces path-based batch_spec.

    SubdirReaderDataConnector recognizes data assets using two criteria:
      - for files directly in 'base_directory' with recognized extensions (.csv, .tsv, .parquet, .xls, .xlsx, .json),
        it uses the name of the file without the extension
      - for other files or directories in 'base_directory', is uses the file or directory name

    SubdirReaderDataConnector sees all files inside a directory of base_directory as batches of one datasource.

    SubdirReaderDataConnector can also include configured reader_options which will be added to batch_spec generated
    by this generator.
    """

    _default_reader_options = {}
    recognized_batch_definition_keys = {
        "data_asset_name",
        "partition_id",
        "execution_environment",
        "data_connector",
        "batch_spec_passthrough",
        "limit",
    }

    def __init__(
        self,
        name="default",
        execution_environment=None,
        base_directory="/data",
        reader_options=None,
        known_extensions=None,
        reader_method=None,
        batch_definition_defaults=None,
    ):
        super().__init__(
            name,
            execution_environment=execution_environment,
            batch_definition_defaults=batch_definition_defaults,
        )
        if reader_options is None:
            reader_options = self._default_reader_options

        if known_extensions is None:
            known_extensions = KNOWN_EXTENSIONS

        self._known_extensions = known_extensions
        self._reader_options = reader_options
        self._reader_method = reader_method
        self._base_directory = base_directory

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

    def get_available_data_asset_names(self):
        if not os.path.isdir(self.base_directory):
            return {"names": [], "is_complete_list": True}
        known_assets = self._get_valid_file_options(base_directory=self.base_directory)
        return {"names": known_assets, "is_complete_list": True}

    # TODO: deprecate generator_asset argument
    def get_available_partition_ids(self, generator_asset=None, data_asset_name=None):
        assert (generator_asset and not data_asset_name) or (
            not generator_asset and data_asset_name
        ), "Please provide either generator_asset or data_asset_name."
        if generator_asset:
            warnings.warn(
                "The 'generator_asset' argument will be deprecated and renamed to 'data_asset_name'. "
                "Please update code accordingly.",
                DeprecationWarning,
            )
            data_asset_name = generator_asset

        # If the generator asset names a single known *file*, return ONLY that
        for extension in self.known_extensions:
            if os.path.isfile(
                os.path.join(self.base_directory, data_asset_name + extension)
            ):
                return [data_asset_name]
        if os.path.isfile(os.path.join(self.base_directory, data_asset_name)):
            return [data_asset_name]

        # Otherwise, subdir files are partition ids
        return [
            path
            for (path, type) in self._get_valid_file_options(
                base_directory=os.path.join(self.base_directory, data_asset_name)
            )
        ]

    def _build_batch_spec(self, batch_definition, batch_spec=None):
        """

        Args:
            batch_definition:

        Returns:
            batch_spec

        """
        batch_spec = batch_spec or {}

        try:
            data_asset_name = batch_definition.pop("data_asset_name")
        except KeyError:
            raise BatchSpecError(
                "Unable to build BatchKwargs: no data_asset_name provided in batch_definition.",
                batch_spec=batch_definition,
            )

        if "partition_id" in batch_definition:
            partition_id = batch_definition.pop("partition_id")
            # Find the path
            path = None
            for extension in self.known_extensions:
                if os.path.isfile(
                    os.path.join(
                        self.base_directory, data_asset_name, partition_id + extension
                    )
                ):
                    path = os.path.join(
                        self.base_directory, data_asset_name, partition_id + extension
                    )

            if path is None:
                logger.warning(
                    "Unable to find path with the provided partition; searching for asset-name partitions."
                )
                # Fall through to this case in the event that there is not a subdir available, or if partition_id was
                # not provided
                if os.path.isfile(os.path.join(self.base_directory, data_asset_name)):
                    path = os.path.join(self.base_directory, data_asset_name)

                for extension in self.known_extensions:
                    if os.path.isfile(
                        os.path.join(self.base_directory, data_asset_name + extension)
                    ):
                        path = os.path.join(
                            self.base_directory, data_asset_name + extension
                        )

            if path is None:
                raise BatchSpecError(
                    "Unable to build batch kwargs from for asset '%s'"
                    % data_asset_name,
                    batch_definition,
                )
            return self._build_batch_spec_from_path(path, batch_definition, batch_spec)

        else:
            return self.yield_batch_spec(
                data_asset_name=data_asset_name,
                batch_definition=batch_definition,
                batch_spec=batch_spec,
            )

    def _get_valid_file_options(self, base_directory=None):
        valid_options = []
        if base_directory is None:
            base_directory = self.base_directory
        file_options = os.listdir(base_directory)
        for file_option in file_options:
            for extension in self.known_extensions:
                if (
                    file_option.endswith(extension)
                    and not file_option.startswith(".")
                    and (file_option[: -len(extension)], "file") not in valid_options
                ):
                    valid_options.append((file_option[: -len(extension)], "file"))
                elif os.path.isdir(os.path.join(self.base_directory, file_option)):
                    # Make sure there's at least one valid file inside the subdir
                    subdir_options = self._get_valid_file_options(
                        base_directory=os.path.join(base_directory, file_option)
                    )
                    if (
                        len(subdir_options) > 0
                        and (file_option, "directory") not in valid_options
                    ):
                        valid_options.append((file_option, "directory"))
        return valid_options

    def _get_iterator(self, data_asset_name, batch_definition, batch_spec):
        logger.debug(
            "Beginning SubdirReaderDataConnector _get_iterator for data_asset_name: %s"
            % data_asset_name
        )
        # If the data asset is a file, then return the path.
        # Otherwise, use files in a subdir as batches
        if os.path.isdir(os.path.join(self.base_directory, data_asset_name)):
            subdir_options = os.listdir(
                os.path.join(self.base_directory, data_asset_name)
            )
            path_list = []
            for file_option in subdir_options:
                for extension in self.known_extensions:
                    if file_option.endswith(extension) and not file_option.startswith(
                        "."
                    ):
                        path_list.append(
                            os.path.join(
                                self.base_directory, data_asset_name, file_option
                            )
                        )

            return self._build_batch_spec_path_iter(
                path_list, batch_definition, batch_spec
            )
        else:
            for extension in self.known_extensions:
                path = os.path.join(self.base_directory, data_asset_name + extension)
                if os.path.isfile(path):
                    return iter(
                        [
                            self._build_batch_spec_from_path(
                                path, batch_definition, batch_spec
                            )
                        ]
                    )
            # If we haven't returned yet, raise
            raise BatchSpecError(
                "No valid files found when searching {:s} using configured known_extensions: "
                "{:s} ".format(
                    os.path.join(self.base_directory, data_asset_name),
                    ", ".join(map(str, self.known_extensions)),
                ),
                batch_spec=PathBatchSpec(
                    path=os.path.join(self.base_directory, data_asset_name)
                ),
            )

    def _build_batch_spec_path_iter(self, path_list, batch_definition, batch_spec):
        for path in path_list:
            yield self._build_batch_spec_from_path(path, batch_definition, batch_spec)

    def _build_batch_spec_from_path(self, path, batch_definition, batch_spec):
        batch_spec["path"] = path
        batch_spec = self._execution_environment.execution_engine.process_batch_definition(
            batch_definition=batch_definition, batch_spec=batch_spec
        )

        return PathBatchSpec(batch_spec)
