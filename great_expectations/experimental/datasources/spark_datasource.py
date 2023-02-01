from __future__ import annotations

import copy
import logging
import pathlib
import re
from typing import TYPE_CHECKING, Dict, List, Optional, Pattern, Tuple, Type, Union

import pydantic
from typing_extensions import ClassVar, Literal

import great_expectations.exceptions as ge_exceptions
from great_expectations.alias_types import PathStr
from great_expectations.core.batch_spec import PathBatchSpec
from great_expectations.experimental.datasources.interfaces import (
    Batch,
    BatchRequest,
    BatchRequestOptions,
    BatchSortersDefinition,
    DataAsset,
    Datasource,
    TestConnectionError,
)

if TYPE_CHECKING:
    from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class SparkDatasourceError(Exception):
    pass


class CSVSparkAsset(DataAsset):
    # Overridden inherited instance fields
    type: Literal["csv_spark"] = "csv_spark"

    # Spark specific attributes
    base_directory: pathlib.Path
    regex: Pattern

    # Internal attributes
    _unnamed_regex_param_prefix: str = pydantic.PrivateAttr(
        default="batch_request_param_"
    )

    def test_connection(self) -> None:
        """Test the connection for the CSVAsset.

        Raises:
            TestConnectionError
        """
        success = False
        for filepath in self.base_directory.iterdir():
            if self.regex.match(filepath.name):
                # if one file in the path matches the regex, we consider this asset valid
                success = True
                break
        if not success:
            raise TestConnectionError(
                f"No file at path: {self.base_directory} matched the regex: {self.regex}."
            )

    def _fully_specified_batch_requests_with_path(
        self, batch_request: BatchRequest
    ) -> List[Tuple[BatchRequest, pathlib.Path]]:
        """Generates a list fully specified batch requests from partial specified batch request

        Args:
            batch_request: A batch request

        Returns:
            A list of pairs (batch_request, path) where 'batch_request' is a fully specified
            batch request and 'path' is the path to the corresponding file on disk.
            This list will be empty if no files exist on disk that correspond to the input
            batch request.
        """
        option_to_group_id = self._option_name_to_regex_group_id()
        group_id_to_option = {v: k for k, v in option_to_group_id.items()}
        batch_requests_with_path: List[Tuple[BatchRequest, pathlib.Path]] = []

        all_files: List[pathlib.Path] = list(
            pathlib.Path(self.base_directory).iterdir()
        )

        file_name: pathlib.Path
        for file_name in all_files:
            match = self.regex.match(file_name.name)
            if match:
                # Create the batch request that would correlate to this regex match
                match_options = {}
                for group_id in range(1, self.regex.groups + 1):
                    match_options[group_id_to_option[group_id]] = match.group(group_id)
                # Determine if this file_name matches the batch_request
                allowed_match = True
                for key, value in batch_request.options.items():
                    if match_options[key] != value:
                        allowed_match = False
                        break
                if allowed_match:
                    batch_requests_with_path.append(
                        (
                            BatchRequest(
                                datasource_name=self.datasource.name,
                                data_asset_name=self.name,
                                options=match_options,
                            ),
                            self.base_directory / file_name,
                        )
                    )
                    logger.debug(f"Matching path: {self.base_directory / file_name}")
        if not batch_requests_with_path:
            logger.warning(
                f"Batch request {batch_request} corresponds to no data files."
            )
        return batch_requests_with_path

    def batch_request_options_template(
        self,
    ) -> BatchRequestOptions:
        template: BatchRequestOptions = self._option_name_to_regex_group_id()
        for k in template.keys():
            template[k] = None
        return template

    def get_batch_request(
        self, options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        # All regex values passed to options must be strings to be used in the regex
        option_names_to_group = self._option_name_to_regex_group_id()
        if options:
            for option, value in options.items():
                if option in option_names_to_group and not isinstance(value, str):
                    raise ge_exceptions.InvalidBatchRequestError(
                        f"All regex matching options must be strings. The value of '{option}' is "
                        f"not a string: {value}"
                    )
        return super().get_batch_request(options)

    def _option_name_to_regex_group_id(self) -> BatchRequestOptions:
        option_to_group: BatchRequestOptions = dict(self.regex.groupindex)
        named_groups = set(self.regex.groupindex.values())
        for i in range(1, self.regex.groups + 1):
            if i not in named_groups:
                option_to_group[f"{self._unnamed_regex_param_prefix}{i}"] = i
        return option_to_group

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        self._validate_batch_request(batch_request)
        batch_list: List[Batch] = []

        for request, path in self._fully_specified_batch_requests_with_path(
            batch_request
        ):
            batch_spec = PathBatchSpec(
                path=str(path),
                reader_options={
                    "header": True,
                    "inferSchema": True,
                },
            )
            execution_engine: ExecutionEngine = self.datasource.get_execution_engine()
            data, markers = execution_engine.get_batch_data_and_markers(
                batch_spec=batch_spec
            )

            # batch_definition (along with batch_spec and markers) is only here to satisfy a
            # legacy constraint when computing usage statistics in a validator. We hope to remove
            # it in the future.
            # imports are done inline to prevent a circular dependency with core/batch.py
            from great_expectations.core import IDDict
            from great_expectations.core.batch import BatchDefinition

            batch_definition = BatchDefinition(
                datasource_name=self.datasource.name,
                data_connector_name="experimental",
                data_asset_name=self.name,
                batch_identifiers=IDDict(request.options),
                batch_spec_passthrough=None,
            )

            batch_metadata = copy.deepcopy(request.options)
            batch_metadata["base_directory"] = path

            # Some pydantic annotations are postponed due to circular imports.
            # Batch.update_forward_refs() will set the annotations before we
            # instantiate the Batch class since we can import them in this scope.
            Batch.update_forward_refs()
            batch_list.append(
                Batch(
                    datasource=self.datasource,
                    data_asset=self,
                    batch_request=request,
                    data=data,
                    metadata=batch_metadata,
                    legacy_batch_markers=markers,
                    legacy_batch_spec=batch_spec,
                    legacy_batch_definition=batch_definition,
                )
            )
        self.sort_batches(batch_list)
        return batch_list


class SparkDatasource(Datasource):
    # class attrs
    asset_types: ClassVar[List[Type[DataAsset]]] = [CSVSparkAsset]

    # instance attributes
    type: Literal["spark"] = "spark"
    name: str
    assets: Dict[str, CSVSparkAsset] = {}

    @property
    def execution_engine_type(self) -> Type[ExecutionEngine]:
        """Return the SparkDFExecutionEngine unless the override is set"""
        from great_expectations.execution_engine.sparkdf_execution_engine import (
            SparkDFExecutionEngine,
        )

        return SparkDFExecutionEngine

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the SparkDatasource.

        Args:
            test_assets: If assets have been passed to the SparkDatasource, whether to test them as well.

        Raises:
            TestConnectionError
        """
        # Only self.assets can be tested for PandasDatasource
        if self.assets and test_assets:
            for asset in self.assets.values():
                asset.test_connection()

    def add_csv_asset(
        self,
        name: str,
        base_directory: PathStr,
        regex: Union[str, re.Pattern],
        order_by: Optional[BatchSortersDefinition] = None,
    ) -> CSVSparkAsset:
        """Adds a csv asset to this Spark datasource

        Args:
            name: The name of the csv asset
            base_directory: base directory path, relative to which CSV file paths will be collected
            regex: regex pattern that matches csv filenames that is used to label the batches
            order_by: sorting directive via either List[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
        """
        asset = CSVSparkAsset(
            name=name,
            base_directory=base_directory,  # type: ignore[arg-type]  # str will be coerced to Path
            regex=regex,  # type: ignore[arg-type]  # str with will coerced to Pattern
            order_by=order_by or [],  # type: ignore[arg-type]  # coerce list[str]
        )
        return self.add_asset(asset)
