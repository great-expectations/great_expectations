from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, List, Optional

from great_expectations.core.batch_spec import PathBatchSpec, S3BatchSpec
from great_expectations.datasource.data_connector.util import (
    list_s3_keys,
    sanitize_prefix_for_s3,
)
from great_expectations.experimental.datasources.data_asset.data_connector import (
    FilePathDataConnector,
)

try:
    import boto3
except ImportError:
    boto3 = None


if TYPE_CHECKING:
    from great_expectations.core.batch import BatchDefinition


logger = logging.getLogger(__name__)


class S3DataConnector(FilePathDataConnector):
    """Extension of FilePathDataConnector used to connect to S3.


    Args:
        datasource_name (str): required name for datasource
        bucket (str): bucket for S3
        batching_regex (dict): regex configuration for filtering data_references
        prefix (str): S3 prefix
        delimiter (str): S3 delimiter
        max_keys (int): S3 max_keys (default is 1000)
        boto3_options (dict): optional boto3 options
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters (list): optional list of sorters for sorting data_references
        # TODO: <Alex>ALEX</Alex>
    """

    def __init__(
        self,
        datasource_name: str,
        data_asset_name: str,
        bucket: str,
        batching_regex: re.Pattern,
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        boto3_options: Optional[dict] = None,
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: Optional[list] = None,
        # TODO: <Alex>ALEX</Alex>
    ) -> None:
        self._bucket = bucket
        self._prefix = sanitize_prefix_for_s3(prefix)
        self._delimiter = delimiter
        self._max_keys = max_keys

        if boto3_options is None:
            boto3_options = {}

        try:
            self._s3 = boto3.client("s3", **boto3_options)
        except (TypeError, AttributeError):
            raise ImportError(
                "Unable to load boto3 (it is required for ConfiguredAssetS3DataConnector)."
            )

        super().__init__(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            batching_regex=batching_regex,
            # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
            # TODO: <Alex>ALEX</Alex>
            # sorters=sorters,
            # TODO: <Alex>ALEX</Alex>
        )

    def build_batch_spec(self, batch_definition: BatchDefinition) -> S3BatchSpec:
        """
        Build BatchSpec from batch_definition by calling DataConnector's build_batch_spec function.

        Args:
            batch_definition (BatchDefinition): to be used to build batch_spec

        Returns:
            BatchSpec built from batch_definition
        """
        batch_spec: PathBatchSpec = super().build_batch_spec(
            batch_definition=batch_definition
        )
        return S3BatchSpec(batch_spec)

    # Interface Method
    def get_data_references(self) -> List[str]:
        query_options: dict = {
            "Bucket": self._bucket,
            "Prefix": self._prefix,
            "Delimiter": self._delimiter,
            "MaxKeys": self._max_keys,
        }
        path_list: List[str] = [
            key
            for key in list_s3_keys(
                s3=self._s3,
                query_options=query_options,
                iterator_dict={},
                recursive=False,
            )
        ]
        return path_list

    # Interface Method
    def _get_full_file_path(self, path: str) -> str:
        if self._file_path_template_map_fn is None:
            raise ValueError(
                f"""Converting file paths to fully-qualified object references for "{self.__class__.__name__}" \
requires "file_path_template_map_fn: Callable" to be set.
"""
            )

        template_arguments: dict = {
            "bucket": self._bucket,
            "path": path,
        }

        return self._file_path_template_map_fn(template_arguments=template_arguments)
