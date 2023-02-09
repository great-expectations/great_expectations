from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, List, Optional

from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch_spec import PathBatchSpec, S3BatchSpec
from great_expectations.experimental.datasources.data_asset.data_connector.file_path_data_connector import (
    FilePathDataConnector,
)
from great_expectations.experimental.datasources.data_asset.data_connector.file_path_data_connector_util import (
    list_s3_keys,
    sanitize_prefix_for_s3,
)

try:
    import boto3
except ImportError:
    boto3 = None


if TYPE_CHECKING:
    from great_expectations.core.batch import BatchDefinition


logger = logging.getLogger(__name__)


@public_api
class S3DataConnector(FilePathDataConnector):
    """Extension of FilePathDataConnector used to connect to S3.


    Args:
        name (str): required name for DataConnector
        datasource_name (str): required name for datasource
        bucket (str): bucket for S3
        regex (dict): regex configuration for filtering data_references
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
        name: str,
        datasource_name: str,
        data_asset_name: str,
        execution_engine_name: str,
        bucket: str,
        regex: Optional[re.Pattern] = None,
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        boto3_options: Optional[dict] = None,
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: Optional[list] = None,
        # TODO: <Alex>ALEX</Alex>
    ) -> None:
        super().__init__(
            name=name,
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            execution_engine_name=execution_engine_name,
            regex=regex,
            # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
            # TODO: <Alex>ALEX</Alex>
            # sorters=sorters,
            # TODO: <Alex>ALEX</Alex>
        )

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

    def _get_data_reference_list(self) -> List[str]:
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

    def _get_full_file_path(self, path: str) -> str:
        template_arguments: dict = {
            "bucket": self._bucket,
            "path": path,
        }
        return self.resolve_data_reference(template_arguments=template_arguments)
