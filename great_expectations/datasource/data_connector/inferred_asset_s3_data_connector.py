import logging
from typing import List, Optional

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import aws
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch import BatchDefinition
from great_expectations.core.batch_spec import PathBatchSpec, S3BatchSpec
from great_expectations.datasource.data_connector.inferred_asset_file_path_data_connector import (
    InferredAssetFilePathDataConnector,
)
from great_expectations.datasource.data_connector.util import (
    list_s3_keys,
    sanitize_prefix_for_gcs_and_s3,
)
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)

INVALID_S3_CHARS = ["*"]


@public_api
class InferredAssetS3DataConnector(InferredAssetFilePathDataConnector):
    """An Inferred Asset Data Connector used to connect to AWS Simple Storage Service (S3).

    This Data Connector uses regular expressions to traverse through S3 buckets and implicitly
    determine Data Asset name.

    Much of the interaction is performed using the `boto3` S3 client. Please refer to
    the `official AWS documentation <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`_ for
    more information.

    Args:
        name: The name of the Data Connector.
        datasource_name: The name of this Data Connector's Datasource.
        bucket: The S3 bucket name.
        execution_engine: The Execution Engine object to used by this Data Connector to read the data.
        default_regex: A regex configuration for filtering data references. The dict can include a regex `pattern` and
            a list of `group_names` for capture groups.
        sorters: A list of sorters for sorting data references.
        prefix: Infer as Data Assets only blobs that begin with this prefix.
        delimiter: When included, will remove any prefix up to the delimiter from the inferred Data Asset names.
        max_keys: Max blob filepaths to return.
        boto3_options: Options passed to the S3 client.
        batch_spec_passthrough: Dictionary with keys that will be added directly to the batch spec.
        id: The unique identifier for this Data Connector used when running in cloud mode.
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        datasource_name: str,
        bucket: str,
        execution_engine: Optional[ExecutionEngine] = None,
        default_regex: Optional[dict] = None,
        sorters: Optional[list] = None,
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        boto3_options: Optional[dict] = None,
        batch_spec_passthrough: Optional[dict] = None,
        id: Optional[str] = None,
    ) -> None:
        logger.debug(f'Constructing InferredAssetS3DataConnector "{name}".')

        super().__init__(
            name=name,
            id=id,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            default_regex=default_regex,
            sorters=sorters,
            batch_spec_passthrough=batch_spec_passthrough,
        )

        self._bucket = bucket
        self._prefix = sanitize_prefix_for_gcs_and_s3(text=prefix)
        self._delimiter = delimiter
        self._max_keys = max_keys

        if boto3_options is None:
            boto3_options = {}

        try:
            self._s3 = aws.boto3.client("s3", **boto3_options)
        except (TypeError, AttributeError):
            raise ImportError(
                "Unable to load boto3 (it is required for InferredAssetS3DataConnector)."
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

    def _get_data_reference_list(
        self, data_asset_name: Optional[str] = None
    ) -> List[str]:
        """
        List objects in the underlying data store to create a list of data_references.

        This method is used to refresh the cache.
        """
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
                recursive=True,
            )
        ]
        return path_list

    def _get_full_file_path(
        self,
        path: str,
        data_asset_name: Optional[str] = None,
    ) -> str:
        # data_asset_name isn't used in this method.
        # It's only kept for compatibility with parent methods.
        _check_valid_s3_path(path)
        template_arguments: dict = {
            "bucket": self._bucket,
            "path": path,
        }
        return self.resolve_data_reference(template_arguments=template_arguments)


def _check_valid_s3_path(
    path: str,
) -> None:
    """Performs a basic check for validity of the S3 path"""
    bad_chars: list = [c for c in INVALID_S3_CHARS if c in path]
    if len(bad_chars) > 0:
        msg: str = (
            f"The parsed S3 path={path} contains the invalid characters {bad_chars}."
            "Please make sure your regex is correct and characters are escaped."
        )
        if "*" in bad_chars:
            msg += "Note: `*` is internally used to replace the regex for `.`."
        raise gx_exceptions.ParserError(msg)
