import logging
from typing import List, Optional

from great_expectations.compatibility import google
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch import BatchDefinition
from great_expectations.core.batch_spec import GCSBatchSpec, PathBatchSpec
from great_expectations.datasource.data_connector.inferred_asset_file_path_data_connector import (
    InferredAssetFilePathDataConnector,
)
from great_expectations.datasource.data_connector.util import list_gcs_keys
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


@public_api
class InferredAssetGCSDataConnector(InferredAssetFilePathDataConnector):
    """An Inferred Asset Data Connector used to connect to Google Cloud Storage (GCS).

    This Data Connector uses regular expressions to traverse through GCS buckets and implicitly
    determine Data Asset name. Please note that in order to maintain consistency with Google's official SDK,
    we utilize parameter names `bucket_or_name` and `max_results`. Since we convert these keys from YAML to Python and
    directly pass them in to the GCS connection object, maintaining consistency is necessary for proper usage.

    This DataConnector supports the following methods of authentication:
        1. Standard gcloud auth / GOOGLE_APPLICATION_CREDENTIALS environment variable workflow
        2. Manual creation of credentials from google.oauth2.service_account.Credentials.from_service_account_file
        3. Manual creation of credentials from google.oauth2.service_account.Credentials.from_service_account_info

    Much of the interaction is performed using a GCS Storage Client. Please refer to
    the `official Google documentation <https://googleapis.dev/python/google-api-core/latest/auth.html>`_ for more
    information.

    Args:
        name: The name of the Data Connector.
        datasource_name: The name of this Data Connector's Datasource.
        bucket_or_name: Bucket name for Google Cloud Storage.
        execution_engine: The Execution Engine object to used by this Data Connector to read the data.
        default_regex: A regex configuration for filtering data references. The dict can include a regex `pattern` and
            a list of `group_names` for capture groups.
        sorters: A list of sorters for sorting data references.
        prefix: Infer as Data Assets only blobs that begin with this prefix.
        delimiter: When included, will remove any prefix up to the delimiter from the inferred Data Asset names.
        max_results: Max blob filepaths to return.
        gcs_options: Options passed to the GCS Storage Client.
        batch_spec_passthrough: Dictionary with keys that will be added directly to the batch spec.
        id: The unique identifier for this Data Connector used when running in cloud mode.
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        datasource_name: str,
        bucket_or_name: str,
        execution_engine: Optional[ExecutionEngine] = None,
        default_regex: Optional[dict] = None,
        sorters: Optional[list] = None,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        max_results: Optional[int] = None,
        gcs_options: Optional[dict] = None,
        batch_spec_passthrough: Optional[dict] = None,
        id: Optional[str] = None,
    ) -> None:
        logger.debug(f'Constructing InferredAssetGCSDataConnector "{name}".')

        super().__init__(
            name=name,
            id=id,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            default_regex=default_regex,
            sorters=sorters,
            batch_spec_passthrough=batch_spec_passthrough,
        )

        self._bucket_or_name = bucket_or_name
        self._prefix = prefix
        self._delimiter = delimiter
        self._max_results = max_results

        if gcs_options is None:
            gcs_options = {}

        try:
            credentials = None  # If configured with gcloud CLI / env vars
            if "filename" in gcs_options:
                filename = gcs_options.pop("filename")
                credentials = (
                    google.service_account.Credentials.from_service_account_file(
                        filename=filename
                    )
                )
            elif "info" in gcs_options:
                info = gcs_options.pop("info")
                credentials = (
                    google.service_account.Credentials.from_service_account_info(
                        info=info
                    )
                )
            self._gcs = google.storage.Client(credentials=credentials, **gcs_options)
        except (TypeError, AttributeError, ModuleNotFoundError):
            raise ImportError(
                "Unable to load GCS Client (it is required for InferredAssetGCSDataConnector)."
            )

    def build_batch_spec(self, batch_definition: BatchDefinition) -> GCSBatchSpec:
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
        return GCSBatchSpec(batch_spec)

    def _get_data_reference_list(
        self, data_asset_name: Optional[str] = None
    ) -> List[str]:
        query_options: dict = {
            "bucket_or_name": self._bucket_or_name,
            "prefix": self._prefix,
            "delimiter": self._delimiter,
            "max_results": self._max_results,
        }

        path_list: List[str] = [
            key
            for key in list_gcs_keys(
                gcs_client=self._gcs,
                query_options=query_options,
                recursive=True,
            )
        ]
        return path_list

    def _get_full_file_path(
        self, path: str, data_asset_name: Optional[str] = None
    ) -> str:
        # data_asset_name isn't used in this method.
        # It's only kept for compatibility with parent methods.
        template_arguments: dict = {
            "bucket_or_name": self._bucket_or_name,
            "path": path,
        }
        return self.resolve_data_reference(template_arguments=template_arguments)
