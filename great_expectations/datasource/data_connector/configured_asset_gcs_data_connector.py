import logging
from typing import List, Optional

from great_expectations.compatibility import google
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch import BatchDefinition
from great_expectations.core.batch_spec import GCSBatchSpec, PathBatchSpec
from great_expectations.datasource.data_connector.asset import Asset
from great_expectations.datasource.data_connector.configured_asset_file_path_data_connector import (
    ConfiguredAssetFilePathDataConnector,
)
from great_expectations.datasource.data_connector.util import list_gcs_keys
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


@public_api
class ConfiguredAssetGCSDataConnector(ConfiguredAssetFilePathDataConnector):
    """Extension of ConfiguredAssetFilePathDataConnector used to connect to GCS.

    A ConfiguredAssetGCSDataConnector requires an explicit specification of each DataAsset you want to connect to.
    This allows more fine-tuning, but also requires more setup. Please note that in order to maintain consistency
    with Google's official SDK, we utilize terms like "bucket_or_name" and "max_results". Since we convert these keys from YAML
    to Python and directly pass them in to the GCS connection object, maintaining consistency is necessary for proper usage.

    This DataConnector supports the following methods of authentication:
        1. Standard gcloud auth / GOOGLE_APPLICATION_CREDENTIALS environment variable workflow
        2. Manual creation of credentials from google.oauth2.service_account.Credentials.from_service_account_file
        3. Manual creation of credentials from google.oauth2.service_account.Credentials.from_service_account_info

    Args:
        name (str): required name for DataConnector
        datasource_name (str): required name for datasource
        bucket_or_name (str): bucket name for Google Cloud Storage
        assets (dict): dict of asset configuration (required for ConfiguredAssetDataConnector)
        execution_engine (ExecutionEngine): optional reference to ExecutionEngine
        default_regex (dict): optional regex configuration for filtering data_references
        sorters (list): optional list of sorters for sorting data_references
        prefix (str): GCS prefix
        delimiter (str): GCS delimiter
        max_results (int): max blob filepaths to return
        gcs_options (dict): wrapper object for optional GCS `**kwargs`
        batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        datasource_name: str,
        bucket_or_name: str,
        assets: dict,
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
        logger.debug(f'Constructing ConfiguredAssetGCSDataConnector "{name}".')

        super().__init__(
            name=name,
            id=id,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            assets=assets,
            default_regex=default_regex,
            sorters=sorters,
            batch_spec_passthrough=batch_spec_passthrough,
        )
        if gcs_options is None:
            gcs_options = {}

        self._bucket_or_name = bucket_or_name
        self._prefix = prefix
        self._delimiter = delimiter
        self._max_results = max_results

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
                "Unable to load GCS Client (it is required for ConfiguredAssetGCSDataConnector)."
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

    def _get_data_reference_list_for_asset(self, asset: Optional[Asset]) -> List[str]:
        query_options: dict = {
            "bucket_or_name": self._bucket_or_name,
            "prefix": self._prefix,
            "delimiter": self._delimiter,
            "max_results": self._max_results,
        }

        if asset is not None:
            if asset.bucket:
                query_options["bucket_or_name"] = asset.bucket_or_name
            if asset.prefix:
                query_options["prefix"] = asset.prefix
            if asset.delimiter:
                query_options["delimiter"] = asset.delimiter
            if asset.max_results:
                query_options["max_results"] = asset.max_results

        path_list: List[str] = [
            key
            for key in list_gcs_keys(
                gcs_client=self._gcs,
                query_options=query_options,
                recursive=False,
            )
        ]
        return path_list

    def _get_full_file_path_for_asset(
        self, path: str, asset: Optional[Asset] = None
    ) -> str:
        # asset isn't used in this method.
        # It's only kept for compatibility with parent methods.
        template_arguments: dict = {
            "bucket_or_name": self._bucket_or_name,
            "path": path,
        }
        return self.resolve_data_reference(template_arguments=template_arguments)
