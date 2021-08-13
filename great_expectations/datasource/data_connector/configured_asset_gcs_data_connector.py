import logging
import os
from typing import List, Optional

logger = logging.getLogger(__name__)

try:
    from google.cloud import storage
except ImportError:
    storage = None
    logger.debug(
        "Unable to load GCS connection object; install optional google dependency for support"
    )

from great_expectations.core.batch import BatchDefinition
from great_expectations.core.batch_spec import GCSBatchSpec, PathBatchSpec
from great_expectations.datasource.data_connector import (
    ConfiguredAssetFilePathDataConnector,
)
from great_expectations.datasource.data_connector.asset import Asset
from great_expectations.datasource.data_connector.util import list_gcs_keys
from great_expectations.execution_engine import ExecutionEngine


class ConfiguredAssetGCSDataConnector(ConfiguredAssetFilePathDataConnector):
    def __init__(
        self,
        name: str,
        datasource_name: str,
        bucket: str,
        assets: dict,
        execution_engine: Optional[ExecutionEngine] = None,
        default_regex: Optional[dict] = None,
        sorters: Optional[list] = None,
        prefix: str = "",
        delimiter: str = "/",
        gcs_options: Optional[dict] = None,
        batch_spec_passthrough: Optional[dict] = None,
    ):
        logger.debug(f'Constructing ConfiguredAssetGCSDataConnector "{name}".')

        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            assets=assets,
            default_regex=default_regex,
            sorters=sorters,
            batch_spec_passthrough=batch_spec_passthrough,
        )
        self._bucket = bucket
        self._prefix = os.path.join(prefix, "")
        self._delimiter = delimiter

        if gcs_options is None:
            gcs_options = {}

        try:
            self._gcs = storage.Client(**gcs_options)
        except (TypeError, AttributeError):
            raise ImportError(
                "Unable to load GCS Client (it is required for ConfiguredAssetGCSDataConnector)."
            )

    def build_batch_spec(self, batch_definition: BatchDefinition) -> GCSBatchSpec:
        batch_spec: PathBatchSpec = super().build_batch_spec(
            batch_definition=batch_definition
        )
        return GCSBatchSpec(batch_spec)

    def _get_data_reference_list_for_asset(self, asset: Optional[Asset]) -> List[str]:
        # TODO(cdkini): Change to GCS-specific usage
        query_options: dict = {
            "Bucket": self._bucket,
            "Prefix": self._prefix,
            "Delimiter": self._delimiter,
        }

        # TODO(cdkini): Change to GCS-specific usage
        if asset is not None:
            if asset.bucket:
                query_options["Bucket"] = asset.bucket
            if asset.prefix:
                query_options["Prefix"] = asset.prefix
            if asset.delimiter:
                query_options["Delimiter"] = asset.delimiter

        path_list: List[str] = [
            key
            for key in list_gcs_keys(
                gcs=self._gcs,
                query_options=query_options,
                recursive=False,
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
        return f"gs://{os.path.join(self._bucket, path)}"
