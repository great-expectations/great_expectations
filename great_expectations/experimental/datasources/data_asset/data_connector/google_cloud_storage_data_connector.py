from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Callable, List, Optional

from great_expectations.core.batch_spec import GCSBatchSpec, PathBatchSpec
from great_expectations.datasource.data_connector.util import (
    list_gcs_keys,
)
from great_expectations.experimental.datasources.data_asset.data_connector import (
    FilePathDataConnector,
)

if TYPE_CHECKING:
    from google.cloud.storage.client import Client as GCSClient

    from great_expectations.core.batch import BatchDefinition


logger = logging.getLogger(__name__)


class GoogleCloudStorageDataConnector(FilePathDataConnector):
    """Extension of FilePathDataConnector used to connect to Google Cloud Storage (GCS).

    Args:
        datasource_name: The name of the Datasource associated with this DataConnector instance
        data_asset_name: The name of the DataAsset using this DataConnector instance
        batching_regex: A regex pattern for partitioning data references
        gcs_client: Reference to instantiated Google Cloud Storage client handle
        bucket_or_name (str): bucket name for Google Cloud Storage
        prefix (str): GCS prefix
        delimiter (str): GCS delimiter
        max_results (int): max blob filepaths to return
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters (list): optional list of sorters for sorting data_references
        file_path_template_map_fn: Format function mapping path to fully-qualified resource on network file storage
        # TODO: <Alex>ALEX</Alex>
    """

    def __init__(
        self,
        datasource_name: str,
        data_asset_name: str,
        batching_regex: re.Pattern,
        # TODO: <Alex>ALEX</Alex>
        gcs_client: GCSClient,
        bucket_or_name: str,
        prefix: str = "",
        delimiter: str = "/",
        max_results: Optional[int] = None,
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: Optional[list] = None,
        # TODO: <Alex>ALEX</Alex>
        file_path_template_map_fn: Optional[Callable] = None,
    ) -> None:
        self._gcs_client: GCSClient = gcs_client

        self._bucket_or_name = bucket_or_name
        self._prefix = prefix
        self._delimiter = delimiter
        self._max_results = max_results

        super().__init__(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            batching_regex=batching_regex,
            # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
            # TODO: <Alex>ALEX</Alex>
            # sorters=sorters,
            # TODO: <Alex>ALEX</Alex>
            file_path_template_map_fn=file_path_template_map_fn,
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

    # Interface Method
    def get_data_references(self) -> List[str]:
        query_options: dict = {
            "bucket_or_name": self._bucket_or_name,
            "prefix": self._prefix,
            "delimiter": self._delimiter,
            "max_results": self._max_results,
        }
        path_list: List[str] = list_gcs_keys(
            gcs_client=self._gcs_client,
            query_options=query_options,
            recursive=False,
        )
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
            "bucket_or_name": self._bucket_or_name,
            "path": path,
        }

        return self._file_path_template_map_fn(**template_arguments)
