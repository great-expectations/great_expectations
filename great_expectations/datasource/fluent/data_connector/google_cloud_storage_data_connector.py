from __future__ import annotations

import logging
import re
import warnings
from typing import TYPE_CHECKING, Callable, ClassVar, List, Optional, Type

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.batch_spec import GCSBatchSpec, PathBatchSpec
from great_expectations.datasource.fluent.data_connector import (
    FilePathDataConnector,
)
from great_expectations.datasource.fluent.data_connector.file_path_data_connector import (
    sanitize_prefix_for_gcs_and_s3,
)

if TYPE_CHECKING:
    from great_expectations.compatibility import google
    from great_expectations.core.batch import LegacyBatchDefinition


logger = logging.getLogger(__name__)


class _GCSOptions(pydantic.BaseModel):
    gcs_prefix: str = ""
    gcs_delimiter: str = "/"
    gcs_max_results: int = 1000
    gcs_recursive_file_discovery: bool = False


class GoogleCloudStorageDataConnector(FilePathDataConnector):
    """Extension of FilePathDataConnector used to connect to Google Cloud Storage (GCS).

    Args:
        datasource_name: The name of the Datasource associated with this DataConnector instance
        data_asset_name: The name of the DataAsset using this DataConnector instance
        gcs_client: Reference to instantiated Google Cloud Storage client handle
        bucket_or_name (str): bucket name for Google Cloud Storage
        prefix (str): GCS prefix
        delimiter (str): GCS delimiter
        max_results (int): max blob filepaths to return
        recursive_file_discovery (bool): Flag to indicate if files should be searched recursively from subfolders
        file_path_template_map_fn: Format function mapping path to fully-qualified resource on GCS
    """  # noqa: E501

    asset_level_option_keys: ClassVar[tuple[str, ...]] = (
        "gcs_prefix",
        "gcs_delimiter",
        "gcs_max_results",
        "gcs_recursive_file_discovery",
    )
    asset_options_type: ClassVar[Type[_GCSOptions]] = _GCSOptions

    def __init__(  # noqa: PLR0913
        self,
        datasource_name: str,
        data_asset_name: str,
        gcs_client: google.Client,
        bucket_or_name: str,
        prefix: str = "",
        delimiter: str = "/",
        max_results: Optional[int] = None,
        recursive_file_discovery: bool = False,
        file_path_template_map_fn: Optional[Callable] = None,
    ) -> None:
        self._gcs_client: google.Client = gcs_client

        self._bucket_or_name = bucket_or_name

        self._prefix: str = prefix
        self._sanitized_prefix: str = sanitize_prefix_for_gcs_and_s3(text=prefix)

        self._delimiter = delimiter
        self._max_results = max_results

        self._recursive_file_discovery = recursive_file_discovery

        super().__init__(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            file_path_template_map_fn=file_path_template_map_fn,
        )

    @classmethod
    def build_data_connector(  # noqa: PLR0913
        cls,
        datasource_name: str,
        data_asset_name: str,
        gcs_client: google.Client,
        bucket_or_name: str,
        prefix: str = "",
        delimiter: str = "/",
        max_results: Optional[int] = None,
        recursive_file_discovery: bool = False,
        file_path_template_map_fn: Optional[Callable] = None,
    ) -> GoogleCloudStorageDataConnector:
        """Builds "GoogleCloudStorageDataConnector", which links named DataAsset to Google Cloud Storage.

        Args:
            datasource_name: The name of the Datasource associated with this "GoogleCloudStorageDataConnector" instance
            data_asset_name: The name of the DataAsset using this "GoogleCloudStorageDataConnector" instance
            gcs_client: Reference to instantiated Google Cloud Storage client handle
            bucket_or_name: bucket name for Google Cloud Storage
            prefix: GCS prefix
            delimiter: GCS delimiter
            recursive_file_discovery: Flag to indicate if files should be searched recursively from subfolders
            max_results: max blob filepaths to return
            file_path_template_map_fn: Format function mapping path to fully-qualified resource on GCS

        Returns:
            Instantiated "GoogleCloudStorageDataConnector" object
        """  # noqa: E501
        return GoogleCloudStorageDataConnector(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            gcs_client=gcs_client,
            bucket_or_name=bucket_or_name,
            prefix=prefix,
            delimiter=delimiter,
            max_results=max_results,
            recursive_file_discovery=recursive_file_discovery,
            file_path_template_map_fn=file_path_template_map_fn,
        )

    @classmethod
    def build_test_connection_error_message(
        cls,
        data_asset_name: str,
        bucket_or_name: str,
        prefix: str = "",
        delimiter: str = "/",
        recursive_file_discovery: bool = False,
    ) -> str:
        """Builds helpful error message for reporting issues when linking named DataAsset to Google Cloud Storage.

        Args:
            data_asset_name: The name of the DataAsset using this "GoogleCloudStorageDataConnector" instance
            bucket_or_name: bucket name for Google Cloud Storage
            prefix: GCS prefix
            delimiter: GCS delimiter
            recursive_file_discovery: Flag to indicate if files should be searched recursively from subfolders

        Returns:
            Customized error message
        """  # noqa: E501
        test_connection_error_message_template: str = 'No file in bucket "{bucket_or_name}" with prefix "{prefix}" and recursive file discovery set to "{recursive_file_discovery}" found using delimiter "{delimiter}" for DataAsset "{data_asset_name}".'  # noqa: E501
        return test_connection_error_message_template.format(
            **{
                "data_asset_name": data_asset_name,
                "bucket_or_name": bucket_or_name,
                "prefix": prefix,
                "delimiter": delimiter,
                "recursive_file_discovery": recursive_file_discovery,
            }
        )

    @override
    def build_batch_spec(self, batch_definition: LegacyBatchDefinition) -> GCSBatchSpec:
        """
        Build BatchSpec from batch_definition by calling DataConnector's build_batch_spec function.

        Args:
            batch_definition (LegacyBatchDefinition): to be used to build batch_spec

        Returns:
            BatchSpec built from batch_definition
        """
        batch_spec: PathBatchSpec = super().build_batch_spec(batch_definition=batch_definition)
        return GCSBatchSpec(batch_spec)

    # Interface Method
    @override
    def get_data_references(self) -> List[str]:
        query_options: dict = {
            "bucket_or_name": self._bucket_or_name,
            "prefix": self._sanitized_prefix,
            "delimiter": self._delimiter,
            "max_results": self._max_results,
        }
        path_list: List[str] = list_gcs_keys(
            gcs_client=self._gcs_client,
            query_options=query_options,
            recursive=self._recursive_file_discovery,
        )
        return path_list

    # Interface Method
    @override
    def _get_full_file_path(self, path: str) -> str:
        if self._file_path_template_map_fn is None:
            raise ValueError(  # noqa: TRY003
                f"""Converting file paths to fully-qualified object references for "{self.__class__.__name__}" \
requires "file_path_template_map_fn: Callable" to be set.
"""  # noqa: E501
            )

        template_arguments: dict = {
            "bucket_or_name": self._bucket_or_name,
            "path": path,
        }

        return self._file_path_template_map_fn(**template_arguments)

    @override
    def _preprocess_batching_regex(self, regex: re.Pattern) -> re.Pattern:
        regex = re.compile(f"{re.escape(self._sanitized_prefix)}{regex.pattern}")
        return super()._preprocess_batching_regex(regex=regex)


def list_gcs_keys(
    gcs_client,
    query_options: dict,
    recursive: bool = False,
) -> List[str]:
    """
    Utilizes the GCS connection object to retrieve blob names based on user-provided criteria.

    For InferredAssetGCSDataConnector, we take `bucket_or_name` and `prefix` and search for files using RegEx at and below the level
    specified by those parameters. However, for ConfiguredAssetGCSDataConnector, we take `bucket_or_name` and `prefix` and
    search for files using RegEx only at the level specified by that bucket and prefix.

    This restriction for the ConfiguredAssetGCSDataConnector is needed because paths on GCS are comprised not only the leaf file name
    but the full path that includes both the prefix and the file name. Otherwise, in the situations where multiple data assets
    share levels of a directory tree, matching files to data assets will not be possible due to the path ambiguity.

    Please note that the SDK's `list_blobs` method takes in a `delimiter` key that drastically alters the traversal of a given bucket:
        - If a delimiter is not set (default), the traversal is recursive and the output will contain all blobs in the current directory
          as well as those in any nested directories.
        - If a delimiter is set, the traversal will continue until that value is seen; as the default is "/", traversal will be scoped
          within the current directory and end before visiting nested directories.

    In order to provide users with finer control of their config while also ensuring output that is in line with the `recursive` arg,
    we deem it appropriate to manually override the value of the delimiter only in cases where it is absolutely necessary.

    Args:
        gcs_client (storage.Client): GCS connnection object responsible for accessing bucket
        query_options (dict): GCS query attributes ("bucket_or_name", "prefix", "delimiter", "max_results")
        recursive (bool): True for InferredAssetGCSDataConnector and False for ConfiguredAssetGCSDataConnector (see above)

    Returns:
        List of keys representing GCS file paths (as filtered by the `query_options` dict)
    """  # noqa: E501
    # Delimiter determines whether or not traversal of bucket is recursive
    # Manually set to appropriate default if not already set by user
    delimiter = query_options["delimiter"]
    if delimiter is None and not recursive:
        warnings.warn(
            'In order to access blobs with a ConfiguredAssetGCSDataConnector, \
            or with a Fluent datasource without enabling recursive file discovery, \
            the delimiter that has been passed to gcs_options in your config cannot be empty; \
            please note that the value is being set to the default "/" in order to work with the Google SDK.'  # noqa: E501
        )
        query_options["delimiter"] = "/"
    elif delimiter is not None and recursive:
        warnings.warn(
            "In order to access blobs with an InferredAssetGCSDataConnector, \
            or enabling recursive file discovery with a Fluent datasource, \
            the delimiter that has been passed to gcs_options in your config must be empty; \
            please note that the value is being set to None in order to work with the Google SDK."
        )
        query_options["delimiter"] = None

    keys: List[str] = []
    for blob in gcs_client.list_blobs(**query_options):
        name: str = blob.name
        if name.endswith("/"):  # GCS includes directories in blob output
            continue

        keys.append(name)

    return keys
