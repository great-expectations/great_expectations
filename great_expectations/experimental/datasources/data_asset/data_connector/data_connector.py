import logging
from typing import Any, Callable, Dict, List, Optional

from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch import BatchDefinition, BatchRequestBase
from great_expectations.core.id_dict import BatchSpec
from great_expectations.core.util import AzureUrl, DBFSPath, GCSUrl, S3Url

logger = logging.getLogger(__name__)


class DataConnectorStorageDataReferenceResolver:
    DATA_CONNECTOR_NAME_TO_STORAGE_NAME_MAP: Dict[str, str] = {
        "InferredAssetS3DataConnector": "S3",
        "ConfiguredAssetS3DataConnector": "S3",
        "InferredAssetGCSDataConnector": "GCS",
        "ConfiguredAssetGCSDataConnector": "GCS",
        "InferredAssetAzureDataConnector": "ABS",
        "ConfiguredAssetAzureDataConnector": "ABS",
        "InferredAssetDBFSDataConnector": "DBFS",
        "ConfiguredAssetDBFSDataConnector": "DBFS",
    }
    STORAGE_NAME_EXECUTION_ENGINE_NAME_PATH_RESOLVERS: Dict[
        tuple[str, str], Callable
    ] = {
        (
            "S3",
            "PandasExecutionEngine",
        ): lambda template_arguments: S3Url.OBJECT_URL_TEMPLATE.format(
            **template_arguments
        ),
        (
            "S3",
            "SparkDFExecutionEngine",
        ): lambda template_arguments: S3Url.OBJECT_URL_TEMPLATE.format(
            **template_arguments
        ),
        (
            "GCS",
            "PandasExecutionEngine",
        ): lambda template_arguments: GCSUrl.OBJECT_URL_TEMPLATE.format(
            **template_arguments
        ),
        (
            "GCS",
            "SparkDFExecutionEngine",
        ): lambda template_arguments: GCSUrl.OBJECT_URL_TEMPLATE.format(
            **template_arguments
        ),
        (
            "ABS",
            "PandasExecutionEngine",
        ): lambda template_arguments: AzureUrl.AZURE_BLOB_STORAGE_HTTPS_URL_TEMPLATE.format(
            **template_arguments
        ),
        (
            "ABS",
            "SparkDFExecutionEngine",
        ): lambda template_arguments: AzureUrl.AZURE_BLOB_STORAGE_WASBS_URL_TEMPLATE.format(
            **template_arguments
        ),
        (
            "DBFS",
            "SparkDFExecutionEngine",
        ): lambda template_arguments: DBFSPath.convert_to_protocol_version(
            **template_arguments
        ),
        (
            "DBFS",
            "PandasExecutionEngine",
        ): lambda template_arguments: DBFSPath.convert_to_file_semantics_version(
            **template_arguments
        ),
    }

    @staticmethod
    def resolve_data_reference(
        data_connector_name: str,
        execution_engine_name: str,
        template_arguments: dict,
    ):
        """Resolve file path for a (data_connector_name, execution_engine_name) combination."""
        storage_name: str = DataConnectorStorageDataReferenceResolver.DATA_CONNECTOR_NAME_TO_STORAGE_NAME_MAP[
            data_connector_name
        ]
        return DataConnectorStorageDataReferenceResolver.STORAGE_NAME_EXECUTION_ENGINE_NAME_PATH_RESOLVERS[
            (storage_name, execution_engine_name)
        ](
            template_arguments
        )


# noinspection SpellCheckingInspection
@public_api
class DataConnector:
    """The base class for all Data Connectors.

    Data Connectors produce identifying information, called Batch Specs, that Execution Engines
    can use to get individual batches of data. They add flexibility in how to obtain data
    such as with time-based partitioning, downsampling, or other techniques appropriate
    for the Datasource.

    For example, a DataConnector could produce a SQL query that logically represents "rows in
    the Events table with a timestamp on February 7, 2012," which an SqlAlchemy Datasource
    could use to materialize a SqlAlchemy Dataset corresponding to that Batch of data and
    ready for validation.

    A Batch is a sample from a data asset, sliced according to a particular rule. For example,
    an hourly slide of the Events table or “most recent Users records.” It is the primary
    unit of validation in the Great Expectations Data Context. Batches include metadata that
    identifies how they were constructed--the same Batch Spec assembled by the data connector.
    While not every Datasource will enable re-fetching a specific batch of data, GX can store
    snapshots of batches or store metadata from an external data version control system.

    Args:
        name: The name of the Data Connector.
    """

    def __init__(
        self,
        name: str,
        data_asset_name: str,
    ) -> None:
        self._name: str = name
        self._data_asset_name: str = data_asset_name

        # This is a dictionary which maps data_references onto batch_requests.
        self._data_references_cache: Dict = {}

        self._data_context_root_directory: Optional[str] = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @property
    def data_context_root_directory(self) -> str:
        return self._data_context_root_directory

    @data_context_root_directory.setter
    def data_context_root_directory(self, data_context_root_directory: str) -> None:
        self._data_context_root_directory = data_context_root_directory

    def build_batch_spec(self, batch_definition: BatchDefinition) -> BatchSpec:
        """
        Builds batch_spec from batch_definition by generating batch_spec params and adding any pass_through params

        Args:
            batch_definition (BatchDefinition): required batch_definition parameter for retrieval
        Returns:
            BatchSpec object built from BatchDefinition

        """
        batch_spec_params: dict = (
            self._generate_batch_spec_parameters_from_batch_definition(
                batch_definition=batch_definition
            )
        )
        batch_spec = BatchSpec(**batch_spec_params)
        return batch_spec

    def get_batch_definition_list_from_batch_request(
        self, batch_request: BatchRequestBase
    ) -> List[BatchDefinition]:
        raise NotImplementedError

    def get_data_reference_list_count(self) -> int:
        raise NotImplementedError

    def get_unmatched_data_references(self) -> List[Any]:
        raise NotImplementedError

    def _generate_batch_spec_parameters_from_batch_definition(
        self, batch_definition: BatchDefinition
    ) -> dict:
        raise NotImplementedError

    def _refresh_data_references_cache(
        self,
    ) -> None:
        raise NotImplementedError

    def _map_data_reference_to_batch_definition_list(
        self, data_reference: Any
    ) -> Optional[List[BatchDefinition]]:
        raise NotImplementedError

    def _map_batch_definition_to_data_reference(
        self, batch_definition: BatchDefinition
    ) -> Any:
        raise NotImplementedError

    def _get_data_reference_list(self) -> List[str]:
        """
        List objects in the underlying data store to create a list of data_references.
        This method is used to refresh the cache by classes that extend this base DataConnector class
        """
        raise NotImplementedError

    def resolve_data_reference(self, template_arguments: dict):
        """Resolve file path for a (data_connector_name, execution_engine_name) combination."""
        return DataConnectorStorageDataReferenceResolver.resolve_data_reference(
            data_connector_name=self.__class__.__name__,
            execution_engine_name=self._execution_engine.__class__.__name__,
            template_arguments=template_arguments,
        )

    def self_check(self, pretty_print=True, max_examples=3):
        """
        Checks the configuration of the current DataConnector by doing the following :

        1. refresh or create data_reference_cache
        2. print unmatched data_references, and allow the user to modify the regex or glob configuration if necessary
        3. select a random data_reference and attempt to retrieve and print the first few rows to user

        When used as part of the test_yaml_config() workflow, the user will be able to know if the data_connector is properly configured.

        Args:
            pretty_print (bool): should the output be printed?
            max_examples (int): how many data_references should be printed?

        Returns:
            report_obj (dict): dictionary containing self_check output

        """
        if len(self._data_references_cache) == 0:
            self._refresh_data_references_cache()

        if pretty_print:
            print(f"	{self.name}", ":", self.__class__.__name__)
            print()

        report_obj = {
            "class_name": self.__class__.__name__,
        }

        unmatched_data_references = self.get_unmatched_data_references()
        len_unmatched_data_references = len(unmatched_data_references)
        if pretty_print:
            print(
                f"\n\tUnmatched data_references ({min(len_unmatched_data_references, max_examples)} of {len_unmatched_data_references}):{unmatched_data_references[:max_examples]}\n"
            )

        report_obj["unmatched_data_reference_count"] = len_unmatched_data_references
        report_obj["example_unmatched_data_references"] = unmatched_data_references[
            :max_examples
        ]

        return report_obj
