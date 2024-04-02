from __future__ import annotations

import logging
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import great_expectations.exceptions as gx_exceptions
from great_expectations._docs_decorators import public_api
from great_expectations.core.batch import (
    BatchMarkers,  # noqa: TCH001
    BatchRequestBase,  # noqa: TCH001
    LegacyBatchDefinition,  # noqa: TCH001
)
from great_expectations.core.id_dict import BatchSpec

if TYPE_CHECKING:
    from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


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
        datasource_name: The name of this Data Connector's Datasource.
        execution_engine: The Execution Engine object to used by this Data Connector to read the data.
        batch_spec_passthrough: Dictionary with keys that will be added directly to the batch spec.
        id: The unique identifier for this Data Connector used when running in cloud mode.
    """  # noqa: E501

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        datasource_name: str,
        execution_engine: ExecutionEngine,
        batch_spec_passthrough: Optional[dict] = None,
        id: Optional[str] = None,
    ) -> None:
        if execution_engine is None:
            raise gx_exceptions.DataConnectorError(  # noqa: TRY003
                "A non-existent/unknown ExecutionEngine instance was referenced."
            )

        self._name = name
        self._id = id
        self._datasource_name = datasource_name
        self._execution_engine = execution_engine

        # This is a dictionary which maps data_references onto batch_requests.
        self._data_references_cache: Dict = {}

        self._data_context_root_directory: Optional[str] = None
        self._batch_spec_passthrough = batch_spec_passthrough or {}

    @property
    def batch_spec_passthrough(self) -> dict:
        return self._batch_spec_passthrough

    @property
    def name(self) -> str:
        return self._name

    @property
    def id(self) -> Optional[str]:
        return self._id

    @property
    def datasource_name(self) -> str:
        return self._datasource_name

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self._execution_engine

    @property
    def data_context_root_directory(self) -> str:
        return self._data_context_root_directory  # type: ignore[return-value]

    @data_context_root_directory.setter
    def data_context_root_directory(self, data_context_root_directory: str) -> None:
        self._data_context_root_directory = data_context_root_directory

    def get_batch_data_and_metadata(
        self,
        batch_definition: LegacyBatchDefinition,
    ) -> Tuple[Any, BatchSpec, BatchMarkers]:  # batch_data
        """
        Uses batch_definition to retrieve batch_data and batch_markers by building a batch_spec from batch_definition,
        then using execution_engine to return batch_data and batch_markers

        Args:
            batch_definition (LegacyBatchDefinition): required batch_definition parameter for retrieval

        """  # noqa: E501
        batch_spec: BatchSpec = self.build_batch_spec(batch_definition=batch_definition)
        batch_data, batch_markers = self._execution_engine.get_batch_data_and_markers(
            batch_spec=batch_spec
        )
        self._execution_engine.load_batch_data(batch_definition.id, batch_data)
        return (
            batch_data,
            batch_spec,
            batch_markers,
        )

    def build_batch_spec(self, batch_definition: LegacyBatchDefinition) -> BatchSpec:
        """
        Builds batch_spec from batch_definition by generating batch_spec params and adding any pass_through params

        Args:
            batch_definition (LegacyBatchDefinition): required batch_definition parameter for retrieval
        Returns:
            BatchSpec object built from BatchDefinition

        """  # noqa: E501
        batch_spec_params: dict = self._generate_batch_spec_parameters_from_batch_definition(
            batch_definition=batch_definition
        )
        # batch_spec_passthrough via Data Connector config
        batch_spec_passthrough: dict = deepcopy(self.batch_spec_passthrough)

        # batch_spec_passthrough from batch_definition supersedes batch_spec_passthrough from Data Connector config  # noqa: E501
        if isinstance(batch_definition.batch_spec_passthrough, dict):
            batch_spec_passthrough.update(batch_definition.batch_spec_passthrough)

        batch_spec_params.update(batch_spec_passthrough)
        batch_spec = BatchSpec(**batch_spec_params)
        return batch_spec

    def _refresh_data_references_cache(
        self,
    ) -> None:
        raise NotImplementedError

    def _get_data_reference_list(self, data_asset_name: Optional[str] = None) -> List[str]:
        """
        List objects in the underlying data store to create a list of data_references.
        This method is used to refresh the cache by classes that extend this base DataConnector class

        Args:
            data_asset_name (str): optional data_asset_name to retrieve more specific results

        """  # noqa: E501
        raise NotImplementedError

    def _get_data_reference_list_from_cache_by_data_asset_name(
        self, data_asset_name: str
    ) -> List[Any]:
        """
        Fetch data_references corresponding to data_asset_name from the cache.
        """
        raise NotImplementedError

    def get_data_reference_count(self) -> int:
        raise NotImplementedError

    def get_unmatched_data_references(self) -> List[Any]:
        raise NotImplementedError

    @public_api
    def get_available_data_asset_names(self) -> List[str]:
        """Return the list of asset names known by this data connector.

        Returns:
            A list of available names
        """
        raise NotImplementedError

    def get_available_data_asset_names_and_types(self) -> List[Tuple[str, str]]:
        """
        Return the list of asset names and types known by this DataConnector.

        Returns:
            A list of tuples consisting of available names and types
        """
        # NOTE: Josh 20211001 only implemented in InferredAssetSqlDataConnector
        raise NotImplementedError

    def get_batch_definition_list_from_batch_request(
        self,
        batch_request: BatchRequestBase,
    ) -> List[LegacyBatchDefinition]:
        raise NotImplementedError

    def _map_data_reference_to_batch_definition_list(
        self, data_reference: Any, data_asset_name: Optional[str] = None
    ) -> Optional[List[LegacyBatchDefinition]]:
        raise NotImplementedError

    def _map_batch_definition_to_data_reference(
        self, batch_definition: LegacyBatchDefinition
    ) -> Any:
        raise NotImplementedError

    def _generate_batch_spec_parameters_from_batch_definition(
        self, batch_definition: LegacyBatchDefinition
    ) -> dict:
        raise NotImplementedError

    def _validate_batch_request(self, batch_request: BatchRequestBase) -> None:
        """
        Validate batch_request by checking:
            1. if configured datasource_name matches batch_request's datasource_name
            2. if current data_connector_name matches batch_request's data_connector_name
        Args:
            batch_request (BatchRequestBase): batch_request object to validate

        """
        if batch_request.datasource_name != self.datasource_name:
            raise ValueError(  # noqa: TRY003
                f"""datasource_name in BatchRequest: "{batch_request.datasource_name}" does not match DataConnector datasource_name: "{self.datasource_name}"."""  # noqa: E501
            )
        if batch_request.data_connector_name != self.name:
            raise ValueError(  # noqa: TRY003
                f"""data_connector_name in BatchRequest: "{batch_request.data_connector_name}" does not match DataConnector name: "{self.name}"."""  # noqa: E501
            )
