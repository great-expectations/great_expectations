from __future__ import annotations

import logging
import pathlib
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from great_expectations.core.id_dict import BatchSpec

if TYPE_CHECKING:
    from great_expectations.core.batch import BatchDefinition
    from great_expectations.experimental.datasources.interfaces import BatchRequest


logger = logging.getLogger(__name__)


# noinspection SpellCheckingInspection
class DataConnector(ABC):
    """The abstract base class for all Data Connectors.

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
        datasource_name: The name of the Datasource associated with this DataConnector instance
        data_asset_name: The name of the DataAsset using this DataConnector instance
    """

    def __init__(
        self,
        datasource_name: str,
        data_asset_name: str,
    ) -> None:
        self._datasource_name: str = datasource_name
        self._data_asset_name: str = data_asset_name

        # This is a dictionary which maps data_references onto batch_requests.
        self._data_references_cache: Dict[str, List[BatchDefinition] | None] = {}

        self._data_context_root_directory: Optional[pathlib.Path] = None

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @property
    def datasource_name(self) -> str:
        return self._datasource_name

    @property
    def data_context_root_directory(self) -> Optional[pathlib.Path]:
        return self._data_context_root_directory

    @data_context_root_directory.setter
    def data_context_root_directory(
        self, data_context_root_directory: Optional[pathlib.Path]
    ) -> None:
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

    @abstractmethod
    def get_batch_definition_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[BatchDefinition]:
        """
        This interface method, implemented by subclasses, examines "BatchRequest" and converts it to one or more
        "BatchDefinition" objects, each of which can be later converted to ExecutionEngine-specific "BatchSpec" object
        for loading "Batch" of data.

        Args:
            batch_request: (BatchRequest) input "BatchRequest" object

        Returns:
            List[BatchDefinition] -- list of "BatchDefinition" objects, each corresponding to "Batch" of data downstream
        """
        pass

    @abstractmethod
    def get_data_reference_count(self) -> int:
        """
        This interface method returns number of all cached data references (useful for troubleshooting purposes).

        Returns:
            int -- number of data references identified
        """
        pass

    @abstractmethod
    def get_unmatched_data_references(self) -> List[Any]:
        """
        This interface method returns cached data references that could not be matched based on "BatchRequest" options.

        Returns:
            List[Any] -- unmatched data references (type depends on cloud storage environment, SQL DBMS, etc.)
        """
        pass

    @abstractmethod
    def _generate_batch_spec_parameters_from_batch_definition(
        self, batch_definition: BatchDefinition
    ) -> dict:
        """
        This interface method, implemented by subclasses, examines "BatchDefinition" and converts it to
        ExecutionEngine-specific "BatchSpec" object for loading "Batch" of data.  Implementers will typically define
        their own interfaces that their subclasses must implement in order to provide storage-specific specifics.

        Args:
            batch_definition: (BatchDefinition) input "BatchRequest" object

        Returns:
            dict -- dictionary of "BatchSpec" properties
        """
        pass

    @abstractmethod
    def _refresh_data_references_cache(self) -> None:
        """
        This interface method, implemented by subclasses, populates cache, whose keys are data references and values
        are "BatchDefinition" objects.  Subsequently, "BatchDefinition" objects are subjected to querying and sorting.
        """
        pass

    @abstractmethod
    def _map_data_reference_to_batch_definition_list(
        self, data_reference: Any
    ) -> Optional[List[BatchDefinition]]:
        """
        This interface method, implemented by subclasses, examines "data_reference" handle and converts it to zero or
        more "BatchDefinition" objects, based on partitioning behavior of given subclass (e.g., Regular Expressions for
        file path based DataConnector implementations).  Type of "data_reference" is storage dependent.

        Args:
            data_reference: input "data_reference" handle

        Returns:
            Optional[List[BatchDefinition]] -- list of "BatchDefinition" objects, based on partitioning "data_reference"
            handle provided
        """
        pass

    @abstractmethod
    def _map_batch_definition_to_data_reference(
        self, batch_definition: BatchDefinition
    ) -> Any:
        """
        This interface method, implemented by subclasses, examines "BatchDefinition" object and converts it to exactly
        one "data_reference" handle, based on partitioning behavior of given subclass (e.g., Regular Expressions for
        file path based DataConnector implementations).  Type of "data_reference" is storage dependent.  This method is
        then used to create storage system specific "BatchSpec" parameters for retrieving "Batch" of data≥

        Args:
            batch_definition: input "BatchDefinition" object

        Returns:
            handle provided
        """
        pass

    @abstractmethod
    def _get_data_reference_list(self) -> List[str]:
        """
        List objects in the underlying data store to create a list of data_references.
        This method is used to refresh the cache by classes that extend this base DataConnector class
        """
        pass
