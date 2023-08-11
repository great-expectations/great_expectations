from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, List, Type

from great_expectations.core.id_dict import BatchSpec

if TYPE_CHECKING:
    from great_expectations.core.batch import BatchDefinition
    from great_expectations.datasource.fluent import BatchRequest


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

    # needed to select the asset level kwargs needed to build the DataConnector
    asset_level_option_keys: ClassVar[tuple[str, ...]] = ()
    asset_options_type: ClassVar[Type] = dict

    def __init__(
        self,
        datasource_name: str,
        data_asset_name: str,
    ) -> None:
        self._datasource_name: str = datasource_name
        self._data_asset_name: str = data_asset_name

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @property
    def datasource_name(self) -> str:
        return self._datasource_name

    @abstractmethod
    def get_batch_definition_list(
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

    def test_connection(self) -> bool:
        """Test the connection to data, accessible to the present "DataConnector" object.

        Raises:
            bool: True of connection test succeeds; False, otherwise.
        """
        return (
            self.get_unmatched_data_reference_count() < self.get_data_reference_count()
        )

    @abstractmethod
    def get_data_references(self) -> List[Any]:
        """
        This interface method lists objects in the underlying data store used to create a list of data_references (type depends on cloud storage environment, SQL DBMS, etc.).
        """
        pass

    @abstractmethod
    def get_data_reference_count(self) -> int:
        """
        This interface method returns number of all (e.g., cached) data references (useful for diagnostics).

        Returns:
            int -- number of data references identified
        """
        pass

    @abstractmethod
    def get_matched_data_references(self) -> List[Any]:
        """
        This interface method returns (e.g., cached) data references that were successfully matched based on "BatchRequest" options.

        Returns:
            List[Any] -- unmatched data references (type depends on cloud storage environment, SQL DBMS, etc.)
        """
        pass

    @abstractmethod
    def get_matched_data_reference_count(self) -> int:
        """
        This interface method returns number of all (e.g., cached) matched data references (useful for diagnostics).

        Returns:
            int -- number of data references identified
        """
        pass

    @abstractmethod
    def get_unmatched_data_references(self) -> List[Any]:
        """
        This interface method returns (e.g., cached) data references that could not be matched based on "BatchRequest" options.

        Returns:
            List[Any] -- unmatched data references (type depends on cloud storage environment, SQL DBMS, etc.)
        """
        pass

    @abstractmethod
    def get_unmatched_data_reference_count(self) -> int:
        """
        This interface method returns number of all (e.g., cached) unmatched data references (useful for diagnostics).

        Returns:
            int -- number of data references identified
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

    @staticmethod
    def _batch_definition_matches_batch_request(
        batch_definition: BatchDefinition, batch_request: BatchRequest
    ) -> bool:
        if not (
            batch_request.datasource_name == batch_definition.datasource_name
            and batch_request.data_asset_name == batch_definition.data_asset_name
        ):
            return False

        if batch_request.options:
            for key, value in batch_request.options.items():
                if value is not None and not (
                    (key in batch_definition.batch_identifiers)
                    and (
                        batch_definition.batch_identifiers[key]
                        == batch_request.options[key]
                    )
                ):
                    return False

        return True
